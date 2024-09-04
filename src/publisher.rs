use std::{collections::VecDeque, io::Write, net::SocketAddr, sync::Arc};

use ahash::HashSet;
use crossbeam::channel::RecvTimeoutError;
use dashmap::DashSet;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use crate::{
    chunk::{Chunk, ChunkBuffer, ChunkBufferAllocator},
    chunk_socket::{ChunkSocket, ReceivedChunk},
    multiplex_socket::{
        transmit_to_and_wait, ChunkReceiver, MultiplexSocket, TransmitAndWaitError,
    },
    protocol::{
        BarrierReleased, ChannelDisconnected, ChannelHeader, ConfirmJoinChannel, ConnectionInfo,
        Message, MESSAGE_PAYLOAD_OFFSET,
    },
    ChannelId, SequenceNumber,
};

const RETRANSMIT_MILLIS: u64 = 100;

/// A chunk that has been sent but not yet acknowledged by all subscribers.
struct UnacknowledgedChunk {
    sent_time: std::time::Instant,
    buffer: ChunkBuffer,
    packet_size: usize,
    missing_acks: HashSet<SocketAddr>,
    retransmit_count: usize,
}

pub struct Offer {
    socket: ChunkSocket,
    offer_id: ChannelId,

    // The sequence number of the last message sent.
    seq_sent: SequenceNumber,

    // The sequence number of the last message acknowledged by all subscribers.
    seq_ack: SequenceNumber,

    // Chunks, that have been sent but not yet acknowledged by all subscribers.
    //
    // The first element corresponds to self.seq_ack + 1 and the last element corresponds to
    // self.seq_sent.
    unacknowledged_chunks: VecDeque<Option<UnacknowledgedChunk>>,

    used_offer_ids: Arc<DashSet<ChannelId>>,
    receiver: ChunkReceiver,
    new_clients: HashSet<SocketAddr>,
    clients: HashSet<SocketAddr>,
    buffer_allocator: Arc<ChunkBufferAllocator>,
    multicast_addr: SockAddr,
}

impl Drop for Offer {
    fn drop(&mut self) {
        self.used_offer_ids.remove(&self.offer_id);
    }
}

impl Offer {
    // fn unacknowledged_chunk_mut(
    //     &mut self,
    //     seq: SequenceNumber,
    // ) -> Option<&mut UnacknowledgedChunk> {
    //     let index = seq.wrapping_sub(self.seq_ack) as usize - 1;
    //     self.unacknowledged_chunks
    //         .get_mut(index)
    //         .and_then(|c| c.as_mut())
    // }

    fn unacknowledged_chunks_count(&self) -> usize {
        self.unacknowledged_chunks
            .iter()
            .filter(|c| c.is_some())
            .count()
    }

    fn process_chunk(&mut self, chunk: ReceivedChunk) {
        match chunk.validate() {
            Ok(Chunk::JoinChannel(_)) => {
                log::debug!("received join channel {}", self.offer_id);
                self.new_clients.insert(chunk.addr().as_socket().unwrap());
            }
            Ok(Chunk::Ack(ack)) => {
                let ack_seq: u16 = ack.header.seq.into();
                let offset = ack_seq.wrapping_sub(self.seq_ack).wrapping_sub(1);
                log::debug!("received ack from {}", chunk.addr().as_socket().unwrap());
                log::debug!("received ack: {ack_seq} ({offset}) {}", self.seq_sent);
                if offset > u16::MAX / 2 {
                    // this ack is probably from the past
                } else if let Some(Some(c)) = self.unacknowledged_chunks.get_mut(offset as usize) {
                    log::debug!("removing ack from {:?}", c.missing_acks);
                    c.missing_acks.remove(&chunk.addr().as_socket().unwrap());
                }
            }
            Ok(chunk) => {
                log::debug!("ignore unexpected chunk: {:?}", chunk);
            }
            Err(err) => {
                log::error!("received invalid chunk: {}", err);
            }
        }
    }

    fn process_pending_chunks(&mut self) {
        while let Ok(chunk) = self.receiver.try_recv() {
            self.process_chunk(chunk);
        }
    }

    fn wait_for_chunk(&mut self) {
        if let Ok(chunk) = self.receiver.recv() {
            self.process_chunk(chunk);
        }
    }

    fn wait_for_chunk_timeout(&mut self, timeout: std::time::Duration) {
        if let Ok(chunk) = self.receiver.recv_timeout(timeout) {
            self.process_chunk(chunk);
        }
    }

    fn process_unacknlowedged_chunks(&mut self) {
        // Retransmit chunks, check for disconnects, and remove acknowledged chunks
        let mut clients_to_remove = HashSet::default();
        for (offset, chunk) in self.unacknowledged_chunks.iter_mut().enumerate() {
            if let Some(c) = chunk {
                if c.missing_acks.is_empty() {
                    chunk.take();
                    continue;
                }

                let millis = (1 << c.retransmit_count) * RETRANSMIT_MILLIS;
                if c.sent_time.elapsed().as_millis() > millis.into() {
                    if c.retransmit_count < 5 {
                        log::debug!(
                            "retransmitting chunk: {}",
                            self.seq_ack.wrapping_add(offset as u16).wrapping_add(1)
                        );
                        self.socket
                            .send_chunk_buffer_to(&c.buffer, c.packet_size, &self.multicast_addr)
                            .unwrap();

                        // TODO: should we reset the sent time?
                        c.sent_time = std::time::Instant::now();
                        c.retransmit_count += 1;
                    } else {
                        log::debug!(
                            "time out for chunk {} and subscribers: {:?}",
                            offset,
                            c.missing_acks
                        );
                        clients_to_remove.extend(c.missing_acks.iter().cloned());
                    }
                }
            }
        }
        if !clients_to_remove.is_empty() {
            log::warn!("clients timed out: {:?}", clients_to_remove);
            self.clients.retain(|c| !clients_to_remove.contains(c));

            for c in &mut self.unacknowledged_chunks.iter_mut().flatten() {
                c.missing_acks.retain(|a| !clients_to_remove.contains(a));
            }
        }

        // Remove acknowledged chunks from the front
        while let Some(None) = self.unacknowledged_chunks.front() {
            log::debug!("removing acknowledged chunk");
            self.unacknowledged_chunks.pop_front();
            self.seq_ack = self.seq_ack.wrapping_add(1);
        }
    }

    fn process(&mut self) {
        // Process pending acks etc
        self.process_pending_chunks();

        // Retransmit chunks
        self.process_unacknlowedged_chunks();
    }

    fn process_blocking(&mut self) {
        self.wait_for_chunk();
        self.process();
    }

    pub fn id(&self) -> ChannelId {
        self.offer_id
    }

    pub fn has_subscribers(&self) -> bool {
        !self.clients.is_empty()
    }

    pub fn accept(&mut self) -> Option<SocketAddr> {
        self.process();

        if let Some(client) = self
            .new_clients
            .iter()
            .next()
            .cloned()
            .and_then(|q| self.new_clients.take(&q))
        {
            let mut retries = 0;

            'outer: while retries < 5 {
                self.socket
                    .send_chunk_to(
                        &ConfirmJoinChannel {
                            header: ChannelHeader {
                                channel_id: self.offer_id.into(),
                                seq: self.seq_sent.into(),
                            },
                        },
                        &client.into(),
                    )
                    .unwrap();

                let start = std::time::Instant::now();

                // Wait for ack
                while start.elapsed().as_secs() < 1 {
                    if let Ok(chunk) = self.receiver.try_recv() {
                        match chunk.validate() {
                            Ok(Chunk::Ack(ack)) => {
                                if <zerocopy::network_endian::U16 as Into<u16>>::into(
                                    ack.header.seq,
                                ) == self.seq_sent
                                    && chunk.addr() == &client.into()
                                {
                                    self.clients.insert(client);
                                    break 'outer;
                                }
                            }
                            Ok(_) => {
                                self.process_chunk(chunk);
                            }
                            Err(err) => {
                                log::error!("received invalid chunk: {}", err);
                            }
                        }
                    }
                }

                retries += 1;
                log::debug!("retrying join channel");
            }

            Some(client)
        } else {
            None
        }
    }

    fn send_ack_chunk_buffer(
        &mut self,
        chunk: ChunkBuffer,
        packet_size: usize,
    ) -> Result<(), std::io::Error> {
        self.process();

        while self.unacknowledged_chunks_count() > 100 {
            log::debug!("too many unacknowledged chunks, blockin!");
            self.wait_for_chunk_timeout(std::time::Duration::from_millis(RETRANSMIT_MILLIS));
            self.process();
        }

        self.socket
            .send_chunk_buffer_to(&chunk, packet_size, &self.multicast_addr)?;
        // TODO: verify that the ack in the chunk is the same as self.seq_sent + 1
        self.unacknowledged_chunks
            .push_back(Some(UnacknowledgedChunk {
                sent_time: std::time::Instant::now(),
                buffer: chunk,
                packet_size,
                missing_acks: self.clients.clone(),
                retransmit_count: 0,
            }));
        Ok(())
    }

    pub fn write_message(&mut self) -> MessageWriter {
        MessageWriter {
            offer: self,
            buffer: None,
            cursor: MESSAGE_PAYLOAD_OFFSET,
        }
    }

    pub fn flush(&mut self) {
        while self.seq_ack < self.seq_sent {
            self.process_blocking();
        }
    }
}

pub struct MessageWriter<'a> {
    offer: &'a mut Offer,
    buffer: Option<ChunkBuffer>,
    cursor: usize,
}

impl Drop for MessageWriter<'_> {
    fn drop(&mut self) {
        // Ignore errors
        let _ = self.flush();
    }
}

impl Write for MessageWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut src_bytes = buf;

        while !src_bytes.is_empty() {
            let mut buffer = self.buffer.take().unwrap_or_else(|| {
                let mut buffer = self.offer.buffer_allocator.allocate();
                let seq = self.offer.seq_sent.wrapping_add(1);
                buffer.init::<Message>(&Message {
                    header: ChannelHeader {
                        channel_id: self.offer.offer_id.into(),
                        seq: seq.into(),
                    },
                });
                self.offer.seq_sent = seq;
                buffer
            });

            let remaining_buffer = &mut buffer[self.cursor..];
            let len = remaining_buffer.len().min(src_bytes.len());
            remaining_buffer[..len].copy_from_slice(&src_bytes[..len]);
            self.cursor += len;

            if self.cursor == buffer.len() {
                self.offer.send_ack_chunk_buffer(buffer, self.cursor)?;
                self.cursor = MESSAGE_PAYLOAD_OFFSET;
            } else {
                self.buffer = Some(buffer);
            }
            src_bytes = &src_bytes[len..];
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(buffer) = self.buffer.take() {
            self.offer.send_ack_chunk_buffer(buffer, self.cursor)?;
            self.cursor = MESSAGE_PAYLOAD_OFFSET;
        }
        Ok(())
    }
}

pub struct BarrierGroupDesc {
    /// The amount of time to wait before retransmitting the barrier release message.
    pub retransmit_timeout: std::time::Duration,

    /// The number of times the barrier release message is retransmitted, before closing the
    /// connection to clients that have not acknowledged the release.
    pub retransmit_count: usize,
}

#[derive(Debug, Default)]
struct BarrierGroupState {
    new_clients: HashSet<SocketAddr>,
    clients: HashSet<SocketAddr>,
    arrived: HashSet<SocketAddr>,
    seq: SequenceNumber,
}

impl BarrierGroupState {
    /// Returns true if the client is connected to the barrier group and false otherwise.
    fn client_reached_barrier(&mut self, client: SocketAddr, seq: SequenceNumber) -> bool {
        if self.clients.contains(&client) {
            if self.seq == seq {
                self.arrived.insert(client);
            }
            true
        } else {
            false
        }
    }

    /// Returns if all remotes have arrived at the barrier.
    fn all_remotes_arrived(&self) -> bool {
        debug_assert!(self.clients.len() != self.arrived.len() || self.clients == self.arrived);
        self.clients.len() == self.arrived.len()
    }

    /// Processes a single chunk
    fn process_chunk(&mut self, chunk: Chunk, addr: SocketAddr) -> bool {
        match chunk {
            Chunk::JoinBarrierGroup(_) => {
                self.new_clients.insert(addr);
                true
            }
            Chunk::BarrierReached(reached) => {
                if self.clients.contains(&addr) {
                    self.client_reached_barrier(addr, reached.0.seq.into());
                    true
                } else {
                    log::warn!("received barrier reached from non-client");
                    false
                }
            }
            Chunk::LeaveChannel(_) => {
                if self.clients.contains(&addr) {
                    self.clients.remove(&addr);
                    self.arrived.remove(&addr);
                }
                false
            }
            _ => {
                log::warn!("received invalid chunk: {chunk:?}");
                self.clients.contains(&addr)
            }
        }
    }
}

pub struct BarrierGroup {
    channel_id: ChannelId,
    desc: BarrierGroupDesc,
    state: BarrierGroupState,
    receiver: ChunkReceiver,
    socket: ChunkSocket,
    multicast_addr: SocketAddr,
}

impl BarrierGroup {
    fn try_process(&mut self) -> bool {
        let mut processed = false;
        while let Ok(chunk) = self.receiver.try_recv() {
            if let (Ok(chunk), Some(addr)) = (chunk.validate(), chunk.addr().as_socket()) {
                if !self.state.process_chunk(chunk, addr) {
                    let _ = self
                        .socket
                        .send_chunk_to(&ChannelDisconnected(self.channel_id.into()), &addr.into());
                }
                processed = true;
            }
        }
        processed
    }

    fn process(&mut self) {
        if let Ok(chunk) = self.receiver.recv() {
            if let (Ok(chunk), Some(addr)) = (chunk.validate(), chunk.addr().as_socket()) {
                if !self.state.process_chunk(chunk, addr) {
                    let _ = self
                        .socket
                        .send_chunk_to(&ChannelDisconnected(self.channel_id.into()), &addr.into());
                }
            }
        }
        self.try_process();
    }

    pub fn accept_client(&mut self, client: SocketAddr) -> Result<(), TransmitAndWaitError> {
        transmit_to_and_wait(
            &self.socket,
            &client,
            &ConfirmJoinChannel {
                header: ChannelHeader {
                    channel_id: self.channel_id.into(),
                    seq: self.state.seq.into(),
                },
            },
            self.desc.retransmit_timeout,
            self.desc.retransmit_count,
            &self.receiver,
            |chunk, addr| {
                if let Chunk::Ack(ack) = chunk {
                    let ack_seq: u16 = ack.header.seq.into();
                    if ack_seq == self.state.seq && addr == client {
                        log::debug!("client {} joined barrier group", client);
                        self.state.clients.insert(addr);
                        return true;
                    }
                } else {
                    self.state.process_chunk(chunk, addr);
                }
                false
            },
        )
    }

    pub fn try_accept(&mut self) -> Result<SocketAddr, TransmitAndWaitError> {
        self.try_process();

        if let Some(client) = self
            .state
            .new_clients
            .iter()
            .next()
            .copied()
            .and_then(|q| self.state.new_clients.take(&q))
        {
            log::debug!("accepting client {}", client);
            self.accept_client(client)?;
            Ok(client)
        } else {
            Err(TransmitAndWaitError::RecvError(RecvTimeoutError::Timeout))
        }
    }

    pub fn has_remotes(&self) -> bool {
        !self.state.clients.is_empty()
    }

    pub fn try_wait(&mut self) -> bool {
        self.try_process();
        if self.state.all_remotes_arrived() {
            self.wait();
            true
        } else {
            false
        }
    }

    pub fn wait(&mut self) {
        // Wait until everyone has arrived
        if !self.state.all_remotes_arrived() {
            self.process();
        }

        // Release the barrier
        let release_seq = self.state.seq.into();
        // Alread increment the seq here, to allow remotes to allready confirm the next barrier
        // while we are waiting for the acks.
        self.state.seq = self.state.seq.wrapping_add(1);
        let mut missing_acks = self.state.arrived.clone();
        self.state.arrived.clear();

        let release_time = std::time::Instant::now();

        let _ = transmit_to_and_wait(
            &self.socket,
            &self.multicast_addr,
            &BarrierReleased(ChannelHeader {
                channel_id: self.channel_id.into(),
                seq: release_seq,
            }),
            self.desc.retransmit_timeout,
            self.desc.retransmit_count,
            &self.receiver,
            |chunk, addr| {
                match chunk {
                    Chunk::Ack(ack) => {
                        if self.state.clients.contains(&addr) {
                            if release_seq == ack.header.seq {
                                missing_acks.remove(&addr);
                            }
                        } else {
                            log::warn!("received ack from non-client");
                            let _ = self.socket.send_chunk_to(
                                &ChannelDisconnected(self.channel_id.into()),
                                &addr.into(),
                            );
                        }
                    }
                    Chunk::BarrierReached(reached) => {
                        if self.state.clients.contains(&addr) {
                            let reached_seq: u16 = reached.0.seq.into();
                            if reached_seq == self.state.seq {
                                missing_acks.remove(&addr);
                                self.state.arrived.insert(addr);
                            }
                        } else {
                            log::warn!("received barrier reached from non-client");
                            let _ = self.socket.send_chunk_to(
                                &ChannelDisconnected(self.channel_id.into()),
                                &addr.into(),
                            );
                        }
                    }
                    _ => {
                        if !self.state.process_chunk(chunk, addr) {
                            missing_acks.remove(&addr);
                            let _ = self.socket.send_chunk_to(
                                &ChannelDisconnected(self.channel_id.into()),
                                &addr.into(),
                            );
                        }
                    }
                }

                missing_acks.is_empty()
            },
        );
        log::debug!(
            "barrier released confirmation time: {:?}",
            release_time.elapsed()
        );

        if !missing_acks.is_empty() {
            log::warn!("clients timed out: {:?}", missing_acks);
            for c in &missing_acks {
                let _ = self
                    .socket
                    .send_chunk_to(&ChannelDisconnected(self.channel_id.into()), &(*c).into());
                self.state.clients.remove(c);
                debug_assert!(!self.state.arrived.contains(c));
            }
        }
    }
}

pub struct PublisherConfig {
    pub addr: std::net::SocketAddrV4,
    pub multicast_addr: std::net::SocketAddrV4,
    pub chunk_size: u16,
}

struct ClientConnection {
    addr: std::net::SocketAddr,
}

pub struct Publisher {
    used_channel_ids: Arc<DashSet<u16>>,
    socket: MultiplexSocket,
    multicast_addr: std::net::SocketAddr,
    buffer_allocator: Arc<ChunkBufferAllocator>,
}

#[derive(thiserror::Error, Debug)]
pub enum CreateChannelError {
    #[error("offer limit reached")]
    ChannelLimitReached,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl Publisher {
    pub fn new(config: PublisherConfig) -> Self {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();
        socket.bind(&config.addr.into()).unwrap();
        log::debug!("bound to {}", config.addr);
        let buffer_allocator = Arc::new(ChunkBufferAllocator::new(config.chunk_size.into()));
        // let multicast_addr = SocketAddr::V4(config.multicast_addr);

        let handle_unchannelled = move |socket: &ChunkSocket, chunk: ReceivedChunk| {
            if let Ok(Chunk::Connect(_)) = chunk.validate() {
                if let Err(err) = socket.send_chunk_to(
                    &ConnectionInfo {
                        chunk_size: config.chunk_size.into(),
                        multicast_addr: config.multicast_addr.ip().octets(),
                        multicast_port: config.multicast_addr.port().into(),
                    },
                    chunk.addr(),
                ) {
                    log::error!("failed to send connection info: {}", err);
                }
            }
        };

        Publisher {
            used_channel_ids: Arc::new(DashSet::new()),
            socket: MultiplexSocket::with_unchannelled_handler(
                socket,
                buffer_allocator.clone(),
                handle_unchannelled,
            )
            .unwrap(),
            buffer_allocator,
            multicast_addr: config.multicast_addr.into(),
        }
    }

    pub fn create_barrier_group(
        &self,
        desc: BarrierGroupDesc,
    ) -> Result<BarrierGroup, CreateChannelError> {
        for offer_id in 0..=ChannelId::MAX {
            if self.used_channel_ids.insert(offer_id) {
                let receiver = self.socket.listen_to_channel(offer_id);
                return Ok(BarrierGroup {
                    channel_id: offer_id,
                    state: BarrierGroupState::default(),
                    receiver,
                    desc,
                    socket: self.socket.socket().try_clone()?,
                    multicast_addr: self.multicast_addr,
                });
            }
        }

        Err(CreateChannelError::ChannelLimitReached)
    }

    pub fn create_offer(&self) -> Result<Offer, CreateChannelError> {
        for offer_id in 0..=ChannelId::MAX {
            if self.used_channel_ids.insert(offer_id) {
                let receiver = self.socket.listen_to_channel(offer_id);
                return Ok(Offer {
                    socket: self.socket.socket().try_clone()?,
                    offer_id,
                    seq_sent: 0,
                    seq_ack: 0,
                    unacknowledged_chunks: VecDeque::new(),
                    used_offer_ids: self.used_channel_ids.clone(),
                    receiver,
                    new_clients: HashSet::default(),
                    clients: HashSet::default(),
                    buffer_allocator: self.buffer_allocator.clone(),
                    multicast_addr: self.multicast_addr.into(),
                });
            }
        }

        Err(CreateChannelError::ChannelLimitReached)
    }
}
