use std::{collections::VecDeque, io::Write, net::SocketAddr, sync::Arc};

use ahash::{HashMap, HashSet};
use crossbeam::queue::SegQueue;
use dashmap::DashSet;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use crate::{
    chunk::{Chunk, ChunkBuffer, ChunkBufferAllocator},
    chunk_socket::{ChunkSocket, ReceivedChunk},
    multiplex_socket::{ChunkReceiver, MultiplexSocket},
    protocol::{
        ChannelHeader, ConfirmJoinChannel, ConnectionInfo, Message, MESSAGE_PAYLOAD_OFFSET,
    },
    OfferId, SequenceNumber,
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
    offer_id: OfferId,

    // The sequence number of the last message sent.
    seq_sent: SequenceNumber,

    // The sequence number of the last message acknowledged by all subscribers.
    seq_ack: SequenceNumber,

    // Chunks, that have been sent but not yet acknowledged by all subscribers.
    //
    // The first element corresponds to self.seq_ack + 1 and the last element corresponds to
    // self.seq_sent.
    unacknowledged_chunks: VecDeque<Option<UnacknowledgedChunk>>,

    used_offer_ids: Arc<DashSet<OfferId>>,
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
                } else {
                    if let Some(c) = self.unacknowledged_chunks.get_mut(offset as usize) {
                        if let Some(c) = c {
                            log::debug!("removing ack from {:?}", c.missing_acks);
                            c.missing_acks.remove(&chunk.addr().as_socket().unwrap());
                        }
                    }
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
                        log::debug!("time out for chunk {} and subscribers: {:?}", offset, c.missing_acks);
                        clients_to_remove.extend(c.missing_acks.iter().cloned());
                    }
                }
            }
        }
        if !clients_to_remove.is_empty() {
            log::warn!("clients timed out: {:?}", clients_to_remove);
            self.clients.retain(|c| !clients_to_remove.contains(&c));

            for chunk in &mut self.unacknowledged_chunks {
                if let Some(c) = chunk {
                    c.missing_acks.retain(|a| !clients_to_remove.contains(a));
                }
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

    pub fn id(&self) -> OfferId {
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
                        ConfirmJoinChannel {
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
                                    self.clients.insert(client.clone());
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
                buffer.init::<Message>(Message {
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

pub struct PublisherConfig {
    pub addr: std::net::SocketAddrV4,
    pub multicast_addr: std::net::SocketAddrV4,
    pub chunk_size: u16,
}

struct ClientConnection {
    addr: std::net::SocketAddr,
}

pub struct Publisher {
    used_offer_ids: Arc<DashSet<u16>>,
    socket: MultiplexSocket,
    multicast_addr: std::net::SocketAddr,
    buffer_allocator: Arc<ChunkBufferAllocator>,
}

#[derive(thiserror::Error, Debug)]
pub enum CreateOfferError {
    #[error("offer limit reached")]
    OfferLimitReached,

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
                    ConnectionInfo {
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
            used_offer_ids: Arc::new(DashSet::new()),
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

    pub fn create_offer(&self) -> Result<Offer, CreateOfferError> {
        for offer_id in 0..=OfferId::MAX {
            if self.used_offer_ids.insert(offer_id) {
                let receiver = self.socket.listen_to_channel(offer_id);
                return Ok(Offer {
                    socket: self.socket.socket().try_clone()?,
                    offer_id,
                    seq_sent: 0,
                    seq_ack: 0,
                    unacknowledged_chunks: VecDeque::new(),
                    used_offer_ids: self.used_offer_ids.clone(),
                    receiver,
                    new_clients: HashSet::default(),
                    clients: HashSet::default(),
                    buffer_allocator: self.buffer_allocator.clone(),
                    multicast_addr: self.multicast_addr.into(),
                });
            }
        }

        Err(CreateOfferError::OfferLimitReached)
    }
}
