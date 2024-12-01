//! Broadcast groups allow reliable 1-to-many communication.
//!
//! Broadcast groups allow the coordinator to reliably send messages to all
//! members. The coordinator can create broadcast groups using
//! [`Coordinator::create_broadcast_group`](crate::session::Coordinator::create_broadcast_group) and members can join broadcast groups using
//! [`Member::join_broadcast_group`](crate::session::Member::join_broadcast_group).
//!
//! # Example
//! #### Coordinator
//! ```no_run
//! use camure::session::Coordinator;
//! use std::io::Write;
//!
//! let bind_addr = "192.168.0.100:12345".parse()?;
//! let multicast_addr = "234.0.0.0:55555".parse()?;
//! let coordinator = Coordinator::start_session(bind_addr, multicast_addr)?;
//! 
//! let mut sender = coordinator.create_broadcast_group(Some(0))?;
//! sender.accept().unwrap();
//!
//! for _ in 0..1000 {
//!     sender.write_message().write_all(b"SOME DATA")?;
//! }
//! sender.wait()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! #### Member
//! ```no_run
//! use camure::session::Member;
//! use std::io::Read;
//!
//! let coordinator_addr = "192.168.0.100:12345".parse()?;
//! let member = Member::join_session(coordinator_addr)?;
//!
//! let mut receiver = member.join_broadcast_group(0).unwrap();
//!
//! for _ in 0..1000 {
//!     let mut buf = String::new();
//!     let message = receiver.recv()?;
//!     let mut message_reader = message.read();
//!     message_reader.read_to_string(&mut buf)?;
//!     println!("{}", buf);
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::{
    collections::VecDeque,
    io::{Read, Write},
    mem::ManuallyDrop,
    net::SocketAddr,
};

use ahash::HashSet;
use socket2::SockAddr;
use zerocopy::FromBytes;

use crate::{
    chunk::{Chunk, ChunkBuffer},
    chunk_socket::ReceivedChunk,
    group::{
        GroupCoordinator, GroupCoordinatorState, GroupCoordinatorTypeImpl, GroupMember,
        GroupMemberState, GroupMemberTypeImpl,
    },
    protocol::{
        self, BroadcastFinalMessageFragment, BroadcastFirstMessageFragment, BroadcastMessage,
        BroadcastMessageFragment, SequenceNumber, MESSAGE_PAYLOAD_OFFSET,
    },
    session::GroupId,
    utils::{display_addr, sock_addr_to_socket_addr},
};

/// A chunk that has been sent but not yet acknowledged by all subscribers.
#[derive(Debug)]
struct UnacknowledgedChunk {
    initial_send_time: std::time::Instant,
    retransmit_time: std::time::Instant,
    buffer: ChunkBuffer,
    packet_size: usize,
    missing_acks: HashSet<SockAddr>,
    retransmit_count: u32,
}

#[derive(Debug, Default)]
pub(crate) struct BroadcastGroupSenderState {
    // The first sequence number not acknowledged by all members.
    seq_not_ack: SequenceNumber,

    // Chunks, that have been sent but not yet acknowledged by all members.
    //
    // The first element corresponds to self.seq_not_ack and the last element corresponds to
    // self.seq_sent.
    unacknowledged_chunks: VecDeque<Option<UnacknowledgedChunk>>,

    // The number of packets that are currently in flight.
    packets_in_flight: usize,
}

impl BroadcastGroupSenderState {
    fn remove_addr(&mut self, addr: &SockAddr) {
        for chunk in &mut self.unacknowledged_chunks {
            if let Some(chunk) = chunk {
                chunk.missing_acks.remove(addr);
            }
        }
    }
}

impl GroupCoordinatorTypeImpl for BroadcastGroupSenderState {
    const GROUP_TYPE: protocol::GroupType = protocol::GROUP_TYPE_BROADCAST;

    fn process_join_cancelled(&mut self, addr: &SockAddr, _: &GroupCoordinatorState) {
        self.remove_addr(addr);
    }

    fn process_member_disconnected(&mut self, addr: &SockAddr, _: &GroupCoordinatorState) {
        self.remove_addr(addr);
    }

    fn process_chunk(&mut self, chunk: Chunk, addr: &SockAddr, _: &GroupCoordinatorState) {
        match chunk {
            Chunk::SessionJoin(_)
            | Chunk::SessionWelcome(_)
            | Chunk::SessionHeartbeat(_)
            | Chunk::GroupJoin(_)
            | Chunk::GroupWelcome(_)
            | Chunk::GroupLeave(_)
            | Chunk::GroupDisconnected(_) => {
                // These chunks cannot be forwared to this function, as they should have been
                // filtered out by either the multiplex socket or the GroupCoordinator.
                tracing::error!(
                    ?chunk,
                    from = %display_addr(addr),
                    "received invalid chunk in broadcast group",
                );
                unreachable!();
            }
            Chunk::BarrierReached(_)
            | Chunk::BarrierReleased(_)
            | Chunk::BroadcastMessage(_)
            | Chunk::BroadcastFirstMessageFragment(_)
            | Chunk::BroadcastMessageFragment(_)
            | Chunk::BroadcastFinalMessageFragment(_) => {
                // These chunks could be forwarded to this function, but they would never been
                // send by this crate.
                tracing::error!(
                    ?chunk,
                    from = %display_addr(addr),
                    "received invalid message in broadcast group receiver",
                );
            }
            Chunk::GroupAck(ack) => {
                let offset = ack.seq - self.seq_not_ack;
                if offset < self.unacknowledged_chunks.len() {
                    // TODO: check if this is actually the correct chunk: checksum?
                    if let Some(chunk) = &mut self.unacknowledged_chunks[offset as usize] {
                        if chunk.missing_acks.remove(addr) {
                            tracing::trace!(
                                seq = <_ as Into<u16>>::into(ack.seq),
                                from = %display_addr(addr),
                                rtt = ?chunk.initial_send_time.elapsed(),
                                "received ack",
                            );
                        } else {
                            tracing::trace!(
                                seq = <_ as Into<u16>>::into(ack.seq),
                                from = %display_addr(addr),
                                rtt = ?chunk.initial_send_time.elapsed(),
                                "received duplicate ack",
                            );
                        }
                    } else {
                        tracing::trace!(
                            seq = <_ as Into<u16>>::into(ack.seq),
                            from = %display_addr(addr),
                            "received duplicate ack",
                        );
                    }
                } else {
                    tracing::trace!(
                        seq = <_ as Into<u16>>::into(ack.seq),
                        from = %display_addr(addr),
                        "received invalid ack, duplicate?",
                    );
                }
            }
        }
    }
}

/// A sender for broadcast messages.
pub struct BroadcastGroupSender {
    pub(crate) group: GroupCoordinator<BroadcastGroupSenderState>,
    pub(crate) initial_retransmit_delay: std::time::Duration,
    pub(crate) max_retransmit_delay: std::time::Duration,
    pub(crate) max_packets_in_flight: usize,
}

impl BroadcastGroupSender {
    pub(crate) fn new(group: GroupCoordinator<BroadcastGroupSenderState>) -> Self {
        Self {
            group,
            initial_retransmit_delay: std::time::Duration::from_millis(16),
            max_retransmit_delay: std::time::Duration::from_secs(1),
            max_packets_in_flight: 10,
        }
    }

    #[tracing::instrument(skip(self))]
    fn process_unacknlowedged_chunks(&mut self) -> std::io::Result<()> {
        let inner = &mut self.group.inner;
        let mut dead_members = vec![];

        for (index, maybe_chunk) in inner.unacknowledged_chunks.iter_mut().enumerate() {
            let chunk_seq = inner.seq_not_ack.skip(index);

            if let Some(chunk) = maybe_chunk {
                if chunk.missing_acks.is_empty() {
                    tracing::trace!(
                        seq = <_ as Into<u16>>::into(chunk_seq),
                        elapsed = ?chunk.initial_send_time.elapsed(),
                        "all members acknowledged chunk",
                    );
                    // TODO: use take_if when it becomes stable
                    maybe_chunk.take();
                    inner.packets_in_flight -= 1;
                } else {
                    if chunk.retransmit_time < std::time::Instant::now() {
                        for addr in &chunk.missing_acks {
                            if !self.group.session_members.is_alive(addr) {
                                dead_members.push(addr.clone());
                            }
                        }

                        tracing::debug!(
                            seq = <_ as Into<u16>>::into(chunk_seq),
                            elapsed = ?chunk.initial_send_time.elapsed(),
                            last_retransmit = ?chunk.retransmit_time.elapsed(),
                            retransmit_count = chunk.retransmit_count,
                            missing_acks = ?chunk.missing_acks.iter().map(display_addr).collect::<Vec<_>>(),
                            "retransmitting chunk",
                        );
                        chunk.retransmit_count += 1;

                        self.group.channel.send_chunk_buffer_to(
                            &chunk.buffer,
                            chunk.packet_size,
                            &self.group.multicast_addr,
                        )?;

                        let retransmit_delay =
                            self.initial_retransmit_delay * 2u32.pow(chunk.retransmit_count);
                        let retransmit_delay =
                            std::cmp::min(retransmit_delay, self.max_retransmit_delay);
                        chunk.retransmit_time = std::time::Instant::now() + retransmit_delay;
                    }
                }
            }
        }

        // Remove acknowledged chunks from the front
        while let Some(None) = inner.unacknowledged_chunks.front() {
            inner.unacknowledged_chunks.pop_front();
            inner.seq_not_ack = inner.seq_not_ack.next();
        }

        for addr in dead_members {
            tracing::debug!(addr = %display_addr(&addr), "timeout");
            self.group.remove(&addr)?;
        }

        Ok(())
    }

    /// Returns the group id.
    pub fn id(&self) -> GroupId {
        self.group.id().into()
    }

    /// Returns true if the group has members.
    pub fn has_members(&self) -> bool {
        !self.group.state.members.is_empty()
    }

    /// Processes incoming messages and sends retransmissions if necessary.
    ///
    /// This method must be called periodically to ensure that messages are sent
    /// and received. It is also called by the `accept` and `try_accept`
    /// methods and when writing a message.
    #[tracing::instrument(skip(self))]
    pub fn process(&mut self) -> std::io::Result<()> {
        self.group.try_recv()?;
        self.process_unacknlowedged_chunks()
    }

    /// Processes incoming messages and sends retransmissions if necessary.
    ///
    /// This method must be called periodically to ensure that messages are sent
    /// and received. It is also called by the `accept` and `try_accept`
    /// methods and when writing a message.
    #[tracing::instrument(skip(self))]
    pub fn try_process(&mut self) -> std::io::Result<()> {
        self.group.try_recv()?;
        self.process_unacknlowedged_chunks()
    }

    /// Accepts a new connection.
    ///
    /// This method blocks until a new connection is established or an error
    /// occurs.
    #[tracing::instrument(skip(self))]
    pub fn accept(&mut self) -> std::io::Result<SocketAddr> {
        self.process()?;
        self.group.accept().and_then(sock_addr_to_socket_addr)
    }

    /// Tries to accept a new connection.
    ///
    /// This method does not block and returns `Ok(None)` if no new connection
    /// is available.
    #[tracing::instrument(skip(self))]
    pub fn try_accept(&mut self) -> std::io::Result<Option<SocketAddr>> {
        self.process_unacknlowedged_chunks()?;

        let addr = self.group.try_accept()?;
        if let Some(addr) = addr {
            let addr = sock_addr_to_socket_addr(addr)?;
            Ok(Some(addr))
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(skip(self, buffer))]
    fn send_message_buffer(
        &mut self,
        mut buffer: ChunkBuffer,
        fragment_index: usize,
        last: bool,
        packet_size: usize,
    ) -> std::io::Result<()> {
        buffer.init(&BroadcastMessage {
            seq: self.group.state.seq,
            group_id: self.group.id(),
        });

        match (fragment_index, last) {
            (0, true) => {}
            (0, false) => {
                buffer[0] = protocol::CHUNK_ID_BROADCAST_FIRST_MESSAGE_FRAGMENT;
            }
            (_, false) => {
                buffer[0] = protocol::CHUNK_ID_BROADCAST_MESSAGE_FRAGMENT;
            }
            (_, true) => {
                buffer[0] = protocol::CHUNK_ID_BROADCAST_FINAL_MESSAGE_FRAGMENT;
            }
        }

        tracing::trace_span!("wait for packages").in_scope(|| -> std::io::Result<()> {
            loop {
                self.process()?;
                if self.group.inner.packets_in_flight >= self.max_packets_in_flight {
                    tracing::trace!(
                        packets_in_flight = self.group.inner.packets_in_flight,
                        max_packets_in_flight = self.max_packets_in_flight,
                        "too many packets in flight",
                    );
                } else {
                    break;
                }
            }
            Ok(())
        })?;

        self.group
            .send_chunk_buffer_to_group(&buffer, packet_size)?;
        let send_time = std::time::Instant::now();

        self.group
            .inner
            .unacknowledged_chunks
            .push_back(Some(UnacknowledgedChunk {
                retransmit_time: send_time + self.initial_retransmit_delay,
                initial_send_time: send_time,
                buffer,
                packet_size,
                missing_acks: self.group.state.members.clone(),
                retransmit_count: 0,
            }));
        self.group.inner.packets_in_flight += 1;
        self.group.state.seq = self.group.state.seq.next();

        Ok(())
    }

    /// Start writing a new message to be broadcasted.
    pub fn write_message(&mut self) -> MessageWriter {
        MessageWriter {
            buffer: ManuallyDrop::new(self.group.buffer_allocator().allocate()),
            sender: self,
            fragment_count: 0,
            cursor: MESSAGE_PAYLOAD_OFFSET,
        }
    }

    /// Waits until all messages have been received by all members.
    pub fn wait(&mut self) -> Result<(), std::io::Error> {
        while self.group.inner.packets_in_flight > 0 {
            self.process()?;
        }
        Ok(())
    }
}

/// A writer for a broadcast message.
///
/// This struct implements the [`Write`](std::io::Write) trait and can be used
/// to write a message. The final chunk of the message is sent when the writer
/// is dropped. If the message is split across multiple chunks, intermediate
/// chunks are sent as soon as they are complete.
pub struct MessageWriter<'a> {
    sender: &'a mut BroadcastGroupSender,
    buffer: ManuallyDrop<ChunkBuffer>,
    fragment_count: usize,
    cursor: usize,
}

impl Drop for MessageWriter<'_> {
    #[tracing::instrument(skip(self))]
    fn drop(&mut self) {
        let buffer = unsafe { ManuallyDrop::take(&mut self.buffer) };
        self.sender
            .send_message_buffer(buffer, self.fragment_count, true, self.cursor)
            .unwrap();
    }
}

impl Write for MessageWriter<'_> {
    #[tracing::instrument(skip(self))]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut src_bytes = buf;

        while !src_bytes.is_empty() {
            let mut remaining_buffer = &mut self.buffer[self.cursor..];
            if remaining_buffer.is_empty() {
                let new_buffer = self.sender.group.buffer_allocator().allocate();
                self.sender.send_message_buffer(
                    std::mem::replace(&mut self.buffer, new_buffer),
                    self.fragment_count,
                    false,
                    self.cursor,
                )?;
                self.fragment_count += 1;
                self.cursor = MESSAGE_PAYLOAD_OFFSET;
                remaining_buffer = &mut self.buffer[self.cursor..];
            }

            let len = remaining_buffer.len().min(src_bytes.len());
            remaining_buffer[..len].copy_from_slice(&src_bytes[..len]);
            self.cursor += len;
            src_bytes = &src_bytes[len..];
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Represents a received message.
///
/// Use the [`read`](Message::read) method to read the message.
pub struct Message {
    chunks: Vec<ReceivedChunk>,
}

impl Message {
    /// Reads the message.
    ///
    /// This method returns a [`MessageReader`](MessageReader) that implements
    /// the [`Read`](std::io::Read) trait and can be used to read the
    /// message.
    pub fn read(&self) -> MessageReader {
        MessageReader {
            chunks: &self.chunks,
            cursor: MESSAGE_PAYLOAD_OFFSET,
        }
    }
}

/// A reader for a received message.
///
/// This struct implements the [`Read`](std::io::Read) trait and can be used to
/// read the message.
pub struct MessageReader<'a> {
    chunks: &'a [ReceivedChunk],
    cursor: usize,
}

impl Read for MessageReader<'_> {
    #[tracing::instrument(skip(self))]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let total_len = buf.len();
        let mut dst_bytes = buf;
        while !dst_bytes.is_empty() {
            let mut remaining_buffer =
                &self.chunks[0].buffer()[self.cursor..self.chunks[0].packet_size()];
            if remaining_buffer.is_empty() {
                if self.chunks.len() == 1 {
                    return Ok(total_len - dst_bytes.len());
                }
                self.chunks = &self.chunks[1..];
                self.cursor = MESSAGE_PAYLOAD_OFFSET;
                remaining_buffer =
                    &self.chunks[0].buffer()[self.cursor..self.chunks[0].packet_size()];
            }

            let len = remaining_buffer.len().min(dst_bytes.len());
            dst_bytes[..len].copy_from_slice(&remaining_buffer[..len]);
            self.cursor += len;
            dst_bytes = &mut dst_bytes[len..];
        }
        Ok(total_len)
    }
}

#[derive(Debug, Default)]
pub(crate) struct BroadcastGroupReceiverState {
    next_seq: SequenceNumber,
    chunks: VecDeque<Option<ReceivedChunk>>,
}

impl BroadcastGroupReceiverState {}

impl GroupMemberTypeImpl for BroadcastGroupReceiverState {
    const GROUP_TYPE: protocol::GroupType = protocol::GROUP_TYPE_BROADCAST;

    fn process_group_join(&mut self, seq: SequenceNumber, _group: &GroupMemberState) {
        self.next_seq = seq;
    }

    fn process_chunk(&mut self, chunk: Chunk, addr: &SockAddr, _group: &GroupMemberState) -> bool {
        match chunk {
            Chunk::SessionJoin(_)
            | Chunk::SessionWelcome(_)
            | Chunk::SessionHeartbeat(_)
            | Chunk::GroupJoin(_)
            | Chunk::GroupWelcome(_)
            | Chunk::GroupLeave(_)
            | Chunk::GroupDisconnected(_) => {
                // These chunks cannot be forwared to this function, as they should have been
                // filtered out by either the multiplex socket or the GroupCoordinator.
                tracing::error!(
                    ?chunk,
                    from = %display_addr(addr),
                    "received invalid chunk in broadcast group",
                );
                unreachable!();
            }
            Chunk::BarrierReached(_) | Chunk::BarrierReleased(_) | Chunk::GroupAck(_) => {
                // These chunks could be forwarded to this function, but they would never been
                // send by this crate.
                tracing::error!(
                    ?chunk,
                    from = %display_addr(addr),
                    "received invalid message in broadcast group receiver",
                );
                false
            }
            Chunk::BroadcastMessage(BroadcastMessage { seq, .. })
            | Chunk::BroadcastFirstMessageFragment(BroadcastFirstMessageFragment { seq, .. })
            | Chunk::BroadcastMessageFragment(BroadcastMessageFragment { seq, .. })
            | Chunk::BroadcastFinalMessageFragment(BroadcastFinalMessageFragment { seq, .. }) => {
                let offset = *seq - self.next_seq;
                if offset > u16::MAX as usize / 2 {
                    // This is most likely an old packet, just ignore it
                    tracing::trace!(
                        from = %display_addr(addr),
                        seq = <_ as Into<u16>>::into(*seq),
                        expected_seq = <_ as Into<u16>>::into(self.next_seq),
                        "received old chunk",
                    );
                    false
                } else {
                    tracing::trace!(
                        from = %display_addr(addr),
                        seq = <_ as Into<u16>>::into(*seq),
                        expected_seq = <_ as Into<u16>>::into(self.next_seq),
                        "received chunk",
                    );

                    if offset as usize >= self.chunks.len() {
                        self.chunks.resize_with(offset as usize + 1, || None);
                        true
                    } else {
                        self.chunks[offset as usize].is_none()
                    }
                }
            }
        }
    }

    fn take_chunk(&mut self, chunk: ReceivedChunk, _: &GroupMemberState) {
        let msg_seq = match BroadcastMessage::ref_from_prefix(&chunk.buffer()[1..]) {
            Some(msg) => msg.seq,
            None => {
                unreachable!();
            }
        };
        let offset = msg_seq - self.next_seq;
        debug_assert!(offset < u16::MAX as usize / 2);
        debug_assert!(offset < self.chunks.len());
        debug_assert!(self.chunks[offset].is_none());
        tracing::trace!(
            from = %display_addr(&chunk.addr()),
            seq = <_ as Into<u16>>::into(msg_seq),
            expected_seq = <_ as Into<u16>>::into(self.next_seq),
            offset,
            "inserting chunk",
        );
        self.chunks[offset] = Some(chunk);
    }
}

/// A receiver for broadcast messages.
pub struct BroadcastGroupReceiver {
    pub(crate) group: GroupMember<BroadcastGroupReceiverState>,
}

impl BroadcastGroupReceiver {
    /// Receives a message from the sender.
    ///
    /// This method blocks until a message is received.
    pub fn recv(&mut self) -> std::io::Result<Message> {
        loop {
            let chunks = &mut self.group.inner_mut()?.chunks;
            let mut chunk_count = 0;

            for (index, chunk) in chunks.iter().enumerate() {
                match chunk {
                    Some(chunk) => match chunk.buffer()[0] {
                        protocol::CHUNK_ID_BROADCAST_MESSAGE => {
                            if index == 0 {
                                chunk_count = 1;
                                break;
                            } else {
                                tracing::error!(
                                    ?chunk,
                                    expected_kind = "CHUNK_ID_BROADCAST_MESSAGE_FRAGMENT or CHUNK_ID_BROADCAST_FINAL_MESSAGE_FRAGMENT",
                                    "unepxected chunk"
                                );
                                return Err(std::io::ErrorKind::InvalidData.into());
                            }
                        }
                        protocol::CHUNK_ID_BROADCAST_FIRST_MESSAGE_FRAGMENT => {
                            if index == 0 {
                            } else {
                                tracing::error!(
                                    ?chunk,
                                    expected_kind = "CHUNK_ID_BROADCAST_MESSAGE_FRAGMENT or CHUNK_ID_BROADCAST_FINAL_MESSAGE_FRAGMENT",
                                    "unepxected chunk"
                                );
                                return Err(std::io::ErrorKind::InvalidData.into());
                            }
                        }
                        protocol::CHUNK_ID_BROADCAST_MESSAGE_FRAGMENT => {
                            if index == 0 {
                                tracing::error!(
                                    ?chunk,
                                    expected_kind = "CHUNK_ID_BROADCAST_FIST_MESSAGE_FRAGMENT or CHUNK_ID_BROADCAST_MESSAGE",
                                    "unepxected chunk"
                                );
                                return Err(std::io::ErrorKind::InvalidData.into());
                            }
                        }
                        protocol::CHUNK_ID_BROADCAST_FINAL_MESSAGE_FRAGMENT => {
                            if index == 0 {
                                tracing::error!(
                                    ?chunk,
                                    expected_kind = "CHUNK_ID_BROADCAST_FIST_MESSAGE_FRAGMENT or CHUNK_ID_BROADCAST_MESSAGE",
                                    "unepxected chunk"
                                );
                                return Err(std::io::ErrorKind::InvalidData.into());
                            } else {
                                chunk_count = index + 1;
                                break;
                            }
                        }
                        _ => {
                            panic!("unexpected chunk: {:?}", chunk);
                        }
                    },
                    None => {
                        break;
                    }
                }
            }

            if chunk_count == 0 {
                self.group.recv()?;
            } else {
                let chunks = chunks
                    .drain(..chunk_count)
                    .map(|c| c.unwrap())
                    .collect::<Vec<_>>();

                let seq = &mut self.group.inner_mut()?.next_seq;
                for _ in 0..chunk_count {
                    *seq = seq.next()
                }
                return Ok(Message { chunks });
            }
        }
    }
}

// pub struct Subscription {
//     control_receiver: ChunkReceiver,
//     multicast_receiver: ChunkReceiver,
//     buffer_allocator: Arc<ChunkBufferAllocator>,
//     sequence: SequenceNumber,
//     control_socket: ChunkSocket,

//     /// stores the chunks starting from the last received sequence number.
//     chunks: VecDeque<Option<ReceivedChunk>>,
// }

// #[derive(thiserror::Error, Debug)]
// pub enum RecvError {
//     #[error("I/O error: {0}")]
//     Io(#[from] std::io::Error),

//     #[error("Disconnected")]
//     Recv(#[from] crossbeam::channel::RecvError),
// }

// impl Subscription {
//     pub fn recv(&mut self) -> Result<Message, RecvError> {
//         loop {
//             let chunk = self.multicast_receiver.recv()?;
//             match chunk.validate() {
//                 Ok(Chunk::Message(msg, _)) => {
//                     self.control_socket.send_chunk(&Ack {
//                         header: ChannelHeader {
//                             seq: msg.header.seq,
//                             channel_id: msg.header.channel_id,
//                         },
//                     })?;

//                     let seq: u16 = msg.header.seq.into();
//                     let offset =
// seq.wrapping_sub(self.sequence.wrapping_add(1));                     if
// offset > u16::MAX / 2 {                         // This is most likely an old
// packet, just ignore it                     } else if seq !=
// self.sequence.wrapping_add(1) {                         panic!(
//                             "unexpected sequence number: expected {}, got
// {}",                             self.sequence.wrapping_add(1),
//                             seq
//                         );
//                     } else {
//                         log::debug!("received message: {:?}", msg);
//                         self.sequence = seq;
//                         return Ok(Message::SingleChunk(chunk));
//                     }
//                 }
//                 Ok(chunk) => {
//                     log::debug!("ignore unexpected chunk: {:?}", chunk);
//                 }
//                 Err(err) => {
//                     log::error!("received invalid chunk: {}", err);
//                 }
//             }
//         }
//     }
// }
