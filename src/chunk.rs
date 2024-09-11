use std::{
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
};

use crossbeam::channel::{Receiver, Sender, TryRecvError};
use zerocopy::network_endian::U16;

use crate::protocol::{
    self, BarrierReached, BarrierReleased, BroadcastFinalMessageFragment, BroadcastFirstMessageFragment, BroadcastMessage, BroadcastMessageFragment, ChunkHeader, ChunkIdentifier, GroupAck, GroupDisconnected, GroupJoin, GroupLeave, GroupWelcome, SequenceNumber, SessionHeartbeat, SessionJoin, SessionWelcome
};

#[derive(Debug)]
pub enum Chunk<'a> {
    // Session related chunks
    SessionJoin(&'a SessionJoin),
    #[allow(unused)] // TODO: use #[expect(unused)] when it is stable
    SessionWelcome(&'a SessionWelcome),
    SessionHeartbeat(&'a SessionHeartbeat),

    // Group related chunks
    GroupJoin(&'a GroupJoin),
    GroupWelcome(&'a GroupWelcome),
    GroupAck(&'a GroupAck),
    GroupLeave(&'a GroupLeave),
    GroupDisconnected(&'a GroupDisconnected),

    // Broadcast related chunks
    BroadcastMessage(&'a BroadcastMessage),
    BroadcastFirstMessageFragment(&'a BroadcastFirstMessageFragment),
    BroadcastMessageFragment(&'a BroadcastMessageFragment),
    BroadcastFinalMessageFragment(&'a BroadcastFinalMessageFragment),


    // Barrier related chunk
    BarrierReached(&'a BarrierReached),
    BarrierReleased(&'a BarrierReleased),
    // ConfirmJoinChannel(&'a ConfirmJoinChannel),
    // Ack(&'a Ack),
    // Message(&'a Message, &'a [u8]),
    // JoinBarrierGroup(&'a JoinBarrierGroup),
    // BarrierReached(&'a BarrierReached),
    // BarrierReleased(&'a BarrierReleased),
    // LeaveChannel(&'a LeaveChannel),
}

impl Chunk<'_> {
    pub fn channel_id(&self) -> Option<U16> {
        match self {
            Chunk::GroupJoin(GroupJoin { group_id, .. }) => Some(*group_id),
            Chunk::GroupDisconnected(GroupDisconnected(group_id)) => Some(*group_id),
            Chunk::GroupWelcome(GroupWelcome { group_id, .. }) => Some(*group_id),
            Chunk::GroupAck(GroupAck { group_id, .. }) => Some(*group_id),
            Chunk::GroupLeave(GroupLeave(group_id)) => Some(*group_id),
            Chunk::BarrierReached(BarrierReached { group_id, .. }) => Some(*group_id),
            Chunk::BarrierReleased(BarrierReleased { group_id, .. }) => Some(*group_id),
            Chunk::BroadcastMessage(BroadcastMessage { group_id, .. }) => Some(*group_id),
            Chunk::BroadcastFirstMessageFragment(BroadcastFirstMessageFragment { group_id, .. }) => Some(*group_id),
            Chunk::BroadcastMessageFragment(BroadcastMessageFragment { group_id, .. }) => Some(*group_id),
            Chunk::BroadcastFinalMessageFragment(BroadcastFinalMessageFragment { group_id, .. }) => Some(*group_id),

            Chunk::SessionJoin(_) | Chunk::SessionWelcome(_) | Chunk::SessionHeartbeat(_) => None,
        }
    }

    pub fn requires_ack(&self) -> Option<SequenceNumber> {
        match self {
            Chunk::GroupWelcome(GroupWelcome { seq, .. }) => Some(*seq),
            Chunk::BarrierReached(BarrierReached { seq, .. }) => Some(*seq),
            Chunk::BarrierReleased(BarrierReleased { seq, .. }) => Some(*seq),
            Chunk::BroadcastMessage(BroadcastMessage { seq, .. }) => Some(*seq),
            Chunk::BroadcastFirstMessageFragment(BroadcastFirstMessageFragment { seq, .. }) => Some(*seq),
            Chunk::BroadcastMessageFragment(BroadcastMessageFragment { seq, .. }) => Some(*seq),
            Chunk::BroadcastFinalMessageFragment(BroadcastFinalMessageFragment { seq, .. }) => Some(*seq),

            Chunk::GroupLeave(_)
            | Chunk::GroupAck(_)
            | Chunk::GroupDisconnected(_)
            | Chunk::GroupJoin(_)
            | Chunk::SessionJoin(_)
            | Chunk::SessionWelcome(_)
            | Chunk::SessionHeartbeat(_) => None,
        }
    }
}

/// A buffer for a single chunk.
///
/// This buffer is used to store the bytes of a single chunk. It is created
/// using `ChunkBufferAllocator::allocate` and is automatically returned to the
/// allocator when it is dropped. This avoids unnecessary allocations and
/// deallocations.
///
/// The first buffer indicates the kind of the chunk as defined in
/// `crate::protocol::kind`. It is followed by the data specific to its kind
/// which is implemented by the `ChunkKindData` trait. Optionally, it is
/// followed by a payload.
#[derive(Debug)]
pub struct ChunkBuffer {
    bytes: ManuallyDrop<Box<[u8]>>,
    allocator: Sender<Box<[u8]>>,
}

impl Drop for ChunkBuffer {
    fn drop(&mut self) {
        // If send() returns an error, the allocator has been dropped. In this case we
        // can also drop the buffer.
        let _ = self
            .allocator
            .send(unsafe { ManuallyDrop::take(&mut self.bytes) });
    }
}

// impl AsRef<[u8]> for ChunkBuffer {
//     fn as_ref(&self) -> &[u8] {
//         &self.bytes
//     }
// }

// impl AsMut<[u8]> for ChunkBuffer {
//     fn as_mut(&mut self) -> &mut [u8] {
//         &mut self.bytes
//     }
// }

impl Deref for ChunkBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl DerefMut for ChunkBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ChunkValidationError {
    #[error("Invalid chunk identifier: {0}")]
    InvalidChunkId(ChunkIdentifier),

    #[error("Invalid packet size: expected {expected}, got {actual}")]
    InvalidPacketSize { expected: usize, actual: usize },
}

impl ChunkBuffer {
    pub fn init<T: ChunkHeader>(&mut self, kind_data: &T) {
        self.bytes[0] = T::id().into();
        kind_data.write_to_prefix(&mut self.bytes[1..]);
    }

    pub fn init_with_payload<T: ChunkHeader>(&mut self, kind_data: &T, payload: &[u8]) {
        self.bytes[0] = T::id().into();
        kind_data.write_to_prefix(&mut self.bytes[1..]);
        let payload_offset = 1 + std::mem::size_of::<T>();
        self.bytes[payload_offset..payload_offset + payload.len()].copy_from_slice(payload);
    }

    fn kind_data_slice(&self, kind_size: usize) -> &[u8] {
        &self.bytes[1..1 + kind_size]
    }

    fn payload_slice(&self, kind_size: usize, packet_size: usize) -> &[u8] {
        &self.bytes[1 + kind_size..packet_size]
    }

    fn get_kind_data_ref<T: ChunkHeader>(
        &self,
        packet_size: usize,
    ) -> Result<&T, ChunkValidationError> {
        let kind_data_size = std::mem::size_of::<T>();

        match T::ref_from(self.kind_data_slice(kind_data_size)) {
            Some(data) => Ok(data),
            None => Err(ChunkValidationError::InvalidPacketSize {
                expected: 1 + kind_data_size,
                actual: packet_size,
            }),
        }
    }

    fn get_kind_data_and_payload_ref<T: ChunkHeader>(
        &self,
        packet_size: usize,
    ) -> Result<(&T, &[u8]), ChunkValidationError> {
        let kind_data_size = std::mem::size_of::<T>();

        match T::ref_from(self.kind_data_slice(kind_data_size)) {
            Some(data) => Ok((data, self.payload_slice(kind_data_size, packet_size))),
            None => Err(ChunkValidationError::InvalidPacketSize {
                expected: 1 + kind_data_size,
                actual: packet_size,
            }),
        }
    }

    pub fn validate(&self, packet_size: usize) -> Result<Chunk, ChunkValidationError> {
        match self.bytes[0] {
            protocol::CHUNK_ID_SESSION_JOIN => Ok(Chunk::SessionJoin(
                self.get_kind_data_ref::<SessionJoin>(packet_size)?,
            )),
            protocol::CHUNK_ID_SESSION_WELCOME => Ok(Chunk::SessionWelcome(
                self.get_kind_data_ref::<SessionWelcome>(packet_size)?,
            )),
            protocol::CHUNK_ID_SESSION_HEARTBEAT => Ok(Chunk::SessionHeartbeat(
                self.get_kind_data_ref::<SessionHeartbeat>(packet_size)?,
            )),
            protocol::CHUNK_ID_GROUP_JOIN => Ok(Chunk::GroupJoin(
                self.get_kind_data_ref::<GroupJoin>(packet_size)?,
            )),
            protocol::CHUNK_ID_GROUP_WELCOME => Ok(Chunk::GroupWelcome(
                self.get_kind_data_ref::<GroupWelcome>(packet_size)?,
            )),
            protocol::CHUNK_ID_GROUP_ACK => Ok(Chunk::GroupAck(
                self.get_kind_data_ref::<GroupAck>(packet_size)?,
            )),
            protocol::CHUNK_ID_GROUP_LEAVE => Ok(Chunk::GroupLeave(
                self.get_kind_data_ref::<GroupLeave>(packet_size)?,
            )),
            protocol::CHUNK_ID_GROUP_DISCONNECTED => Ok(Chunk::GroupDisconnected(
                self.get_kind_data_ref::<GroupDisconnected>(packet_size)?,
            )),
            protocol::CHUNK_ID_BROADCAST_MESSAGE => Ok(Chunk::BroadcastMessage(
                self.get_kind_data_ref::<BroadcastMessage>(packet_size)?,
            )),
            protocol::CHUNK_ID_BROADCAST_FIRST_MESSAGE_FRAGMENT => Ok(Chunk::BroadcastFirstMessageFragment(
                self.get_kind_data_ref::<BroadcastFirstMessageFragment>(packet_size)?,
            )),
            protocol::CHUNK_ID_BROADCAST_MESSAGE_FRAGMENT => Ok(Chunk::BroadcastMessageFragment(
                self.get_kind_data_ref::<BroadcastMessageFragment>(packet_size)?,
            )),
            protocol::CHUNK_ID_BROADCAST_FINAL_MESSAGE_FRAGMENT => Ok(Chunk::BroadcastFinalMessageFragment(
                self.get_kind_data_ref::<BroadcastFinalMessageFragment>(packet_size)?,
            )),
            protocol::CHUNK_ID_BARRIER_REACHED => Ok(Chunk::BarrierReached(
                self.get_kind_data_ref::<BarrierReached>(packet_size)?,
            )),
            protocol::CHUNK_ID_BARRIER_RELEASED => Ok(Chunk::BarrierReleased(
                self.get_kind_data_ref::<BarrierReleased>(packet_size)?,
            )),
            id => Err(ChunkValidationError::InvalidChunkId(id)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChunkBufferAllocator {
    chunk_size: usize,
    sender: Sender<Box<[u8]>>,
    receiver: Receiver<Box<[u8]>>,
}

impl ChunkBufferAllocator {
    pub fn new(chunk_size: usize) -> Self {
        let (sender, receiver) = crossbeam::channel::unbounded();
        ChunkBufferAllocator {
            chunk_size,
            sender,
            receiver,
        }
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn with_initial_capacity(chunk_size: usize, capacity: usize) -> Self {
        let allocator = Self::new(chunk_size);
        for _ in 0..capacity {
            match allocator.sender.send(allocator.allocate_bytes()) {
                Ok(_) => (),
                Err(_) => unreachable!(), /* This cannot happen, as we on both, the sender and
                                           * receiver. */
            }
        }
        allocator
    }

    fn allocate_bytes(&self) -> Box<[u8]> {
        vec![0; self.chunk_size].into_boxed_slice()
    }

    pub fn allocate(&self) -> ChunkBuffer {
        match self.receiver.try_recv() {
            Ok(bytes) => ChunkBuffer {
                bytes: ManuallyDrop::new(bytes),
                allocator: self.sender.clone(),
            },
            Err(TryRecvError::Empty) => ChunkBuffer {
                bytes: ManuallyDrop::new(self.allocate_bytes()),
                allocator: self.sender.clone(),
            },
            Err(TryRecvError::Disconnected) => unreachable!(), /* This cannot happen, as we own
                                                                * both, the sender and receiver. */
        }
    }
}
