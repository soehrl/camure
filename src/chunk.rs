use std::{
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
};

use crossbeam::channel::{Receiver, Sender, TryRecvError};

use crate::protocol::{
    kind, Ack, BarrierReached, BarrierReleased, ChannelDisconnected, ChunkKindData,
    ConfirmJoinChannel, Connect, ConnectionInfo, JoinBarrierGroup, JoinChannel, LeaveChannel,
    Message,
};

#[derive(Debug)]
pub enum Chunk<'a> {
    Connect(&'a Connect),
    ConnectionInfo(&'a ConnectionInfo),
    JoinChannel(&'a JoinChannel),
    ConfirmJoinChannel(&'a ConfirmJoinChannel),
    Ack(&'a Ack),
    Message(&'a Message, &'a [u8]),
    JoinBarrierGroup(&'a JoinBarrierGroup),
    BarrierReached(&'a BarrierReached),
    BarrierReleased(&'a BarrierReleased),
    LeaveChannel(&'a LeaveChannel),
    ChannelDisconnected(&'a ChannelDisconnected),
}

impl Chunk<'_> {
    pub fn channel_id(&self) -> Option<u16> {
        match self {
            Chunk::JoinChannel(join) => Some(join.channel_id.into()),
            Chunk::ConfirmJoinChannel(confirm) => Some(confirm.header.channel_id.into()),
            Chunk::Ack(ack) => Some(ack.header.channel_id.into()),
            Chunk::Message(msg, _) => Some(msg.header.channel_id.into()),
            Chunk::JoinBarrierGroup(join) => Some(join.0.into()),
            Chunk::BarrierReached(b) => Some(b.0.channel_id.into()),
            Chunk::BarrierReleased(b) => Some(b.0.channel_id.into()),
            Chunk::LeaveChannel(c) => Some(c.0.into()),
            Chunk::ChannelDisconnected(c) => Some(c.0.into()),
            _ => None,
        }
    }
}

/// A buffer for a single chunk.
///
/// This buffer is used to store the bytes of a single chunk. It is created using
/// `ChunkBufferAllocator::allocate` and is automatically returned to the allocator when it is
/// dropped. This avoids unnecessary allocations and deallocations.
///
/// The first buffer indicates the kind of the chunk as defined in `crate::protocol::kind`. It is
/// followed by the data specific to its kind which is implemented by the `ChunkKindData` trait.
/// Optionally, it is followed by a payload.
#[derive(Debug)]
pub struct ChunkBuffer {
    bytes: ManuallyDrop<Box<[u8]>>,
    allocator: Sender<Box<[u8]>>,
}

impl Drop for ChunkBuffer {
    fn drop(&mut self) {
        // If send() returns an error, the allocator has been dropped. In this case we can also
        // drop the buffer.
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
    #[error("Invalid chunk kind: {0}")]
    InvalidChunkKind(u8),

    #[error("Invalid packet size: expected {expected}, got {actual}")]
    InvalidPacketSize { expected: usize, actual: usize },
}

impl ChunkBuffer {
    pub fn init<T: ChunkKindData>(&mut self, kind_data: &T) {
        self.bytes[0] = T::kind();
        kind_data.write_to_prefix(&mut self.bytes[1..]);
    }

    pub fn init_with_payload<T: ChunkKindData>(&mut self, kind_data: &T, payload: &[u8]) {
        self.bytes[0] = T::kind();
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

    fn get_kind_data_ref<T: ChunkKindData>(
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

    fn get_kind_data_and_payload_ref<T: ChunkKindData>(
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
            kind::CONNECT => Ok(Chunk::Connect(
                self.get_kind_data_ref::<Connect>(packet_size)?,
            )),
            kind::CONNECTION_INFO => Ok(Chunk::ConnectionInfo(
                self.get_kind_data_ref::<ConnectionInfo>(packet_size)?,
            )),
            kind::JOIN_CHANNEL => Ok(Chunk::JoinChannel(
                self.get_kind_data_ref::<JoinChannel>(packet_size)?,
            )),
            kind::CONFIRM_JOIN_CHANNEL => Ok(Chunk::ConfirmJoinChannel(
                self.get_kind_data_ref::<ConfirmJoinChannel>(packet_size)?,
            )),
            kind::ACK => Ok(Chunk::Ack(self.get_kind_data_ref::<Ack>(packet_size)?)),
            kind::MESSAGE => {
                let (data, payload) = self.get_kind_data_and_payload_ref::<Message>(packet_size)?;
                // return Err(ChunkValidationError::InvalidPacketSize {
                //     expected: 1 + std::mem::size_of::<Message>() + data_len as usize,
                //     actual: packet_size,
                // });
                Ok(Chunk::Message(data, payload))
            }
            kind::JOIN_BARRIER_GROUP => Ok(Chunk::JoinBarrierGroup(
                self.get_kind_data_ref::<JoinBarrierGroup>(packet_size)?,
            )),
            kind::BARRIER_REACHED => Ok(Chunk::BarrierReached(
                self.get_kind_data_ref::<BarrierReached>(packet_size)?,
            )),
            kind::BARRIER_RELEASED => Ok(Chunk::BarrierReleased(
                self.get_kind_data_ref::<BarrierReleased>(packet_size)?,
            )),
            kind::LEAVE_CHANNEL => Ok(Chunk::LeaveChannel(
                self.get_kind_data_ref::<LeaveChannel>(packet_size)?,
            )),
            kind::CHANNEL_DISCONNECTED => Ok(Chunk::ChannelDisconnected(
                self.get_kind_data_ref::<ChannelDisconnected>(packet_size)?,
            )),
            kind => Err(ChunkValidationError::InvalidChunkKind(kind)),
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

    pub fn with_initial_capacity(chunk_size: usize, capacity: usize) -> Self {
        let allocator = Self::new(chunk_size);
        for _ in 0..capacity {
            match allocator.sender.send(allocator.allocate_bytes()) {
                Ok(_) => (),
                Err(_) => unreachable!(), // This cannot happen, as we on both, the sender and
                                          // receiver.
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
            Err(TryRecvError::Disconnected) => unreachable!(), // This cannot happen, as we own
                                                               // both, the sender and receiver.
        }
    }
}
