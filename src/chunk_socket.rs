use std::sync::Arc;

use socket2::{SockAddr, Socket};

use crate::{
    chunk::{Chunk, ChunkBuffer, ChunkBufferAllocator, ChunkValidationError},
    protocol::ChunkKindData,
};

#[derive(Debug)]
pub struct ReceivedChunk {
    buffer: ChunkBuffer,
    addr: SockAddr,
    packet_size: usize,
}

impl ReceivedChunk {
    fn new(buffer: ChunkBuffer, addr: SockAddr, packet_size: usize) -> Self {
        Self {
            buffer,
            addr,
            packet_size,
        }
    }

    pub fn validate(&self) -> Result<Chunk, ChunkValidationError> {
        self.buffer.validate(self.packet_size)
    }

    pub fn addr(&self) -> &SockAddr {
        &self.addr
    }

    pub fn buffer(&self) -> &ChunkBuffer {
        &self.buffer
    }

    pub fn packet_size(&self) -> usize {
        self.packet_size
    }
}

/// A socket that sends and receives chunks.
pub struct ChunkSocket {
    socket: Socket,
    buffer_allocator: Arc<ChunkBufferAllocator>,
}

impl ChunkSocket {
    pub fn new(socket: Socket, buffer_allocator: Arc<ChunkBufferAllocator>) -> Self {
        Self {
            socket,
            buffer_allocator,
        }
    }

    pub fn receive_chunk(&self) -> Result<ReceivedChunk, std::io::Error> {
        let mut buffer = self.buffer_allocator.allocate();

        let (size, addr) = {
            let buffer = &mut buffer[..];
            let buffer = unsafe {
                std::mem::transmute::<&mut [u8], &mut [std::mem::MaybeUninit<u8>]>(buffer)
            };

            self.socket.recv_from(buffer)?
        };
        Ok(ReceivedChunk::new(buffer, addr, size))
    }

    pub fn send_chunk_buffer(
        &self,
        buffer: &ChunkBuffer,
        packet_size: usize,
    ) -> Result<(), std::io::Error> {
        self.socket.send(&buffer[..packet_size])?;
        Ok(())
    }

    pub fn send_chunk_buffer_to(
        &self,
        buffer: &ChunkBuffer,
        packet_size: usize,
        addr: &SockAddr,
    ) -> Result<(), std::io::Error> {
        self.socket.send_to(&buffer[..packet_size], addr)?;
        Ok(())
    }

    pub fn send_chunk<T: ChunkKindData>(&self, kind_data: T) -> Result<(), std::io::Error> {
        let mut buffer = self.buffer_allocator.allocate();
        buffer.init(kind_data);
        let size = 1 + std::mem::size_of::<T>();
        self.send_chunk_buffer(&buffer, size)?;
        Ok(())
    }

    pub fn send_chunk_to<T: ChunkKindData>(
        &self,
        kind_data: T,
        addr: &SockAddr,
    ) -> Result<(), std::io::Error> {
        let mut buffer = self.buffer_allocator.allocate();
        buffer.init(kind_data);
        let size = 1 + std::mem::size_of::<T>();
        self.send_chunk_buffer_to(&buffer, size, addr)?;
        Ok(())
    }

    pub fn send_chunk_with_payload<T: ChunkKindData>(
        &self,
        kind_data: T,
        payload: &[u8],
    ) -> Result<(), std::io::Error> {
        let mut buffer = self.buffer_allocator.allocate();
        buffer.init(kind_data);
        let size = 1 + std::mem::size_of::<T>() + payload.len();
        if size > buffer.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "payload too large",
            ));
        }
        self.send_chunk_buffer(&buffer, size)?;
        Ok(())
    }

    pub fn send_chunk_with_payload_to<T: ChunkKindData>(
        &self,
        kind_data: T,
        payload: &[u8],
        addr: &SockAddr,
    ) -> Result<(), std::io::Error> {
        let mut buffer = self.buffer_allocator.allocate();
        buffer.init_with_payload(kind_data, payload);
        let size = 1 + std::mem::size_of::<T>() + payload.len();
        if size > buffer.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "payload too large",
            ));
        }
        self.send_chunk_buffer_to(&buffer, size, addr)?;
        Ok(())
    }

    pub fn try_clone(&self) -> Result<Self, std::io::Error> {
        Ok(Self {
            socket: self.socket.try_clone()?,
            buffer_allocator: self.buffer_allocator.clone(),
        })
    }
}
