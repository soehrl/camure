use std::{io::IoSlice, sync::Arc};

use socket2::{SockAddr, Socket};

use crate::{
    chunk::{Chunk, ChunkBuffer, ChunkBufferAllocator, ChunkValidationError},
    protocol::ChunkHeader,
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
#[derive(Debug, Clone)]
pub struct ChunkSocket {
    socket: Arc<Socket>,
    buffer_allocator: Arc<ChunkBufferAllocator>,
    chunk_size: usize,
}

impl ChunkSocket {
    pub fn new(socket: Arc<Socket>, buffer_allocator: Arc<ChunkBufferAllocator>) -> Self {
        Self {
            socket,
            chunk_size: buffer_allocator.chunk_size(),
            buffer_allocator,
        }
    }

    pub fn buffer_allocator(&self) -> &Arc<ChunkBufferAllocator> {
        &self.buffer_allocator
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

    // #[inline]
    // pub fn send_chunk_buffer(
    //     &self,
    //     buffer: &ChunkBuffer,
    //     packet_size: usize,
    // ) -> Result<(), std::io::Error> {
    //     let sent_bytes = self.socket.send(&buffer[..packet_size])?;
    //     debug_assert_eq!(sent_bytes, packet_size);
    //     Ok(())
    // }

    #[inline]
    pub fn send_chunk_buffer_to(
        &self,
        buffer: &ChunkBuffer,
        packet_size: usize,
        addr: &SockAddr,
    ) -> Result<(), std::io::Error> {
        let sent_bytes = self.socket.send_to(&buffer[..packet_size], addr)?;
        debug_assert_eq!(sent_bytes, packet_size);
        Ok(())
    }

    // pub fn send_chunk<T: ChunkHeader>(&self, kind_data: &T) -> Result<(), std::io::Error> {
    //     let data_size = 1 + std::mem::size_of::<T>();
    //     debug_assert!(data_size <= self.chunk_size);

    //     let kind_id = T::id();
    //     let kind_id_buf = [kind_id];
    //     let bufs = [
    //         IoSlice::new(&kind_id_buf),
    //         IoSlice::new(kind_data.as_bytes()),
    //     ];

    //     let sent_bytes = self.socket.send_vectored(&bufs)?;
    //     debug_assert_eq!(sent_bytes, data_size);

    //     Ok(())
    // }

    pub fn send_chunk_to<T: ChunkHeader>(
        &self,
        kind_data: &T,
        addr: &SockAddr,
    ) -> Result<(), std::io::Error> {
        let data_size = 1 + std::mem::size_of::<T>();
        debug_assert!(data_size <= self.chunk_size);

        let kind_id = T::id();
        let kind_id_buf = [kind_id];
        let bufs = [
            IoSlice::new(&kind_id_buf),
            IoSlice::new(kind_data.as_bytes()),
        ];

        let sent_bytes = self.socket.send_to_vectored(&bufs, addr)?;
        debug_assert_eq!(sent_bytes, data_size);

        Ok(())
    }

    // pub fn send_chunk_with_payload<T: ChunkHeader>(
    //     &self,
    //     kind_data: &T,
    //     payload: &[u8],
    // ) -> Result<(), std::io::Error> {
    //     let data_size = 1 + std::mem::size_of::<T>() + payload.len();
    //     if data_size > self.chunk_size {
    //         return Err(std::io::Error::new(
    //             std::io::ErrorKind::InvalidInput,
    //             "payload too large",
    //         ));
    //     }

    //     let kind_id = T::id();
    //     let kind_id_buf = [kind_id];
    //     let bufs = [
    //         IoSlice::new(&kind_id_buf),
    //         IoSlice::new(kind_data.as_bytes()),
    //         IoSlice::new(payload),
    //     ];

    //     let sent_bytes = self.socket.send_vectored(&bufs)?;
    //     debug_assert_eq!(sent_bytes, data_size);

    //     Ok(())
    // }

    pub fn send_chunk_with_payload_to<T: ChunkHeader>(
        &self,
        kind_data: &T,
        payload: &[u8],
        addr: &SockAddr,
    ) -> Result<(), std::io::Error> {
        let data_size = 1 + std::mem::size_of::<T>() + payload.len();
        if data_size > self.chunk_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "payload too large",
            ));
        }

        let kind_id = T::id();
        let kind_id_buf = [kind_id];
        let bufs = [
            IoSlice::new(&kind_id_buf),
            IoSlice::new(kind_data.as_bytes()),
            IoSlice::new(payload),
        ];

        let sent_bytes = self.socket.send_to_vectored(&bufs, addr)?;
        debug_assert_eq!(sent_bytes, data_size);

        Ok(())
    }
}
