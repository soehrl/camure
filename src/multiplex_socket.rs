use std::sync::Arc;

use ahash::HashMap;
use crossbeam::channel::{Receiver, RecvError, Sender};
use socket2::Socket;

use crate::{
    chunk::ChunkBufferAllocator,
    chunk_socket::{ChunkSocket, ReceivedChunk},
    protocol::ChunkKindData,
};

type ChannelId = u16;

pub type ChunkSender = Sender<ReceivedChunk>;
pub type ChunkReceiver = Receiver<ReceivedChunk>;
type ChannelListenerReceiver = Receiver<(ChannelId, ChunkSender)>;
type ChannelListenerSender = Sender<(ChannelId, ChunkSender)>;

struct ChannelConnections {
    channel_receiver: ChannelListenerReceiver,
    channels: HashMap<ChannelId, ChunkSender>,
}

impl ChannelConnections {
    fn new(channel_receiver: ChannelListenerReceiver) -> Self {
        Self {
            channel_receiver,
            channels: HashMap::default(),
        }
    }

    fn send(&mut self, channel_id: ChannelId, chunk: ReceivedChunk) -> Result<(), RecvError> {
        let mut chunk = Some(chunk);

        while let Some(c) = chunk.take() {
            match self.channels.get(&channel_id) {
                Some(sender) => {
                    log::debug!("try sending");
                    if let Err(err) = sender.send(c) {
                        // This sender is disconnected, remove it from the map and try again.
                        self.channels.remove(&channel_id);
                        chunk = Some(err.0);
                    }
                }
                None => {
                    let (id, sender) = self.channel_receiver.recv()?;
                    self.channels.insert(id, sender);
                    log::debug!("received channel {}", id);
                    chunk = Some(c);
                }
            }
        }

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SendError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("buffer is not a valid chunk")]
    InvalidChunk,
}

pub struct MultiplexSocket {
    socket: ChunkSocket,
    channel_sender: ChannelListenerSender,
}

impl MultiplexSocket {
    fn receiver_thread<F: Fn(&ChunkSocket, ReceivedChunk) + Send + 'static>(
        socket: ChunkSocket,
        channel_sender_receiver: ChannelListenerReceiver,
        process_unchannelled_chunk: F,
    ) {
        let mut channel_connections = ChannelConnections::new(channel_sender_receiver);

        loop {
            match socket.receive_chunk() {
                Ok(chunk) => match chunk.validate() {
                    Ok(c) => {
                        if let Some(channel_id) = c.channel_id() {
                            if let Err(err) = channel_connections.send(channel_id, chunk) {
                                log::error!("failed to forward join channel: {}", err);
                            }
                        } else {
                            process_unchannelled_chunk(&socket, chunk)
                        }
                    }
                    Err(err) => log::error!("received invalid chunk: {}", err),
                },
                Err(err) => {
                    log::error!("failed to read from socket: {}", err);
                }
            }
        }
    }

    fn spawn_receiver_thread<F: Fn(&ChunkSocket, ReceivedChunk) + Send + 'static>(
        socket: ChunkSocket,
        channel_sender_receiver: ChannelListenerReceiver,
        process_unchannelled_chunk: F,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            Self::receiver_thread(socket, channel_sender_receiver, process_unchannelled_chunk);
        })
    }

    fn ignore_unchannelled_chunk(_: &ChunkSocket, c: ReceivedChunk) {
        log::debug!("ignoring unchannelled chunk: {:?}", c);
    }

    pub fn new(
        socket: Socket,
        buffer_allocator: Arc<ChunkBufferAllocator>,
    ) -> Result<Self, std::io::Error> {
        Self::with_unchannelled_handler(socket, buffer_allocator, Self::ignore_unchannelled_chunk)
    }

    pub fn with_unchannelled_handler<F: Fn(&ChunkSocket, ReceivedChunk) + Send + 'static>(
        socket: Socket,
        buffer_allocator: Arc<ChunkBufferAllocator>,
        process_unchannelled_chunk: F,
    ) -> Result<Self, std::io::Error> {
        let (channel_sender, channel_receiver) = crossbeam::channel::unbounded();

        Self::spawn_receiver_thread(
            ChunkSocket::new(socket.try_clone().unwrap(), buffer_allocator.clone()),
            channel_receiver,
            process_unchannelled_chunk,
        );

        Ok(Self {
            socket: ChunkSocket::new(socket, buffer_allocator),
            channel_sender,
        })
    }

    pub fn send_chunk<T: ChunkKindData>(&self, kind_data: T) -> Result<(), std::io::Error> {
        self.socket.send_chunk(kind_data)
    }

    pub fn send_chunk_with_payload<T: ChunkKindData>(
        &self,
        kind_data: T,
        payload: &[u8],
    ) -> Result<(), std::io::Error> {
        self.socket.send_chunk_with_payload(kind_data, payload)
    }

    /// Registers a channel listener for the given channel id.
    ///
    /// Any previous listener for the same channel id is replaced.
    pub fn listen_to_channel(&self, channel_id: ChannelId) -> ChunkReceiver {
        let (sender, receiver) = crossbeam::channel::unbounded();
        if self.channel_sender.send((channel_id, sender)).is_err() {
            // The channel cannot be disconnected as the receiver thread which holds the receiver
            // only exits when the client is dropped.
            unreachable!();
        }
        receiver
    }

    pub fn socket(&self) -> &ChunkSocket {
        &self.socket
    }
}
