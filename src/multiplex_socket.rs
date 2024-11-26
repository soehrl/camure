use std::{num::NonZeroUsize, sync::Arc};

use ahash::HashMap;
use crossbeam::channel::{Receiver, RecvTimeoutError, Sender, TryRecvError, TrySendError};
use dashmap::DashMap;
use socket2::{SockAddr, Socket};

use crate::{
    chunk::{Chunk, ChunkBuffer, ChunkBufferAllocator},
    chunk_socket::{ChunkSocket, ReceivedChunk},
    protocol::{self, ChunkHeader},
    utils::display_addr,
};

pub type ChunkSender = Sender<ReceivedChunk>;
pub type ChunkReceiver = Receiver<ReceivedChunk>;

pub enum CallbackReason<'a> {
    ChunkHandled {
        addr: &'a SockAddr,
    },
    UnhandledChunk {
        chunk: Chunk<'a>,
        addr: &'a SockAddr,
    },
    Timeout,
}

pub trait Callback: Fn(&MultiplexSocket, CallbackReason) + Send + 'static {}
impl<F: Fn(&MultiplexSocket, CallbackReason) + Send + 'static> Callback for F {}

#[derive(thiserror::Error, Debug)]
enum ForwardChunkError {
    #[error("channel is full")]
    RecvBufferFull(ReceivedChunk),

    #[error("channel is disconnected")]
    Disconnected(ReceivedChunk),
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessError<E> {
    #[error("I/O error: {0}")]
    RecvError(#[from] RecvTimeoutError),

    #[error("callback error: {0}")]
    Callback(E),
}

pub struct Channel {
    socket: Arc<MultiplexSocket>,
    receiver: ChunkReceiver,
    channel_id: protocol::GroupId,
}

impl Drop for Channel {
    fn drop(&mut self) {
        self.socket.channels.remove(&self.channel_id);
        // debug_assert!(self
        //     .receiver
        //     .try_recv()
        //     .is_err_and(|e| e == TryRecvError::Disconnected));
    }
}

impl Channel {
    #[inline]
    pub fn id(&self) -> protocol::GroupId {
        self.channel_id
    }

    #[inline]
    pub fn buffer_allocator(&self) -> &Arc<ChunkBufferAllocator> {
        self.socket.buffer_allocator()
    }

    // #[inline]
    // pub fn send_chunk<H: ChunkHeader>(&self, kind_data: &H) -> Result<(),
    // std::io::Error> {     self.socket.send_chunk(kind_data)
    // }

    #[inline]
    pub fn send_chunk_buffer_to(
        &self,
        buffer: &ChunkBuffer,
        packet_size: usize,
        addr: &SockAddr,
    ) -> Result<(), std::io::Error> {
        self.socket.send_chunk_buffer_to(buffer, packet_size, addr)
    }

    #[inline]
    pub fn send_chunk_to<H: ChunkHeader>(
        &self,
        kind_data: &H,
        addr: &SockAddr,
    ) -> Result<(), std::io::Error> {
        self.socket.send_chunk_to(kind_data, addr)
    }

    #[inline]
    pub fn send_chunk_with_payload_to<H: ChunkHeader>(
        &self,
        kind_data: &H,
        payload: &[u8],
        addr: &SockAddr,
    ) -> Result<(), std::io::Error> {
        self.socket
            .send_chunk_with_payload_to(kind_data, payload, addr)
    }

    #[inline]
    pub fn receiver(&self) -> &ChunkReceiver {
        &self.receiver
    }

    #[inline]
    pub fn recv(&self) -> Result<ReceivedChunk, std::io::Error> {
        self.receiver
            .recv()
            .map_err(|_| std::io::ErrorKind::NotConnected.into())
    }

    #[inline]
    pub fn try_recv(&self) -> Result<Option<ReceivedChunk>, std::io::Error> {
        match self.receiver.try_recv() {
            Ok(chunk) => Ok(Some(chunk)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(std::io::ErrorKind::NotConnected.into()),
        }
    }

    #[inline]
    pub fn recv_until(
        &self,
        deadline: std::time::Instant,
    ) -> Result<ReceivedChunk, std::io::Error> {
        self.receiver
            .recv_deadline(deadline)
            .map_err(|err| match err {
                RecvTimeoutError::Timeout => std::io::ErrorKind::TimedOut.into(),
                RecvTimeoutError::Disconnected => std::io::ErrorKind::NotConnected.into(),
            })
    }

    // #[inline]
    // pub fn recv_timeout(&self, timeout: Duration) -> Result<ReceivedChunk, ()> {
    //     self.receiver.recv_timeout(timeout).map_err(|_| ())
    // }

    /// Processes chunks for a duration
    pub fn process_for<T, E, F: Fn(Chunk, &SockAddr) -> Result<Option<T>, E>>(
        &self,
        duration: std::time::Duration,
        p: F,
    ) -> Result<T, ProcessError<E>> {
        let deadline = std::time::Instant::now() + duration;
        self.process_until(deadline, p)
    }

    /// Processes chunks until the deadline is reached.
    pub fn process_until<T, E, F: Fn(Chunk, &SockAddr) -> Result<Option<T>, E>>(
        &self,
        deadline: std::time::Instant,
        p: F,
    ) -> Result<T, ProcessError<E>> {
        loop {
            let chunk = self.receiver.recv_deadline(deadline)?;

            if let Ok(c) = chunk.validate() {
                match p(c, chunk.addr()) {
                    Ok(Some(v)) => return Ok(v),
                    Ok(None) => {}
                    Err(e) => return Err(ProcessError::Callback(e)),
                }
            } else {
                unreachable!("should be filtered out by the socket");
            }
        }
    }

    // pub fn wait_for_chunk<T, P: FnMut(Chunk, &SockAddr) -> Option<T>>(
    //     &mut self,
    //     timeout: std::time::Duration,
    //     mut p: P,
    // ) -> Result<T, RecvTimeoutError> {
    //     let start = std::time::Instant::now();
    //     let deadline = start + timeout;

    //     loop {
    //         match self.receiver.recv_deadline(deadline) {
    //             Ok(chunk) => {
    //                 if let Ok(c) = chunk.validate() {
    //                     if let Some(val) = p(c, chunk.addr()) {
    //                         return Ok(val);
    //                     }
    //                 }
    //             }
    //             Err(err) => return Err(err),
    //         }
    //     }
    // }
}

pub struct MultiplexSocket {
    inner: ChunkSocket,
    channels: DashMap<protocol::GroupId, ChunkSender>,
}

impl MultiplexSocket {
    fn forward_chunk(
        &self,
        channel_id: protocol::GroupId,
        chunk: ReceivedChunk,
        cache: &mut HashMap<protocol::GroupId, ChunkSender>,
    ) -> Result<(), ForwardChunkError> {
        // First try to send the chunk to the cached channel.
        let chunk = if let Some(sender) = cache.get(&channel_id) {
            match sender.try_send(chunk) {
                Ok(_) => return Ok(()),
                Err(TrySendError::Full(chunk)) => {
                    return Err(ForwardChunkError::RecvBufferFull(chunk))
                }
                Err(TrySendError::Disconnected(chunk)) => {
                    cache.remove(&channel_id);
                    chunk
                }
            }
        } else {
            chunk
        };

        if let Some(sender) = self.channels.get(&channel_id) {
            match sender.try_send(chunk) {
                Ok(_) => {
                    cache.insert(channel_id, sender.clone());
                    Ok(())
                }
                Err(TrySendError::Full(chunk)) => {
                    return Err(ForwardChunkError::RecvBufferFull(chunk))
                }
                Err(TrySendError::Disconnected(_)) => {
                    unreachable!();
                }
            }
        } else {
            return Err(ForwardChunkError::Disconnected(chunk));
        }
    }

    fn receiver_thread<F: Callback>(
        receiver_socket: Arc<MultiplexSocket>,
        sender_socket: Arc<MultiplexSocket>,
        callback: F,
    ) {
        // Caches the channels for faster lookup.
        let mut channel_cache = HashMap::default();

        // If we hold the only strong reference to the socket, we can exit the loop and
        // drop the socket.
        let exit = if Arc::ptr_eq(&receiver_socket, &sender_socket) {
            Arc::strong_count(&receiver_socket) <= 2
        } else {
            Arc::strong_count(&receiver_socket) <= 1
        };

        while !exit {
            match receiver_socket.inner.receive_chunk() {
                Ok(chunk) => match chunk.validate() {
                    Ok(c) => {
                        let _ = tracing::trace_span!(
                            "handle chunk",
                            from = %display_addr(chunk.addr()),
                            ?chunk,
                        )
                        .enter();
                        tracing::trace!(
                            ?c,
                            from = %display_addr(chunk.addr()),
                            "received chunk"
                        );

                        if let Some(channel_id) = c.channel_id() {
                            callback(
                                &receiver_socket,
                                CallbackReason::ChunkHandled { addr: chunk.addr() },
                            );
                            let ack = c.requires_ack().map(|c| (c, chunk.addr().clone()));

                            match receiver_socket.forward_chunk(
                                channel_id.into(),
                                chunk,
                                &mut channel_cache,
                            ) {
                                Ok(_) => {
                                    if let Some((ack, addr)) = ack {
                                        if let Err(err) = sender_socket.send_chunk_to(
                                            &protocol::GroupAck {
                                                group_id: channel_id.into(),
                                                seq: ack,
                                            },
                                            &addr,
                                        ) {
                                            tracing::error!(
                                                %ack,
                                                %channel_id,
                                                %err,
                                                to = %display_addr(&addr),
                                                "failed to send ack"
                                            );
                                            todo!("close connection");
                                        } else {
                                            tracing::trace!(
                                                %ack,
                                                %channel_id,
                                                to = %display_addr(&addr),
                                                "send ack"
                                            );
                                        }
                                    }
                                }
                                Err(ForwardChunkError::RecvBufferFull(chunk)) => {
                                    tracing::warn!(
                                        %channel_id,
                                        ?chunk,
                                        "chunk dropped due to full channel buffer"
                                    );
                                }
                                Err(ForwardChunkError::Disconnected(chunk)) => {
                                    tracing::warn!(
                                        %channel_id,
                                        ?chunk,
                                        "chunk dropped due to disconnected channel"
                                    );
                                    // FIXME: we received a chunk for a channel
                                    // we are not
                                    // connected to. We should forward this to
                                    // the creator of the
                                    // channel to handle this appropriately.

                                    // if let Err(err) = receiver_socket
                                    //     .send_chunk_to(&
                                    // GroupDisconnected(channel_id),
                                    // chunk.addr())
                                    // {
                                    //     tracing::error!(
                                    //         %channel_id,
                                    //         to =
                                    // %display_addr(&chunk.addr()),
                                    //         err = %err,
                                    //         "failed to send group disconnect
                                    // message"
                                    //     );
                                    //     todo!("close connection");
                                    // } else {
                                    //     tracing::trace!(
                                    //         %channel_id,
                                    //         to =
                                    // %display_addr(&chunk.addr()),
                                    //         "sent group disconnect message"
                                    //     );
                                    // }
                                }
                            }
                        } else {
                            callback(
                                &receiver_socket,
                                CallbackReason::UnhandledChunk {
                                    chunk: c,
                                    addr: chunk.addr(),
                                },
                            );
                        }
                    }
                    Err(err) => {
                        tracing::error!(chunk = ?chunk, err = %err, "received invalid chunk")
                    }
                },
                Err(err) => match err.kind() {
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
                        tracing::trace_span!("timeout").in_scope(|| {
                            callback(&receiver_socket, CallbackReason::Timeout);
                        });
                    }
                    _ => {
                        tracing::error!("failed to read from socket: {}", err);
                        todo!("close connection");
                    }
                },
            }
        }
    }

    fn spawn_receiver_thread<F: Callback>(
        receiver_socket: Arc<MultiplexSocket>,
        sender_socket: Arc<MultiplexSocket>,
        callback: F,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            Self::receiver_thread(receiver_socket, sender_socket, callback);
        })
    }

    // pub fn new(socket: Socket, buffer_allocator: Arc<ChunkBufferAllocator>) ->
    // Arc<Self> {     Self::with_callback(socket, buffer_allocator, |_, _| {})
    // }

    pub fn with_sender_socket(socket: Socket, sender_socket: Arc<MultiplexSocket>) -> Arc<Self> {
        let result = Arc::new(Self {
            inner: ChunkSocket::new(
                Arc::new(socket),
                sender_socket.inner.buffer_allocator().clone(),
            ),
            channels: DashMap::default(),
        });

        Self::spawn_receiver_thread(result.clone(), sender_socket, |_, _| {});

        result
    }

    pub fn with_callback<F: Callback>(
        socket: Socket,
        buffer_allocator: Arc<ChunkBufferAllocator>,
        callback: F,
    ) -> Arc<Self> {
        let result = Arc::new(Self {
            inner: ChunkSocket::new(Arc::new(socket), buffer_allocator),
            channels: DashMap::default(),
        });

        Self::spawn_receiver_thread(result.clone(), result.clone(), callback);

        result
    }

    // pub fn with_callback_and_sender_socket<F: Callback>(
    //     socket: Socket,
    //     sender_socket: Arc<MultiplexSocket>,
    //     buffer_allocator: Arc<ChunkBufferAllocator>,
    //     callback: F,
    // ) -> Arc<Self> {
    //     let result = Arc::new(Self {
    //         inner: ChunkSocket::new(Arc::new(socket), buffer_allocator),
    //         channels: DashMap::default(),
    //     });

    //     Self::spawn_receiver_thread(result.clone(), sender_socket, callback);

    //     result
    // }

    #[inline]
    pub fn buffer_allocator(&self) -> &Arc<ChunkBufferAllocator> {
        self.inner.buffer_allocator()
    }

    #[inline]
    pub fn send_chunk_buffer_to(
        &self,
        buffer: &ChunkBuffer,
        packet_size: usize,
        addr: &SockAddr,
    ) -> Result<(), std::io::Error> {
        self.inner.send_chunk_buffer_to(buffer, packet_size, addr)
    }

    // #[inline]
    // pub fn send_chunk<T: ChunkHeader>(&self, kind_data: &T) -> Result<(),
    // std::io::Error> {     self.inner.send_chunk(kind_data)
    // }

    #[inline]
    pub fn send_chunk_to<T: ChunkHeader>(
        &self,
        kind_data: &T,
        addr: &SockAddr,
    ) -> Result<(), std::io::Error> {
        self.inner.send_chunk_to(kind_data, addr)
    }

    // #[inline]
    // pub fn send_chunk_with_payload<T: ChunkHeader>(
    //     &self,
    //     kind_data: &T,
    //     payload: &[u8],
    // ) -> Result<(), std::io::Error> {
    //     self.inner.send_chunk_with_payload(kind_data, payload)
    // }

    #[inline]
    pub fn send_chunk_with_payload_to<T: ChunkHeader>(
        &self,
        kind_data: &T,
        payload: &[u8],
        addr: &SockAddr,
    ) -> Result<(), std::io::Error> {
        self.inner
            .send_chunk_with_payload_to(kind_data, payload, addr)
    }

    pub fn allocate_channel(
        self: &Arc<Self>,
        channel_id: protocol::GroupId,
        receive_capacity: Option<NonZeroUsize>,
    ) -> Option<Channel> {
        let mut result = None;

        self.channels.entry(channel_id).or_insert_with(|| {
            let (sender, receiver) = if let Some(capacity) = receive_capacity {
                crossbeam::channel::bounded(capacity.get())
            } else {
                crossbeam::channel::unbounded()
            };
            result = Some(receiver);
            sender
        });

        result.map(|receiver| Channel {
            receiver,
            channel_id,
            socket: self.clone(),
        })
    }
}

// pub fn wait_for_chunk2<T, P: FnMut(Chunk, SocketAddr) -> Option<T>>(
//     r: &[&ChunkReceiver],
//     timeout: std::time::Duration,
//     mut p: P,
// ) -> Result<T, RecvTimeoutError> {
//     let start = std::time::Instant::now();
//     let deadline = start + timeout;

//     let mut sel = Select::new();
//     for r in r {
//         sel.recv(r);
//     }

//     loop {
//         match sel.select_timeout(deadline - std::time::Instant::now()) {
//             Ok(recv) => {
//                 let index = recv.index();
//                 let chunk = recv.recv(r[index])?;
//                 if let (Ok(chunk), Some(addr)) = (chunk.validate(),
// chunk.addr().as_socket()) {                     if let Some(v) = p(chunk,
// addr) {                         return Ok(v);
//                     }
//                 }
//             }
//             Err(_) => return Err(RecvTimeoutError::Timeout),
//         }
//     }
// }

// pub fn wait_for_chunk<P: FnMut(Chunk, SocketAddr) -> bool>(
//     r: &ChunkReceiver,
//     timeout: std::time::Duration,
//     mut p: P,
// ) -> Result<(), RecvTimeoutError> {
//     let start = std::time::Instant::now();
//     let deadline = start + timeout;

//     loop {
//         match r.recv_deadline(deadline) {
//             Ok(chunk) => {
//                 if let (Ok(chunk), Some(addr)) = (chunk.validate(),
// chunk.addr().as_socket()) {                     if p(chunk, addr) {
//                         return Ok(());
//                     }
//                 }
//             }
//             Err(err) => return Err(err),
//         }
//     }
// }

// #[derive(thiserror::Error, Debug)]
// pub enum TransmitAndWaitError {
//     #[error("receive error: {0}")]
//     RecvError(#[from] RecvTimeoutError),

//     #[error("send error: {0}")]
//     SendError(#[from] std::io::Error),
// }

// pub fn transmit_and_wait<C: ChunkHeader, T, P: FnMut(Chunk, SocketAddr) ->
// Option<T>>(     socket: &ChunkSocket,
//     kind_data: &C,
//     retransmit_timeout: std::time::Duration,
//     retransmit_count: usize,
//     r: &[&ChunkReceiver],
//     mut p: P,
// ) -> Result<T, TransmitAndWaitError> {
//     for _ in 0..retransmit_count + 1 {
//         socket.send_chunk(kind_data)?;

//         match wait_for_chunk2(r, retransmit_timeout, &mut p) {
//             Ok(v) => return Ok(v),
//             Err(RecvTimeoutError::Timeout) => continue,
//             Err(RecvTimeoutError::Disconnected) => {
//                 return Err(TransmitAndWaitError::RecvError(
//                     RecvTimeoutError::Disconnected,
//                 ))
//             }
//         }
//     }

//     Err(TransmitAndWaitError::RecvError(RecvTimeoutError::Timeout))
// }

// pub fn transmit_to_and_wait<T: ChunkHeader, P: FnMut(Chunk, SocketAddr) ->
// bool>(     socket: &ChunkSocket,
//     addr: &SocketAddr,
//     kind_data: &T,
//     retransmit_timeout: std::time::Duration,
//     retransmit_count: usize,
//     r: &ChunkReceiver,
//     mut p: P,
// ) -> Result<(), TransmitAndWaitError> {
//     for _ in 0..retransmit_count + 1 {
//         socket.send_chunk_to(kind_data, &(*addr).into())?;

//         match wait_for_chunk(r, retransmit_timeout, &mut p) {
//             Ok(_) => return Ok(()),
//             Err(RecvTimeoutError::Timeout) => continue,
//             Err(RecvTimeoutError::Disconnected) => {
//                 return Err(TransmitAndWaitError::RecvError(
//                     RecvTimeoutError::Disconnected,
//                 ))
//             }
//         }
//     }

//     Err(TransmitAndWaitError::RecvError(RecvTimeoutError::Timeout))
// }
