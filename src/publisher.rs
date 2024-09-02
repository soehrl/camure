use std::{
    io::Write,
    net::{SocketAddr, UdpSocket},
    sync::Arc,
};

use ahash::HashMap;
use crossbeam::queue::SegQueue;
use dashmap::DashSet;

use crate::{
    chunk::{Chunk, ChunkBuffer, ChunkBufferAllocator},
    chunk_socket::{ChunkSocket, ReceivedChunk},
    multiplex_socket::{ChunkReceiver, MultiplexSocket},
    protocol::{
        ChannelHeader, ConfirmJoinChannel, ConnectionInfo, Message, MESSAGE_PAYLOAD_OFFSET,
    },
    OfferId, SequenceNumber,
};

#[derive(Default)]
struct Subscriber {
    latest_ack: Option<SequenceNumber>,
}

pub struct Offer {
    socket: ChunkSocket,
    offer_id: OfferId,
    sequence: SequenceNumber,
    used_offer_ids: Arc<DashSet<OfferId>>,
    receiver: ChunkReceiver,
    new_clients: SegQueue<SocketAddr>,
    clients: HashMap<SocketAddr, Subscriber>,
    buffer_allocator: Arc<ChunkBufferAllocator>,
    multicast_addr: SocketAddr,
}

impl Drop for Offer {
    fn drop(&mut self) {
        self.used_offer_ids.remove(&self.offer_id);
    }
}

impl Offer {
    fn process_chunk(&mut self, chunk: ReceivedChunk) {
        match chunk.validate() {
            Ok(Chunk::JoinChannel(_)) => {
                log::debug!("received join channel");
                self.new_clients.push(chunk.addr());
            }
            Ok(Chunk::Ack(ack)) => {
                if let Some(subscriber) = self.clients.get_mut(&chunk.addr()) {
                    subscriber.latest_ack = Some(ack.header.seq.into());
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

    pub fn id(&self) -> OfferId {
        self.offer_id
    }

    pub fn accecpt(&mut self) -> Option<SocketAddr> {
        self.process_pending_chunks();

        if let Some(client) = self.new_clients.pop() {
            self.clients.insert(client, Subscriber::default());
            self.socket
                .send_chunk_to(
                    ConfirmJoinChannel {
                        header: ChannelHeader {
                            channel_id: self.offer_id.into(),
                            seq: self.sequence.into(),
                        },
                    },
                    client,
                )
                .unwrap();

            // Wait for ack
            loop {
                if let Ok(chunk) = self.receiver.recv() {
                    self.process_chunk(chunk);
                }
                if let Some(subscriber) = self.clients.get(&client) {
                    if subscriber.latest_ack == Some(self.sequence) {
                        break;
                    }
                }
            }

            Some(client)
        } else {
            None
        }
    }

    fn send_chunk_buffer(&mut self, chunk: ChunkBuffer, packet_size: usize) -> Result<(), std::io::Error> {
        log::debug!("sending chunk: {:?}", &chunk[..packet_size]);
        self.socket.send_chunk_buffer_to(&chunk, packet_size, self.multicast_addr)
            // TODO: add it to unacknowledged chunks
    }

    pub fn write_message(&mut self) -> MessageWriter {
        MessageWriter {
            offer: self,
            buffer: None,
            cursor: MESSAGE_PAYLOAD_OFFSET,
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
                let seq = self.offer.sequence.wrapping_add(1);
                buffer.init::<Message>(Message {
                    header: ChannelHeader {
                        channel_id: self.offer.offer_id.into(),
                        seq: seq.into(),
                    },
                });
                self.offer.sequence = seq;
                buffer
            });

            let remaining_buffer = &mut buffer[self.cursor..];
            let len = remaining_buffer.len().min(src_bytes.len());
            remaining_buffer[..len].copy_from_slice(&src_bytes[..len]);
            self.cursor += len;

            if self.cursor == buffer.len() {
                self.offer.send_chunk_buffer(buffer, self.cursor)?;
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
            self.offer.send_chunk_buffer(buffer, self.cursor)?;
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
        let socket = UdpSocket::bind(config.addr).unwrap();
        let buffer_allocator = Arc::new(ChunkBufferAllocator::new(config.chunk_size.into()));
        let multicast_addr = SocketAddr::V4(config.multicast_addr);

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
            multicast_addr,
        }
    }

    pub fn create_offer(&self) -> Result<Offer, CreateOfferError> {
        for offer_id in 0..=OfferId::MAX {
            if self.used_offer_ids.insert(offer_id) {
                let receiver = self.socket.listen_to_channel(offer_id);
                return Ok(Offer {
                    socket: self.socket.socket().try_clone()?,
                    offer_id,
                    sequence: 0,
                    used_offer_ids: self.used_offer_ids.clone(),
                    receiver,
                    new_clients: SegQueue::new(),
                    clients: HashMap::default(),
                    buffer_allocator: self.buffer_allocator.clone(),
                    multicast_addr: self.multicast_addr,
                });
            }
        }

        Err(CreateOfferError::OfferLimitReached)
    }
}
