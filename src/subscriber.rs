use std::{
    collections::VecDeque,
    io::Read,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};

use ahash::HashSet;
use crossbeam::channel::{RecvError, RecvTimeoutError};
use zerocopy::FromBytes;

use crate::{
    chunk::{Chunk, ChunkBufferAllocator},
    chunk_socket::ReceivedChunk,
    multiplex_socket::{ChunkReceiver, MultiplexSocket},
    protocol::{kind, Ack, ChannelHeader, ConnectionInfo, JoinChannel, MESSAGE_PAYLOAD_OFFSET},
    OfferId, SequenceNumber,
};

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error("failed to connect to server")]
    Io(#[from] std::io::Error),

    #[error("failed to receive connection info")]
    ConnectionInfo,
}

#[derive(thiserror::Error, Debug)]
pub enum SubscribeError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("operation timed out")]
    Timeout,

    #[error("already joined channel")]
    AlreadyJoined,
}

pub enum Message {
    SingleChunk(ReceivedChunk),
}

impl Message {
    pub fn read(&self) -> MessageReader {
        match self {
            Message::SingleChunk(chunk) => MessageReader::Single {
                chunk,
                cursor: MESSAGE_PAYLOAD_OFFSET,
            },
        }
    }
}

pub enum MessageReader<'a> {
    Single {
        chunk: &'a ReceivedChunk,
        cursor: usize,
    },
}

impl Read for MessageReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        match self {
            MessageReader::Single { chunk, cursor } => {
                let packet_size = chunk.packet_size();
                let buffer = chunk.buffer();
                let remaining_buffer = &buffer[*cursor..packet_size];
                let len = remaining_buffer.len().min(buf.len());
                buf[..len].copy_from_slice(&remaining_buffer[..len]);
                *cursor += len;
                Ok(len)
            }
        }
    }
}

pub struct Subscription {
    control_receiver: ChunkReceiver,
    multicast_receiver: ChunkReceiver,
    buffer_allocator: Arc<ChunkBufferAllocator>,
    sequence: SequenceNumber,

    /// stores the chunks starting from the last received sequence number.
    chunks: VecDeque<Option<ReceivedChunk>>,
}

impl Subscription {
    pub fn recv(&mut self) -> Result<Message, RecvError> {
        loop {
            let chunk = self.multicast_receiver.recv()?;
            match chunk.validate() {
                Ok(Chunk::Message(msg, _)) => {
                    let seq: u16 = msg.header.seq.into();
                    if seq != self.sequence + 1 {
                        panic!(
                            "unexpected sequence number: expected {}, got {}",
                            self.sequence + 1,
                            seq
                        );
                    } else {
                        log::debug!("received message: {:?}", msg);
                        self.sequence = seq;
                        return Ok(Message::SingleChunk(chunk));
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
    }
}

const CONNECTION_INFO_PACKET_SIZE: usize = std::mem::size_of::<ConnectionInfo>() + 1;

pub struct Subscriber {
    joined_channels: HashSet<OfferId>,
    control_socket: MultiplexSocket,
    multicast_socket: MultiplexSocket,
    buffer_allocator: Arc<ChunkBufferAllocator>,
    resend_timeout: Duration,
    timeout: Duration,
}

impl Subscriber {
    pub fn connect(addr: SocketAddr) -> Result<Self, ConnectionError> {
        let control_socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
        control_socket.connect(addr)?;

        let multicast_socket;
        let buffer_allocator;

        let mut buffer = [0; CONNECTION_INFO_PACKET_SIZE];
        loop {
            if let Ok(1) = control_socket.send(&[0]) {
                match control_socket.recv(&mut buffer) {
                    Ok(CONNECTION_INFO_PACKET_SIZE) => {
                        if buffer[0] != kind::CONNECTION_INFO {
                            log::error!("received invalid chunk: {:?}", buffer);
                            continue;
                        }
                        let conn_info = match ConnectionInfo::ref_from(&buffer[1..]) {
                            Some(conn_info) => conn_info,
                            None => unreachable!(),
                        };
                        log::debug!("received connection info: {:?}", conn_info);
                        multicast_socket = std::net::UdpSocket::bind(SocketAddrV4::new(
                            conn_info.multicast_addr.into(),
                            conn_info.multicast_port.into(),
                        ))?;
                        multicast_socket.join_multicast_v4(
                            &conn_info.multicast_addr.into(),
                            &Ipv4Addr::UNSPECIFIED,
                        )?;
                        buffer_allocator = ChunkBufferAllocator::new(conn_info.chunk_size.into());
                        break;
                    }
                    Ok(c) => {
                        log::error!("Received invalid chunk: {:?}", c);
                    }
                    Err(err) => {
                        log::error!("Failed to receive connection info: {}", err);
                    }
                }
            }

            std::thread::sleep(std::time::Duration::from_secs(1));
        }

        let buffer_allocator = Arc::new(buffer_allocator);
        let control_socket = MultiplexSocket::new(control_socket, buffer_allocator.clone())?;
        let multicast_socket = MultiplexSocket::new(multicast_socket, buffer_allocator.clone())?;

        Ok(Self {
            control_socket,
            multicast_socket,
            buffer_allocator,
            joined_channels: HashSet::default(),
            timeout: Duration::from_secs(5),
            resend_timeout: Duration::from_millis(10),
        })
    }

    pub fn subscribe(&mut self, offer_id: OfferId) -> Result<Subscription, SubscribeError> {
        let deadline = std::time::Instant::now() + self.timeout;

        if self.joined_channels.contains(&offer_id) {
            return Err(SubscribeError::AlreadyJoined);
        }

        let control_receiver = self.control_socket.listen_to_channel(offer_id);

        while std::time::Instant::now() < deadline {
            self.control_socket.send_chunk(JoinChannel {
                channel_id: offer_id.into(),
            })?;
            let receive_deadline = std::time::Instant::now() + self.resend_timeout;

            loop {
                match control_receiver.recv_deadline(receive_deadline) {
                    Ok(chunk) => match chunk.validate() {
                        Ok(Chunk::ConfirmJoinChannel(confirm)) => {
                            self.joined_channels.insert(offer_id);
                            log::info!("joined channel: {:?}", confirm);
                            self.control_socket.send_chunk(Ack {
                                header: ChannelHeader {
                                    channel_id: offer_id.into(),
                                    seq: confirm.header.seq,
                                },
                            })?;
                            return Ok(Subscription {
                                control_receiver,
                                multicast_receiver: self
                                    .multicast_socket
                                    .listen_to_channel(offer_id),
                                buffer_allocator: self.buffer_allocator.clone(),
                                sequence: confirm.header.seq.into(),
                                chunks: VecDeque::new(),
                            });
                        }
                        Ok(c) => {
                            log::debug!("ignore chunk: {:?}", c);
                        }
                        Err(err) => {
                            log::error!("received invalid chunk: {}", err);
                        }
                    },
                    Err(RecvTimeoutError::Timeout) => {
                        // Resend the join channel packet, if the timeout has not been reached.
                        break;
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        unreachable!();
                    }
                }
            }
        }

        Err(SubscribeError::Timeout)
    }
}
