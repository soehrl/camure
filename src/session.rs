use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use dashmap::DashMap;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use zerocopy::FromBytes;

use crate::{
    barrier::{
        BarrierGroupCoordinator, BarrierGroupCoordinatorState, BarrierGroupMember,
        BarrierGroupMemberState,
    },
    broadcast::{
        BroadcastGroupReceiver, BroadcastGroupReceiverState, BroadcastGroupSender,
        BroadcastGroupSenderState,
    },
    chunk::{Chunk, ChunkBufferAllocator},
    group::{GroupCoordinator, GroupCoordinatorTypeImpl, GroupMember, GroupMemberTypeImpl},
    multiplex_socket::{Callback, CallbackReason, MultiplexSocket},
    protocol::{
        SessionHeartbeat, SessionJoin, SessionWelcome, CHUNK_ID_SESSION_WELCOME,
        WELCOME_PACKET_SIZE,
    },
    utils::ExponentialBackoff,
};

pub(crate) struct MemberVitals {
    timeout: std::time::Duration,
    received_heartbeats: DashMap<SockAddr, std::time::Instant>,
}

impl MemberVitals {
    pub fn new(timeout: std::time::Duration) -> Self {
        Self {
            timeout,
            received_heartbeats: DashMap::new(),
        }
    }

    pub fn update_heartbeat(&self, addr: SockAddr) {
        self.received_heartbeats
            .insert(addr, std::time::Instant::now());
    }

    pub fn is_alive(&self, addr: &SockAddr) -> bool {
        self.received_heartbeats
            .get(addr)
            .map(|instant| instant.elapsed() < self.timeout)
            .unwrap_or(false)
    }
}

/// Indicates an error within the multicast coordinator configuration.
#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    /// The chunk size is too small.
    ///
    /// The minimum chunk size is 508 bytes.
    #[error("Chunk size too small (minimum is 508)")]
    ChunkSizeTooSmall,

    /// The heartbeat interval is zero.
    #[error("Heartbeat interval must be greater than zero")]
    HeartbeatIntervalZero,

    /// The client timeout is zero.
    #[error("Client timeout must be greater than zero")]
    ClientTimeoutZero,

    /// The client timeout is less than the heartbeat interval.
    #[error("Client timeout must be greater than the heartbeat interval")]
    ClientTimeoutLessThanHeartbeat,
}

/// Additional configuration for the multicast coordinator.
pub struct CoordinatorConfig {
    /// The chunk size represents the maximum payload size of a socket.
    ///
    /// To avoid fragmentation, the chunk size plus the size of the protocol
    /// header should not exceed the maximum transmission unit (MTU) of the
    /// network. So, for a typical MTU of 1500, this should be set to a
    /// maximum of 1472.
    ///
    /// This value must be at least 508 as this defines the maximum size of a
    /// packet that is deliverable over the internet.
    ///
    /// The default is 1472.
    pub chunk_size: u16,

    /// The interval at which the clients should send heartbeats.
    ///
    /// This must be less than the client timeout.
    ///
    /// The default is 1 second.
    pub heartbeat_interval: std::time::Duration,

    /// The time to wait for a client to send a heartbeat before considering it
    /// disconnected.
    ///
    /// The default is 5 seconds.
    pub client_timeout: std::time::Duration,
}

impl CoordinatorConfig {
    /// Validates the configuration.
    ///
    /// See [`MulticastServerConfig`] and [`ConfigError`] for details.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.chunk_size < 508 {
            return Err(ConfigError::ChunkSizeTooSmall);
        }

        if self.heartbeat_interval.is_zero() {
            return Err(ConfigError::HeartbeatIntervalZero);
        }

        if self.client_timeout.is_zero() {
            return Err(ConfigError::ClientTimeoutZero);
        }

        if self.client_timeout <= self.heartbeat_interval {
            return Err(ConfigError::ClientTimeoutLessThanHeartbeat);
        }
        Ok(())
    }
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            chunk_size: 1472,
            heartbeat_interval: std::time::Duration::from_secs(1),
            client_timeout: std::time::Duration::from_secs(5),
        }
    }
}

/// The identifier of a channel.
pub type GroupId = u16;

/// Indicates an error during channel creation.
#[derive(thiserror::Error, Debug)]
pub enum GroupCreateError {
    /// The channel ID is already in use.
    #[error("Group ID {0} is already in use")]
    GroupIdInUse(GroupId),

    #[error("Group IDs exhausted")]
    GroupIdsExhausted,
}

/// Indicates an error when initializing the multicast session.
#[derive(thiserror::Error, Debug)]
pub enum StartSessionError {
    /// The configuration is invalid.
    ///
    /// See [`ConfigError`] for details.
    #[error("Configuration error: {0}")]
    ConfigError(#[from] ConfigError),

    /// The multicast address is not a valid multicast address.
    ///
    /// See [`MulticastServer::bind`] for details.
    #[error("Invalid multicast address: {0}")]
    NotAMulticastAddress(SocketAddr),

    /// Failed to create the socket.
    #[error("Failed to create socket: {0}")]
    SocketCreateError(std::io::Error),

    /// Failed to bind the socket.
    #[error("Failed to bind socket: {0}")]
    SocketBindError(std::io::Error),
}

/// The server instance for multicast communication.
pub struct Coordinator {
    client_vitals: Arc<MemberVitals>,
    socket: Arc<MultiplexSocket>,
    multicast_address: SockAddr,
}

impl Coordinator {
    /// Starts a session using the given bind address and multicast address.
    ///
    /// The bind address is the address that members need to join the session
    /// (using [`Member::join_session`]). The multicast address is used for
    /// communication. For IPv4, this must be in the range of `224.0.0.0/4`,
    /// i.e., the most significant octet must be in the range of 224 to 239 as
    /// described in [IETF RFC 5771](https://tools.ietf.org/html/rfc5771). For
    /// IPv6, it must be in the range of `ff00::/8` as described in [IETF RFC
    /// 4291](https://datatracker.ietf.org/doc/html/rfc4291).
    ///
    /// See [`Self::start_session_with_config`] for additional configuration
    /// options.
    pub fn start_session(
        bind_address: SocketAddr,
        multicast_address: SocketAddr,
    ) -> Result<Self, StartSessionError> {
        Self::start_session_with_config(
            bind_address,
            multicast_address,
            CoordinatorConfig::default(),
        )
    }

    /// Binds the server to the given address and multicast address with
    /// additional configuration.
    ///
    /// See [`MulticastServerConfig`] for the available configuration options
    /// and [`Self::start_session`] for more information regarding the address
    /// parameters.
    pub fn start_session_with_config(
        bind_address: SocketAddr,
        multicast_address: SocketAddr,
        config: CoordinatorConfig,
    ) -> Result<Self, StartSessionError> {
        config.validate()?;

        if !multicast_address.ip().is_multicast() {
            return Err(StartSessionError::NotAMulticastAddress(multicast_address));
        }

        let client_vitals = Arc::new(MemberVitals::new(config.client_timeout));

        let chunk_allocator = Arc::new(ChunkBufferAllocator::new(config.chunk_size.into()));

        let socket = Socket::new(
            Domain::for_address(bind_address),
            Type::DGRAM,
            Some(Protocol::UDP),
        )
        .map_err(StartSessionError::SocketCreateError)?;

        socket
            .bind(&bind_address.into())
            .map_err(StartSessionError::SocketBindError)?;

        let socket = MultiplexSocket::with_callback(
            socket,
            chunk_allocator,
            Self::create_callback(
                multicast_address,
                config.chunk_size,
                config.heartbeat_interval,
                client_vitals.clone(),
            ),
        );

        Ok(Self {
            client_vitals,
            socket,
            multicast_address: multicast_address.into(),
        })
    }

    /// Create the callback for unhandled chunks
    fn create_callback(
        multicast_address: SocketAddr,
        chunk_size: u16,
        heartbeat_interval: Duration,
        member_vitals: Arc<MemberVitals>,
    ) -> impl Callback {
        move |socket, reason| match reason {
            CallbackReason::ChunkHandled { addr } => {
                member_vitals.update_heartbeat(addr.clone());
            }
            CallbackReason::UnhandledChunk {
                addr,
                chunk: Chunk::SessionHeartbeat(SessionHeartbeat),
            } => {
                member_vitals.update_heartbeat(addr.clone());
            }
            CallbackReason::UnhandledChunk {
                addr,
                chunk: Chunk::SessionJoin(SessionJoin),
            } => {
                // If the message is not sent, the member will not join the session which is
                // probably what we want
                if socket
                    .send_chunk_to(
                        &SessionWelcome {
                            multicast_addr: multicast_address.into(),
                            chunk_size: chunk_size.into(),
                            heartbeat_interval: (heartbeat_interval.as_nanos() as u64).into(),
                        },
                        addr,
                    )
                    .is_ok()
                {
                    member_vitals.update_heartbeat(addr.clone());
                }
            }
            _ => {}
        }
    }

    fn create_group<I: GroupCoordinatorTypeImpl>(
        &self,
        desired_channel_id: Option<GroupId>,
        receive_capacity: Option<NonZeroUsize>,
        inner: I,
    ) -> Result<GroupCoordinator<I>, GroupCreateError> {
        let channel = if let Some(channel_id) = desired_channel_id {
            self.socket
                .allocate_channel(channel_id.into(), receive_capacity)
                .ok_or(GroupCreateError::GroupIdInUse(channel_id))?
        } else {
            let mut channel = None;
            for channel_id in 1..=GroupId::MAX {
                if let Some(c) = self
                    .socket
                    .allocate_channel(channel_id.into(), receive_capacity)
                {
                    channel = Some(c);
                    break;
                }
            }
            channel.ok_or(GroupCreateError::GroupIdsExhausted)?
        };

        Ok(GroupCoordinator {
            channel,
            state: Default::default(),
            inner,
            multicast_addr: self.multicast_address.clone(),
            session_members: self.client_vitals.clone(),
        })
    }

    pub fn create_barrier_group(
        &self,
        desired_channel_id: Option<GroupId>,
    ) -> Result<BarrierGroupCoordinator, GroupCreateError> {
        let group = self.create_group(
            desired_channel_id,
            Some(1024.try_into().unwrap()),
            BarrierGroupCoordinatorState::default(),
        )?;
        Ok(BarrierGroupCoordinator { group })
    }

    pub fn create_broadcast_group(
        &self,
        desired_channel_id: Option<GroupId>,
    ) -> Result<BroadcastGroupSender, GroupCreateError> {
        let group = self.create_group(
            desired_channel_id,
            Some(1024.try_into().unwrap()),
            BroadcastGroupSenderState::default(),
        )?;
        Ok(BroadcastGroupSender::new(group))
    }
}

/// Indicates an error when joining a multicast session.
#[derive(thiserror::Error, Debug)]
pub enum JoinSessionError {
    /// Failed to create the socket.
    #[error("Failed to create socket: {0}")]
    SocketCreateError(std::io::Error),

    /// Failed to bind multicast socket.
    #[error("Failed to bind multicast socket: {0}")]
    SocketBindError(std::io::Error),

    /// Failed to connect to the coordinator address.
    #[error("Failed to bind socket: {0}")]
    SocketConnectError(std::io::Error),

    /// Failed to send join session request.
    #[error("Failed to send join session request: {0}")]
    SendError(std::io::Error),

    /// Failed to receive welcome packet.
    #[error("Failed to receive welcome packet: {0}")]
    RecvError(std::io::Error),

    /// Invalid response from coordinator.
    #[error("Invalid response from coordinator")]
    InvalidResponse,
}

/// Indicates an error when joining a group.
#[derive(thiserror::Error, Debug)]
pub enum JoinGroupError {
    /// THe group was already joined.
    #[error("Group ID {0} is already joined")]
    AlreadyJoined(GroupId),

    #[error("Group ID {0} is not joined")]
    IoError(#[from] std::io::Error),
}

/// A member of a multicast session.
pub struct Member {
    coordinator_socket: Arc<MultiplexSocket>,
    multicast_socket: Arc<MultiplexSocket>,
    coordinator_address: SockAddr,
}

impl Member {
    fn receive_welcome(
        coordinator_socket: &Socket,
        coordinator_address: &SockAddr,
    ) -> Result<[u8; WELCOME_PACKET_SIZE], JoinSessionError> {
        for deadline in ExponentialBackoff::new() {
            coordinator_socket
                .send_to(&[0], &coordinator_address)
                .map_err(JoinSessionError::SendError)?;

            coordinator_socket
                .set_read_timeout(Some(deadline - std::time::Instant::now()))
                .map_err(JoinSessionError::RecvError)?;

            // TODO: use [`std::mem::MaybeUninit::uninit_array`] once it is stable
            let mut buffer = [std::mem::MaybeUninit::<u8>::uninit(); WELCOME_PACKET_SIZE];
            let packet_size = match coordinator_socket.recv(&mut buffer) {
                Ok(packet_size) => packet_size,
                Err(err)
                    if err.kind() == std::io::ErrorKind::WouldBlock
                        || err.kind() == std::io::ErrorKind::TimedOut =>
                {
                    continue
                }
                Err(err) => return Err(JoinSessionError::RecvError(err)),
            };
            if packet_size != WELCOME_PACKET_SIZE {
                return Err(JoinSessionError::InvalidResponse);
            }
            // TODO: use [`std::mem::MaybeUninit::array_assume_init`] once it is stable
            let packet = unsafe { std::mem::transmute::<_, &[u8; WELCOME_PACKET_SIZE]>(&buffer) };
            if packet[0] != CHUNK_ID_SESSION_WELCOME {
                return Err(JoinSessionError::InvalidResponse);
            } else {
                return Ok(*packet);
            }
        }
        unreachable!()
    }

    /// Attempts to join a multicast session.
    ///
    /// The `addr` parameter corresponds to the `bind_addr` parameter of
    /// [`Coordinator::start_session`].
    pub fn join_session(addr: SocketAddr) -> Result<Self, JoinSessionError> {
        let join_session_span = tracing::trace_span!("join_session", addr = %addr);
        let _join_session_span_guard = join_session_span.enter();

        let coordinator_socket =
            Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))
                .map_err(JoinSessionError::SocketCreateError)?;

        let coordinator_address = addr.into();

        // Don't connect, otherwise sent_to will not work
        // coordinator_socket
        //     .connect(&addr.into())
        //     .map_err(JoinSessionError::SocketConnectError)?;

        let packet = Self::receive_welcome(&coordinator_socket, &coordinator_address)?;

        let welcome = match SessionWelcome::ref_from(&packet[1..]) {
            Some(welcome) => welcome,

            // The alignment of `Welcome` is one and the size matches, so this should never happen
            None => unreachable!(),
        };

        coordinator_socket
            .set_read_timeout(Some(Duration::from_nanos(
                welcome.heartbeat_interval.into(),
            )))
            .map_err(JoinSessionError::RecvError)?;

        let multicast_addr: SocketAddr = (&welcome.multicast_addr)
            .try_into()
            .map_err(|_| JoinSessionError::InvalidResponse)?;

        if !multicast_addr.ip().is_multicast() {
            return Err(JoinSessionError::InvalidResponse);
        }

        let multicast_socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))
            .map_err(JoinSessionError::SocketCreateError)?;
        multicast_socket
            .set_reuse_address(true)
            .map_err(JoinSessionError::SocketCreateError)?;
        multicast_socket
            .bind(&multicast_addr.into())
            .map_err(JoinSessionError::SocketBindError)?;

        match multicast_addr.ip() {
            IpAddr::V4(ip) => {
                multicast_socket
                    .join_multicast_v4(&ip.into(), &Ipv4Addr::UNSPECIFIED)
                    .map_err(JoinSessionError::SocketBindError)?;
            }
            IpAddr::V6(ip) => {
                multicast_socket
                    .join_multicast_v6(&ip.into(), 0)
                    .map_err(JoinSessionError::SocketBindError)?;
            }
        }

        let buffer_allocator = Arc::new(ChunkBufferAllocator::new(welcome.chunk_size.into()));

        let coordinator_address_copy = coordinator_address.clone();
        let coordinator_socket = MultiplexSocket::with_callback(
            coordinator_socket,
            buffer_allocator.clone(),
            move |socket, reason| match reason {
                CallbackReason::Timeout => {
                    // We ran into a timeout, which is set to the heartbeat interval
                    if let Err(err) = socket.send_chunk_to(&SessionHeartbeat, &coordinator_address_copy) {
                        tracing::error!(%err, "failed to send heartbeat");
                        todo!("handle error");
                    } else {
                        tracing::trace!("sent heartbeat");
                    }
                }
                _ => {}
            },
        );
        let multicast_socket =
            MultiplexSocket::with_sender_socket(multicast_socket, coordinator_socket.clone());

        Ok(Self {
            coordinator_socket,
            multicast_socket,
            coordinator_address,
        })
    }

    fn join_group<I: GroupMemberTypeImpl>(
        &self,
        channel_id: GroupId,
        inner: I,
    ) -> Result<GroupMember<I>, JoinGroupError> {
        let coordinator_channel = self
            .coordinator_socket
            .allocate_channel(channel_id.into(), Some(1024.try_into().unwrap()))
            .ok_or(JoinGroupError::AlreadyJoined(channel_id))?;

        let multicast_channel = self
            .multicast_socket
            .allocate_channel(channel_id.into(), Some(1024.try_into().unwrap()))
            .ok_or(JoinGroupError::AlreadyJoined(channel_id))?;

        GroupMember::join(
            self.coordinator_address.clone(),
            coordinator_channel,
            multicast_channel,
            inner,
        )
        .map_err(JoinGroupError::IoError)
    }

    pub fn join_barrier_group(
        &self,
        channel_id: GroupId,
    ) -> Result<BarrierGroupMember, JoinGroupError> {
        let group = self.join_group(channel_id, BarrierGroupMemberState::default())?;
        Ok(BarrierGroupMember { group })
    }

    pub fn join_broadcast_group(
        &self,
        channel_id: GroupId,
    ) -> Result<BroadcastGroupReceiver, JoinGroupError> {
        let group = self.join_group(channel_id, BroadcastGroupReceiverState::default())?;
        Ok(BroadcastGroupReceiver { group })
    }
}

#[test]
fn join_session() -> Result<(), Box<dyn std::error::Error>> {
    let port = crate::test::get_port();
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let multicast_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)), port);

    let _coordinator = Coordinator::start_session(bind_addr, multicast_addr)?;
    let _member = Member::join_session(bind_addr)?;

    Ok(())
}
