use std::{sync::Arc, time::Instant};

use ahash::HashSet;
use crossbeam::{channel::RecvTimeoutError, select};
use socket2::SockAddr;

use crate::{
    chunk::{Chunk, ChunkBuffer, ChunkBufferAllocator},
    chunk_socket::ReceivedChunk,
    multiplex_socket::{Channel, ProcessError},
    protocol::{
        self, ChunkHeader, GroupAck, GroupDisconnected, GroupId, GroupJoin, GroupLeave, GroupType,
        GroupWelcome,
    },
    session::MemberVitals,
    utils::{display_addr, ExponentialBackoff},
};

#[derive(Debug, Default)]
pub struct GroupCoordinatorState {
    pub members: HashSet<SockAddr>,
    pub member_requests: HashSet<SockAddr>,
    pub seq: protocol::SequenceNumber,
}

impl GroupCoordinatorState {
    fn get_request(&mut self) -> Option<&SockAddr> {
        self.member_requests.iter().next()
    }
}

pub trait GroupCoordinatorTypeImpl {
    const GROUP_TYPE: GroupType;

    #[allow(unused_variables)]
    fn process_join_request(&mut self, addr: &SockAddr, group: &GroupCoordinatorState) {}

    #[allow(unused_variables)]
    fn process_join_cancelled(&mut self, addr: &SockAddr, group: &GroupCoordinatorState) {}

    #[allow(unused_variables)]
    fn process_member_joined(&mut self, addr: &SockAddr, group: &GroupCoordinatorState) {}

    #[allow(unused_variables)]
    fn process_member_disconnected(&mut self, addr: &SockAddr, group: &GroupCoordinatorState) {}

    #[allow(unused_variables)]
    fn process_chunk(&mut self, chunk: Chunk, addr: &SockAddr, group: &GroupCoordinatorState) {}
}

pub struct GroupCoordinator<I: GroupCoordinatorTypeImpl> {
    pub channel: Channel,
    pub multicast_addr: SockAddr,
    pub session_members: Arc<MemberVitals>,
    pub state: GroupCoordinatorState,
    pub inner: I,
}

impl<I: GroupCoordinatorTypeImpl> Drop for GroupCoordinator<I> {
    fn drop(&mut self) {
        for addr in &self.state.members {
            let _ = self
                .channel
                .send_chunk_to(&GroupDisconnected(self.channel.id().into()), addr);
        }
    }
}

impl<I: GroupCoordinatorTypeImpl> GroupCoordinator<I> {
    // This function was not very useful as all member removal events need slightly
    // different handling of the channel disconnect message
    /*
    /// Removes a member from the channel.
    ///
    /// If it is present in the channel, the member is removed and a `MemberDisconnected` event is
    /// emitted. If `disconnect_message` is provided, a `ChannelDisconnected` message is sent to
    /// the member. If the member is not present in the channel, this is a no-op.
    fn remove_member<F: CoordinatorChannelEventHandler>(
        &mut self,
        addr: &SockAddr,
        disconnect_message: Option<&[u8]>,
        event_handler: F,
    ) {
        if self.state.members.remove(addr) {
            event_handler(CoordinatorChannelEvent::MemberDisconnected(addr));
            if let Some(message) = disconnect_message {
                let _ = self.channel.send_chunk_with_payload_to(
                    &ChannelDisconnected(self.channel.id().into()),
                    message,
                    addr,
                );
            }
        }
    }
    */

    pub fn id(&self) -> GroupId {
        self.channel.id()
    }

    pub fn buffer_allocator(&self) -> &Arc<ChunkBufferAllocator> {
        self.channel.buffer_allocator()
    }

    pub fn accept(&mut self) -> Result<SockAddr, std::io::Error> {
        let addr = loop {
            if let Some(addr) = self.state.get_request() {
                break addr.clone();
            } else {
                self.recv()?;
            }
        };

        log::trace!(
            "accept group join request for group {} from {}",
            self.channel.id(),
            display_addr(&addr),
        );

        'outer: for deadline in ExponentialBackoff::new() {
            log::trace!(
                "send group welcome for group {} to {}",
                self.channel.id(),
                display_addr(&addr),
            );
            self.channel.send_chunk_to(
                &GroupWelcome {
                    group_id: self.channel.id(),
                    seq: self.state.seq,
                },
                &addr,
            )?;

            while !self.state.members.contains(&addr) {
                if !self.session_members.is_alive(&addr) {
                    log::trace!(
                        "member {} is not alive anymore, ignore join request",
                        display_addr(&addr),
                    );
                    self.state.member_requests.remove(&addr);
                    self.inner.process_join_cancelled(&addr, &self.state);
                    return Err(std::io::ErrorKind::ConnectionAborted.into());
                }

                match self.recv_until(deadline) {
                    Ok(_) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::TimedOut => continue 'outer,
                    Err(err) => {
                        self.state.member_requests.remove(&addr);
                        self.inner.process_join_cancelled(&addr, &self.state);
                        return Err(err);
                    }
                }
            }

            log::trace!(
                "join group({}) for {} was successful",
                self.channel.id(),
                display_addr(&addr),
            );

            debug_assert!(self.state.members.contains(&addr));
            self.state.member_requests.remove(&addr);
            self.inner.process_member_joined(&addr, &self.state);
            return Ok(addr);
        }

        unreachable!();
    }

    pub fn try_accept(&mut self) -> Result<Option<SockAddr>, std::io::Error> {
        self.try_recv()?;
        if self.state.member_requests.is_empty() {
            Ok(None)
        } else {
            self.accept().map(Some)
        }
    }

    pub fn remove(&mut self, addr: &SockAddr) -> Result<(), std::io::Error> {
        if let Some(addr) = self.state.members.take(addr) {
            self.inner.process_member_disconnected(&addr, &self.state);
            self.channel.send_chunk_with_payload_to(
                &GroupDisconnected(self.channel.id()),
                b"timeout",
                &addr,
            )?;
        }
        Ok(())
    }

    fn process_join(&mut self, join: &GroupJoin, addr: &SockAddr) {
        debug_assert_eq!(
            self.channel.id(),
            join.group_id,
            "this is enforced by the socket"
        );

        log::trace!(
            "received group join for group {} from {}",
            self.channel.id(),
            display_addr(addr),
        );

        if I::GROUP_TYPE != join.group_type {
            // TODO: what to do in case the send fails?
            let _ = self.channel.send_chunk_with_payload_to(
                &GroupDisconnected(self.channel.id().into()),
                b"invalid channel type",
                addr,
            );
            return;
        }

        if self.state.members.contains(addr) {
            // We received a join request from a member that already joined the channel.
            // This can happen in two cases:
            // 1. The member ran into a timeout after requesting to join and resend the join
            //    request, but ended up receiving both. In this case we can ignore this.
            // 2. The member disconnected and we were not able to receive the leave message
            //    for some reason. In this case we should first handle a member leave event
            //    and then treating this as a new join.
            // Currently, the two cases cannot be distinguished and just disconnect the
            // member for simplicity.
            self.state.members.remove(addr);
            self.inner.process_member_disconnected(addr, &self.state);
            let _ = self.channel.send_chunk_with_payload_to(
                &GroupDisconnected(self.channel.id().into()),
                b"duplicate join",
                addr,
            );
            return;
        }

        self.state.member_requests.insert(addr.clone());
        self.inner.process_join_request(addr, &self.state);
    }

    fn process_ack(&mut self, ack: &GroupAck, addr: &SockAddr) {
        if self.state.member_requests.contains(addr) {
            if ack.seq == self.state.seq {
                log::trace!(
                    "received ack for group welcome {} from {}",
                    self.channel.id(),
                    display_addr(addr),
                );
                self.state.members.insert(addr.clone());
            } else {
                log::trace!(
                    "INGORED: received ack for group welcome {} from {} with invalid seq {} (expected {})",
                    self.channel.id(),
                    display_addr(addr),
                    ack.seq,
                    self.state.seq,
                );
            }
        } else {
            self.inner
                .process_chunk(Chunk::GroupAck(ack), addr, &self.state);
        }
    }

    fn process_leave(&mut self, GroupLeave(channel_id): &GroupLeave, addr: &SockAddr) {
        debug_assert_eq!(
            self.channel.id(),
            *channel_id,
            "this is enforced by the socket"
        );

        if let Some(addr) = self.state.members.take(addr) {
            self.inner.process_member_disconnected(&addr, &self.state);
        }
        let _ = self.channel.send_chunk_with_payload_to(
            &GroupDisconnected(self.channel.id().into()),
            b"leave",
            addr,
        );
    }

    fn process_disconnected(
        &mut self,
        GroupDisconnected(channel_id): &GroupDisconnected,
        addr: &SockAddr,
    ) {
        debug_assert_eq!(
            self.channel.id(),
            *channel_id,
            "this is enforced by the socket"
        );

        if let Some(addr) = self.state.members.take(addr) {
            self.inner.process_member_disconnected(&addr, &self.state);
        }
    }

    /// Process all CHUNK_ID_CHANNEL_* chunks.
    #[inline]
    fn process_chunk(&mut self, chunk: Chunk, addr: &SockAddr) {
        match chunk {
            // Session chunks should not be forwarded to the channel.
            Chunk::SessionJoin(_) => unreachable!("received session join in channel"),
            Chunk::SessionWelcome(_) => unreachable!("received session welcome in channel"),
            Chunk::SessionHeartbeat(_) => unreachable!("received session heartbeat in channel"),
            Chunk::GroupJoin(join) => self.process_join(join, &addr),
            Chunk::GroupWelcome(welcome) => log::trace!(
                "coordinator received channel welcome from {:?}: {:?}",
                addr,
                welcome
            ),
            Chunk::GroupAck(ack) => self.process_ack(ack, &addr),
            Chunk::GroupLeave(leave) => self.process_leave(leave, &addr),
            Chunk::GroupDisconnected(disconnected) => self.process_disconnected(disconnected, addr),
            Chunk::BarrierReached(_)
            | Chunk::BarrierReleased(_)
            | Chunk::BroadcastMessage(_)
            | Chunk::BroadcastFirstMessageFragment(_)
            | Chunk::BroadcastMessageFragment(_)
            | Chunk::BroadcastFinalMessageFragment(_) => {
                self.inner.process_chunk(chunk, addr, &self.state);
            }
        }
    }

    #[inline]
    pub fn process_received_chunk(&mut self, chunk: ReceivedChunk) {
        match chunk.validate() {
            Ok(c) => {
                self.process_chunk(c, chunk.addr());
            }
            Err(e) => {
                // Those should be filtered out by the socket.
                unreachable!("received invalid chunk: {:?}", e);
            }
        }
    }

    /// Waits for a chunk and processes it.
    #[inline]
    pub fn recv(&mut self) -> Result<(), std::io::Error> {
        let chunk = self.channel.recv()?;
        self.process_received_chunk(chunk);
        Ok(())
    }

    ///
    #[inline]
    pub fn try_recv(&mut self) -> Result<(), std::io::Error> {
        if let Some(chunk) = self.channel.try_recv()? {
            self.process_received_chunk(chunk);
        }
        Ok(())
    }

    /// Waits for a chunk until the deadline and processes it if one is
    /// received.
    #[inline]
    pub fn recv_until(&mut self, deadline: std::time::Instant) -> Result<(), std::io::Error> {
        let chunk = self.channel.recv_until(deadline)?;
        self.process_received_chunk(chunk);
        Ok(())
    }

    #[inline]
    pub fn send_chunk_buffer_to_group(
        &mut self,
        buffer: &ChunkBuffer,
        packet_size: usize,
    ) -> Result<(), std::io::Error> {
        log::trace!(
            "sending chunk buffer to {}",
            display_addr(&self.multicast_addr)
        );
        self.channel
            .send_chunk_buffer_to(buffer, packet_size, &self.multicast_addr)
    }

    #[inline]
    pub fn send_chunk_to_group<H: protocol::ChunkHeader>(
        &mut self,
        header: &H,
    ) -> Result<(), std::io::Error> {
        self.channel.send_chunk_to(header, &self.multicast_addr)
    }

    #[inline]
    pub fn send_chunk_with_payload_to_group<H: protocol::ChunkHeader>(
        &mut self,
        header: &H,
        payload: &[u8],
    ) -> Result<(), std::io::Error> {
        self.channel
            .send_chunk_with_payload_to(header, payload, &self.multicast_addr)
    }
}

#[derive(Debug, Default)]
pub struct GroupMemberState {
    // pub seq: protocol::SequenceNumber,
}

pub trait GroupMemberTypeImpl {
    const GROUP_TYPE: GroupType;

    #[allow(unused_variables)]
    fn process_group_join(&mut self, seq: protocol::SequenceNumber, group: &GroupMemberState) {}

    #[allow(unused_variables)]
    fn process_disconnected(&mut self, group: &GroupMemberState) {}

    #[allow(unused_variables)]
    fn process_chunk(&mut self, chunk: Chunk, addr: &SockAddr, group: &GroupMemberState) -> bool {
        false
    }

    #[allow(unused_variables)]
    fn take_chunk(&mut self, chunk: ReceivedChunk, group: &GroupMemberState) {}
}

pub struct ConnectedGroupMember<I: GroupMemberTypeImpl> {
    group_id: protocol::GroupId,
    coordinator_channel: Channel,
    multicast_channel: Channel,
    inner: I,
    state: GroupMemberState,
    coordinator_addr: SockAddr,
}

impl<I: GroupMemberTypeImpl> Drop for ConnectedGroupMember<I> {
    fn drop(&mut self) {
        let _ = self.leave();
    }
}

impl<I: GroupMemberTypeImpl> ConnectedGroupMember<I> {
    // Attempts to leave the channel.
    //
    // It will send a LeaveChannel request and waits for a response. However, it may
    // time out while waiting for the response.
    fn leave(&mut self) -> Result<(), std::io::Error> {
        for _ in 0..3 {
            self.coordinator_channel.send_chunk_to(
                &GroupLeave(self.coordinator_channel.id()),
                &self.coordinator_addr,
            )?;

            match self.coordinator_channel.process_for(
                std::time::Duration::from_secs(5),
                |chunk, _| match chunk {
                    Chunk::GroupDisconnected(_) => Ok(Some(())),
                    _ => Ok(None),
                },
            ) {
                Ok(_) => return Ok(()),
                Err(ProcessError::RecvError(RecvTimeoutError::Timeout)) => {
                    // The channel did not respond in time.
                    continue;
                }
                Err(ProcessError::RecvError(RecvTimeoutError::Disconnected)) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "channel disconnected",
                    ));
                }
                Err(ProcessError::Callback(())) => {
                    // We do not return an error
                    unreachable!();
                }
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "connection timed out",
        ))
    }

    fn process_disconnected(
        &mut self,
        GroupDisconnected(group_id): &GroupDisconnected,
        _: &SockAddr,
    ) -> std::io::Result<()> {
        debug_assert_eq!(self.group_id, *group_id, "this is enforced by the socket");
        Err(std::io::ErrorKind::ConnectionAborted.into())
    }

    /// Process all CHUNK_ID_CHANNEL_* chunks.
    #[inline]
    fn process_chunk(&mut self, chunk: Chunk, addr: &SockAddr) -> std::io::Result<bool> {
        match chunk {
            // Session chunks should not be forwarded to the channel.
            Chunk::SessionJoin(_) => unreachable!("received session join in channel"),
            Chunk::SessionWelcome(_) => unreachable!("received session welcome in channel"),
            Chunk::SessionHeartbeat(_) => unreachable!("received session heartbeat in channel"),
            Chunk::GroupJoin(_) | Chunk::GroupWelcome(_) | Chunk::GroupLeave(_) => {
                log::trace!(
                    "INGORED: received unexpected chunk from group {}: {:?}",
                    display_addr(addr),
                    chunk
                );
                Ok(false)
            }
            Chunk::GroupDisconnected(disconnected) => {
                self.process_disconnected(disconnected, addr).map(|_| false)
            }
            Chunk::GroupAck(_)
            | Chunk::BroadcastMessage(_)
            | Chunk::BroadcastFirstMessageFragment(_)
            | Chunk::BroadcastMessageFragment(_)
            | Chunk::BroadcastFinalMessageFragment(_)
            | Chunk::BarrierReached(_)
            | Chunk::BarrierReleased(_) => Ok(self.inner.process_chunk(chunk, addr, &self.state)),
        }
    }

    #[inline]
    pub fn process_received_chunk(&mut self, chunk: ReceivedChunk) -> std::io::Result<()> {
        match chunk.validate() {
            Ok(c) => {
                if self.process_chunk(c, chunk.addr())? {
                    self.inner.take_chunk(chunk, &self.state);
                }
                Ok(())
            }
            Err(e) => {
                // Those should be filtered out by the socket.
                unreachable!("received invalid chunk: {:?}", e);
            }
        }
    }

    /// Waits for a chunk and processes it.
    #[inline]
    pub fn recv(&mut self) -> Result<(), std::io::Error> {
        select! {
            recv(self.coordinator_channel.receiver()) -> chunk => {
                let chunk = chunk.map_err(|_| std::io::Error::from(std::io::ErrorKind::ConnectionAborted))?;
                self.process_received_chunk(chunk)
            }
            recv(self.multicast_channel.receiver()) -> chunk => {
                let chunk = chunk.map_err(|_| std::io::Error::from(std::io::ErrorKind::ConnectionAborted))?;
                self.process_received_chunk(chunk)
            }
        }
    }

    /// Waits for a chunk and processes it.
    #[inline]
    pub fn try_recv(&mut self) -> Result<(), std::io::Error> {
        while let Some(chunk) = self.coordinator_channel.try_recv()? {
            self.process_received_chunk(chunk)?;
        }

        while let Some(chunk) = self.multicast_channel.try_recv()? {
            self.process_received_chunk(chunk)?;
        }

        Ok(())
    }

    /// Waits for a chunk until the deadline and processes it if one is
    /// received.
    #[inline]
    pub fn recv_until(&mut self, deadline: std::time::Instant) -> Result<(), std::io::Error> {
        let timeout = deadline - std::time::Instant::now();
        select! {
            recv(self.coordinator_channel.receiver()) -> chunk => {
                let chunk = chunk.map_err(|_| std::io::Error::from(std::io::ErrorKind::ConnectionAborted))?;
                self.process_received_chunk(chunk)
            }
            recv(self.multicast_channel.receiver()) -> chunk => {
                let chunk = chunk.map_err(|_| std::io::Error::from(std::io::ErrorKind::ConnectionAborted))?;
                self.process_received_chunk(chunk)
            }
            default(timeout) => {
                Err(std::io::Error::from(std::io::ErrorKind::TimedOut))
            }
        }
    }
}

pub enum GroupMember<I: GroupMemberTypeImpl> {
    Connected(ConnectedGroupMember<I>),
    Disconnected,
}

impl<I: GroupMemberTypeImpl> GroupMember<I> {
    pub fn join(
        coordinator_addr: SockAddr,
        coordinator_channel: Channel,
        multicast_channel: Channel,
        inner: I,
    ) -> Result<Self, std::io::Error> {
        let group_id = coordinator_channel.id();

        for deadline in ExponentialBackoff::new() {
            log::trace!(
                "send join request with type {} to group {}",
                I::GROUP_TYPE,
                group_id,
            );
            coordinator_channel.send_chunk_to(
                &GroupJoin {
                    group_id,
                    group_type: I::GROUP_TYPE,
                },
                &coordinator_addr,
            )?;

            loop {
                match coordinator_channel.recv_until(deadline) {
                    Ok(chunk) => match chunk.validate() {
                        Ok(Chunk::GroupWelcome(welcome)) => {
                            log::trace!("received welcome from group {}: {:?}", group_id, welcome);
                            let state = GroupMemberState {};
                            let mut inner = inner;
                            inner.process_group_join(welcome.seq, &state);
                            return Ok(Self::Connected(ConnectedGroupMember {
                                group_id: group_id,
                                coordinator_channel,
                                multicast_channel,
                                inner: inner,
                                state,
                                coordinator_addr,
                            }));
                        }
                        Ok(Chunk::GroupDisconnected(_)) => {
                            log::trace!("received disconnect from group {}", group_id,);
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::ConnectionAborted,
                                "channel disconnected",
                            ));
                        }
                        Ok(
                            Chunk::SessionJoin(_)
                            | Chunk::SessionWelcome(_)
                            | Chunk::SessionHeartbeat(_)
                            | Chunk::GroupJoin(_)
                            | Chunk::GroupAck(_)
                            | Chunk::GroupLeave(_)
                            | Chunk::BroadcastMessage(_)
                            | Chunk::BroadcastFirstMessageFragment(_)
                            | Chunk::BroadcastMessageFragment(_)
                            | Chunk::BroadcastFinalMessageFragment(_)
                            | Chunk::BarrierReached(_)
                            | Chunk::BarrierReleased(_),
                        ) => {
                            log::trace!(
                                "IGNORED: received unexpected chunk from group {}: {:?}",
                                group_id,
                                chunk
                            );
                        }
                        Err(err) => {
                            log::trace!(
                                "IGNORED: received invalid chunk as join resquest response: {:?}",
                                err
                            );
                        }
                    },
                    Err(err) if err.kind() == std::io::ErrorKind::TimedOut => break,
                    Err(err) => return Err(err),
                }
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "connection timed out",
        ))
    }

    #[inline]
    pub fn id(&self) -> std::io::Result<GroupId> {
        match self {
            Self::Connected(inner) => Ok(inner.group_id),
            Self::Disconnected => Err(std::io::ErrorKind::NotConnected.into()),
        }
    }

    #[inline]
    pub fn inner(&self) -> std::io::Result<&I> {
        match self {
            Self::Connected(inner) => Ok(&inner.inner),
            Self::Disconnected => Err(std::io::ErrorKind::NotConnected.into()),
        }
    }

    #[inline]
    pub fn inner_mut(&mut self) -> std::io::Result<&mut I> {
        match self {
            Self::Connected(inner) => Ok(&mut inner.inner),
            Self::Disconnected => Err(std::io::ErrorKind::NotConnected.into()),
        }
    }

    #[inline]
    pub fn recv(&mut self) -> std::io::Result<()> {
        match self {
            Self::Connected(inner) => inner.recv(),
            Self::Disconnected => Err(std::io::ErrorKind::NotConnected.into()),
        }
    }

    #[inline]
    pub fn recv_until(&mut self, dealine: Instant) -> std::io::Result<()> {
        match self {
            Self::Connected(inner) => inner.recv_until(dealine),
            Self::Disconnected => Err(std::io::ErrorKind::NotConnected.into()),
        }
    }

    #[inline]
    pub fn send_chunk<H: ChunkHeader>(&self, header: &H) -> Result<(), std::io::Error> {
        match self {
            Self::Connected(inner) => inner
                .coordinator_channel
                .send_chunk_to(header, &inner.coordinator_addr),
            Self::Disconnected => Err(std::io::ErrorKind::NotConnected.into()),
        }
    }
}

// #[cfg(test)]
// mod test {
//     use crate::session::{Coordinator, Member};
//     use crate::test::*;
//     use std::{
//         net::{IpAddr, Ipv4Addr, SocketAddr},
//         thread,
//     };

//     #[test]
//     fn immediate_session_close_after_group_join() -> Result<()> {
//         init_logger();

//         let port = crate::test::get_port();
//         let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0,
// 1)), port);         let multicast_addr =
// SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)), port);

//         let coordinator = Coordinator::start_session(bind_addr,
// multicast_addr)?;         let member = Member::join_session(bind_addr)?;

//         thread::scope(|s| {
//             s.spawn(|| {
//                 let mut barrier_group_coordinator =
// coordinator.create_group().
// barrier_group_coordinator.accept().unwrap();

//                 for _ in 0..10 {
//                     barrier_group_coordinator.wait().unwrap();
//                 }
//             });

//             s.spawn(|| {
//                 let mut barrier_group_member =
// member.join_barrier_group(0).unwrap();

//                 for _ in 0..10 {
//                     barrier_group_member.wait().unwrap();
//                 }
//             });
//         });

//         Ok(())
//     }
// }
