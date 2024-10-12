use std::net::SocketAddr;

use ahash::HashSet;
use socket2::SockAddr;

use crate::{
    chunk::Chunk,
    group::{
        GroupCoordinator, GroupCoordinatorState, GroupCoordinatorTypeImpl, GroupMember,
        GroupMemberState, GroupMemberTypeImpl,
    },
    protocol::{self, BarrierReleased, SequenceNumber},
    utils::{display_addr, sock_addr_to_socket_addr, ExponentialBackoff},
};

#[derive(Debug, Default)]
pub(crate) struct BarrierGroupCoordinatorState {
    arrived: HashSet<SockAddr>,
    ack_required: HashSet<SockAddr>,
}

impl BarrierGroupCoordinatorState {
    fn swap_arrived_and_ack_required(&mut self) {
        std::mem::swap(&mut self.arrived, &mut self.ack_required);
    }
}

impl GroupCoordinatorTypeImpl for BarrierGroupCoordinatorState {
    const GROUP_TYPE: protocol::GroupType = protocol::GROUP_TYPE_BARRIER;

    fn process_join_cancelled(&mut self, addr: &SockAddr, _: &GroupCoordinatorState) {
        self.arrived.remove(addr);
    }

    fn process_member_disconnected(&mut self, addr: &SockAddr, _: &GroupCoordinatorState) {
        self.arrived.remove(addr);
        self.ack_required.remove(addr);
    }

    fn process_chunk(&mut self, chunk: Chunk, addr: &SockAddr, group: &GroupCoordinatorState) {
        match chunk {
            Chunk::SessionJoin(_)
            | Chunk::SessionWelcome(_)
            | Chunk::SessionHeartbeat(_)
            | Chunk::GroupJoin(_)
            | Chunk::GroupWelcome(_)
            | Chunk::GroupLeave(_)
            | Chunk::GroupDisconnected(_) => {
                // These chunks cannot be forwared to this function, as they should have been
                // filtered out by either the multiplex socket or the GroupCoordinator.
                tracing::error!(
                    ?chunk,
                    from = %display_addr(addr),
                    "received invalid chunk in barrier group",
                );
                unreachable!();
            }
            Chunk::BroadcastMessage(_)
            | Chunk::BroadcastFirstMessageFragment(_)
            | Chunk::BroadcastMessageFragment(_)
            | Chunk::BroadcastFinalMessageFragment(_) => {
                // These chunks could be forwarded to this function, but they would never been
                // send by this crate.
                tracing::error!(
                    ?chunk,
                    from = %display_addr(addr),
                    "received broadcast message in barrier group",
                );
            }
            Chunk::GroupAck(ack) => {
                if ack.seq == group.seq.prev() {
                    if group.members.contains(addr) || group.member_requests.contains(addr) {
                        self.ack_required.remove(addr);

                        tracing::trace!(
                            from = %display_addr(addr),
                            seq = <_ as Into<u16>>::into(ack.seq),
                            "received ack",
                        );
                    } else {
                        tracing::warn!(
                            from = %display_addr(addr),
                            seq = <_ as Into<u16>>::into(ack.seq),
                            "received ack from non-group-member",
                        );
                    }
                } else {
                    tracing::trace!(
                        from = %display_addr(addr),
                        seq = <_ as Into<u16>>::into(ack.seq),
                        expected_seq = <_ as Into<u16>>::into(group.seq.prev()),
                        "received ack with unexpected seq",
                    );
                }
            }
            Chunk::BarrierReached(reached) => {
                if reached.seq == group.seq {
                    if group.members.contains(addr) || group.member_requests.contains(addr) {
                        self.arrived.insert(addr.clone());

                        tracing::trace!(
                            from = %display_addr(addr),
                            seq = <_ as Into<u16>>::into(reached.seq),
                            "received barrier-reached",
                        );
                    } else {
                        tracing::warn!(
                            from = %display_addr(addr),
                            seq = <_ as Into<u16>>::into(reached.seq),
                            "received barrier-reached from non-group-member",
                        );
                    }
                } else {
                    tracing::trace!(
                        from = %display_addr(addr),
                        seq = <_ as Into<u16>>::into(reached.seq),
                        expected_seq = <_ as Into<u16>>::into(group.seq),
                        "received barrier-reached with unexpected seq",
                    );
                }
            }
            Chunk::BarrierReleased(released) => {
                tracing::error!(
                    from = %display_addr(addr),
                    seq = <_ as Into<u16>>::into(released.seq),
                    "received barrier-released",
                );
            }
        }
    }

    fn process_join_request(&mut self, _addr: &SockAddr, _group: &GroupCoordinatorState) {}
}

pub struct BarrierGroupCoordinator {
    pub(crate) group: GroupCoordinator<BarrierGroupCoordinatorState>,
    // pub(crate) state: BarrierGroupCoordinatorState,
}

impl BarrierGroupCoordinator {
    // pub fn new(channel: GroupCoordinator) -> Self {
    //     Self {
    //         channel,
    //         arrived: HashSet::new(),
    //     }
    // }

    fn all_members_arrived(&self) -> bool {
        // Assert that arrived.len() == members.len() => arrived == members
        debug_assert!(
            self.group.state.members.len() != self.group.inner.arrived.len()
                || self.group.state.members == self.group.inner.arrived
        );
        self.group.state.members.len() == self.group.inner.arrived.len()
    }

    pub fn has_members(&self) -> bool {
        !self.group.state.members.is_empty()
    }

    #[tracing::instrument(skip(self))]
    pub fn accept(&mut self) -> std::io::Result<SocketAddr> {
        self.group.accept().and_then(sock_addr_to_socket_addr)
    }

    #[tracing::instrument(skip(self))]
    pub fn try_accept(&mut self) -> std::io::Result<Option<SocketAddr>> {
        let addr = self.group.try_accept()?;
        if let Some(addr) = addr {
            Ok(Some(sock_addr_to_socket_addr(addr)?))
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(skip(self))]
    fn release_barrier(&mut self) -> std::io::Result<()> {
        let release_seq = self.group.state.seq;
        self.group.state.seq = self.group.state.seq.next();

        debug_assert!(self.group.inner.ack_required.is_empty());
        self.group.inner.swap_arrived_and_ack_required();

        // Send barrier released and wait for acks
        for deadline in ExponentialBackoff::new() {
            let send_barrier_released_span = tracing::trace_span!("send barrier released");
            let _send_barrier_released_span_guard = send_barrier_released_span.enter();

            self.group.send_chunk_to_group(&BarrierReleased {
                seq: release_seq,
                group_id: self.group.channel.id(),
            })?;

            loop {
                match self.group.recv_until(deadline) {
                    Ok(_) => {
                        if self.group.inner.ack_required.is_empty() {
                            tracing::trace!("all acks received");
                            return Ok(());
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::TimedOut => break,
                    Err(err) => return Err(err),
                }
            }

            let mut dead_members = vec![];
            for addr in &self.group.inner.ack_required {
                if !self.group.session_members.is_alive(addr) {
                    dead_members.push(addr.clone());
                }
            }
            for addr in dead_members {
                tracing::debug!(addr = %display_addr(&addr), "timeout");
                self.group.remove(&addr)?;
            }

            if self.group.inner.ack_required.is_empty() {
                tracing::trace!("all acks received");
                return Ok(());
            }
        }
        unreachable!();
    }

    #[tracing::instrument(skip(self))]
    pub fn wait(&mut self) -> std::io::Result<()> {
        if self.group.state.members.is_empty() {
            tracing::trace!("no members in group");
            return Ok(());
        }

        // Wait until everyone has arrived
        tracing::trace_span!("waiting for all members to arrive").in_scope(
            || -> std::io::Result<()> {
                while !self.all_members_arrived() {
                    let now = std::time::Instant::now();
                    if let Err(err) = self
                        .group
                        .recv_until(now + std::time::Duration::from_millis(100))
                    {
                        if err.kind() == std::io::ErrorKind::TimedOut {
                            let mut dead_members = vec![];
                            for addr in &self.group.state.members {
                                if !self.group.session_members.is_alive(addr) {
                                    dead_members.push(addr.clone());
                                }
                            }
                            for addr in dead_members {
                                tracing::debug!(addr = %display_addr(&addr), "timeout");
                                self.group.remove(&addr)?;
                            }
                        } else {
                            return Err(err);
                        }
                    }
                }
                Ok(())
            },
        )?;

        self.release_barrier()
    }
}

#[derive(Debug, Default)]
pub(crate) struct BarrierGroupMemberState {
    next: protocol::SequenceNumber,
    released: protocol::SequenceNumber,
}

impl BarrierGroupMemberState {}

impl GroupMemberTypeImpl for BarrierGroupMemberState {
    const GROUP_TYPE: protocol::GroupType = protocol::GROUP_TYPE_BARRIER;

    fn process_group_join(&mut self, seq: SequenceNumber, _group: &GroupMemberState) {
        self.next = seq;
        self.released = seq.prev();
    }

    fn process_chunk(&mut self, chunk: Chunk, addr: &SockAddr, _group: &GroupMemberState) -> bool {
        match chunk {
            Chunk::SessionJoin(_)
            | Chunk::SessionWelcome(_)
            | Chunk::SessionHeartbeat(_)
            | Chunk::GroupJoin(_)
            | Chunk::GroupWelcome(_)
            | Chunk::GroupLeave(_)
            | Chunk::GroupDisconnected(_) => {
                // These chunks cannot be forwared to this function, as they should have been
                // filtered out by either the multiplex socket or the GroupCoordinator.
                tracing::error!(
                    ?chunk,
                    from = %display_addr(addr),
                    "received invalid chunk in barrier group",
                );
                unreachable!();
            }
            Chunk::BroadcastMessage(_)
            | Chunk::BroadcastFirstMessageFragment(_)
            | Chunk::BroadcastMessageFragment(_)
            | Chunk::BroadcastFinalMessageFragment(_) => {
                // These chunks could be forwarded to this function, but they would never been
                // send by this crate.
                tracing::error!(
                    ?chunk,
                    from = %display_addr(addr),
                    "received broadcast message in barrier group",
                );
            }
            Chunk::BarrierReached(reached) => {
                tracing::error!(
                    from = %display_addr(addr),
                    seq = <_ as Into<u16>>::into(reached.seq),
                    "received barrier-reached",
                );
            }
            Chunk::GroupAck(ack) => {
                if ack.seq == self.next {
                    tracing::trace!(
                        from = %display_addr(addr),
                        seq = <_ as Into<u16>>::into(ack.seq),
                        "received ack for barrier-reached",
                    );
                    self.next = self.next.next();
                } else {
                    tracing::trace!(
                        from = %display_addr(addr),
                        seq = <_ as Into<u16>>::into(ack.seq),
                        expected_seq = <_ as Into<u16>>::into(self.next),
                        "received ack for barrier-reached with unexpected seq",
                    );
                }
            }
            Chunk::BarrierReleased(released) => {
                if released.seq == self.released.next() {
                    tracing::trace!(
                        from = %display_addr(addr),
                        seq = <_ as Into<u16>>::into(released.seq),
                        "received barrier-released",
                    );
                    self.released = released.seq;
                } else {
                    tracing::trace!(
                        from = %display_addr(addr),
                        seq = <_ as Into<u16>>::into(released.seq),
                        expected_seq = <_ as Into<u16>>::into(self.released.next()),
                        "received barrier-released with unexpected seq",
                    );
                }
                if self.next == self.released {
                    tracing::trace!(
                        from = %display_addr(addr),
                        seq = <_ as Into<u16>>::into(released.seq),
                        next = <_ as Into<u16>>::into(self.next),
                        released = <_ as Into<u16>>::into(self.released),
                        "ack for barrier-reached got lost",
                    );

                    self.next = self.next.next();
                }
            }
        }

        false
    }
}

pub struct BarrierGroupMember {
    pub(crate) group: GroupMember<BarrierGroupMemberState>,
}

impl BarrierGroupMember {
    fn send_reached(&mut self) -> std::io::Result<SequenceNumber> {
        let reached = self.group.inner()?.next;

        for deadline in ExponentialBackoff::new() {
            self.group.send_chunk(&protocol::BarrierReached {
                seq: reached,
                group_id: self.group.id()?,
            })?;

            // return Ok(reached);
            loop {
                match self.group.recv_until(deadline) {
                    Ok(_) => {
                        if self.group.inner()?.next == reached.next() {
                            return Ok(reached);
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::TimedOut => break,
                    Err(err) => return Err(err),
                }
            }
        }

        unreachable!();
    }

    #[tracing::instrument(skip(self))]
    pub fn wait(&mut self) -> std::io::Result<()> {
        let reached = self.send_reached()?;

        while self.group.inner()?.released != reached {
            self.group.recv()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::session::{Coordinator, Member};
    use crate::test::*;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        thread,
    };

    #[test]
    fn test_barrier_group() -> Result<()> {
        init_logger();

        let port = crate::test::get_port();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
        let connect_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let multicast_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(234, 0, 0, 0)), port);

        let coordinator = Coordinator::start_session(bind_addr, multicast_addr)?;
        let member = Member::join_session(connect_addr)?;

        thread::scope(|s| {
            s.spawn(|| {
                let mut barrier_group_coordinator =
                    coordinator.create_barrier_group(Some(0)).unwrap();
                barrier_group_coordinator.accept().unwrap();

                for _ in 0..10 {
                    barrier_group_coordinator.wait().unwrap();
                }
            });

            s.spawn(|| {
                let mut barrier_group_member = member.join_barrier_group(0).unwrap();

                for _ in 0..10 {
                    barrier_group_member.wait().unwrap();
                }
            });
        });

        Ok(())
    }
}

// #[derive(Debug, Default)]
// struct BarrierGroupState {
//     new_clients: HashSet<SocketAddr>,
//     clients: HashSet<SocketAddr>,
//     arrived: HashSet<SocketAddr>,
//     seq: SequenceNumber,
// }

// impl BarrierGroupState {
//     /// Returns true if the client is connected to the barrier group and
// false otherwise.     fn client_reached_barrier(&mut self, client: SocketAddr,
// seq: SequenceNumber) -> bool {         if self.clients.contains(&client) {
//             if self.seq == seq {
//                 self.arrived.insert(client);
//             }
//             true
//         } else {
//             false
//         }
//     }

//     /// Returns if all remotes have arrived at the barrier.
//     fn all_remotes_arrived(&self) -> bool {
//     }

//     /// Processes a single chunk
//     fn process_chunk(&mut self, chunk: Chunk, addr: SocketAddr) -> bool {
//         match chunk {
//             Chunk::JoinBarrierGroup(_) => {
//                 self.new_clients.insert(addr);
//                 true
//             }
//             Chunk::BarrierReached(reached) => {
//                 if self.clients.contains(&addr) {
//                     self.client_reached_barrier(addr, reached.0.seq.into());
//                     true
//                 } else {
//                     log::warn!("received barrier reached from non-client");
//                     false
//                 }
//             }
//             Chunk::LeaveChannel(_) => {
//                 if self.clients.contains(&addr) {
//                     self.clients.remove(&addr);
//                     self.arrived.remove(&addr);
//                 }
//                 false
//             }
//             _ => {
//                 log::warn!("received invalid chunk: {chunk:?}");
//                 self.clients.contains(&addr)
//             }
//         }
//     }
// }

// pub struct BarrierGroup {
//     channel_id: ChannelId,
//     desc: BarrierGroupDesc,
//     state: BarrierGroupState,
//     receiver: ChunkReceiver,
//     socket: ChunkSocket,
//     multicast_addr: SocketAddr,
// }

// impl BarrierGroup {
//     fn try_process(&mut self) -> bool {
//         let mut processed = false;
//         while let Ok(chunk) = self.receiver.try_recv() {
//             if let (Ok(chunk), Some(addr)) = (chunk.validate(),
// chunk.addr().as_socket()) {                 if
// !self.state.process_chunk(chunk, addr) {                     let _ = self
//                         .socket
//
// .send_chunk_to(&ChannelDisconnected(self.channel_id.into()), &addr.into());
//                 }
//                 processed = true;
//             }
//         }
//         processed
//     }

//     fn process(&mut self) {
//         if let Ok(chunk) = self.receiver.recv() {
//             if let (Ok(chunk), Some(addr)) = (chunk.validate(),
// chunk.addr().as_socket()) {                 if
// !self.state.process_chunk(chunk, addr) {                     let _ = self
//                         .socket
//
// .send_chunk_to(&ChannelDisconnected(self.channel_id.into()), &addr.into());
//                 }
//             }
//         }
//         self.try_process();
//     }

//     pub fn accept_client(&mut self, client: SocketAddr) -> Result<(),
// TransmitAndWaitError> {         transmit_to_and_wait(
//             &self.socket,
//             &client,
//             &ConfirmJoinChannel {
//                 header: ChannelHeader {
//                     channel_id: self.channel_id.into(),
//                     seq: self.state.seq.into(),
//                 },
//             },
//             self.desc.retransmit_timeout,
//             self.desc.retransmit_count,
//             &self.receiver,
//             |chunk, addr| {
//                 if let Chunk::Ack(ack) = chunk {
//                     let ack_seq: u16 = ack.header.seq.into();
//                     if ack_seq == self.state.seq && addr == client {
//                         log::debug!("client {} joined barrier group",
// client);                         self.state.clients.insert(addr);
//                         return true;
//                     }
//                 } else {
//                     self.state.process_chunk(chunk, addr);
//                 }
//                 false
//             },
//         )
//     }

//     pub fn try_accept(&mut self) -> Result<SocketAddr, TransmitAndWaitError>
// {         self.try_process();

//         if let Some(client) = self
//             .state
//             .new_clients
//             .iter()
//             .next()
//             .copied()
//             .and_then(|q| self.state.new_clients.take(&q))
//         {
//             log::debug!("accepting client {}", client);
//             self.accept_client(client)?;
//             Ok(client)
//         } else {
//             Err(TransmitAndWaitError::RecvError(RecvTimeoutError::Timeout))
//         }
//     }

//     pub fn has_remotes(&self) -> bool {
//         !self.state.clients.is_empty()
//     }

//     pub fn try_wait(&mut self) -> bool {
//         self.try_process();
//         if self.state.all_remotes_arrived() {
//             self.wait();
//             true
//         } else {
//             false
//         }
//     }

//     pub fn wait(&mut self) {
// }
