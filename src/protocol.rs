//! This module describes the used protocol.
//!
//! These internals are not part of the semver guarantees of this crate and may
//! change at any time. All programs participating in a multicast session must
//! use the same version of this crate.
//!
//! A ['Chunk'] describes the layout of the UDP payload and always has the
//! following form:
//! ```test
//! +----+--------+---------+
//! | ID | Header | Payload |
//! +----+--------+---------+
//! ```
//! The ID is a single byte that identifies the type of the chunk. The header is
//! content and length depends on the chunk type. Some chunks types require and
//! additional payload which is placed after the header.
use std::{fmt::{self, Display}, net::{Ipv6Addr, SocketAddr}};

use zerocopy::{byteorder::network_endian::*, AsBytes, FromBytes, FromZeroes, Unaligned};

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned, Clone, Copy, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct SequenceNumber(U16);

impl From<u16> for SequenceNumber {
    fn from(val: u16) -> Self {
        Self(val.into())
    }
}

impl Into<u16> for SequenceNumber {
    fn into(self) -> u16 {
        self.0.into()
    }
}

impl std::ops::Sub for SequenceNumber {
    type Output = usize;

    fn sub(self, rhs: Self) -> Self::Output {
        let val: u16 = self.0.into();
        val.wrapping_sub(rhs.0.into()).into()
    }
}

impl Display for SequenceNumber {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <u16 as Display>::fmt(&self.0.into(), f)
    }
}

impl SequenceNumber {
    pub fn skip(self, offset: usize) -> Self {
        let val: u16 = self.0.into();
        let val = val.wrapping_add(offset as u16);
        SequenceNumber(val.into())
    }

    pub fn next(self) -> Self {
        let val: u16 = self.0.into();
        SequenceNumber(val.wrapping_add(1).into())
    }

    pub fn prev(self) -> Self {
        let val: u16 = self.0.into();
        SequenceNumber(val.wrapping_sub(1).into())
    }
}

pub type GroupId = U16;
pub type GroupType = u8;
pub const GROUP_TYPE_BROADCAST: GroupType = 0;
pub const GROUP_TYPE_BARRIER: GroupType = 1;

pub type ChunkIdentifier = u8;

// Session management
pub const CHUNK_ID_SESSION_JOIN: ChunkIdentifier = 0;
pub const CHUNK_ID_SESSION_WELCOME: ChunkIdentifier = 1;
pub const CHUNK_ID_SESSION_HEARTBEAT: ChunkIdentifier = 2;

// General channel management
pub const CHUNK_ID_GROUP_JOIN: ChunkIdentifier = 10;
pub const CHUNK_ID_GROUP_WELCOME: ChunkIdentifier = 11;
pub const CHUNK_ID_GROUP_ACK: ChunkIdentifier = 12;
pub const CHUNK_ID_GROUP_LEAVE: ChunkIdentifier = 13;
pub const CHUNK_ID_GROUP_DISCONNECTED: ChunkIdentifier = 14;

// Broadcast group
pub const CHUNK_ID_BROADCAST_MESSAGE: ChunkIdentifier = 20;
pub const CHUNK_ID_BROADCAST_FIRST_MESSAGE_FRAGMENT: ChunkIdentifier = 21;
pub const CHUNK_ID_BROADCAST_MESSAGE_FRAGMENT: ChunkIdentifier = 22;
pub const CHUNK_ID_BROADCAST_FINAL_MESSAGE_FRAGMENT: ChunkIdentifier = 23;

pub const CHUNK_ID_BARRIER_REACHED: ChunkIdentifier = 30;
pub const CHUNK_ID_BARRIER_RELEASED: ChunkIdentifier = 31;

pub const WELCOME_PACKET_SIZE: usize = std::mem::size_of::<SessionWelcome>() + 1;
pub const MESSAGE_PAYLOAD_OFFSET: usize = 1 + std::mem::size_of::<BroadcastMessage>();

pub trait ChunkHeader: AsBytes + FromBytes + FromZeroes + Unaligned {
    fn id() -> ChunkIdentifier;
}

macro_rules! impl_chunk_header {
    ($header_type:ident) => {
        paste::paste! {
            impl ChunkHeader for $header_type {
                fn id() -> ChunkIdentifier {
                    [< CHUNK_ID_ $header_type:snake:upper >]
                }
            }
        }
    };
}

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct UnvalidatedSocketAddr {
    pub addr: [u8; 16],
    pub port: U16,
    pub flow_info: U32,
    pub scope_id: U32,
}

impl From<&UnvalidatedSocketAddr> for SocketAddr {
    fn from(value: &UnvalidatedSocketAddr) -> Self {
        let addr: Ipv6Addr = value.addr.into();
        match addr.to_canonical() {
            std::net::IpAddr::V4(ip) => {
                SocketAddr::V4(std::net::SocketAddrV4::new(ip, value.port.into()))
            }
            std::net::IpAddr::V6(ip) => SocketAddr::V6(std::net::SocketAddrV6::new(
                ip,
                value.port.into(),
                value.flow_info.into(),
                value.scope_id.into(),
            )),
        }
    }
}

impl From<SocketAddr> for UnvalidatedSocketAddr {
    fn from(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(addr_v4) => Self {
                addr: addr_v4.ip().to_ipv6_mapped().octets(),
                port: addr_v4.port().into(),
                flow_info: 0.into(),
                scope_id: 0.into(),
            },
            SocketAddr::V6(addr_v6) => Self {
                addr: addr_v6.ip().octets(),
                port: addr_v6.port().into(),
                flow_info: addr_v6.flowinfo().into(),
                scope_id: addr_v6.scope_id().into(),
            },
        }
    }
}

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct SessionJoin;
impl_chunk_header!(SessionJoin);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct SessionWelcome {
    pub multicast_addr: UnvalidatedSocketAddr,
    pub chunk_size: U16,
    pub heartbeat_interval: U64,
}
impl_chunk_header!(SessionWelcome);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct SessionHeartbeat;
impl_chunk_header!(SessionHeartbeat);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct GroupJoin {
    pub group_id: GroupId,
    pub group_type: GroupType,
}
impl_chunk_header!(GroupJoin);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct GroupWelcome {
    pub group_id: GroupId,
    pub seq: SequenceNumber,
}
impl_chunk_header!(GroupWelcome);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct GroupAck {
    pub group_id: GroupId,
    pub seq: SequenceNumber,
}
impl_chunk_header!(GroupAck);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct GroupLeave(pub GroupId);
impl_chunk_header!(GroupLeave);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct GroupDisconnected(pub GroupId);
impl_chunk_header!(GroupDisconnected);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct BarrierReached {
    pub group_id: GroupId,
    pub seq: SequenceNumber,
}
impl_chunk_header!(BarrierReached);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct BarrierReleased {
    pub group_id: GroupId,
    pub seq: SequenceNumber,
}
impl_chunk_header!(BarrierReleased);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct BroadcastMessage {
    pub group_id: GroupId,
    pub seq: SequenceNumber,
}
impl_chunk_header!(BroadcastMessage);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct BroadcastFirstMessageFragment {
    pub group_id: GroupId,
    pub seq: SequenceNumber,
}
impl_chunk_header!(BroadcastFirstMessageFragment);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct BroadcastMessageFragment {
    pub group_id: GroupId,
    pub seq: SequenceNumber,
}
impl_chunk_header!(BroadcastMessageFragment);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct BroadcastFinalMessageFragment {
    pub group_id: GroupId,
    pub seq: SequenceNumber,
}
impl_chunk_header!(BroadcastFinalMessageFragment);
