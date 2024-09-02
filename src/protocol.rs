use zerocopy::{byteorder::network_endian::*, AsBytes, FromBytes, FromZeroes, Unaligned};
pub type SequenceNumber = U16;

pub type ChannelId = U16;

pub type ChunkKind = u8;

pub mod kind {
    use super::ChunkKind;

    pub const CONNECT: ChunkKind = 0;
    pub const CONNECTION_INFO: ChunkKind = 1;
    pub const JOIN_CHANNEL: ChunkKind = 2;
    pub const CONFIRM_JOIN_CHANNEL: ChunkKind = 3;
    pub const ACK: ChunkKind = 4;
    pub const MESSAGE: ChunkKind = 5;
}

pub const MESSAGE_PAYLOAD_OFFSET: usize = 1 + std::mem::size_of::<Message>();

pub trait ChunkKindData: AsBytes + FromBytes + FromZeroes + Unaligned {
    fn kind() -> ChunkKind;
}

macro_rules! impl_chunk_data {
    ($kind:ident) => {
        paste::paste! {
            impl ChunkKindData for $kind {
                fn kind() -> ChunkKind {
                    kind::[< $kind:snake:upper >]
                }
            }
        }
    };
}

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct Connect {}
impl_chunk_data!(Connect);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct ConnectionInfo {
    pub multicast_addr: [u8; 4],
    pub multicast_port: U16,
    pub chunk_size: U16,
}
impl_chunk_data!(ConnectionInfo);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct JoinChannel {
    pub channel_id: ChannelId,
}
impl_chunk_data!(JoinChannel);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct ChannelHeader {
    pub channel_id: ChannelId,
    pub seq: SequenceNumber,
}

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct ConfirmJoinChannel {
    pub header: ChannelHeader,
}
impl_chunk_data!(ConfirmJoinChannel);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct Ack {
    pub header: ChannelHeader,
}
impl_chunk_data!(Ack);

#[derive(Debug, FromBytes, AsBytes, FromZeroes, Unaligned)]
#[repr(C)]
pub struct Message {
    pub header: ChannelHeader,
}
impl_chunk_data!(Message);
