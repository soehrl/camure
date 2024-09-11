//! High-performance 1-to-many communication and synchronization primitives
//! using UDP multicast.
//!
//! This crates provides a set of communication and synchronization primitives
//! similar to the collective communication routines found in MPI. In contrast
//! to MPI, this crates allows for more flexible communication patterns without
//! sacrificing performance and is designed to be used in mid-sized distributed
//! systems. The underlying protocol is session based which allows nodes to join
//! and leave at any time. One node explicitly takes over the role of the
//! session [coordinator](session::Coordinator) and is responsible for creating
//! the session. All other nodes must join the session as a
//! [member](session::Member). For more information on how to establish and join
//! sessions, see the [session] module.
//!
//! # Groups
//! Each session is divided into multiple groups that operate independently of
//! each other. Groups are always created by the coordinator and must be
//! explicitly joined by members. There are currently two different types of
//! groups.
//!
//! ## Barrier
//!
//! ## Broadcast
//!
//! # Debugging
//! If you encounter a lot of timeouts, hangs, or other issues, you can enable
//! logging to get more information about what is happening. This crate uses the
//! [log](https://crates.io/crates/log) crate for logging. So, choose a
//! logging implementation according to their
//! [documentation](https://docs.rs/log/latest/log/) and make sure to set the
//! log level to `trace` for this crate. In order to avoid any overhead caused
//! by logging ensure that the compile time filter is set to debug or higher
//! (see [Compile Time Filters](https://docs.rs/log/latest/log/#compile-time-filters) for more
//! information).
//!
//! # Future Work
//! - **Improve Member Management**: Currrently, the chunk allocation strategy
//!   is suboptimal and can be improved to increase performance.

pub(crate) mod chunk;
pub(crate) mod chunk_socket;
pub(crate) mod group;
pub(crate) mod multiplex_socket;
pub(crate) mod protocol;
#[cfg(test)]
pub(crate) mod test;
pub(crate) mod utils;

pub mod barrier;
pub mod broadcast;
pub mod session;
