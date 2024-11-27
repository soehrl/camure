# Camure
High-performance 1-to-many communication and synchronization primitives using UDP multicast.

This crates provides a set of communication and synchronization primitives similar to the collective communication routines found in MPI.
In contrast to MPI, this crates allows for more flexible communication patterns without sacrificing performance and is designed to be used in mid-sized distributed systems.
The underlying protocol is session based which allows nodes to join and leave at any time.
One node explicitly takes over the role of the session coordinator and is responsible for creating the session.
All other nodes must join the session as a member.
