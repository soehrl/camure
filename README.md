# Camure
High-performance 1-to-many communication and synchronization primitives using UDP multicast.

This crates provides a set of communication and synchronization primitives similar to the collective communication routines found in MPI.
In contrast to MPI, this crates allows for more flexible communication patterns without sacrificing performance and is designed to be used in mid-sized distributed systems.
The underlying protocol is session based which allows nodes to join and leave at any time.
One node explicitly takes over the role of the session coordinator and is responsible for creating the session.
All other nodes must join the session as a member.

## Getting Started
If you use Rust you can add camure as dependency using:
```sh
cargo add camure
```
If you want to use the library from languages other than Rust, please take a look at [camure-ffi](https://github.com/soehrl/camure-ffi).

Below are a few examples that will get you started quickly.
The full documentation can be found [here](https://docs.rs/camure/).

### Barrier Groups
#### Coordinator
```rust
use camure::session::Coordinator;

let bind_addr = "192.168.0.100:12345".parse()?;
let multicast_addr = "234.0.0.0:55555".parse()?;
let coordinator = Coordinator::start_session(bind_addr, multicast_addr)?;
 
let mut barrier_group_coordinator = coordinator.create_barrier_group(Some(0))?;
barrier_group_coordinator.accept()?;
 
for _ in 0..1000 {
    barrier_group_coordinator.wait()?;
}
```
#### Member
```rust
use camure::session::Member;

let coordinator_addr = "192.168.0.100:12345".parse()?;
let member = Member::join_session(coordinator_addr)?;

let mut barrier_group_member = member.join_barrier_group(0)?;
for _ in 0..1000 {
    barrier_group_member.wait()?;
}
```

### Broadcast Groups
#### Coordinator
```rust
use camure::session::Coordinator;
use std::io::Write;

let bind_addr = "192.168.0.100:12345".parse()?;
let multicast_addr = "234.0.0.0:55555".parse()?;
let coordinator = Coordinator::start_session(bind_addr, multicast_addr)?;
 
let mut sender = coordinator.create_broadcast_group(Some(0))?;
sender.accept().unwrap();

for _ in 0..1000 {
    sender.write_message().write_all(b"SOME DATA")?;
}
sender.wait()?;
```
#### Member
```rust
use camure::session::Member;
use std::io::Read;

let coordinator_addr = "192.168.0.100:12345".parse()?;
let member = Member::join_session(coordinator_addr)?;

let mut receiver = member.join_broadcast_group(0).unwrap();

for _ in 0..1000 {
    let mut buf = String::new();
    let message = receiver.recv()?;
    let mut message_reader = message.read();
    message_reader.read_to_string(&mut buf)?;
    println!("{}", buf);
}
```
