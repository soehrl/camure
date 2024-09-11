use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Instant,
};

use multicast::session::{Coordinator, GroupId};

fn main() {
    env_logger::init();

    let mut args = std::env::args();
    let _ = args.next().unwrap();
    let bind_addr: SocketAddr = args.next().unwrap().parse().unwrap();
    let multicast_addr: SocketAddr = args.next().unwrap().parse().unwrap();
    let group_id: Option<GroupId> = args.next().map(|s| s.parse().unwrap());

    let coordinator = Coordinator::start_session(bind_addr, multicast_addr).unwrap();

    let vrm = coordinator.create_barrier_group(Some(0)).unwrap();
    let mut barrier_group_coordinator = coordinator.create_barrier_group(group_id).unwrap();
    barrier_group_coordinator.accept().unwrap();

    for _ in 0..1000 {
        barrier_group_coordinator.wait().unwrap();
    }

    let before = Instant::now();
    for _ in 0..1000 {
        barrier_group_coordinator.wait().unwrap();
    }
    let after = Instant::now();
    println!("1000 barriers took {:?}", after - before);
}
