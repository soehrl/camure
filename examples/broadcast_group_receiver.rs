use std::{
    io::Read,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Instant,
};

use multicast::session::{GroupId, Member};

fn main() {
    env_logger::init();

    let mut args = std::env::args();
    let _ = args.next().unwrap();
    let connect_addr: SocketAddr = args.next().unwrap().parse().unwrap();
    let group_id: GroupId = args.next().map(|s| s.parse().unwrap()).unwrap_or(0);

    let member = Member::join_session(connect_addr).unwrap();

    let mut receiver = member.join_broadcast_group(group_id).unwrap();

    for _ in 0..1000 {
        // let mut buf = String::new();
        receiver.recv().unwrap().read();
    }

    let before = Instant::now();
    for _ in 0..1000 {
        // let mut buf = String::new();
        receiver.recv().unwrap().read();
    }
    let after = Instant::now();
    println!("received in {:?}", after - before);
}
