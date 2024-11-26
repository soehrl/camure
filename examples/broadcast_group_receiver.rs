use std::{
    io::Read,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Instant,
};

use multicast::session::{GroupId, Member};

fn main() {
    use tracing_subscriber::layer::SubscriberExt;
    let subscriber =
        tracing_subscriber::Registry::default().with(tracing_subscriber::fmt::Layer::default());
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

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
