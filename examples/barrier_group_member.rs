use std::{net::SocketAddr, time::Instant};

use camure::session::{GroupId, Member};

fn main() {
    use tracing_subscriber::layer::SubscriberExt;
    let subscriber =
        tracing_subscriber::Registry::default().with(tracing_tape::TapeRecorder::default());
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let mut args = std::env::args();
    let _ = args.next().unwrap();
    let connect_addr: SocketAddr = args.next().unwrap().parse().unwrap();
    let group_id: GroupId = args.next().map(|s| s.parse().unwrap()).unwrap_or(0);

    let member = Member::join_session(connect_addr).unwrap();

    let mut barrier_group_member = member.join_barrier_group(group_id).unwrap();

    for _ in 0..1000 {
        barrier_group_member.wait().unwrap();
    }

    let before = Instant::now();
    for _ in 0..10000 {
        barrier_group_member.wait().unwrap();
    }
    let after = Instant::now();
    println!("1000 barriers took {:?}", after - before);
}
