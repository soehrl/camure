use std::io::Write;

use subscriptions::publisher::{Publisher, PublisherConfig};

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let publisher = Publisher::new(PublisherConfig {
        addr: "0.0.0.0:12345".parse().unwrap(),
        multicast_addr: "224.0.0.0:5555".parse().unwrap(),
        chunk_size: 1024,
    });

    let mut offer = publisher.create_offer().unwrap();

    loop {
        if let Some(addr) = offer.accecpt() {
            log::info!("new client: {:?}", addr);
            offer.write_message().write_all(b"hello").unwrap();
        }
    }
}
