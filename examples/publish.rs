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
    log::info!("publisher created");

    let mut offer = publisher.create_offer().unwrap();
    log::info!("created offer with id {}", offer.id());

    loop {
        if let Some(addr) = offer.accept() {
            log::info!("{:?} subscribed to offer {}", addr, offer.id());
            offer.write_message().write_all(b"welcome").unwrap();
        }

        if offer.has_subscribers() {
            offer.write_message().write_all(b"nice that you are still here").unwrap();
        }
    }
}
