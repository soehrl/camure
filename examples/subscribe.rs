use std::io::Read;

use subscriptions::subscriber::Subscriber;

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let mut subscriber = Subscriber::connect("127.0.0.1:12345".parse().unwrap()).unwrap();
    log::info!("subscriber connected");

    let mut subscription = subscriber.subscribe(0).unwrap();
    log::info!("subscribed to channel 0");

    while let Ok(msg) = subscription.recv() {
        let mut msg = msg.read();
        let mut s = String::new();
        msg.read_to_string(&mut s).unwrap();
        log::info!("received: {}", s);
    }
}
