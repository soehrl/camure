use std::sync::atomic::AtomicU16;

static NEXT_PORT: AtomicU16 = AtomicU16::new(55555);
pub fn get_port() -> u16 {
    let port = NEXT_PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if port == 0 {
        panic!("No more ports available");
    }
    port
}

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub fn init_logger() {
    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::Registry::default().with(tracing_subscriber::fmt::layer());
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
