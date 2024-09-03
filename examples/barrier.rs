use multicast::{
    publisher::{Publisher, PublisherConfig},
    subscriber::Subscriber,
};
use rand::distributions::Distribution;

fn client(name: &String) {
    let mut s = Subscriber::connect("127.0.0.1:12345".parse().unwrap()).unwrap();

    let mut barrier = s
        .join_barrier_group(0)
        .unwrap();
    log::info!("[{name}] joined barrier group");

    let sleep_distribution = rand::distributions::Uniform::new(
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(2),
    );
    let mut rng = rand::thread_rng();

    loop {
        log::info!("[{name}] before");
        barrier.wait().unwrap();
        log::info!("[{name}] after");

        std::thread::sleep(sleep_distribution.sample(&mut rng));
    }
}

fn server() {
    let mut p = Publisher::new(PublisherConfig {
        addr: "0.0.0.0:12345".parse().unwrap(),
        multicast_addr: "224.0.0.0:5555".parse().unwrap(),
        chunk_size: 1024,
    });

    let mut barrier = p
        .create_barrier_group(multicast::publisher::BarrierGroupDesc {
            timeout: std::time::Duration::from_secs(1),
            retries: 5,
        })
        .unwrap();
    log::info!("[server] barrier group created");

    let sleep_distribution = rand::distributions::Uniform::new(
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(2),
    );
    let mut rng = rand::thread_rng();

    loop {
        std::thread::sleep(sleep_distribution.sample(&mut rng));

        while let Some(client) = barrier.try_accept() {
            log::info!("new client: {client}");
        }

        log::info!("[server] before");
        barrier.wait();
        log::info!("[server] after");
    }
}

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let args = std::env::args().collect::<Vec<_>>();

    if args.len() > 1 {
        client(&args[1]);
    } else {
        let executable = std::env::current_exe().unwrap_or(format!("./{}", args[0]).into());

        for client in ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"] {
            std::process::Command::new(&executable)
                .arg(client)
                .spawn()
                .unwrap();
        }
        server();
    }
}

