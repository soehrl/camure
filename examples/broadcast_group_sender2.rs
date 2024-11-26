use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Instant,
};

use multicast::session::{Coordinator, GroupId};

const LOREM_IPSUM: &[u8; 5992] = br"
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam posuere hendrerit sem, id malesuada nisl pulvinar et. Maecenas venenatis nisl at nibh faucibus, vitae tempor magna auctor. Vestibulum interdum mi diam, vel molestie justo condimentum eu. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Nam odio sem, gravida sed dignissim a, tempor dignissim magna. Duis a lacus a magna gravida fermentum sit amet vitae justo. Pellentesque gravida lacus eget ante ultrices dapibus. Curabitur et iaculis felis. Etiam mollis diam a est fermentum, id vestibulum arcu rutrum. Aenean lobortis fermentum dolor, a consectetur dui ultrices vitae. Suspendisse id ultrices diam. Praesent eget varius lorem. Sed dignissim, libero vel rhoncus cursus, nulla massa bibendum nulla, nec fermentum nisi risus bibendum magna. Integer quis mauris in odio vulputate dictum ut id lacus.

Nam posuere quam metus, ac blandit odio feugiat eget. Etiam congue id risus eu sagittis. Etiam iaculis imperdiet odio, varius vestibulum dolor ullamcorper id. Praesent at ante vitae metus pretium bibendum quis eget orci. Duis quis velit luctus, pulvinar libero a, finibus augue. Integer elementum rhoncus urna consectetur finibus. Praesent vel urna nec mi feugiat rutrum. Vivamus at nunc metus. Morbi euismod mi condimentum ex venenatis luctus a ac velit. Donec rhoncus tortor nec augue feugiat feugiat. Donec et ultrices nibh, a molestie turpis.

Proin a feugiat nisl, eu suscipit eros. Sed id elementum dolor. Sed sagittis enim ipsum, non elementum massa lobortis in. Curabitur et viverra risus. Maecenas ultrices tristique gravida. Quisque tempus, ex id fermentum fermentum, lacus lacus varius purus, a rhoncus augue diam eget metus. Nunc non convallis neque. Fusce ac semper metus. Sed blandit diam quis est porttitor, mattis pulvinar nulla malesuada. Nullam posuere nunc tincidunt imperdiet efficitur. Nunc laoreet maximus purus in luctus. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Sed a venenatis nisi.

In dignissim vitae purus in mattis. In congue augue at elit molestie, in consectetur tellus maximus. In sodales et massa ac ultrices. Phasellus congue imperdiet arcu, vel cursus enim. Duis at justo tellus. Praesent non nisl sem. Duis sodales velit in felis faucibus, vel suscipit eros interdum. Aenean semper ante nec sapien condimentum luctus. Vivamus ac elit at quam maximus ultrices. Ut varius nisl sed ex posuere, nec tristique eros varius.

Curabitur non dapibus est, sit amet dignissim velit. Mauris augue nisi, facilisis non tincidunt in, lacinia ut dui. Fusce fermentum ultrices orci, et commodo sapien congue quis. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Pellentesque sit amet ante velit. Etiam arcu nulla, varius id leo sit amet, bibendum mollis eros. Quisque blandit mattis tellus, a egestas nulla suscipit in. Nulla fringilla interdum suscipit. Duis ut nisi id neque efficitur lacinia. Sed iaculis eu ligula vitae dictum.

Aliquam sit amet pellentesque lacus, sed mattis neque. Nulla elementum eros eget lorem efficitur scelerisque. In a nunc lectus. Vestibulum dui ante, fringilla quis ullamcorper quis, scelerisque non purus. Sed sit amet bibendum arcu, eu feugiat arcu. Nulla eu nisi a nisl lobortis porttitor nec sit amet risus. Nam et euismod nunc. Nullam vitae est eleifend, facilisis eros consequat, bibendum enim. Curabitur orci turpis, convallis ut purus ac, fringilla tempor massa. Pellentesque eu posuere felis, a auctor tellus.

Praesent vel tempor enim, a semper eros. Donec tortor quam, pulvinar quis arcu ac, mollis blandit ipsum. Sed volutpat ante in mattis lobortis. Integer eget massa euismod, vulputate sapien ac, egestas est. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Integer congue nec enim at blandit. Nam ornare erat quam, in ultricies justo euismod non. Cras rhoncus sapien eget ligula tristique elementum. Nullam ullamcorper, odio non eleifend maximus, magna turpis convallis augue, vel pellentesque orci sem eu sapien.

Vestibulum semper enim quis tincidunt fermentum. Nunc orci lectus, rutrum in urna vitae, commodo egestas urna. Aliquam erat volutpat. Mauris eu ante sed ex ultricies efficitur a ut nibh. Suspendisse potenti. Aenean nec odio vel dui vehicula suscipit id aliquam lectus. Praesent a metus maximus, pharetra felis id, varius nibh. Aliquam aliquet orci arcu, non interdum sem auctor sit amet. Praesent convallis nibh ut leo posuere, sed laoreet turpis convallis.

Aliquam lobortis nunc sed orci posuere, ut tincidunt justo porttitor. Nam turpis turpis, varius in mi in, volutpat bibendum sem. Praesent lobortis nulla ut sollicitudin interdum. Praesent et arcu rutrum libero rutrum auctor vulputate sit amet turpis. Vestibulum ut lacinia massa. Maecenas at tempor massa. Aenean convallis nunc quis maximus malesuada. Praesent ac condimentum dui. Nullam quis iaculis turpis, ac pellentesque dui. Donec semper ligula in augue posuere convallis. Vestibulum vel velit scelerisque, facilisis justo at, tempor enim. Fusce eu ornare augue.

Etiam non augue dapibus, pellentesque felis eu, efficitur leo. Praesent vehicula risus sed nunc aliquam lobortis. Sed eget efficitur augue. Donec mollis laoreet vehicula. Duis in finibus lorem. Duis sit amet risus sem. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae; Nulla consequat, enim vel dictum blandit, turpis turpis facilisis ligula, a porta nisi eros id quam. Donec eu finibus erat. Nullam tincidunt pellentesque orci, at placerat tortor laoreet vitae. Nulla facilisis suscipit est blandit venenatis. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae; Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae; Vivamus id ipsum condimentum, ornare turpis id, convallis dolor. Quisque porta a diam quis sollicitudin.
";

fn main() {
    use tracing_subscriber::layer::SubscriberExt;
    let subscriber =
        tracing_subscriber::Registry::default().with(tracing_subscriber::fmt::Layer::default());
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let mut args = std::env::args();
    let _ = args.next().unwrap();
    let bind_addr: SocketAddr = args.next().unwrap().parse().unwrap();
    let multicast_addr: SocketAddr = args.next().unwrap().parse().unwrap();
    let group_id: Option<GroupId> = args.next().map(|s| s.parse().unwrap());

    let coordinator = Coordinator::start_session(bind_addr, multicast_addr).unwrap();

    let mut sender = coordinator.create_broadcast_group(group_id).unwrap();

    loop {
        if let Some(addr) = sender.try_accept().unwrap() {
            println!("accepted connection from {}", addr);
        }
        // while let Some(addr) = sender.try_accept().unwrap() {
        //     println!("accepted connection from {}", addr);
        // }

        if sender.has_members() {
            println!("broadcasting to members");
            sender.write_message().write_all(LOREM_IPSUM).unwrap();
        } else {
            println!("no members to broadcast to");
        }
    }
}
