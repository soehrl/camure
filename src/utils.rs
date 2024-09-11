use std::{
    fmt::{self, Display},
    net::SocketAddr,
    time::{Duration, Instant},
};

use socket2::SockAddr;

pub struct DisplaySockAddr<'a>(&'a SockAddr);

impl Display for DisplaySockAddr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0.as_socket() {
            Some(addr) => write!(f, "{}", addr),
            None => write!(f, "{:?}", self.0),
        }
    }
}

pub fn display_addr(addr: &SockAddr) -> DisplaySockAddr {
    DisplaySockAddr(addr)
}

pub struct ExponentialBackoff {
    current_wait_time: Duration,
    max_wait_time: Duration,
}

impl ExponentialBackoff {
    pub fn new() -> Self {
        Self {
            current_wait_time: Duration::from_millis(100),
            max_wait_time: Duration::from_secs(1),
        }
    }
}

impl Iterator for ExponentialBackoff {
    type Item = Instant;

    fn next(&mut self) -> Option<Self::Item> {
        let now = Instant::now();
        let wait_time = self.current_wait_time;
        self.current_wait_time *= 2;
        if self.current_wait_time > self.max_wait_time {
            self.current_wait_time = self.max_wait_time;
        }
        Some(now + wait_time)
    }
}

pub fn sock_addr_to_socket_addr(addr: SockAddr) -> Result<SocketAddr, std::io::Error> {
    match addr.as_socket() {
        Some(addr) => Ok(addr),
        None => Err(std::io::ErrorKind::AddrNotAvailable.into()),
    }
}
