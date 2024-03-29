use rand::seq::SliceRandom;
use std::net::SocketAddr;

use solana_client::connection_cache::{ConnectionCache, NonblockingClientConnection};

pub struct MultiCache {
    pub connection_caches: Vec<ConnectionCache>,
}

impl MultiCache {
    pub fn new(connection_caches: Vec<ConnectionCache>) -> Self {
        Self { connection_caches }
    }

    pub fn get_nonblocking_connection(&self, addr: &SocketAddr) -> NonblockingClientConnection {
        self._get_random_cache().get_nonblocking_connection(addr)
    }

    fn _get_random_cache(&self) -> &ConnectionCache {
        let mut rng = rand::thread_rng();
        self.connection_caches
            .choose(&mut rng)
            .expect("expected conn caches to be non-empty")
    }
}
