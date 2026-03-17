use std::sync::atomic::{AtomicUsize, Ordering};

use crate::speedtest::servers::SpeedtestServer;

pub fn normalize_server_pool(
    selected_server: &SpeedtestServer,
    server_pool: &[SpeedtestServer],
) -> Vec<SpeedtestServer> {
    if server_pool.is_empty() {
        vec![selected_server.clone()]
    } else {
        server_pool.to_vec()
    }
}

pub struct ActiveConnectionGuard<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> ActiveConnectionGuard<'a> {
    pub fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for ActiveConnectionGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}
