use std::future::Future;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::task::JoinSet;
use tokio::time::timeout;

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

pub async fn resolve_ready_server_pool<F, Fut>(
    server_pool: &[SpeedtestServer],
    default_guid: &str,
    probe: F,
) -> Vec<SpeedtestServer>
where
    F: Fn(SpeedtestServer, String) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = bool> + Send + 'static,
{
    let mut tasks = JoinSet::new();

    for server in server_pool.iter().cloned() {
        let guid = server
            .session_guid
            .clone()
            .unwrap_or_else(|| default_guid.to_string());
        let probe = probe.clone();
        tasks.spawn(async move {
            let server_id = server.id;
            let is_ready = probe(server, guid).await;
            (server_id, is_ready)
        });
    }

    let mut ready_ids = std::collections::HashSet::new();
    while let Some(joined) = tasks.join_next().await {
        if let Ok((server_id, true)) = joined {
            ready_ids.insert(server_id);
        }
    }

    server_pool
        .iter()
        .filter(|server| ready_ids.contains(&server.id))
        .cloned()
        .collect()
}

pub async fn drain_join_set(tasks: &mut JoinSet<()>, grace_period: Duration) {
    let drained = timeout(grace_period, async {
        while tasks.join_next().await.is_some() {}
    })
    .await;

    if drained.is_err() {
        tasks.abort_all();
        let _ = timeout(grace_period, async {
            while tasks.join_next().await.is_some() {}
        })
        .await;
    }
}

#[must_use]
pub fn elapsed_ms_since(start: Option<Instant>) -> u64 {
    start
        .map(|it| Instant::now().saturating_duration_since(it).as_millis() as u64)
        .unwrap_or(0)
}
