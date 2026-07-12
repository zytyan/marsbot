use anyhow::{Context, Result, anyhow};
use rusqlite::{Connection, OpenFlags};
use std::{
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::spawn_blocking,
    time::timeout,
};

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct SqlitePool {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    idle: Mutex<Vec<Connection>>,
    permits: Arc<Semaphore>,
    #[cfg(test)]
    max_size: usize,
}

impl SqlitePool {
    pub(crate) fn new(path: PathBuf, max_size: usize) -> Result<Self> {
        if max_size == 0 {
            return Err(anyhow!("SQLite pool size must be greater than zero"));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                path,
                idle: Mutex::new(Vec::new()),
                permits: Arc::new(Semaphore::new(max_size)),
                #[cfg(test)]
                max_size,
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn max_size(&self) -> usize {
        self.inner.max_size
    }

    pub(crate) async fn get(&self) -> Result<PooledConnection> {
        let permit = timeout(
            CONNECTION_TIMEOUT,
            self.inner.permits.clone().acquire_owned(),
        )
        .await
        .context("timed out waiting for a SQLite connection")?
        .context("SQLite connection pool was closed")?;

        let conn = self
            .inner
            .idle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop();
        let conn = match conn {
            Some(conn) => conn,
            None => {
                let path = self.inner.path.clone();
                spawn_blocking(move || open_connection(path))
                    .await
                    .context("SQLite connection creation task")??
            }
        };

        Ok(PooledConnection {
            conn: Some(conn),
            inner: self.inner.clone(),
            _permit: permit,
        })
    }
}

fn open_connection(path: PathBuf) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
    )?;
    conn.busy_timeout(Duration::from_secs(10))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-80000;",
    )?;
    Ok(conn)
}

pub(crate) struct PooledConnection {
    conn: Option<Connection>,
    inner: Arc<Inner>,
    _permit: OwnedSemaphorePermit,
}

impl Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("pooled connection is present")
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().expect("pooled connection is present")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.inner
                .idle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(conn);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Instant,
    };
    use tokio::{sync::oneshot, task::JoinSet, time::sleep};

    #[tokio::test(flavor = "current_thread")]
    async fn single_connection_waits_without_blocking_the_runtime() {
        let pool = SqlitePool::new(PathBuf::from(":memory:"), 1).unwrap();
        let conn = pool.get().await.unwrap();
        let (started_tx, started_rx) = oneshot::channel();
        let slow = spawn_blocking(move || {
            started_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_secs(2));
            conn.query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
                .unwrap()
        });
        started_rx.await.unwrap();

        let waiting_pool = pool.clone();
        let waiter = tokio::spawn(async move { waiting_pool.get().await.unwrap() });
        sleep(Duration::from_millis(30)).await;
        assert!(!waiter.is_finished());

        sleep(Duration::from_millis(30)).await;
        assert_eq!(slow.await.unwrap(), 1);
        drop(waiter.await.unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn waiting_for_a_connection_times_out_after_ten_seconds() {
        let pool = SqlitePool::new(PathBuf::from(":memory:"), 1).unwrap();
        let conn = pool.get().await.unwrap();
        let (started_tx, started_rx) = oneshot::channel();
        let slow = spawn_blocking(move || {
            started_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_secs(11));
            conn.query_row("SELECT 1", [], |row| row.get::<_, i64>(0))
                .unwrap()
        });
        started_rx.await.unwrap();

        let start = Instant::now();
        let error = match pool.get().await {
            Ok(_) => panic!("connection wait unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(start.elapsed() >= CONNECTION_TIMEOUT);
        assert!(
            error
                .to_string()
                .contains("timed out waiting for a SQLite connection")
        );

        assert_eq!(slow.await.unwrap(), 1);
        let conn = pool.get().await.unwrap();
        assert_eq!(
            spawn_blocking(move || conn
                .query_row("SELECT 2", [], |row| row.get::<_, i64>(0))
                .unwrap())
            .await
            .unwrap(),
            2
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multiple_connections_run_queries_with_varied_delays() {
        const POOL_SIZE: usize = 3;
        let pool = SqlitePool::new(PathBuf::from(":memory:"), POOL_SIZE).unwrap();
        let mut seed = 0x5eed_u64;
        let delays = (0..12)
            .map(|_| {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                20 + seed % 160
            })
            .collect::<Vec<_>>();
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let start = Instant::now();
        let mut tasks = JoinSet::new();

        for (value, &delay) in delays.iter().enumerate() {
            let pool = pool.clone();
            let active = active.clone();
            let peak = peak.clone();
            tasks.spawn(async move {
                let conn = pool.get().await.unwrap();
                spawn_blocking(move || {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(delay));
                    let result = conn
                        .query_row("SELECT ?1", [value as i64], |row| row.get::<_, i64>(0))
                        .unwrap();
                    active.fetch_sub(1, Ordering::SeqCst);
                    result
                })
                .await
                .unwrap()
            });
        }

        let mut values = Vec::new();
        while let Some(result) = tasks.join_next().await {
            values.push(result.unwrap());
        }
        values.sort_unstable();
        assert_eq!(values, (0_i64..delays.len() as i64).collect::<Vec<_>>());
        assert_eq!(peak.load(Ordering::SeqCst), POOL_SIZE);
        assert!(start.elapsed() < Duration::from_millis(delays.iter().sum()));
    }
}
