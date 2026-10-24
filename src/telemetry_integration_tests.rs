use super::*;
use crate::test_support::{api, message, success};
use serde_json::json;

async fn app(api_url: String) -> App {
    let path = env::temp_dir().join(format!(
        "mars-telemetry-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let config = Config {
        token: "TEST".into(),
        slow_message_alert_chat_id: Some(199456161),
        api_prefix: api_url.trim_end_matches("TEST").into(),
        file_prefix: "http://127.0.0.1/file/bot".into(),
        bot_proxy: None,
        http_proxy: None,
        https_proxy: None,
        no_proxy: None,
        database_path: path.clone(),
        log_level: "INFO".into(),
        report_stat_url: None,
        allow_bot_messages: false,
        tokio_worker_threads: 1,
        tokio_max_blocking_threads: 4,
    };
    let http = reqwest::Client::new();
    App {
        trace: None,
        bot: Bot::builder().api_url(api_url).client(http.clone()).build(),
        http,
        config,
        db: Database::open(path, 1).await.unwrap(),
        self_id: 999,
        self_username: None,
        media_groups: Arc::new(Mutex::new(HashMap::new())),
        export_locks: Arc::new(Mutex::new(HashMap::new())),
        tasks: TaskTracker::new(),
    }
}
fn trace(id: i32) -> Trace {
    Trace::new(
        id as u32,
        "message",
        &message(id, 100),
        false,
        102000,
        Instant::now(),
    )
}
fn event(id: i32, t: Trace) -> PhotoEvent {
    PhotoEvent {
        trace: Some(t),
        chat: (*message(id, 100).chat).clone(),
        message_id: id,
        user_id: None,
        file_id: format!("file{id}"),
        file_unique_id: format!("unique{id}"),
    }
}
fn cleanup(app: App) {
    let path = app.config.database_path.clone();
    drop(app);
    for p in [
        path.clone(),
        path.with_extension("db-wal"),
        path.with_extension("db-shm"),
    ] {
        let _ = std::fs::remove_file(p);
    }
}

#[tokio::test]
async fn album_cached_hashes_share_one_reply_and_finish_each_member() {
    let (url, rx, server) = api(vec![success(80, 105)]);
    let app = app(url).await;
    app.db.observe(-1001234567890, [7; 8], 1).await.unwrap();
    let a = trace(10);
    let b = trace(11);
    a.start();
    a.defer();
    b.start();
    b.defer();
    for id in [10, 11] {
        app.db
            .cache_file_hash(format!("unique{id}"), [7; 8])
            .await
            .unwrap();
    }
    app.media_groups.lock().unwrap().insert(
        "album".into(),
        vec![event(10, a.clone()), event(11, b.clone())],
    );
    app.process_group("album".into()).await.unwrap();
    let req = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(req["message_thread_id"], 7);
    for t in [a, b] {
        let v = t.snapshot("test");
        assert_eq!(v["status"], "completed");
        assert_eq!(v["dhash"], "0707070707070707");
        assert_eq!(v["replies"].as_array().unwrap().len(), 1);
        assert_eq!(v["telegram_reply_at_ms"], 105000);
        assert!(v["db_execution_ms"].as_f64().unwrap() > 0.0);
    }
    server.join().unwrap();
    cleanup(app);
}
#[tokio::test]
async fn album_partial_failure_closes_all_members() {
    let (url, rx, server) = api(vec![
        json!({"ok":false,"error_code":400,"description":"file unavailable"}),
    ]);
    let app = app(url).await;
    app.db
        .cache_file_hash("unique10".into(), [7; 8])
        .await
        .unwrap();
    let a = trace(10);
    let b = trace(11);
    a.defer();
    b.defer();
    app.media_groups.lock().unwrap().insert(
        "album".into(),
        vec![event(10, a.clone()), event(11, b.clone())],
    );
    assert!(app.process_group("album".into()).await.is_err());
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
    server.join().unwrap();
    for t in [a, b] {
        let v = t.snapshot("test");
        assert_eq!(v["status"], "failed");
        assert!(!v["completed_at_ms"].is_null());
    }
    cleanup(app);
}
#[tokio::test]
async fn pool_wait_is_separate_from_sql_execution() {
    let app = app("http://127.0.0.1:1/botTEST".into()).await;
    let held = app.db.connection().await.unwrap();
    let t = trace(12);
    let db = app.traced(Some(t.clone())).db;
    let task = tokio::spawn(async move { db.stats(-1001234567890).await.unwrap() });
    sleep(Duration::from_millis(40)).await;
    drop(held);
    task.await.unwrap();
    let v = t.snapshot("test");
    assert!(v["db_pool_wait_ms"].as_f64().unwrap() >= 30.0);
    assert!(v["db_execution_ms"].as_f64().unwrap() > 0.0);
    cleanup(app);
}
#[tokio::test]
async fn command_reply_captures_telegram_date_and_topic() {
    let (url, rx, server) = api(vec![success(70, 109)]);
    let app = app(url).await;
    let t = trace(12);
    let traced = app.traced(Some(t.clone()));
    let mut m = message(12, 100);
    m.text = Some("/help".into());
    traced.handle_message(m, false).await.unwrap();
    t.finish(None);
    let req = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(req["message_thread_id"], 7);
    assert_eq!(req["reply_parameters"]["message_id"], 12);
    assert_eq!(t.snapshot("test")["response_latency_ms"], 9000);
    server.join().unwrap();
    drop(traced);
    cleanup(app);
}

#[test]
fn cancelled_aggregation_finishes_members_without_affecting_later_groups() {
    let a = trace(20);
    a.defer();
    let groups = Arc::new(Mutex::new(HashMap::from([(
        "group".to_owned(),
        vec![event(20, a.clone())],
    )])));
    {
        let _guard = PendingAlbumGuard {
            armed: true,
            groups: groups.clone(),
            key: "group".into(),
        };
    }
    assert_eq!(a.snapshot("test")["status"], "failed");
    assert!(groups.lock().unwrap().is_empty());
    let b = trace(21);
    b.defer();
    groups
        .lock()
        .unwrap()
        .insert("group".into(), vec![event(21, b.clone())]);
    {
        let _guard = PendingAlbumGuard {
            armed: false,
            groups: groups.clone(),
            key: "group".into(),
        };
    }
    assert!(b.snapshot("test")["completed_at_ms"].is_null());
    assert_eq!(groups.lock().unwrap().len(), 1);
}

#[test]
fn blocking_queue_wait_is_separate_from_execution() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let app = app("http://127.0.0.1:1/botTEST".into()).await;
        // Pre-open and return the only connection before occupying the blocking worker.
        drop(app.db.connection().await.unwrap());
        let (tx, rx) = tokio::sync::oneshot::channel();
        let busy = spawn_blocking(move || {
            tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(60));
        });
        rx.await.unwrap();
        let t = trace(30);
        let traced = app.traced(Some(t.clone()));
        traced.db.stats(-1001234567890).await.unwrap();
        busy.await.unwrap();
        let v = t.snapshot("test");
        assert!(v["db_blocking_queue_ms"].as_f64().unwrap() >= 40.0);
        assert!(
            v["db_execution_ms"].as_f64().unwrap() < v["db_blocking_queue_ms"].as_f64().unwrap()
        );
        drop(traced);
        cleanup(app);
    });
}
