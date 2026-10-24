use frankenstein::{
    AsyncTelegramApi,
    client_reqwest::Bot,
    methods::SendMessageParams,
    types::{ChatId, Message},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Notify;

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn alert_parts(text: &str) -> Vec<String> {
    let mut parts = vec![];
    let mut part = String::new();
    let mut units = 0;
    for ch in text.chars() {
        if units + ch.len_utf16() > 3500 {
            parts.push(std::mem::take(&mut part));
            units = 0;
        }
        part.push(ch);
        units += ch.len_utf16();
    }
    if !part.is_empty() {
        parts.push(part);
    }
    parts
}

#[derive(Clone)]
pub(crate) struct Trace(Arc<Inner>);
struct Inner {
    metadata: Value,
    received: Instant,
    received_ms: i64,
    origin_ms: i64,
    state: Mutex<State>,
    done: Notify,
}
struct State {
    started_ms: Option<i64>,
    queue_ms: Option<f64>,
    finished_ms: Option<i64>,
    total_ms: Option<f64>,
    status: &'static str,
    stage: &'static str,
    error: Option<String>,
    deferred: bool,
    slow_logged: bool,
    alerted: bool,
    suppress_alerts: bool,
    dhash: Option<String>,
    replies: Vec<Value>,
    metrics: HashMap<&'static str, f64>,
    active: HashMap<usize, (&'static str, Instant)>,
    next_timer: usize,
}

impl Trace {
    pub(crate) fn new(
        update_id: u32,
        kind: &str,
        message: &Message,
        edited: bool,
        received_ms: i64,
        received: Instant,
    ) -> Self {
        let source = if message.photo.is_some() {
            Some(message)
        } else {
            message
                .reply_to_message
                .as_deref()
                .filter(|m| m.photo.is_some())
        };
        let photo = source.and_then(|m| m.photo.as_ref()).and_then(|v| v.last());
        let media = photo
            .map(|p| (&p.file_id, &p.file_unique_id))
            .or_else(|| {
                message
                    .document
                    .as_ref()
                    .map(|p| (&p.file_id, &p.file_unique_id))
            })
            .or_else(|| {
                message
                    .animation
                    .as_ref()
                    .map(|p| (&p.file_id, &p.file_unique_id))
            })
            .or_else(|| {
                message
                    .audio
                    .as_ref()
                    .map(|p| (&p.file_id, &p.file_unique_id))
            })
            .or_else(|| {
                message
                    .video
                    .as_ref()
                    .map(|p| (&p.file_id, &p.file_unique_id))
            })
            .or_else(|| {
                message
                    .video_note
                    .as_ref()
                    .map(|p| (&p.file_id, &p.file_unique_id))
            })
            .or_else(|| {
                message
                    .voice
                    .as_ref()
                    .map(|p| (&p.file_id, &p.file_unique_id))
            })
            .or_else(|| {
                message
                    .sticker
                    .as_ref()
                    .map(|p| (&p.file_id, &p.file_unique_id))
            });
        let message_ms = (message.date as i64).saturating_mul(1000);
        let edit_ms = message.edit_date.map(|v| (v as i64).saturating_mul(1000));
        let origin_ms = if kind == "callback_query" {
            received_ms
        } else if edited {
            edit_ms.unwrap_or(message_ms)
        } else {
            message_ms
        };
        let chat_id = message.chat.id;
        let link = if let Some(name) = &message.chat.username {
            Some(format!("https://t.me/{name}/{}", message.message_id))
        } else if chat_id < -1_000_000_000_000 {
            Some(format!(
                "https://t.me/c/{}/{}",
                -chat_id - 1_000_000_000_000_i64,
                message.message_id
            ))
        } else {
            None
        };
        Self(Arc::new(Inner {
            metadata: json!({"update_id": update_id, "update_type": kind, "chat_id": chat_id,
                "group_id": if chat_id < 0 {Some(chat_id)} else {None},
                "message_thread_id": message.message_thread_id, "message_id": message.message_id,
                "media_group_id": message.media_group_id, "message_url": link,
                "telegram_message_at_ms": message_ms, "telegram_edit_at_ms": edit_ms,
                "latency_origin_at_ms": origin_ms, "received_at_ms": received_ms,
                "receive_delay_ms": (received_ms - origin_ms).max(0),
                "file_id": media.map(|p| p.0), "file_unique_id": media.map(|p| p.1),
                "image_source_message_id": source.map(|m| m.message_id)}),
            received,
            received_ms,
            origin_ms,
            state: Mutex::new(State {
                started_ms: None,
                queue_ms: None,
                finished_ms: None,
                total_ms: None,
                status: "pending",
                stage: "queued",
                error: None,
                deferred: false,
                slow_logged: false,
                alerted: false,
                suppress_alerts: false,
                dhash: None,
                replies: vec![],
                metrics: HashMap::new(),
                active: HashMap::new(),
                next_timer: 0,
            }),
            done: Notify::new(),
        }))
    }
    pub(crate) fn start(&self) {
        let mut s = self.0.state.lock().unwrap();
        s.started_ms = Some(now_ms());
        s.queue_ms = Some(self.0.received.elapsed().as_secs_f64() * 1000.0);
        s.status = "processing";
        s.stage = "dispatch";
    }
    pub(crate) fn thread_id(&self) -> Option<i32> {
        self.0.metadata["message_thread_id"]
            .as_i64()
            .map(|v| v as i32)
    }
    pub(crate) fn stage(&self, stage: &'static str) {
        self.0.state.lock().unwrap().stage = stage;
    }
    pub(crate) fn suppress_alerts(&self) {
        self.0.state.lock().unwrap().suppress_alerts = true;
    }
    pub(crate) fn ignored(&self) {
        self.0.state.lock().unwrap().status = "ignored";
    }
    pub(crate) fn defer(&self) {
        let mut s = self.0.state.lock().unwrap();
        s.deferred = true;
        s.stage = "album_wait";
    }
    pub(crate) fn deferred(&self) -> bool {
        self.0.state.lock().unwrap().deferred
    }
    pub(crate) fn hash(&self, hash: &[u8; 8]) {
        self.0.state.lock().unwrap().dhash = Some(crate::hex(hash));
    }
    pub(crate) fn reply(&self, m: &Message, target: i32) {
        self.0.state.lock().unwrap().replies.push(json!({"chat_id":m.chat.id, "message_id":m.message_id,
            "message_thread_id":m.message_thread_id, "telegram_at_ms":(m.date as i64).saturating_mul(1000),
            "received_at_ms":now_ms(), "reply_target_message_id":target}));
    }
    pub(crate) fn timer(&self, name: &'static str) -> Timer {
        let mut s = self.0.state.lock().unwrap();
        let id = s.next_timer;
        s.next_timer += 1;
        s.active.insert(id, (name, Instant::now()));
        Timer {
            trace: self.clone(),
            id,
        }
    }
    pub(crate) fn finish(&self, error: Option<String>) {
        {
            let mut s = self.0.state.lock().unwrap();
            if s.finished_ms.is_some() {
                return;
            }
            s.finished_ms = Some(now_ms());
            s.total_ms = Some(self.0.received.elapsed().as_secs_f64() * 1000.0);
            if error.is_some() {
                s.status = "failed";
            } else if s.status != "ignored" {
                s.status = "completed";
            }
            s.error = error;
        }
        self.emit("message_completed");
        self.0.done.notify_one();
    }
    fn latency(&self, s: &State) -> i64 {
        if s.finished_ms.is_some() {
            let response = s
                .replies
                .last()
                .and_then(|v| v["telegram_at_ms"].as_i64())
                .unwrap_or_else(|| {
                    self.0.received_ms.saturating_add(
                        s.total_ms
                            .unwrap_or_else(|| self.0.received.elapsed().as_secs_f64() * 1000.0)
                            as i64,
                    )
                });
            (response - self.0.origin_ms).max(0)
        } else {
            ((self.0.received_ms - self.0.origin_ms) as f64
                + self.0.received.elapsed().as_secs_f64() * 1000.0)
                .max(0.0) as i64
        }
    }
    pub(crate) fn snapshot(&self, event: &str) -> Value {
        let s = self.0.state.lock().unwrap();
        let mut v = self.0.metadata.clone();
        let mut metrics = s.metrics.clone();
        for (name, start) in s.active.values() {
            *metrics.entry(name).or_default() += start.elapsed().as_secs_f64() * 1000.0;
        }
        let o = v.as_object_mut().unwrap();
        o.insert("event".into(), json!(event));
        o.insert("logged_at_ms".into(), json!(now_ms()));
        o.insert(
            "level".into(),
            json!(if s.error.is_some() {
                "ERROR"
            } else if self.latency(&s) > 5000 {
                "WARN"
            } else {
                "INFO"
            }),
        );
        o.insert("started_at_ms".into(), json!(s.started_ms));
        o.insert("completed_at_ms".into(), json!(s.finished_ms));
        o.insert("queue_ms".into(), json!(s.queue_ms));
        o.insert(
            "processing_total_ms".into(),
            json!(
                s.total_ms
                    .unwrap_or_else(|| self.0.received.elapsed().as_secs_f64() * 1000.0)
            ),
        );
        o.insert("response_latency_ms".into(), json!(self.latency(&s)));
        o.insert("status".into(), json!(s.status));
        o.insert("stage".into(), json!(s.stage));
        o.insert("error".into(), json!(s.error));
        o.insert("dhash".into(), json!(s.dhash));
        o.insert("replies".into(), json!(s.replies));
        o.insert(
            "telegram_reply_at_ms".into(),
            s.replies
                .last()
                .map(|r| r["telegram_at_ms"].clone())
                .unwrap_or(Value::Null),
        );
        for name in ["db_pool_wait_ms", "db_blocking_queue_ms", "db_execution_ms"] {
            o.insert(
                name.into(),
                json!(metrics.get(name).copied().unwrap_or(0.0)),
            );
        }
        v
    }
    pub(crate) fn emit(&self, event: &str) {
        eprintln!("{}", self.snapshot(event));
    }
    fn decisions(&self) -> (bool, bool, bool, u64) {
        let mut s = self.0.state.lock().unwrap();
        let latency = self.latency(&s);
        let slow = latency > 5000 && !s.slow_logged;
        let alert = latency > 60000 && !s.alerted && !s.suppress_alerts;
        if slow {
            s.slow_logged = true;
        }
        if alert {
            s.alerted = true;
        }
        let next = if !s.slow_logged { 5001 } else { 60001 };
        (
            slow,
            alert,
            s.finished_ms.is_some(),
            (next - latency).max(1) as u64,
        )
    }
    pub(crate) async fn monitor(
        self,
        bot: Option<Bot>,
        chat_id: Option<i64>,
        token: String,
        tasks: tokio_util::task::TaskTracker,
    ) {
        loop {
            let (slow, alert, finished, wait) = self.decisions();
            if slow {
                self.emit("slow_message");
            }
            if alert {
                self.emit("slow_message_alert");
                if let (Some(bot), Some(chat_id)) = (bot.clone(), chat_id) {
                    let trace = self.clone();
                    let token = token.clone();
                    // Independent task: the observer and business processing never wait for the notification.
                    tasks.spawn(async move {
                        let text = trace.snapshot("slow_message_alert").to_string();
                        let send = async {
                            for part in alert_parts(&text) {
                                let params = SendMessageParams::builder()
                                    .chat_id(ChatId::Integer(chat_id))
                                    .text(part)
                                    .build();
                                bot.send_message(&params).await?;
                            }
                            Ok::<(), frankenstein::Error>(())
                        };
                        match tokio::time::timeout(Duration::from_secs(10), send).await {
                            Ok(Ok(())) => trace.emit("slow_message_alert_sent"),
                            result => {
                                let error = match result {
                                    Ok(Err(e)) => e.to_string().replace(&token, "<REDACTED>"),
                                    _ => "alert request timed out".into(),
                                };
                                let mut v = trace.snapshot("slow_message_alert_failed");
                                v["error"] = json!(error);
                                v["level"] = json!("ERROR");
                                eprintln!("{v}");
                            }
                        }
                    });
                }
            }
            if finished {
                break;
            }
            if self.0.state.lock().unwrap().alerted {
                self.0.done.notified().await;
            } else {
                tokio::select! {_=tokio::time::sleep(Duration::from_millis(wait))=>{}, _=self.0.done.notified()=>{}}
            }
        }
    }
}

pub(crate) struct Timer {
    trace: Trace,
    id: usize,
}
impl Drop for Timer {
    fn drop(&mut self) {
        let mut s = self.trace.0.state.lock().unwrap();
        if let Some((name, start)) = s.active.remove(&self.id) {
            *s.metrics.entry(name).or_default() += start.elapsed().as_secs_f64() * 1000.0;
        }
    }
}

// An unwind or task cancellation must still close the message record.
pub(crate) struct CompletionGuard(pub Trace);
impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if !self.0.deferred() {
            self.0
                .finish(Some("message task cancelled or panicked".into()));
        }
    }
}

pub(crate) struct AlbumCompletionGuard(pub Vec<Trace>);
impl Drop for AlbumCompletionGuard {
    fn drop(&mut self) {
        for trace in &self.0 {
            trace.finish(Some("album task cancelled or panicked".into()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{api, message, success};
    fn trace() -> Trace {
        Trace::new(
            10,
            "message",
            &message(42, 100),
            false,
            102000,
            Instant::now(),
        )
    }
    fn complete_at(t: &Trace, at: i64) {
        let mut s = t.0.state.lock().unwrap();
        s.finished_ms = Some(at);
        s.total_ms = Some((at - t.0.received_ms).max(0) as f64);
    }

    #[test]
    fn alert_parts_respect_telegram_utf16_limit_without_losing_content() {
        let text = "🪐中文".repeat(2500);
        let parts = alert_parts(&text);
        assert!(parts.len() > 1);
        assert_eq!(parts.concat(), text);
        assert!(parts.iter().all(|s| s.encode_utf16().count() <= 3500));
    }
    #[test]
    fn document_message_keeps_file_id_without_computing_image_hash() {
        let mut m = message(43, 100);
        m.document = Some(Box::new(
            serde_json::from_value(json!({"file_id":"doc","file_unique_id":"doc-unique"})).unwrap(),
        ));
        let t = Trace::new(1, "message", &m, false, 102000, Instant::now());
        let v = t.snapshot("test");
        assert_eq!(v["file_id"], "doc");
        assert!(v["dhash"].is_null());
    }
    #[test]
    fn own_messages_are_logged_but_cannot_recursively_send_alerts() {
        let t = trace();
        complete_at(&t, 161001);
        t.suppress_alerts();
        let (slow, alert, _, _) = t.decisions();
        assert!(slow);
        assert!(!alert);
    }
    #[test]
    fn strict_thresholds_and_alert_deduplication() {
        for (ms, slow, alert) in [
            (5000, false, false),
            (5001, true, false),
            (60000, true, false),
            (60001, true, true),
        ] {
            let t = trace();
            complete_at(&t, 100000 + ms);
            let d = t.decisions();
            assert_eq!((d.0, d.1), (slow, alert));
            let d = t.decisions();
            assert!(!d.0 && !d.1);
        }
    }
    #[test]
    fn reply_timestamp_is_from_telegram_and_all_replies_are_preserved() {
        let t = trace();
        t.start();
        t.reply(&message(50, 104), 42);
        t.reply(&message(51, 107), 42);
        t.finish(None);
        let v = t.snapshot("test");
        assert_eq!(v["response_latency_ms"], 7000);
        assert_eq!(v["telegram_reply_at_ms"], 107000);
        assert_eq!(v["replies"].as_array().unwrap().len(), 2);
        assert_eq!(v["message_thread_id"], 7);
        assert_eq!(v["receive_delay_ms"], 2000);
        assert!(v["dhash"].is_null());
    }
    #[test]
    fn edited_and_callback_updates_do_not_use_old_message_age() {
        let mut m = message(42, 10);
        m.edit_date = Some(102);
        let t = Trace::new(1, "edited_message", &m, true, 102000, Instant::now());
        complete_at(&t, 103000);
        assert_eq!(t.snapshot("test")["response_latency_ms"], 1000);
        let t = Trace::new(2, "callback_query", &m, false, 102000, Instant::now());
        complete_at(&t, 103000);
        assert_eq!(t.snapshot("test")["response_latency_ms"], 1000);
    }
    #[test]
    fn image_reference_has_distinct_source_and_null_values_are_explicit() {
        let mut m = message(42, 100);
        m.reply_to_message=Some(Box::new(serde_json::from_value(json!({"message_id":30,"date":80,"chat":{"id":-1001234567890_i64,"type":"supergroup"},"photo":[{"file_id":"photo","file_unique_id":"unique","width":10,"height":10}]})).unwrap()));
        let t = Trace::new(1, "message", &m, false, 102000, Instant::now());
        t.hash(&[0xab; 8]);
        let v = t.snapshot("test");
        assert_eq!(v["image_source_message_id"], 30);
        assert_eq!(v["message_id"], 42);
        assert_eq!(v["file_id"], "photo");
        assert_eq!(v["dhash"], "abababababababab");
    }
    #[test]
    fn timers_track_running_work_and_do_not_mutate_finished_totals() {
        let t = trace();
        let timer = t.timer("db_execution_ms");
        std::thread::sleep(Duration::from_millis(10));
        assert!(t.snapshot("test")["db_execution_ms"].as_f64().unwrap() >= 8.0);
        drop(timer);
        t.finish(None);
        let total = t.snapshot("test")["processing_total_ms"].clone();
        t.finish(Some("second finish must not replace status".into()));
        assert_eq!(t.snapshot("test")["processing_total_ms"], total);
        assert_eq!(t.snapshot("test")["status"], "completed");
    }
    #[test]
    fn ignored_failed_and_cancelled_messages_finish() {
        let t = trace();
        t.ignored();
        t.finish(None);
        assert_eq!(t.snapshot("test")["status"], "ignored");
        let t = trace();
        t.stage("get_file");
        t.finish(Some("network failed".into()));
        assert_eq!(t.snapshot("test")["status"], "failed");
        assert_eq!(t.snapshot("test")["stage"], "get_file");
        let t = trace();
        {
            let _guard = CompletionGuard(t.clone());
        }
        assert_eq!(t.snapshot("test")["status"], "failed");
        let t = trace();
        t.defer();
        {
            let _guard = CompletionGuard(t.clone());
        }
        assert!(t.snapshot("test")["completed_at_ms"].is_null());
        {
            let _guard = AlbumCompletionGuard(vec![t.clone()]);
        }
        assert_eq!(t.snapshot("test")["status"], "failed");
    }
    #[tokio::test]
    async fn in_flight_delayed_message_alerts_without_waiting_for_completion() {
        let (url, rx, server) = api(vec![success(99, 200)]);
        let bot = Bot::builder()
            .api_url(url)
            .client(reqwest::Client::new())
            .build();
        let t = Trace::new(
            10,
            "message",
            &message(42, 100),
            false,
            161001,
            Instant::now(),
        );
        let tasks = tokio_util::task::TaskTracker::new();
        let monitor = tokio::spawn(t.clone().monitor(
            Some(bot),
            Some(199456161),
            "TEST".into(),
            tasks.clone(),
        ));
        t.start(); // Fake Telegram timestamp is already older than a minute at receipt.
        let request =
            tokio::task::spawn_blocking(move || rx.recv_timeout(Duration::from_secs(4)).unwrap())
                .await
                .unwrap();
        assert_eq!(request["chat_id"], 199456161);
        let payload: Value = serde_json::from_str(request["text"].as_str().unwrap()).unwrap();
        assert_eq!(payload["message_id"], 42);
        assert_eq!(payload["message_thread_id"], 7);
        assert!(t.snapshot("test")["completed_at_ms"].is_null());
        t.finish(None);
        monitor.await.unwrap();
        tasks.close();
        tasks.wait().await;
        server.join().unwrap();
        assert!(!t.decisions().1);
    }
    #[tokio::test]
    async fn private_alert_failure_does_not_fail_business_processing() {
        let (url, rx, server) = api(vec![
            json!({"ok":false,"error_code":403,"description":"Forbidden"}),
        ]);
        let bot = Bot::builder()
            .api_url(url)
            .client(reqwest::Client::new())
            .build();
        let t = trace();
        complete_at(&t, 161001);
        let tasks = tokio_util::task::TaskTracker::new();
        t.clone()
            .monitor(Some(bot), Some(199456161), "TEST".into(), tasks.clone())
            .await;
        tasks.close();
        tasks.wait().await;
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        server.join().unwrap();
        assert!(!t.decisions().1);
        assert!(t.snapshot("test")["error"].is_null());
    }
}
