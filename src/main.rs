mod db_pool;
mod telemetry;
#[cfg(test)]
mod telemetry_integration_tests;
#[cfg(test)]
mod test_support;
use telemetry::{CompletionGuard, Trace};

use anyhow::{Context, Result, anyhow};
use db_pool::{PooledConnection, SqlitePool};
use frankenstein::{
    AsyncTelegramApi, ParseMode,
    client_reqwest::Bot,
    input_file::FileUpload,
    methods::{
        AnswerCallbackQueryParams, GetFileParams, GetUpdatesParams, SendDocumentParams,
        SendMessageParams,
    },
    types::{
        AllowedUpdate, CallbackQuery, Chat, ChatId, ChatMember, ChatMemberUpdated, ChatType,
        InlineKeyboardButton, InlineKeyboardMarkup, MaybeInaccessibleMessage, Message, ReplyMarkup,
        ReplyParameters, User,
    },
    updates::{Update, UpdateContent},
};
use opencv::{
    core::{self, MatTraitConst, Vector},
    imgcodecs, imgproc,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{
    collections::HashMap,
    env,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    signal,
    task::spawn_blocking,
    time::{sleep, timeout},
};
use tokio_util::task::TaskTracker;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

const SCHEMA_VERSION: i64 = 1;
const GROUP_WAIT: Duration = Duration::from_millis(1500);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_TOKIO_WORKER_THREADS: usize = 1;
const DEFAULT_TOKIO_MAX_BLOCKING_THREADS: usize = 4;
#[derive(Clone, Debug)]
struct Config {
    token: String,
    slow_message_alert_chat_id: Option<i64>,
    api_prefix: String,
    file_prefix: String,
    bot_proxy: Option<String>,
    http_proxy: Option<String>,
    https_proxy: Option<String>,
    no_proxy: Option<String>,
    database_path: PathBuf,
    log_level: String,
    report_stat_url: Option<String>,
    allow_bot_messages: bool,
    tokio_worker_threads: usize,
    tokio_max_blocking_threads: usize,
}

impl Config {
    fn load() -> Result<Self> {
        let get = |name: &str| env::var(name).ok().filter(|v| !v.is_empty());
        let token = get("BOT_TOKEN").unwrap_or_else(|| "".into());
        let allow_bot_messages = get("ALLOW_BOT_MESSAGES")
            .as_deref()
            .map(parse_bool)
            .transpose()?
            .unwrap_or(false);
        let tokio_worker_threads = parse_thread_count(
            "TOKIO_WORKER_THREADS",
            get("TOKIO_WORKER_THREADS").as_deref(),
            DEFAULT_TOKIO_WORKER_THREADS,
        )?;
        let tokio_max_blocking_threads = parse_thread_count(
            "TOKIO_MAX_BLOCKING_THREADS",
            get("TOKIO_MAX_BLOCKING_THREADS").as_deref(),
            DEFAULT_TOKIO_MAX_BLOCKING_THREADS,
        )?;
        let config = Self {
            token,
            slow_message_alert_chat_id: get("SLOW_MESSAGE_ALERT_CHAT_ID")
                .map(|v| {
                    v.parse::<i64>()
                        .context("SLOW_MESSAGE_ALERT_CHAT_ID must be a positive private chat ID")
                })
                .transpose()?,
            api_prefix: get("BOT_API_URL").unwrap_or_else(|| "https://api.telegram.org/bot".into()),
            file_prefix: get("BOT_FILE_URL")
                .unwrap_or_else(|| "https://api.telegram.org/file/bot".into()),
            bot_proxy: get("BOT_PROXY"),
            http_proxy: get("HTTP_PROXY"),
            https_proxy: get("HTTPS_PROXY"),
            no_proxy: get("NO_PROXY"),
            database_path: get("DATABASE_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("mars.db")),
            log_level: get("LOG_LEVEL").unwrap_or_else(|| "INFO".into()),
            report_stat_url: get("REPORT_STAT_URL"),
            allow_bot_messages,
            tokio_worker_threads,
            tokio_max_blocking_threads,
        };
        config.print();
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.slow_message_alert_chat_id.is_some_and(|id| id <= 0) {
            anyhow::bail!("SLOW_MESSAGE_ALERT_CHAT_ID must be a positive private chat ID");
        }
        if self.token.is_empty() {
            anyhow::bail!("BOT_TOKEN is required");
        }
        for (name, value, suffix) in [
            ("BOT_API_URL", &self.api_prefix, "/bot"),
            ("BOT_FILE_URL", &self.file_prefix, "/file/bot"),
        ] {
            let parsed = reqwest::Url::parse(value).with_context(|| format!("invalid {name}"))?;
            if !matches!(parsed.scheme(), "http" | "https")
                || !value.trim_end_matches('/').ends_with(suffix)
            {
                return Err(anyhow!(
                    "{name} must be an HTTP(S) prefix ending in {suffix}"
                ));
            }
        }
        Ok(())
    }

    fn print(&self) {
        let rows = [
            (
                "SLOW_MESSAGE_ALERT_CHAT_ID",
                self.slow_message_alert_chat_id
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "<unset: private alerts disabled>".into()),
                false,
                "private chat for slow message alerts",
            ),
            ("BOT_TOKEN", mask(&self.token), true, "Telegram Bot token"),
            (
                "BOT_API_URL",
                self.api_prefix.clone(),
                false,
                "Bot API prefix ending in /bot",
            ),
            (
                "BOT_FILE_URL",
                self.file_prefix.clone(),
                false,
                "file API prefix ending in /file/bot",
            ),
            (
                "BOT_PROXY",
                show_url_secret(self.bot_proxy.as_deref()),
                false,
                "override proxy for Bot API",
            ),
            (
                "HTTP_PROXY",
                show_url_secret(self.http_proxy.as_deref()),
                false,
                "standard HTTP proxy",
            ),
            (
                "HTTPS_PROXY",
                show_url_secret(self.https_proxy.as_deref()),
                false,
                "standard HTTPS proxy",
            ),
            (
                "NO_PROXY",
                self.no_proxy.clone().unwrap_or_else(|| "<unset>".into()),
                false,
                "proxy bypass list",
            ),
            (
                "DATABASE_PATH",
                self.database_path.display().to_string(),
                false,
                "SQLite database path",
            ),
            (
                "LOG_LEVEL",
                self.log_level.clone(),
                false,
                "application log threshold",
            ),
            (
                "REPORT_STAT_URL",
                show_url_secret(self.report_stat_url.as_deref()),
                false,
                "optional duplicate statistic endpoint",
            ),
            (
                "ALLOW_BOT_MESSAGES",
                self.allow_bot_messages.to_string(),
                false,
                "allow messages from other bots",
            ),
            (
                "TOKIO_WORKER_THREADS",
                self.tokio_worker_threads.to_string(),
                false,
                "Tokio async worker thread count",
            ),
            (
                "TOKIO_MAX_BLOCKING_THREADS",
                self.tokio_max_blocking_threads.to_string(),
                false,
                "Tokio blocking thread and SQLite pool limit",
            ),
        ];
        for (name, value, required, purpose) in rows {
            eprintln!("{name}: value={value}; required={required}; purpose={purpose}");
        }
    }

    fn api_url(&self) -> String {
        format!("{}{}", self.api_prefix.trim_end_matches('/'), self.token)
    }
    fn file_url(&self, file_path: &str) -> String {
        format!(
            "{}{}/{file_path}",
            self.file_prefix.trim_end_matches('/'),
            self.token
        )
    }
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(anyhow!("expected boolean, got {value:?}")),
    }
}

fn parse_thread_count(name: &str, value: Option<&str>, default: usize) -> Result<usize> {
    let value = match value {
        Some(value) => value
            .parse::<usize>()
            .with_context(|| format!("{name} must be a positive integer"))?,
        None => default,
    };
    if value == 0 {
        anyhow::bail!("{name} must be a positive integer");
    }
    Ok(value)
}

fn mask(value: &str) -> String {
    let chars: Vec<_> = value.chars().collect();
    if chars.len() <= 8 {
        return "*".repeat(chars.len().max(3));
    }
    format!(
        "{}{}{}",
        chars[..4].iter().collect::<String>(),
        "****",
        chars[chars.len() - 4..].iter().collect::<String>()
    )
}

fn show_url_secret(value: Option<&str>) -> String {
    match value {
        None => "<unset>".into(),
        Some(v) if v.contains('@') => mask(v),
        Some(v) => v.into(),
    }
}

fn http_client(config: &Config) -> Result<reqwest::Client> {
    http_client_timeout(config, Duration::from_secs(90))
}

fn http_client_timeout(config: &Config, request_timeout: Duration) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(request_timeout);
    if let Some(proxy) = &config.bot_proxy {
        builder = builder
            .no_proxy()
            .proxy(reqwest::Proxy::all(proxy).context("invalid BOT_PROXY")?);
    }
    builder.build().context("build HTTP client")
}

#[derive(Clone)]
struct Database {
    pool: SqlitePool,
    trace: Option<Trace>,
}

#[derive(Clone, Debug)]
struct MarsInfo {
    dhash: [u8; 8],
    count: i64,
    last_msg_id: i32,
    in_whitelist: bool,
}

#[derive(Clone, Debug)]
struct Observation {
    previous: Option<MarsInfo>,
    ignored: bool,
}

impl Database {
    async fn open(path: PathBuf, pool_size: usize) -> Result<Self> {
        let db = Self {
            pool: SqlitePool::new(path, pool_size)?,
            trace: None,
        };
        db.initialize().await?;
        Ok(db)
    }

    async fn connection(&self) -> Result<PooledConnection> {
        self.stage("database");
        let _timer = self.trace.as_ref().map(|t| t.timer("db_pool_wait_ms"));
        self.pool
            .get()
            .await
            .context("get SQLite connection from pool")
    }

    fn stage(&self, stage: &'static str) {
        if let Some(t) = &self.trace {
            t.stage(stage);
        }
    }
    fn blocking<F, T>(&self, f: F) -> tokio::task::JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.stage("db_blocking_queue");
        let queue = self.trace.as_ref().map(|t| t.timer("db_blocking_queue_ms"));
        let trace = self.trace.clone();
        spawn_blocking(move || {
            drop(queue);
            if let Some(t) = &trace {
                t.stage("db_execute");
            }
            let _execution = trace.as_ref().map(|t| t.timer("db_execution_ms"));
            f()
        })
    }

    async fn initialize(&self) -> Result<()> {
        let conn = self.connection().await?;
        self.blocking(move || -> Result<()> {
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS mars_info (
                    group_id INTEGER NOT NULL,
                    pic_dhash BLOB NOT NULL,
                    count INTEGER NOT NULL DEFAULT 0 CHECK(count >= 0),
                    last_msg_id INTEGER NOT NULL DEFAULT 0 CHECK(last_msg_id >= 0),
                    in_whitelist INTEGER NOT NULL DEFAULT 0 CHECK(in_whitelist IN (0, 1)),
                    PRIMARY KEY(group_id, pic_dhash)
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS fuid_to_dhash (
                    fuid TEXT PRIMARY KEY NOT NULL,
                    dhash BLOB NOT NULL
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS group_user_in_whitelist (
                    group_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    PRIMARY KEY(group_id, user_id)
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS mars_group_stat (
                    group_id INTEGER PRIMARY KEY NOT NULL,
                    image_count INTEGER NOT NULL DEFAULT 0 CHECK(image_count >= 0)
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS mars_stat_meta (
                    key TEXT PRIMARY KEY NOT NULL,
                    value INTEGER NOT NULL
                ) WITHOUT ROWID;
            "#,
            )?;
            let version: i64 = conn.query_row(
                r#"
                SELECT COALESCE(
                    (SELECT value FROM mars_stat_meta WHERE key = 'schema_version'),
                    0
                )
            "#,
                [],
                |r| r.get(0),
            )?;
            if version < SCHEMA_VERSION {
                let tx = conn.unchecked_transaction()?;
                tx.execute("DELETE FROM mars_group_stat", [])?;
                tx.execute(
                    r#"
                    INSERT INTO mars_group_stat (group_id, image_count)
                    SELECT group_id, COUNT(*)
                    FROM mars_info
                    GROUP BY group_id
                "#,
                    [],
                )?;
                let groups: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM mars_group_stat WHERE group_id < 0",
                    [],
                    |r| r.get(0),
                )?;
                tx.execute(
                    r#"
                    INSERT INTO mars_stat_meta (key, value)
                    VALUES ('group_count', ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                "#,
                    [groups],
                )?;
                tx.execute(
                    r#"
                    INSERT INTO mars_stat_meta (key, value)
                    VALUES ('schema_version', ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                "#,
                    [SCHEMA_VERSION],
                )?;
                tx.commit()?;
            }
            Ok(())
        })
        .await
        .context("database initialization task")??;
        Ok(())
    }

    async fn hash_for_file(&self, fuid: String) -> Result<Option<[u8; 8]>> {
        let conn = self.connection().await?;
        self.blocking(move || {
            conn.query_row(
                "SELECT dhash FROM fuid_to_dhash WHERE fuid=?",
                [fuid],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .map(blob_to_hash)
            .transpose()
        })
        .await?
    }

    async fn cache_file_hash(&self, fuid: String, hash: [u8; 8]) -> Result<()> {
        let conn = self.connection().await?;
        self.blocking(move || -> Result<()> {
            conn.execute(
                r#"
                    INSERT INTO fuid_to_dhash (fuid, dhash)
                    VALUES (?, ?)
                    ON CONFLICT(fuid) DO NOTHING
                "#,
                params![fuid, hash.to_vec()],
            )?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn user_whitelisted(&self, group_id: i64, user_id: u64) -> Result<bool> {
        let user_id = i64::try_from(user_id)?;
        let conn = self.connection().await?;
        self.blocking(move || -> Result<bool> {
            Ok(conn.query_row(
                r#"
                    SELECT EXISTS(
                        SELECT 1
                        FROM group_user_in_whitelist
                        WHERE group_id = ? AND user_id = ?
                    )
                "#,
                params![group_id, user_id],
                |row| row.get::<_, i64>(0),
            )? != 0)
        })
        .await?
    }

    async fn observe(&self, group_id: i64, hash: [u8; 8], message_id: i32) -> Result<Observation> {
        let mut conn = self.connection().await?;
        self.blocking(move || -> Result<Observation> {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let existing = read_info(&tx, group_id, hash)?;
            if let Some(info) = &existing
                && (info.last_msg_id == message_id || info.in_whitelist)
            {
                return Ok(Observation {
                    previous: None,
                    ignored: true,
                });
            }
            let is_new = existing.is_none();
            let mut next = existing.clone().unwrap_or(MarsInfo {
                dhash: hash,
                count: 0,
                last_msg_id: 0,
                in_whitelist: false,
            });
            let previous = (next.count > 0).then(|| next.clone());
            next.count += 1;
            next.last_msg_id = message_id;
            tx.execute(
                r#"
                    INSERT INTO mars_info (group_id, pic_dhash, count, last_msg_id, in_whitelist)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(group_id, pic_dhash) DO UPDATE SET
                        count = excluded.count,
                        last_msg_id = excluded.last_msg_id,
                        in_whitelist = excluded.in_whitelist
                "#,
                params![
                    group_id,
                    hash.to_vec(),
                    next.count,
                    next.last_msg_id,
                    i64::from(next.in_whitelist)
                ],
            )?;
            if is_new {
                increase_group_stat(&tx, group_id)?;
            }
            tx.commit()?;
            Ok(Observation {
                previous,
                ignored: false,
            })
        })
        .await?
    }

    async fn info(&self, group_id: i64, hash: [u8; 8]) -> Result<MarsInfo> {
        let conn = self.connection().await?;
        self.blocking(move || -> Result<MarsInfo> {
            Ok(read_info(&conn, group_id, hash)?.unwrap_or(MarsInfo {
                dhash: hash,
                count: 0,
                last_msg_id: 0,
                in_whitelist: false,
            }))
        })
        .await?
    }

    async fn set_image_whitelist(&self, group_id: i64, hash: [u8; 8], enabled: bool) -> Result<()> {
        let conn = self.connection().await?;
        self.blocking(move || -> Result<()> {
            conn.execute(
                r#"
                    INSERT INTO mars_info (group_id, pic_dhash, count, last_msg_id, in_whitelist)
                    VALUES (?, ?, 0, 0, ?)
                    ON CONFLICT(group_id, pic_dhash) DO UPDATE SET
                        in_whitelist = excluded.in_whitelist
                "#,
                params![group_id, hash.to_vec(), i64::from(enabled)],
            )?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn set_user_whitelist(&self, group_id: i64, user_id: u64, enabled: bool) -> Result<bool> {
        let user_id = i64::try_from(user_id)?;
        let conn = self.connection().await?;
        self.blocking(move || -> Result<bool> {
            let changed = if enabled {
                conn.execute(
                    r#"
                        INSERT INTO group_user_in_whitelist (group_id, user_id)
                        VALUES (?, ?)
                        ON CONFLICT DO NOTHING
                    "#,
                    params![group_id, user_id],
                )?
            } else {
                conn.execute(
                    r#"
                        DELETE FROM group_user_in_whitelist
                        WHERE group_id = ? AND user_id = ?
                    "#,
                    params![group_id, user_id],
                )?
            };
            Ok(changed != 0)
        })
        .await?
    }

    async fn stats(&self, group_id: i64) -> Result<(i64, i64)> {
        let conn = self.connection().await?;
        self.blocking(move || -> Result<(i64, i64)> {
            let groups = conn.query_row(
                r#"SELECT COUNT(*) FROM mars_group_stat WHERE group_id < 0"#,
                [],
                |row| row.get(0),
            )?;
            let images = conn.query_row(
                r#"
                    SELECT COALESCE(
                        (SELECT image_count FROM mars_group_stat WHERE group_id = ?),
                        0
                    )
                "#,
                [group_id],
                |row| row.get(0),
            )?;
            Ok((groups, images))
        })
        .await?
    }

    async fn similar(&self, group_id: i64, hash: [u8; 8]) -> Result<Vec<(MarsInfo, u32)>> {
        let conn = self.connection().await?;
        self.blocking(move || -> Result<Vec<(MarsInfo, u32)>> {
            let mut st = conn.prepare(
                r#"
                    SELECT pic_dhash, count, last_msg_id, in_whitelist
                    FROM mars_info
                    WHERE group_id = ?
                "#,
            )?;
            let mut out = st
                .query_map([group_id], |r| {
                    Ok((
                        blob_to_hash(r.get::<_, Vec<u8>>(0)?).map_err(to_sql_error)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get::<_, i64>(3)? != 0,
                    ))
                })?
                .filter_map(Result::ok)
                .map(|(d, c, m, w)| {
                    let distance = hamming(hash, d);
                    (
                        MarsInfo {
                            dhash: d,
                            count: c,
                            last_msg_id: m,
                            in_whitelist: w,
                        },
                        distance,
                    )
                })
                .filter(|(_, d)| *d < 6)
                .collect::<Vec<_>>();
            out.sort_by_key(|(_, d)| *d);
            out.truncate(10);
            Ok(out)
        })
        .await?
    }

    async fn export_zip(&self, group_id: i64) -> Result<PathBuf> {
        let conn = self.connection().await?;
        {
            let queue = self.trace.as_ref().map(|t| t.timer("db_blocking_queue_ms"));
            let trace = self.trace.clone();
            spawn_blocking(move || {
                drop(queue);
                Self::write_export_zip(group_id, conn, trace)
            })
            .await?
        }
    }

    fn write_export_zip(
        group_id: i64,
        conn: PooledConnection,
        trace: Option<Trace>,
    ) -> Result<PathBuf> {
        let archive = env::temp_dir().join(format!("mars-export-{group_id}.zip"));
        let result = (|| -> Result<()> {
            let file = File::create(&archive).context("create export ZIP")?;
            let mut zip = ZipWriter::new(BufWriter::new(file));
            let options = SimpleFileOptions::default()
                .compression_method(CompressionMethod::Deflated)
                .compression_level(Some(1));
            zip.start_file(format!("mars-export-{group_id}.csv"), options)
                .context("start CSV in export ZIP")?;
            writeln!(zip, "group_id,pic_dhash,count,last_msg_id,in_whitelist")?;

            let prepare_timer = trace.as_ref().map(|t| t.timer("db_execution_ms"));
            let mut statement = conn.prepare(
                r#"
                    SELECT pic_dhash, count, last_msg_id, in_whitelist
                    FROM mars_info
                    WHERE group_id = ?
                    ORDER BY pic_dhash
                "#,
            )?;
            let rows = statement.query_map([group_id], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i32>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?;
            drop(prepare_timer);
            let mut rows = rows;
            loop {
                let timer = trace.as_ref().map(|t| t.timer("db_execution_ms"));
                let row = rows.next();
                drop(timer);
                let Some(row) = row else { break };
                let (hash, count, message_id, in_whitelist) = row?;
                writeln!(
                    zip,
                    "{group_id},{},{count},{message_id},{in_whitelist}",
                    hex(&hash)
                )?;
            }

            zip.finish().context("finish export ZIP")?.flush()?;
            Ok(())
        })();
        if let Err(error) = result {
            let _ = std::fs::remove_file(&archive);
            return Err(error);
        }
        Ok(archive)
    }

    async fn checkpoint(&self) -> Result<()> {
        let conn = self.connection().await?;
        let (busy, log_frames, checkpointed_frames) = self
            .blocking(move || -> Result<_> {
                conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .map_err(Into::into)
            })
            .await??;
        if busy != 0 {
            return Err(anyhow!(
                "WAL checkpoint remained busy after checkpointing {checkpointed_frames}/{log_frames} frame(s)"
            ));
        }
        eprintln!("marsbot checkpointed {checkpointed_frames}/{log_frames} WAL frame(s)");
        Ok(())
    }
}
fn to_sql_error(error: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(error.to_string())))
}
fn blob_to_hash(blob: Vec<u8>) -> Result<[u8; 8]> {
    blob.try_into()
        .map_err(|v: Vec<u8>| anyhow!("invalid dhash length {}", v.len()))
}
fn read_info(conn: &Connection, group_id: i64, hash: [u8; 8]) -> Result<Option<MarsInfo>> {
    conn.query_row(
        r#"
            SELECT pic_dhash, count, last_msg_id, in_whitelist
            FROM mars_info
            WHERE group_id = ? AND pic_dhash = ?
        "#,
        params![group_id, hash.to_vec()],
        |row| {
            Ok(MarsInfo {
                dhash: blob_to_hash(row.get::<_, Vec<u8>>(0)?).map_err(to_sql_error)?,
                count: row.get(1)?,
                last_msg_id: row.get(2)?,
                in_whitelist: row.get::<_, i64>(3)? != 0,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}
fn increase_group_stat(conn: &rusqlite::Transaction<'_>, group_id: i64) -> Result<()> {
    let existed: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM mars_group_stat WHERE group_id=?",
            [group_id],
            |r| r.get(0),
        )
        .optional()?;
    conn.execute(
        r#"
            INSERT INTO mars_group_stat (group_id, image_count)
            VALUES (?, 1)
            ON CONFLICT(group_id) DO UPDATE SET
                image_count = image_count + 1
        "#,
        [group_id],
    )?;
    if existed.is_none() && group_id < 0 {
        conn.execute(
            r#"
                INSERT INTO mars_stat_meta (key, value)
                VALUES ('group_count', 1)
                ON CONFLICT(key) DO UPDATE SET
                    value = value + 1
            "#,
            [],
        )?;
    }
    Ok(())
}
fn hamming(a: [u8; 8], b: [u8; 8]) -> u32 {
    (u64::from_be_bytes(a) ^ u64::from_be_bytes(b)).count_ones()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}

fn dhash(bytes: Vec<u8>) -> Result<[u8; 8]> {
    let input = Vector::<u8>::from_slice(&bytes);
    let image = imgcodecs::imdecode(&input, imgcodecs::IMREAD_COLOR)?;
    if image.empty() {
        return Err(anyhow!("OpenCV could not decode image"));
    }
    let mut gray = core::Mat::default();
    imgproc::cvt_color_def(&image, &mut gray, imgproc::COLOR_BGR2GRAY)?;
    let mut resized = core::Mat::default();
    imgproc::resize(
        &gray,
        &mut resized,
        core::Size::new(9, 8),
        0.0,
        0.0,
        imgproc::INTER_AREA,
    )?;
    let mut out = [0_u8; 8];
    for y in 0..8 {
        for x in 0..8 {
            if *resized.at_2d::<u8>(y, x)? > *resized.at_2d::<u8>(y, x + 1)? {
                out[(y * 8 + x) as usize / 8] |= 1 << (7 - ((y * 8 + x) % 8));
            }
        }
    }
    Ok(out)
}

#[derive(Clone)]
struct App {
    trace: Option<Trace>,
    bot: Bot,
    http: reqwest::Client,
    config: Config,
    db: Database,
    self_id: u64,
    self_username: Option<String>,
    media_groups: Arc<Mutex<HashMap<String, Vec<PhotoEvent>>>>,
    export_locks: Arc<Mutex<HashMap<i64, bool>>>,
    tasks: TaskTracker,
}
#[derive(Clone)]
struct PhotoEvent {
    trace: Option<Trace>,
    chat: Chat,
    message_id: i32,
    user_id: Option<u64>,
    file_id: String,
    file_unique_id: String,
}

// Own the pending group even while its aggregation timer is waiting.
struct PendingAlbumGuard {
    armed: bool,
    groups: Arc<Mutex<HashMap<String, Vec<PhotoEvent>>>>,
    key: String,
}
impl Drop for PendingAlbumGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let events = self
            .groups
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key)
            .unwrap_or_default();
        for event in events {
            if let Some(t) = event.trace {
                t.finish(Some("album aggregation cancelled or panicked".into()));
            }
        }
    }
}

impl App {
    fn traced(&self, trace: Option<Trace>) -> Self {
        let mut app = self.clone();
        app.trace = trace.clone();
        app.db.trace = trace;
        app
    }
    fn stage(&self, stage: &'static str) {
        if let Some(t) = &self.trace {
            t.stage(stage);
        }
    }
    async fn reply(
        &self,
        chat_id: i64,
        message_id: i32,
        text: impl Into<String>,
        html: bool,
        keyboard: Option<InlineKeyboardMarkup>,
    ) -> Result<()> {
        let text = text.into();
        let reply = ReplyParameters::builder().message_id(message_id).build();
        let mut message = SendMessageParams::builder()
            .chat_id(ChatId::Integer(chat_id))
            .text(text)
            .reply_parameters(reply)
            .build();
        message.message_thread_id = self.trace.as_ref().and_then(Trace::thread_id);
        if let Some(k) = keyboard {
            message.reply_markup = Some(ReplyMarkup::InlineKeyboardMarkup(k));
        }
        if html {
            message.parse_mode = Some(ParseMode::Html);
        }
        self.stage("reply");
        let result = self.bot.send_message(&message).await?.result;
        if let Some(t) = &self.trace {
            t.reply(&result, message_id);
        }
        Ok(())
    }

    async fn file_hash(&self, event: &PhotoEvent) -> Result<[u8; 8]> {
        if let Some(hash) = self.db.hash_for_file(event.file_unique_id.clone()).await? {
            if let Some(t) = &self.trace {
                t.hash(&hash);
            }
            return Ok(hash);
        }
        self.stage("get_file");
        let remote = self
            .bot
            .get_file(
                &GetFileParams::builder()
                    .file_id(event.file_id.clone())
                    .build(),
            )
            .await?
            .result;
        let path = remote
            .file_path
            .ok_or_else(|| anyhow!("getFile did not return file_path"))?;
        self.stage("file_download");
        let bytes = if path.starts_with("http://") || path.starts_with("https://") {
            self.http
                .get(path)
                .send()
                .await?
                .error_for_status()?
                .bytes()
                .await?
                .to_vec()
        } else if Path::new(&path).is_absolute() {
            tokio::fs::read(&path)
                .await
                .with_context(|| format!("read local Bot API file {path}"))?
        } else {
            self.http
                .get(self.config.file_url(&path))
                .send()
                .await?
                .error_for_status()?
                .bytes()
                .await?
                .to_vec()
        };
        self.stage("dhash");
        let hash = spawn_blocking(move || dhash(bytes)).await??;
        if let Some(t) = &self.trace {
            t.hash(&hash);
        }
        self.db
            .cache_file_hash(event.file_unique_id.clone(), hash)
            .await?;
        Ok(hash)
    }

    async fn handle_photo(&self, event: PhotoEvent) -> Result<Option<(PhotoEvent, MarsInfo)>> {
        self.stage("photo");
        if let Some(user_id) = event.user_id
            && self.db.user_whitelisted(event.chat.id, user_id).await?
        {
            return Ok(None);
        }
        let hash = self.file_hash(&event).await?;
        let observed = self
            .db
            .observe(event.chat.id, hash, event.message_id)
            .await?;
        if observed.ignored {
            return Ok(None);
        }
        if let Some(previous) = observed.previous {
            Ok(Some((event, previous)))
        } else {
            Ok(None)
        }
    }

    async fn handle_message(&self, message: Message, edited: bool) -> Result<()> {
        if message
            .from
            .as_ref()
            .is_some_and(|u| u.id == self.self_id || (u.is_bot && !self.config.allow_bot_messages))
        {
            if let Some(t) = &self.trace {
                t.ignored();
            }
            return Ok(());
        }
        if let Some(text) = &message.text {
            self.handle_command(&message, text).await?;
        }
        let Some(photos) = &message.photo else {
            return Ok(());
        };
        let Some(photo) = photos.last() else {
            return Ok(());
        };
        let event = PhotoEvent {
            trace: self.trace.clone(),
            chat: (*message.chat).clone(),
            message_id: message.message_id,
            user_id: message.from.as_ref().map(|u| u.id),
            file_id: photo.file_id.clone(),
            file_unique_id: photo.file_unique_id.clone(),
        };
        if let Some(group) = &message.media_group_id {
            if edited {
                return Ok(());
            }
            let mut groups = self
                .media_groups
                .lock()
                .map_err(|_| anyhow!("media group lock poisoned"))?;
            let first = !groups.contains_key(group);
            if let Some(t) = &event.trace {
                t.defer();
            }
            groups.entry(group.clone()).or_default().push(event);
            drop(groups);
            if first {
                let app = self.clone();
                let key = group.clone();
                let pending = PendingAlbumGuard {
                    armed: true,
                    groups: self.media_groups.clone(),
                    key: key.clone(),
                };
                self.tasks.spawn(async move {
                    let mut pending = pending;
                    sleep(GROUP_WAIT).await;
                    pending.armed = false;
                    if let Err(e) = app.process_group(key).await {
                        eprintln!("media group error: {e:#}");
                    }
                });
            }
        } else if let Some((event, previous)) = self.handle_photo(event).await? {
            self.maybe_report(event.chat.id, previous.count).await;
            self.reply(
                event.chat.id,
                event.message_id,
                reply_text(&event.chat, &previous, false),
                true,
                whitelist_button(&previous),
            )
            .await?;
        }
        Ok(())
    }

    async fn process_group(&self, group: String) -> Result<()> {
        let events = self
            .media_groups
            .lock()
            .map_err(|_| anyhow!("media group lock poisoned"))?
            .remove(&group)
            .unwrap_or_default();
        let traces = events
            .iter()
            .filter_map(|e| e.trace.clone())
            .collect::<Vec<_>>();
        let _guards = telemetry::AlbumCompletionGuard(traces.clone());
        let result: Result<()> = async {
            let mut best: Option<(PhotoEvent, MarsInfo)> = None;
            for event in events {
                let app = self.traced(event.trace.clone());
                if let Some((event, previous)) = app.handle_photo(event).await?
                    && best.as_ref().is_none_or(|(_, p)| previous.count > p.count)
                {
                    best = Some((event, previous));
                }
            }
            if let Some((event, previous)) = best {
                let app = self.traced(event.trace.clone());
                app.maybe_report(event.chat.id, previous.count).await;
                let reply = ReplyParameters::builder()
                    .message_id(event.message_id)
                    .build();
                let mut params = SendMessageParams::builder()
                    .chat_id(ChatId::Integer(event.chat.id))
                    .text(reply_text(&event.chat, &previous, true))
                    .parse_mode(ParseMode::Html)
                    .reply_parameters(reply)
                    .build();
                params.message_thread_id = event.trace.as_ref().and_then(Trace::thread_id);
                for t in &traces {
                    t.stage("album_reply");
                }
                let response = app.bot.send_message(&params).await?.result;
                for t in &traces {
                    t.reply(&response, event.message_id);
                }
            }
            Ok(())
        }
        .await;
        for t in traces {
            t.finish(
                result
                    .as_ref()
                    .err()
                    .map(|e| format!("{e:#}").replace(&self.config.token, "<REDACTED>")),
            );
        }
        result
    }

    async fn maybe_report(&self, group_id: i64, count: i64) {
        self.stage("stat_report");
        if let Some(url) = &self.config.report_stat_url {
            let body = serde_json::json!({"group_id":group_id,"mars_count":count}).to_string();
            if let Err(e) = self
                .http
                .post(url)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
            {
                eprintln!("stat report failed: {e}");
            }
        }
    }

    async fn handle_command(&self, message: &Message, text: &str) -> Result<()> {
        let Some(command) = parse_command(text, self.self_username.as_deref()) else {
            return Ok(());
        };
        let chat_id = message.chat.id;
        match command {
            "help" | "start" => {
                if matches!(message.chat.type_field, ChatType::Private) || command == "help" {
                    let username = if matches!(message.chat.type_field, ChatType::Private) {
                        None
                    } else {
                        self.self_username.as_deref()
                    };
                    self.reply(
                        chat_id,
                        message.message_id,
                        help_text(username),
                        false,
                        None,
                    )
                    .await?;
                }
            }
            "stat" => {
                let started = Instant::now();
                let (groups, images) = self.db.stats(chat_id).await?;
                let user = message.from.as_ref();
                let white = match user {
                    Some(user) => self.db.user_whitelisted(chat_id, user.id).await?,
                    None => false,
                };
                let identity = user
                    .map(|user| format!("{}(id:{})", user_full_name(user), user.id))
                    .unwrap_or_else(|| "unknown".into());
                let text = format!(
                    "火星车当前一共服务了{groups}个群组\n当前群组ID: {chat_id}\n您是 {identity}，您{}本群的白名单当中\n本群一共记录了 {images} 张不同的图片\n本次统计共耗时 {:.2} ms\n火星车与您同在",
                    if white { "在" } else { "不在" },
                    started.elapsed().as_secs_f64() * 1000.0,
                );
                self.reply(chat_id, message.message_id, text, false, None)
                    .await?;
            }
            "pic_info" | "add_whitelist" | "remove_whitelist" => {
                let Some(event) = photo_reference(message) else {
                    self.reply(
                        chat_id,
                        message.message_id,
                        "火星车没有发现您引用了任何图片。请发送图片使用命令，或回复特定图片。",
                        false,
                        None,
                    )
                    .await?;
                    return Ok(());
                };
                let hash = self.file_hash(&event).await?;
                let info = self.db.info(chat_id, hash).await?;
                match command {
                    "pic_info" => {
                        let text = format!(
                            "File unique id: {}\ndhash: {}\n在本群的火星次数:{}\n{}",
                            event.file_unique_id,
                            hex(&hash).to_ascii_uppercase(),
                            info.count,
                            if info.in_whitelist {
                                "🙈 它在本群的火星白名单中"
                            } else {
                                "🟢 它不在本群的火星白名单当中"
                            }
                        );
                        let button = InlineKeyboardButton::builder()
                            .text("查找DHASH相似图片")
                            .callback_data(format!("find:{}", hex(&hash)))
                            .build();
                        let markup = InlineKeyboardMarkup::builder()
                            .inline_keyboard(vec![vec![button]])
                            .build();
                        self.reply(chat_id, message.message_id, text, false, Some(markup))
                            .await?;
                    }
                    "add_whitelist" => {
                        self.db.set_image_whitelist(chat_id, hash, true).await?;
                        let text = if info.in_whitelist {
                            "这张图片已经在白名单当中了"
                        } else {
                            "成功将图片加入白名单"
                        };
                        self.reply(chat_id, message.message_id, text, false, None)
                            .await?;
                    }
                    _ => {
                        self.db.set_image_whitelist(chat_id, hash, false).await?;
                        let text = if info.in_whitelist {
                            "成功将图片移除白名单"
                        } else {
                            "这张图片并不在白名单中"
                        };
                        self.reply(chat_id, message.message_id, text, false, None)
                            .await?;
                    }
                }
            }
            "add_me_to_whitelist" | "remove_me_from_whitelist" => {
                if let Some(user) = &message.from {
                    let added = self
                        .db
                        .set_user_whitelist(chat_id, user.id, command == "add_me_to_whitelist")
                        .await?;
                    let text = match (command, added) {
                        ("add_me_to_whitelist", true) => format!(
                            "已将用户 {} 加入白名单，您发的任何图片都不会被处理。",
                            user_full_name(user)
                        ),
                        ("add_me_to_whitelist", false) => format!(
                            "用户 {} 已经在本群的白名单中，您发的任何图片都不会被处理。",
                            user_full_name(user)
                        ),
                        ("remove_me_from_whitelist", true) => format!(
                            "已将用户 {} 移除本群白名单，火星车会继续为您服务。",
                            user_full_name(user)
                        ),
                        _ => format!(
                            "用户 {} 不在本群白名单中，火星车正在工作。",
                            user_full_name(user)
                        ),
                    };
                    self.reply(chat_id, message.message_id, text, false, None)
                        .await?;
                }
            }
            "mars_bot_welcome" => self.send_welcome(chat_id).await?,
            "export" => {
                self.reply(
                    chat_id,
                    message.message_id,
                    "想部署自己的火星车，又放不下当前数据？请使用 /ensure_marsbot_export 导出本聊天数据。",
                    false,
                    None,
                )
                .await?
            }
            "ensure_marsbot_export" => self.export(chat_id,message.message_id).await?,
            _ => {}
        }
        Ok(())
    }

    async fn handle_callback(&self, callback: &CallbackQuery) -> Result<()> {
        let Some(data) = &callback.data else {
            return Ok(());
        };
        let Some((chat_id, message_id)) = callback_message(&callback.message) else {
            return Ok(());
        };
        if let Some(hash) = data.strip_prefix("wl:").and_then(parse_hash) {
            if let Some(t) = &self.trace {
                t.hash(&hash);
            }
            self.db.set_image_whitelist(chat_id, hash, true).await?;
            self.answer_callback(&callback.id, "该图片已加入白名单")
                .await?;
        } else if let Some(hash) = data.strip_prefix("find:").and_then(parse_hash) {
            if let Some(t) = &self.trace {
                t.hash(&hash);
            }
            let found = self.db.similar(chat_id, hash).await?;
            let mut text = format!(
                "火星车为您找到了{}张相似的图片\n这些图片的汉明距离小于6\n",
                found.len()
            );
            for (i, (info, distance)) in found.iter().enumerate() {
                text.push_str(&format!(
                    "{}图片{}: 距离: {} 消息ID: {}{}\n",
                    link_start_by_id(chat_id, info.last_msg_id),
                    i + 1,
                    distance,
                    info.last_msg_id,
                    link_end_by_id(chat_id)
                ));
            }
            self.reply(chat_id, message_id, text, true, None).await?;
            self.answer_callback(&callback.id, "查找完成").await?;
        }
        Ok(())
    }

    async fn answer_callback(&self, id: &str, text: &str) -> Result<()> {
        self.stage("answer_callback");
        self.bot
            .answer_callback_query(
                &AnswerCallbackQueryParams::builder()
                    .callback_query_id(id)
                    .text(text)
                    .build(),
            )
            .await?;
        Ok(())
    }
    async fn send_welcome(&self, chat_id: i64) -> Result<()> {
        let params = SendMessageParams::builder()
            .chat_id(ChatId::Integer(chat_id))
            .text(concat!(
                "欢迎使用火星车。\n本 bot 会检测群组中重复发送的普通图片；",
                "只保存群组 ID、图片唯一 ID、dHash、消息 ID 与白名单状态，不保存图片。\n",
                "它不检查文件形式图片、表情包、视频或 GIF。\n",
                "<a href=\"https://github.com/zytyan/pymarsbot\">原项目</a>"
            ))
            .parse_mode(ParseMode::Html)
            .build();
        self.bot.send_message(&params).await?;
        Ok(())
    }
    async fn handle_membership(&self, update: &ChatMemberUpdated) -> Result<()> {
        if matches!(update.chat.type_field, ChatType::Private) {
            return Ok(());
        }
        match (&update.old_chat_member, &update.new_chat_member) {
            (_, ChatMember::Administrator(_)) => {
                let params = SendMessageParams::builder()
                    .chat_id(ChatId::Integer(update.chat.id))
                    .text("火星车的任何功能均不需要管理员权限，您无需将本 bot 设置为群组管理员。")
                    .build();
                self.bot.send_message(&params).await?;
            }
            (ChatMember::Left(_) | ChatMember::Kicked(_), ChatMember::Member(_)) => {
                self.send_welcome(update.chat.id).await?;
            }
            _ => {}
        }
        Ok(())
    }
    async fn export(&self, chat_id: i64, message_id: i32) -> Result<()> {
        let busy = {
            let mut locks = self
                .export_locks
                .lock()
                .map_err(|_| anyhow!("export lock poisoned"))?;
            if let std::collections::hash_map::Entry::Vacant(e) = locks.entry(chat_id) {
                e.insert(true);
                false
            } else {
                true
            }
        };
        if busy {
            self.reply(
                chat_id,
                message_id,
                "当前正在导出数据，或仍在导出冷却时间内，请稍候再试。",
                false,
                None,
            )
            .await?;
            return Ok(());
        }
        let path = match self.db.export_zip(chat_id).await {
            Ok(path) => path,
            Err(error) => {
                self.export_locks
                    .lock()
                    .map_err(|_| anyhow!("export lock poisoned"))?
                    .remove(&chat_id);
                self.reply(
                    chat_id,
                    message_id,
                    format!("导出失败，错误: {error}"),
                    false,
                    None,
                )
                .await?;
                return Ok(());
            }
        };
        self.stage("send_document");
        let result = self
            .bot
            .send_document(
                &SendDocumentParams::builder()
                    .chat_id(ChatId::Integer(chat_id))
                    .document(FileUpload::from(path.clone()))
                    .build(),
            )
            .await
            .map(|r| {
                if let Some(t) = &self.trace {
                    t.reply(&r.result, message_id);
                }
            });
        let _ = tokio::fs::remove_file(&path).await;
        if let Err(error) = result {
            self.export_locks
                .lock()
                .map_err(|_| anyhow!("export lock poisoned"))?
                .remove(&chat_id);
            self.reply(
                chat_id,
                message_id,
                format!("导出失败，错误: {error}"),
                false,
                None,
            )
            .await?;
            return Ok(());
        }
        let locks = self.export_locks.clone();
        tokio::spawn(async move {
            sleep(Duration::from_secs(600)).await;
            if let Ok(mut value) = locks.lock() {
                value.remove(&chat_id);
            }
        });
        Ok(())
    }
}

fn parse_hash(value: &str) -> Option<[u8; 8]> {
    if value.len() != 16 {
        return None;
    }
    let mut out = [0; 8];
    for (i, pair) in value.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(out)
}
fn photo_reference(message: &Message) -> Option<PhotoEvent> {
    let source = message
        .photo
        .as_ref()
        .map(|_| message)
        .or(message.reply_to_message.as_deref())?;
    let photo = source.photo.as_ref()?.last()?;
    Some(PhotoEvent {
        trace: None,
        chat: (*message.chat).clone(),
        message_id: message.message_id,
        user_id: message.from.as_ref().map(|u| u.id),
        file_id: photo.file_id.clone(),
        file_unique_id: photo.file_unique_id.clone(),
    })
}
fn parse_command<'a>(text: &'a str, username: Option<&str>) -> Option<&'a str> {
    let raw = text.split_whitespace().next()?.strip_prefix('/')?;
    let (name, target) = raw.split_once('@').unwrap_or((raw, ""));
    if !target.is_empty() && Some(target) != username {
        return None;
    }
    Some(name)
}
fn user_full_name(user: &User) -> String {
    match user.last_name.as_deref().filter(|name| !name.is_empty()) {
        Some(last_name) => format!("{} {last_name}", user.first_name),
        None => user.first_name.clone(),
    }
}
fn reply_text(chat: &Chat, info: &MarsInfo, grouped: bool) -> String {
    let (s, e) = link(chat, info.last_msg_id);
    match (grouped, info.count) {
        (false, 0..=2) => format!("这张图片已经{s}火星{}次{e}了！", info.count),
        (true, 0..=2) => format!("这一组图片火星了{s}火星{}次{e}了！", info.count),
        (false, 3) => format!("这张图已经{s}火星了3次{e}了，现在本车送你“火星之王”称号！"),
        (true, 3) => format!("您这一组图片已经{s}火星了3次{e}了，现在本车送你“火星之王”称号！"),
        (false, _) => format!(
            "火星之王，收了你的神通吧，这张图都让您{s}火星{}次{e}了！",
            info.count
        ),
        (true, _) => format!(
            "火星之王，收了您的神通吧，这些图都让您{s}火星{}次{e}了！",
            info.count
        ),
    }
}
fn link(chat: &Chat, message_id: i32) -> (String, String) {
    if let Some(username) = &chat.username {
        (
            format!("<a href=\"https://t.me/{username}/{message_id}\">"),
            "</a>".into(),
        )
    } else if chat.id < 0 {
        (
            format!(
                "<a href=\"https://t.me/c/{}/{message_id}\">",
                -chat.id - 1_000_000_000_000_i64
            ),
            "</a>".into(),
        )
    } else {
        (String::new(), String::new())
    }
}
fn link_start_by_id(chat_id: i64, message_id: i32) -> String {
    if chat_id < 0 {
        format!(
            "<a href=\"https://t.me/c/{}/{message_id}\">",
            -chat_id - 1_000_000_000_000_i64
        )
    } else {
        String::new()
    }
}
fn link_end_by_id(chat_id: i64) -> &'static str {
    if chat_id < 0 { "</a>" } else { "" }
}
fn whitelist_button(info: &MarsInfo) -> Option<InlineKeyboardMarkup> {
    (info.count > 5).then(|| {
        InlineKeyboardMarkup::builder()
            .inline_keyboard(vec![vec![
                InlineKeyboardButton::builder()
                    .text("将图片添加至白名单")
                    .callback_data(format!("wl:{}", hex(&info.dhash)))
                    .build(),
            ]])
            .build()
    })
}
fn help_text(username: Option<&str>) -> String {
    let suffix = username.map(|v| format!("@{v}")).unwrap_or_default();
    format!(
        concat!(
            "/help{} 显示本帮助信息\n",
            "/stat{} 显示统计信息\n",
            "/pic_info{} 获取图片信息\n",
            "/add_whitelist{} 将图片添加到白名单\n",
            "/remove_whitelist{} 将图片移除白名单\n",
            "/add_me_to_whitelist{} 将用户加入群组白名单\n",
            "/remove_me_from_whitelist{} 将用户移出群组白名单\n",
            "/export{} 导出说明"
        ),
        suffix, suffix, suffix, suffix, suffix, suffix, suffix, suffix
    )
}
fn callback_message(message: &Option<MaybeInaccessibleMessage>) -> Option<(i64, i32)> {
    match message.as_ref()? {
        MaybeInaccessibleMessage::Message(m) => Some((m.chat.id, m.message_id)),
        MaybeInaccessibleMessage::InaccessibleMessage(m) => Some((m.chat.id, m.message_id)),
    }
}

fn updates_params(offset: Option<i64>) -> GetUpdatesParams {
    let allowed = vec![
        AllowedUpdate::Message,
        AllowedUpdate::EditedMessage,
        AllowedUpdate::ChannelPost,
        AllowedUpdate::EditedChannelPost,
        AllowedUpdate::CallbackQuery,
        AllowedUpdate::MyChatMember,
    ];
    match offset {
        Some(offset) => GetUpdatesParams::builder()
            .offset(offset)
            .timeout(50)
            .allowed_updates(allowed)
            .build(),
        None => GetUpdatesParams::builder()
            .timeout(50)
            .allowed_updates(allowed)
            .build(),
    }
}

async fn wait_for_shutdown() -> Result<&'static str> {
    #[cfg(unix)]
    {
        let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate())
            .context("install SIGTERM handler")?;
        tokio::select! {
            result = signal::ctrl_c() => {
                result.context("install Ctrl-C handler")?;
                Ok("SIGINT")
            }
            _ = terminate.recv() => Ok("SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        signal::ctrl_c().await.context("install Ctrl-C handler")?;
        Ok("Ctrl-C")
    }
}

fn main() -> Result<()> {
    let config = Config::load()?;
    let worker_threads = config.tokio_worker_threads;
    let next_thread_id = AtomicUsize::new(0);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .max_blocking_threads(config.tokio_max_blocking_threads)
        .thread_name_fn(move || {
            if next_thread_id.fetch_add(1, Ordering::Relaxed) < worker_threads {
                "mars-poll".to_owned()
            } else {
                "mars-block".to_owned()
            }
        })
        .enable_all()
        .build()
        .context("build Tokio runtime")?;
    runtime.block_on(run(config))
}

fn spawn_update(app: &App, alert_bot: &Bot, update: Update, received_ms: i64, received: Instant) {
    let trace = match &update.content {
        UpdateContent::Message(m) => Some(Trace::new(
            update.update_id,
            "message",
            m,
            false,
            received_ms,
            received,
        )),
        UpdateContent::EditedMessage(m) => Some(Trace::new(
            update.update_id,
            "edited_message",
            m,
            true,
            received_ms,
            received,
        )),
        UpdateContent::ChannelPost(m) => Some(Trace::new(
            update.update_id,
            "channel_post",
            m,
            false,
            received_ms,
            received,
        )),
        UpdateContent::EditedChannelPost(m) => Some(Trace::new(
            update.update_id,
            "edited_channel_post",
            m,
            true,
            received_ms,
            received,
        )),
        UpdateContent::CallbackQuery(q) => match q.message.as_ref() {
            Some(MaybeInaccessibleMessage::Message(m)) => Some(Trace::new(
                update.update_id,
                "callback_query",
                m,
                false,
                received_ms,
                received,
            )),
            _ => None,
        },
        _ => None,
    };
    if let Some(t) = &trace {
        // Telegram can deliver bot-authored messages; never turn our own alerts into new alerts.
        let own_message = match &update.content {
            UpdateContent::Message(m)
            | UpdateContent::EditedMessage(m)
            | UpdateContent::ChannelPost(m)
            | UpdateContent::EditedChannelPost(m) => {
                m.from.as_ref().is_some_and(|u| u.id == app.self_id)
            }
            _ => false,
        };
        if own_message {
            t.suppress_alerts();
        }
        t.emit("message_received");
        app.tasks.spawn(t.clone().monitor(
            Some(alert_bot.clone()),
            app.config.slow_message_alert_chat_id,
            app.config.token.clone(),
            app.tasks.clone(),
        ));
    }
    let app = app.traced(trace.clone());
    let tasks = app.tasks.clone();
    let guard = trace.clone().map(CompletionGuard);
    tasks.spawn(async move {
        let _guard = guard;
        if let Some(t) = &trace {
            t.start();
        }
        let token = app.config.token.clone();
        let operation_started = Instant::now();
        let update_id = update.update_id;
        let result = dispatch(app, update).await;
        let error = result
            .as_ref()
            .err()
            .map(|e| format!("{e:#}").replace(&token, "<REDACTED>"));
        if let Some(t) = &trace {
            if !t.deferred() || error.is_some() {
                t.finish(error);
            }
        } else {
            let record = serde_json::json!({
                "event": "update_completed", "update_id": update_id,
                "received_at_ms": received_ms,
                "processing_total_ms": operation_started.elapsed().as_secs_f64() * 1000.0,
                "error": error,
            });
            eprintln!("{record}");
        }
    });
}

async fn run(config: Config) -> Result<()> {
    let http = http_client(&config)?;
    let bot = Bot::builder()
        .api_url(config.api_url())
        .client(http.clone())
        .build();
    let alert_bot = Bot::builder()
        .api_url(config.api_url())
        .client(http_client_timeout(&config, Duration::from_secs(10))?)
        .build();
    let me = bot.get_me().await?.result;
    let db = Database::open(
        config.database_path.clone(),
        config.tokio_max_blocking_threads,
    )
    .await?;
    let app = App {
        trace: None,
        bot,
        http,
        config,
        db,
        self_id: me.id,
        self_username: me.username,
        media_groups: Arc::new(Mutex::new(HashMap::new())),
        export_locks: Arc::new(Mutex::new(HashMap::new())),
        tasks: TaskTracker::new(),
    };
    eprintln!("marsbot started as id={}", app.self_id);
    let mut offset: Option<i64> = None;
    let shutdown = wait_for_shutdown();
    tokio::pin!(shutdown);
    loop {
        let params = updates_params(offset);
        tokio::select! {
            biased;
            reason = &mut shutdown => {
                eprintln!("marsbot received {}, stopping updates", reason?);
                break;
            }
            response = app.bot.get_updates(&params) => match response {
                Ok(result) => {
                    let received_ms=telemetry::now_ms(); let received=Instant::now();
                    for update in result.result {
                    offset = Some(i64::from(update.update_id) + 1);
                    spawn_update(&app, &alert_bot, update, received_ms, received);
                }},
                Err(e) => { eprintln!("getUpdates failed: {e}"); sleep(Duration::from_secs(2)).await; }
            }
        }
    }
    app.tasks.close();
    let count = app.tasks.len();
    if count > 0 {
        eprintln!("marsbot waiting for {count} in-flight task(s)");
    }
    let drained = match timeout(SHUTDOWN_TIMEOUT, app.tasks.wait()).await {
        Ok(()) => true,
        Err(_) => {
            eprintln!(
                "marsbot shutdown timed out after {} seconds; skipping WAL checkpoint",
                SHUTDOWN_TIMEOUT.as_secs()
            );
            false
        }
    };
    if drained {
        app.db.checkpoint().await?;
    }
    Ok(())
}
async fn dispatch(app: App, update: Update) -> Result<()> {
    match update.content {
        UpdateContent::Message(m) => app.handle_message(*m, false).await,
        UpdateContent::EditedMessage(m) => app.handle_message(*m, true).await,
        UpdateContent::ChannelPost(m) => app.handle_message(*m, false).await,
        UpdateContent::EditedChannelPost(m) => app.handle_message(*m, true).await,
        UpdateContent::CallbackQuery(q) => app.handle_callback(&q).await,
        UpdateContent::MyChatMember(change) => app.handle_membership(&change).await,
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    #[test]
    fn masking_keeps_edges() {
        assert_eq!(mask("1234567890"), "1234****7890");
    }
    #[test]
    fn parse_hash_roundtrip() {
        let h = [1, 2, 3, 4, 5, 6, 7, 8];
        assert_eq!(parse_hash(&hex(&h)), Some(h));
    }
    #[test]
    fn command_targeting() {
        assert_eq!(parse_command("/stat@mars", Some("mars")), Some("stat"));
        assert_eq!(parse_command("/stat@other", Some("mars")), None);
    }
    #[test]
    fn hamming_uses_xor() {
        assert_eq!(hamming([0; 8], [0, 0, 0, 0, 0, 0, 0, 1]), 1);
    }
    #[test]
    fn thread_count_uses_default_or_positive_value() {
        assert_eq!(parse_thread_count("THREADS", None, 4).unwrap(), 4);
        assert_eq!(parse_thread_count("THREADS", Some("7"), 4).unwrap(), 7);
    }
    #[test]
    fn thread_count_rejects_invalid_values() {
        for value in ["0", "-1", "abc", "184467440737095516160"] {
            let error = parse_thread_count("THREADS", Some(value), 4).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("THREADS must be a positive integer")
            );
        }
    }
    #[tokio::test]
    async fn database_preserves_counting_and_whitelists() {
        let path = env::temp_dir().join(format!(
            "marsbot-test-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        const POOL_SIZE: usize = 3;
        let db = Database::open(path.clone(), POOL_SIZE).await.unwrap();
        assert_eq!(db.pool.max_size(), POOL_SIZE);
        let hash = [7; 8];
        assert!(db.observe(-100, hash, 10).await.unwrap().previous.is_none());
        assert_eq!(
            db.observe(-100, hash, 11)
                .await
                .unwrap()
                .previous
                .unwrap()
                .count,
            1
        );
        assert_eq!(db.stats(-100).await.unwrap(), (1, 1));
        assert!(db.set_user_whitelist(-100, 42, true).await.unwrap());
        assert!(db.user_whitelisted(-100, 42).await.unwrap());
        db.set_image_whitelist(-100, hash, true).await.unwrap();
        assert!(db.observe(-100, hash, 12).await.unwrap().ignored);
        let archive = db.export_zip(-100).await.unwrap();
        let mut zip = zip::ZipArchive::new(File::open(&archive).unwrap()).unwrap();
        assert_eq!(zip.len(), 1);
        let mut csv = String::new();
        {
            let mut entry = zip.by_index(0).unwrap();
            assert_eq!(entry.name(), "mars-export--100.csv");
            assert_eq!(entry.compression(), CompressionMethod::Deflated);
            entry.read_to_string(&mut csv).unwrap();
        }
        let mut rows = csv.lines();
        assert_eq!(
            rows.next(),
            Some("group_id,pic_dhash,count,last_msg_id,in_whitelist")
        );
        assert_eq!(rows.next(), Some("-100,0707070707070707,2,11,1"));
        db.checkpoint().await.unwrap();
        drop(db);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&archive);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }
}
