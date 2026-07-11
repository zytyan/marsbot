use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
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
        ReplyParameters,
    },
    updates::{Update, UpdateContent},
};
use opencv::{
    core::{self, MatTraitConst, Vector},
    imgcodecs, imgproc,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use tokio::{signal, task::spawn_blocking, time::sleep};

const SCHEMA_VERSION: i64 = 1;
const GROUP_WAIT: Duration = Duration::from_millis(1500);

#[derive(Clone, Debug)]
struct Config {
    token: String,
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
    notify_socket: Option<String>,
    watchdog_usec: Option<u64>,
    watchdog_pid: Option<u32>,
}

#[derive(Clone)]
struct SystemdNotifier {
    enabled: bool,
    watchdog_usec: Option<u64>,
    watchdog_pid: Option<u32>,
}

impl SystemdNotifier {
    fn new(config: &Config) -> Self {
        Self {
            enabled: config.notify_socket.is_some(),
            watchdog_usec: config.watchdog_usec,
            watchdog_pid: config.watchdog_pid,
        }
    }

    async fn notify(&self, argument: String) {
        if !self.enabled {
            return;
        }
        let result = spawn_blocking(move || {
            Command::new("systemd-notify")
                .arg("--no-block")
                .arg(argument)
                .status()
        })
        .await;
        if !matches!(result, Ok(Ok(status)) if status.success()) {
            eprintln!("systemd notification failed; ensure systemd-notify is available");
        }
    }

    async fn status(&self, status: &str) {
        self.notify(format!("--status={status}")).await;
    }
    async fn ready(&self) {
        self.notify("--ready".into()).await;
    }

    fn start_watchdog(&self) {
        let Some(usec) = self.watchdog_usec else {
            return;
        };
        if self
            .watchdog_pid
            .is_some_and(|pid| pid != std::process::id())
            || !self.enabled
        {
            return;
        }
        let notifier = self.clone();
        tokio::spawn(async move {
            let interval = Duration::from_micros(usec / 2).max(Duration::from_secs(1));
            loop {
                sleep(interval).await;
                notifier.notify("--watchdog".into()).await;
            }
        });
    }
}

impl Config {
    fn load() -> Result<Self> {
        let get = |name: &str| env::var(name).ok().filter(|v| !v.is_empty());
        let token = get("BOT_TOKEN").ok_or_else(|| anyhow!("BOT_TOKEN is required"))?;
        let allow_bot_messages = get("ALLOW_BOT_MESSAGES")
            .as_deref()
            .map(parse_bool)
            .transpose()?
            .unwrap_or(false);
        let config = Self {
            token,
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
            notify_socket: get("NOTIFY_SOCKET"),
            watchdog_usec: get("WATCHDOG_USEC")
                .map(|v| v.parse())
                .transpose()
                .context("invalid WATCHDOG_USEC")?,
            watchdog_pid: get("WATCHDOG_PID")
                .map(|v| v.parse())
                .transpose()
                .context("invalid WATCHDOG_PID")?,
        };
        config.validate()?;
        config.print();
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
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
                "NOTIFY_SOCKET",
                self.notify_socket
                    .clone()
                    .unwrap_or_else(|| "<unset>".into()),
                false,
                "systemd notification socket",
            ),
            (
                "WATCHDOG_USEC",
                self.watchdog_usec
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "<unset>".into()),
                false,
                "systemd watchdog interval",
            ),
            (
                "WATCHDOG_PID",
                self.watchdog_pid
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "<unset>".into()),
                false,
                "systemd watchdog process",
            ),
        ];
        for (name, value, required, purpose) in rows {
            eprintln!("config {name}: value={value}; required={required}; purpose={purpose}");
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

fn mask(value: &str) -> String {
    let chars: Vec<_> = value.chars().collect();
    if chars.len() <= 8 {
        return "*".repeat(chars.len().max(3));
    }
    format!(
        "{}{}{}",
        chars[..4].iter().collect::<String>(),
        "*".repeat(chars.len() - 8),
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
    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(90));
    if let Some(proxy) = &config.bot_proxy {
        builder = builder
            .no_proxy()
            .proxy(reqwest::Proxy::all(proxy).context("invalid BOT_PROXY")?);
    }
    builder.build().context("build HTTP client")
}

#[derive(Clone)]
struct Database {
    path: Arc<PathBuf>,
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
    async fn open(path: PathBuf) -> Result<Self> {
        let db = Self {
            path: Arc::new(path),
        };
        let clone = db.clone();
        spawn_blocking(move || clone.initialize())
            .await
            .context("database initialization task")??;
        Ok(db)
    }

    fn connection(&self) -> Result<Connection> {
        let conn = Connection::open_with_flags(
            &*self.path,
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

    fn initialize(&self) -> Result<()> {
        let conn = self.connection()?;
        conn.execute_batch("\
            CREATE TABLE IF NOT EXISTS mars_info (
              group_id INTEGER NOT NULL, pic_dhash BLOB NOT NULL, count INTEGER NOT NULL DEFAULT 0 CHECK(count >= 0),
              last_msg_id INTEGER NOT NULL DEFAULT 0 CHECK(last_msg_id >= 0), in_whitelist INTEGER NOT NULL DEFAULT 0 CHECK(in_whitelist IN (0,1)),
              PRIMARY KEY(group_id,pic_dhash)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS fuid_to_dhash (fuid TEXT PRIMARY KEY NOT NULL, dhash BLOB NOT NULL) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS group_user_in_whitelist (group_id INTEGER NOT NULL, user_id INTEGER NOT NULL, PRIMARY KEY(group_id,user_id)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS mars_group_stat (group_id INTEGER PRIMARY KEY NOT NULL, image_count INTEGER NOT NULL DEFAULT 0 CHECK(image_count >= 0)) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS mars_stat_meta (key TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL) WITHOUT ROWID;")?;
        let version: i64 = conn.query_row(
            "SELECT COALESCE((SELECT value FROM mars_stat_meta WHERE key='schema_version'), 0)",
            [],
            |r| r.get(0),
        )?;
        if version < SCHEMA_VERSION {
            let tx = conn.unchecked_transaction()?;
            tx.execute("DELETE FROM mars_group_stat", [])?;
            tx.execute("INSERT INTO mars_group_stat(group_id,image_count) SELECT group_id,COUNT(*) FROM mars_info GROUP BY group_id", [])?;
            let groups: i64 = tx.query_row(
                "SELECT COUNT(*) FROM mars_group_stat WHERE group_id < 0",
                [],
                |r| r.get(0),
            )?;
            tx.execute("INSERT INTO mars_stat_meta(key,value) VALUES('group_count',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [groups])?;
            tx.execute("INSERT INTO mars_stat_meta(key,value) VALUES('schema_version',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [SCHEMA_VERSION])?;
            tx.commit()?;
        }
        Ok(())
    }

    async fn hash_for_file(&self, fuid: String) -> Result<Option<[u8; 8]>> {
        let db = self.clone();
        Ok(spawn_blocking(move || {
            let conn = db.connection()?;
            conn.query_row(
                "SELECT dhash FROM fuid_to_dhash WHERE fuid=?",
                [fuid],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .map(blob_to_hash)
            .transpose()
        })
        .await??)
    }

    async fn cache_file_hash(&self, fuid: String, hash: [u8; 8]) -> Result<()> {
        let db = self.clone();
        spawn_blocking(move || -> Result<()> {
            db.connection()?.execute(
                "INSERT INTO fuid_to_dhash(fuid,dhash) VALUES(?,?) ON CONFLICT(fuid) DO NOTHING",
                params![fuid, hash.to_vec()],
            )?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn user_whitelisted(&self, group_id: i64, user_id: u64) -> Result<bool> {
        let db = self.clone();
        let user_id = i64::try_from(user_id)?;
        Ok(spawn_blocking(move || -> Result<bool> { Ok(db.connection()?.query_row("SELECT EXISTS(SELECT 1 FROM group_user_in_whitelist WHERE group_id=? AND user_id=?)", params![group_id, user_id], |r| r.get::<_, i64>(0))? != 0) }).await??)
    }

    async fn observe(&self, group_id: i64, hash: [u8; 8], message_id: i32) -> Result<Observation> {
        let db = self.clone();
        Ok(spawn_blocking(move || -> Result<Observation> {
            let mut conn = db.connection()?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let existing = read_info(&tx, group_id, hash)?;
            if let Some(info) = &existing {
                if info.last_msg_id == message_id || info.in_whitelist { return Ok(Observation { previous: None, ignored: true }); }
            }
            let is_new = existing.is_none();
            let mut next = existing.clone().unwrap_or(MarsInfo { dhash: hash, count: 0, last_msg_id: 0, in_whitelist: false });
            let previous = (next.count > 0).then(|| next.clone());
            next.count += 1;
            next.last_msg_id = message_id;
            tx.execute("INSERT INTO mars_info(group_id,pic_dhash,count,last_msg_id,in_whitelist) VALUES(?,?,?,?,?) ON CONFLICT(group_id,pic_dhash) DO UPDATE SET count=excluded.count,last_msg_id=excluded.last_msg_id,in_whitelist=excluded.in_whitelist", params![group_id, hash.to_vec(), next.count, next.last_msg_id, i64::from(next.in_whitelist)])?;
            if is_new { increase_group_stat(&tx, group_id)?; }
            tx.commit()?;
            Ok(Observation { previous, ignored: false })
        }).await??)
    }

    async fn info(&self, group_id: i64, hash: [u8; 8]) -> Result<MarsInfo> {
        let db = self.clone();
        Ok(spawn_blocking(move || -> Result<MarsInfo> {
            Ok(
                read_info(&db.connection()?, group_id, hash)?.unwrap_or(MarsInfo {
                    dhash: hash,
                    count: 0,
                    last_msg_id: 0,
                    in_whitelist: false,
                }),
            )
        })
        .await??)
    }

    async fn set_image_whitelist(&self, group_id: i64, hash: [u8; 8], enabled: bool) -> Result<()> {
        let db = self.clone();
        spawn_blocking(move || -> Result<()> { let conn = db.connection()?; conn.execute("INSERT INTO mars_info(group_id,pic_dhash,count,last_msg_id,in_whitelist) VALUES(?,?,0,0,?) ON CONFLICT(group_id,pic_dhash) DO UPDATE SET in_whitelist=excluded.in_whitelist", params![group_id, hash.to_vec(), i64::from(enabled)])?; Ok(()) }).await??;
        Ok(())
    }

    async fn set_user_whitelist(&self, group_id: i64, user_id: u64, enabled: bool) -> Result<bool> {
        let db = self.clone();
        let user_id = i64::try_from(user_id)?;
        Ok(spawn_blocking(move || -> Result<bool> { let conn = db.connection()?; let n = if enabled { conn.execute("INSERT INTO group_user_in_whitelist(group_id,user_id) VALUES(?,?) ON CONFLICT DO NOTHING", params![group_id,user_id])? } else { conn.execute("DELETE FROM group_user_in_whitelist WHERE group_id=? AND user_id=?", params![group_id,user_id])? }; Ok(n != 0) }).await??)
    }

    async fn stats(&self, group_id: i64) -> Result<(i64, i64)> {
        let db = self.clone();
        Ok(spawn_blocking(move || -> Result<(i64,i64)> { let conn = db.connection()?; let groups = conn.query_row("SELECT COALESCE((SELECT value FROM mars_stat_meta WHERE key='group_count'),(SELECT COUNT(*) FROM mars_group_stat WHERE group_id < 0))", [], |r| r.get(0))?; let images = conn.query_row("SELECT COALESCE((SELECT image_count FROM mars_group_stat WHERE group_id=?),0)", [group_id], |r| r.get(0))?; Ok((groups,images)) }).await??)
    }

    async fn similar(&self, group_id: i64, hash: [u8; 8]) -> Result<Vec<(MarsInfo, u32)>> {
        let db = self.clone();
        Ok(spawn_blocking(move || -> Result<Vec<(MarsInfo, u32)>> {
            let conn = db.connection()?;
            let mut st = conn.prepare(
                "SELECT pic_dhash,count,last_msg_id,in_whitelist FROM mars_info WHERE group_id=?",
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
        .await??)
    }

    async fn export_csv(&self, group_id: i64) -> Result<String> {
        let db = self.clone();
        Ok(spawn_blocking(move || -> Result<String> { let conn = db.connection()?; let mut text = String::from("group_id,pic_dhash,count,last_msg_id,in_whitelist\n"); let mut st = conn.prepare("SELECT pic_dhash,count,last_msg_id,in_whitelist FROM mars_info WHERE group_id=? ORDER BY pic_dhash")?; let rows = st.query_map([group_id], |r| Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,i64>(1)?,r.get::<_,i32>(2)?,r.get::<_,i64>(3)?)))?; for row in rows { let (hash,count,msg,white)=row?; text.push_str(&format!("{group_id},{},{count},{msg},{white}\n", hex(&hash))); } Ok(text) }).await??)
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
    conn.query_row("SELECT pic_dhash,count,last_msg_id,in_whitelist FROM mars_info WHERE group_id=? AND pic_dhash=?", params![group_id,hash.to_vec()], |r| Ok(MarsInfo { dhash: blob_to_hash(r.get::<_,Vec<u8>>(0)?).map_err(to_sql_error)?, count:r.get(1)?,last_msg_id:r.get(2)?,in_whitelist:r.get::<_,i64>(3)? != 0 })).optional().map_err(Into::into)
}
fn increase_group_stat(conn: &rusqlite::Transaction<'_>, group_id: i64) -> Result<()> {
    let existed: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM mars_group_stat WHERE group_id=?",
            [group_id],
            |r| r.get(0),
        )
        .optional()?;
    conn.execute("INSERT INTO mars_group_stat(group_id,image_count) VALUES(?,1) ON CONFLICT(group_id) DO UPDATE SET image_count=image_count+1", [group_id])?;
    if existed.is_none() && group_id < 0 {
        conn.execute("INSERT INTO mars_stat_meta(key,value) VALUES('group_count',1) ON CONFLICT(key) DO UPDATE SET value=value+1", [])?;
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
    bot: Bot,
    http: reqwest::Client,
    config: Config,
    db: Database,
    self_id: u64,
    self_username: Option<String>,
    media_groups: Arc<Mutex<HashMap<String, Vec<PhotoEvent>>>>,
    export_locks: Arc<Mutex<HashMap<i64, bool>>>,
}

#[derive(Clone)]
struct PhotoEvent {
    chat: Chat,
    message_id: i32,
    user_id: Option<u64>,
    file_id: String,
    file_unique_id: String,
}

impl App {
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
        match (html, keyboard) {
            (false, None) => {
                self.bot
                    .send_message(
                        &SendMessageParams::builder()
                            .chat_id(ChatId::Integer(chat_id))
                            .text(text)
                            .reply_parameters(reply)
                            .build(),
                    )
                    .await?
            }
            (true, None) => {
                self.bot
                    .send_message(
                        &SendMessageParams::builder()
                            .chat_id(ChatId::Integer(chat_id))
                            .text(text)
                            .reply_parameters(reply)
                            .parse_mode(ParseMode::Html)
                            .build(),
                    )
                    .await?
            }
            (false, Some(k)) => {
                self.bot
                    .send_message(
                        &SendMessageParams::builder()
                            .chat_id(ChatId::Integer(chat_id))
                            .text(text)
                            .reply_parameters(reply)
                            .reply_markup(ReplyMarkup::InlineKeyboardMarkup(k))
                            .build(),
                    )
                    .await?
            }
            (true, Some(k)) => {
                self.bot
                    .send_message(
                        &SendMessageParams::builder()
                            .chat_id(ChatId::Integer(chat_id))
                            .text(text)
                            .reply_parameters(reply)
                            .parse_mode(ParseMode::Html)
                            .reply_markup(ReplyMarkup::InlineKeyboardMarkup(k))
                            .build(),
                    )
                    .await?
            }
        };
        Ok(())
    }

    async fn file_hash(&self, event: &PhotoEvent) -> Result<[u8; 8]> {
        if let Some(hash) = self.db.hash_for_file(event.file_unique_id.clone()).await? {
            return Ok(hash);
        }
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
        let hash = spawn_blocking(move || dhash(bytes)).await??;
        self.db
            .cache_file_hash(event.file_unique_id.clone(), hash)
            .await?;
        Ok(hash)
    }

    async fn handle_photo(&self, event: PhotoEvent) -> Result<Option<(PhotoEvent, MarsInfo)>> {
        if let Some(user_id) = event.user_id {
            if self.db.user_whitelisted(event.chat.id, user_id).await? {
                return Ok(None);
            }
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
            groups.entry(group.clone()).or_default().push(event);
            drop(groups);
            if first {
                let app = self.clone();
                let key = group.clone();
                tokio::spawn(async move {
                    sleep(GROUP_WAIT).await;
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
        let mut best: Option<(PhotoEvent, MarsInfo)> = None;
        for event in events {
            if let Some((event, previous)) = self.handle_photo(event).await? {
                if best.as_ref().is_none_or(|(_, p)| previous.count > p.count) {
                    best = Some((event, previous));
                }
            }
        }
        if let Some((event, previous)) = best {
            self.maybe_report(event.chat.id, previous.count).await;
            self.reply(
                event.chat.id,
                event.message_id,
                reply_text(&event.chat, &previous, true),
                true,
                None,
            )
            .await?;
        }
        Ok(())
    }

    async fn maybe_report(&self, group_id: i64, count: i64) {
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
            "help" | "start" => { if matches!(message.chat.type_field,ChatType::Private) || command=="help" { self.reply(chat_id,message.message_id,help_text(self.self_username.as_deref()),false,None).await?; } }
            "stat" => { let (groups,images)=self.db.stats(chat_id).await?; let user=message.from.as_ref(); let white=match user { Some(u)=>self.db.user_whitelisted(chat_id,u.id).await?, None=>false }; let name=user.map(|u|u.first_name.as_str()).unwrap_or("unknown"); self.reply(chat_id,message.message_id,format!("火星车当前一共服务了{groups}个群组\n当前群组ID: {chat_id}\n您是 {name}，您{}本群白名单当中\n本群一共记录了 {images} 张不同的图片\n火星车与您同在",if white{"在"}else{"不在"}),false,None).await?; }
            "pic_info" | "add_whitelist" | "remove_whitelist" => { let Some(event)=photo_reference(message) else { self.reply(chat_id,message.message_id,"火星车没有发现您引用了任何图片。请发送图片使用命令，或回复特定图片。",false,None).await?; return Ok(()); }; let hash=self.file_hash(&event).await?; let info=self.db.info(chat_id,hash).await?; match command { "pic_info" => { let text=format!("File unique id: {}\ndhash: {}\n在本群的火星次数:{}\n{}",event.file_unique_id,hex(&hash).to_ascii_uppercase(),info.count,if info.in_whitelist{"🙈 它在本群的火星白名单中"}else{"🟢 它不在本群的火星白名单当中"}); let button=InlineKeyboardButton::builder().text("查找DHASH相似图片").callback_data(format!("find:{}",hex(&hash))).build(); self.reply(chat_id,message.message_id,text,false,Some(InlineKeyboardMarkup::builder().inline_keyboard(vec![vec![button]]).build())).await?; }, "add_whitelist" => { self.db.set_image_whitelist(chat_id,hash,true).await?; self.reply(chat_id,message.message_id,if info.in_whitelist{"这张图片已经在白名单当中了"}else{"成功将图片加入白名单"},false,None).await?; }, _ => { self.db.set_image_whitelist(chat_id,hash,false).await?; self.reply(chat_id,message.message_id,if info.in_whitelist{"成功将图片移除白名单"}else{"这张图片并不在白名单中"},false,None).await?; } } }
            "add_me_to_whitelist" | "remove_me_from_whitelist" => if let Some(user)=&message.from { let added=self.db.set_user_whitelist(chat_id,user.id,command=="add_me_to_whitelist").await?; let text=match (command,added) { ("add_me_to_whitelist",true)=>"已将您加入白名单，您发的图片不会被处理。", ("add_me_to_whitelist",false)=>"您已经在本群白名单中。", ("remove_me_from_whitelist",true)=>"已将您移出白名单，火星车会继续工作。", _=>"您不在本群白名单中，火星车正在工作。" }; self.reply(chat_id,message.message_id,text,false,None).await?; }
            "mars_bot_welcome" => self.send_welcome(chat_id).await?,
            "export" => self.reply(chat_id,message.message_id,"想部署自己的火星车，又放不下当前数据？请使用 /ensure_marsbot_export 导出本聊天数据。",false,None).await?,
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
            self.db.set_image_whitelist(chat_id, hash, true).await?;
            self.answer_callback(&callback.id, "该图片已加入白名单")
                .await?;
        } else if let Some(hash) = data.strip_prefix("find:").and_then(parse_hash) {
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
        self.bot.send_message(&SendMessageParams::builder().chat_id(ChatId::Integer(chat_id)).text("欢迎使用火星车。\n本 bot 会检测群组中重复发送的普通图片；只保存群组 ID、图片唯一 ID、dHash、消息 ID 与白名单状态，不保存图片。\n它不检查文件形式图片、表情包、视频或 GIF。\n<a href=\"https://github.com/zytyan/pymarsbot\">原项目</a>").parse_mode(ParseMode::Html).build()).await?;
        Ok(())
    }
    async fn handle_membership(&self, update: &ChatMemberUpdated) -> Result<()> {
        if matches!(update.chat.type_field, ChatType::Private) {
            return Ok(());
        }
        match (&update.old_chat_member, &update.new_chat_member) {
            (_, ChatMember::Administrator(_)) => {
                self.bot.send_message(&SendMessageParams::builder().chat_id(ChatId::Integer(update.chat.id)).text("火星车的任何功能均不需要管理员权限，您无需将本 bot 设置为群组管理员。").build()).await?;
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
            if locks.contains_key(&chat_id) {
                true
            } else {
                locks.insert(chat_id, true);
                false
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
        let csv = self.db.export_csv(chat_id).await?;
        let path = spawn_blocking(move || make_archive(chat_id, csv)).await??;
        let result = self
            .bot
            .send_document(
                &SendDocumentParams::builder()
                    .chat_id(ChatId::Integer(chat_id))
                    .document(FileUpload::from(path.clone()))
                    .build(),
            )
            .await
            .map(|_| ());
        let _ = tokio::fs::remove_file(&path).await;
        if let Err(error) = result {
            self.export_locks
                .lock()
                .map_err(|_| anyhow!("export lock poisoned"))?
                .remove(&chat_id);
            return Err(error.into());
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

fn make_archive(chat_id: i64, csv: String) -> Result<PathBuf> {
    let root = env::temp_dir();
    let csv_path = root.join(format!("mars-export-{chat_id}.csv"));
    let archive = root.join(format!("mars-export-{chat_id}.csv.tar.gz"));
    std::fs::write(&csv_path, csv)?;
    let status = Command::new("tar")
        .arg("-zcf")
        .arg(&archive)
        .arg("-C")
        .arg(&root)
        .arg(csv_path.file_name().unwrap())
        .status()
        .context("run tar")?;
    let _ = std::fs::remove_file(&csv_path);
    if !status.success() {
        return Err(anyhow!("tar failed with {status}"));
    }
    Ok(archive)
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
        "/help{suffix} 显示本帮助信息\n/stat{suffix} 显示统计信息\n/pic_info{suffix} 获取图片信息\n/add_whitelist{suffix} 将图片添加到白名单\n/remove_whitelist{suffix} 将图片移除白名单\n/add_me_to_whitelist{suffix} 将用户加入群组白名单\n/remove_me_from_whitelist{suffix} 将用户移出群组白名单\n/export{suffix} 导出说明"
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

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load()?;
    let notifier = SystemdNotifier::new(&config);
    notifier.status("Initializing marsbot").await;
    let http = http_client(&config)?;
    let bot = Bot::builder()
        .api_url(config.api_url())
        .client(http.clone())
        .build();
    let me = bot.get_me().await?.result;
    let db = Database::open(config.database_path.clone()).await?;
    let app = App {
        bot,
        http,
        config,
        db,
        self_id: me.id,
        self_username: me.username,
        media_groups: Arc::new(Mutex::new(HashMap::new())),
        export_locks: Arc::new(Mutex::new(HashMap::new())),
    };
    eprintln!("marsbot started as id={}", app.self_id);
    notifier.ready().await;
    notifier.status("Running").await;
    notifier.start_watchdog();
    let mut offset: Option<i64> = None;
    loop {
        let params = updates_params(offset);
        tokio::select! { _=signal::ctrl_c()=> { eprintln!("marsbot stopping"); break; }, response=app.bot.get_updates(&params) => match response { Ok(result)=> { for update in result.result { offset=Some(i64::from(update.update_id)+1); let app=app.clone(); tokio::spawn(async move { if let Err(e)=dispatch(app,update).await { eprintln!("update failed: {e:#}"); } }); } }, Err(e)=> { eprintln!("getUpdates failed: {e}"); sleep(Duration::from_secs(2)).await; } } }
    }
    notifier.status("Stopping").await;
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
    #[test]
    fn masking_keeps_edges() {
        assert_eq!(mask("1234567890"), "1234**7890");
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
        let db = Database::open(path.clone()).await.unwrap();
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
        drop(db);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }
}
