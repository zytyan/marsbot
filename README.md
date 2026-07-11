# marsbot (Rust)

`marsbot` detects repeated Telegram photos per chat. It stores only dHash,
Telegram file unique IDs, message IDs, counters, and whitelist state in SQLite;
it never persists image data. The database is compatible with the Python
implementation's `mars.db` and uses WAL mode.

## Build and run

```bash
cargo build
export BOT_TOKEN='your-token'
cargo run
```

The application uses Frankenstein's asynchronous reqwest client for Telegram
Bot API calls. The preinstalled OpenCV configuration is used by the normal
Cargo build; only the crate's `imgcodecs` and `imgproc` features are enabled.

## Configuration

All recognised variables are printed at startup with their value, default,
required status, and purpose. Secrets are masked.

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `BOT_TOKEN` | yes | — | Telegram Bot token |
| `BOT_API_URL` | no | `https://api.telegram.org/bot` | API prefix, ending in `/bot` |
| `BOT_FILE_URL` | no | `https://api.telegram.org/file/bot` | file API prefix, ending in `/file/bot` |
| `BOT_PROXY` | no | — | proxy overriding system proxy variables |
| `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` | no | — | standard reqwest proxy settings |
| `DATABASE_PATH` | no | `mars.db` | SQLite database path |
| `LOG_LEVEL` | no | `INFO` | application log threshold |
| `REPORT_STAT_URL` | no | — | receives duplicate-image JSON statistics |
| `ALLOW_BOT_MESSAGES` | no | `false` | permits messages from other bots; self-messages remain ignored |
| `NOTIFY_SOCKET`, `WATCHDOG_USEC`, `WATCHDOG_PID` | no | — | systemd status and watchdog support |

Both Telegram URL prefixes accept `http://` and `https://`, so a local Bot API
server can be used, for example:

```bash
export BOT_API_URL=http://127.0.0.1:8081/bot
export BOT_FILE_URL=http://127.0.0.1:8081/file/bot
```

When a local server returns an absolute path from `getFile`, marsbot reads that
path directly. Remote URLs are downloaded directly; relative paths use
`BOT_FILE_URL`.
