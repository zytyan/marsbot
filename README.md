# marsbot（Rust）

`marsbot` 是 Telegram 群组/频道的重复图片检测机器人。它下载普通图片计算 dHash，仅持久保存群组 ID、Telegram 图片唯一 ID、dHash、消息 ID、计数和白名单信息；不会保存原始图片。

程序可直接复用 Python 版生成的 `mars.db`，SQLite 启用 WAL 模式，允许多个阻塞工作线程并行读取。

## 构建

本项目使用仓库内的静态 OpenCV 安装目录。若该目录尚未生成，先构建最小 OpenCV：

```bash
cd opencv_static
./build.sh
cd ..
```

然后直接构建：

```bash
cargo build --release
```

`.cargo/config.toml` 会静态链接 `opencv_core`、`opencv_imgcodecs`、`opencv_imgproc`、`libjpeg-turbo`、bundled SQLite 以及 C++ 标准库 `libstdc++`。它不强制静态链接 glibc 和其他系统运行库。

OpenCV 和 Rust 最终链接默认使用 Clang。GitHub Actions 在 Debian 12
环境构建通用 Linux x86_64 产物，并拒绝任何要求高于 glibc 2.36 的二进制。

可用下列命令确认不会加载动态 OpenCV 或动态 C++ 标准库：

```bash
ldd target/release/marsbot | rg "libopencv_.*\.so|libstdc\+\+\.so"
```

无输出即表示这两类动态库未被加载。

## 运行

```bash
export BOT_TOKEN="你的 Bot Token"
cargo run --release
```

程序启动时会打印所有识别的环境变量、是否必需、默认值和用途。Token 及 URL 中的凭据会遮蔽中间内容。

## 配置

| 变量 | 是否必需 | 默认值 | 作用 |
| --- | --- | --- | --- |
| `BOT_TOKEN` | 是 | 无 | Telegram Bot Token |
| `BOT_API_URL` | 否 | `https://api.telegram.org/bot` | Bot API 前缀，必须以 `/bot` 结尾 |
| `BOT_FILE_URL` | 否 | `https://api.telegram.org/file/bot` | 文件 API 前缀，必须以 `/file/bot` 结尾 |
| `BOT_PROXY` | 否 | 无 | Telegram 请求专用代理，优先级最高 |
| `HTTP_PROXY`、`HTTPS_PROXY`、`NO_PROXY` | 否 | 无 | reqwest 使用的标准代理变量 |
| `DATABASE_PATH` | 否 | `mars.db` | SQLite 数据库路径 |
| `LOG_LEVEL` | 否 | `INFO` | 日志级别 |
| `SLOW_MESSAGE_ALERT_CHAT_ID` | 否 | 无（关闭私信，仍记慢日志） | 慢响应告警接收者的正整数私聊 ID；本次部署设置为 `199456161` |
| `REPORT_STAT_URL` | 否 | 无 | 重复图片统计上报地址 |
| `ALLOW_BOT_MESSAGES` | 否 | `false` | 是否处理其他 Bot 的消息；始终忽略自身消息 |
| `TOKIO_WORKER_THREADS` | 否 | `1` | Tokio 异步工作线程数，必须为正整数 |
| `TOKIO_MAX_BLOCKING_THREADS` | 否 | `4` | Tokio `spawn_blocking` 最大线程数，同时作为 SQLite 连接池上限，必须为正整数 |

## 本地 Telegram Bot API

`BOT_API_URL` 和 `BOT_FILE_URL` 均支持 HTTP 与 HTTPS。例如本地 API 服务：

```bash
export BOT_API_URL="http://127.0.0.1:8081/bot"
export BOT_FILE_URL="http://127.0.0.1:8081/file/bot"
```

切换云端到本地服务时，先停止云端轮询，再按 [Telegram 的 logOut 流程](https://core.telegram.org/bots/api#logout) 从云端退出；退出后 10 分钟内不能重新登录云端。切换前检查本地历史积压，避免重复处理。当前版本识别 `BOT_API_URL` 和 `BOT_FILE_URL`，不识别旧的 `BOT_BASE_URL`。本地 API 返回的文件需要对运行 marsbot 的用户可读。

当 `getFile` 返回 HTTP/HTTPS 地址时程序直接下载；返回绝对本地路径时直接读取该路径；返回相对路径时按 `BOT_FILE_URL` 下载。

## 功能

- 单图与相册分组重复检测；OpenCV JPEG 解码、灰度化、面积缩放和 dHash。
- 图片白名单、用户白名单、相似图片查询、统计、帮助和欢迎消息。
- CSV 打包导出和重复图片统计上报。
- 网络操作异步执行；OpenCV、SQLite、压缩导出在 Tokio blocking 线程池执行。

## 测试

```bash
cargo test
cargo build --release
```

推送与 `Cargo.toml` 版本一致的标签（例如 `v0.1.0`）会自动创建 GitHub
Release，并发布 `marsbot-0.1.0-linux-x86_64-glibc2.36.tar.gz` 及其 SHA-256
校验文件。

要进行群内实测，使用测试 Bot 启动程序并设置 `ALLOW_BOT_MESSAGES=true`，再由另一个 Bot 用 curl 向测试群发送命令或同一图片两次。生产 Bot 保持默认值 `false`，避免 Bot 之间形成循环。


## 每条消息的计时与慢响应告警

所有消息（含忽略、无回复、失败、编辑及频道消息）都会向 stderr 输出单行 JSON，systemd 部署可用 `journalctl -u marsbot.service -o cat` 查看。记录不受 `LOG_LEVEL` 过滤，不新增数据库明细表。`message_received` 表示 Bot API 返回该条消息，`message_completed` 表示完整业务处理结束；相册每个成员独立结算，包含聚合等待和相册处理时间。程序异常退出前已输出的收取日志仍可用于定位未完成消息。

日志定位字段包括 `update_id`、`update_type`、`chat_id`、`group_id`、`message_thread_id`、`message_id`、`media_group_id`、`message_url`、`file_id`、`file_unique_id`、`image_source_message_id` 和 `dhash`。普通图片选择 Telegram 最大尺寸版本；文档、音视频、贴纸等消息也记录已有文件 ID，但不计算 dHash；图片引用命令同时记录原图片的消息 ID。没有可处理图片、未得到 dHash 或没有回复时，对应字段为 `null`，不会为了日志额外下载图片。

所有 `*_at_ms` 字段为 UTC Unix 毫秒；Telegram 原始时间精度为秒，转换后尾数为 `000`。`telegram_message_at_ms` 和 `telegram_edit_at_ms` 保留原始消息/编辑时间；`received_at_ms` 为本机收取时间，`started_at_ms` 为任务实际开始时间，`completed_at_ms` 为处理完成时间。`replies` 保存每次业务回复的 Telegram `date`、消息 ID、thread ID、本机收到发送结果的时间及实际回复目标；`telegram_reply_at_ms` 为最后一次成功回复时间。发送文件同样视为回复，相册共享回复关联到每个成员。

耗时字段单位均为毫秒：

- `receive_delay_ms`：Telegram 事件到本机收取的时间差。
- `queue_ms`：收取到任务开始的等待时间。
- `processing_total_ms`：收取到处理完成的总耗时，包含 API 等待、图片读取/下载、排队和相册等待，使用单调时钟；不是设备 CPU 运算耗时。
- `get_file_ms`：`getFile` API 请求的累计等待耗时，包含在途请求的已等待时间，成功和失败都会计入；`get_file_called` 标识是否实际发起请求，缓存命中或尚未请求时为 `false`。
- `file_download_ms`：取得文件路径后读取本地图片或下载文件的累计耗时，与 `get_file_ms` 分开记录。
- `db_pool_wait_ms`：等待/创建 SQLite 连接的累计耗时。
- `db_blocking_queue_ms`：取得连接后，数据库任务等待 blocking 线程的累计耗时。
- `db_execution_ms`：数据库读写、结果读取和事务的累计执行时间，包含 SQLite 锁等待；导出的 CSV/ZIP 编码不计入其中。
- `response_latency_ms`：Telegram 事件到最后一次成功业务回复的时间差；无回复时计算到处理完成，未完成时计算到当前。未来时间产生的负延迟按零处理。

耗时与数据库累计计时使用单调时钟；业务回复时间来自 Telegram 返回对象，仍受两端时钟差和 Telegram 秒级精度影响。编辑消息的阈值起点使用 `edit_date`（缺失时回退原消息 `date`）；按钮回调从本机收取时间开始计时，不把旧消息年龄当作回调延迟。

严格超过 5 秒输出一次 `slow_message`；严格超过 60 秒输出 `slow_message_alert` 并异步私信告警。处理中也会检查，无须等待回复；收取时已积压超过一分钟会立即触发。每个收到的 update 在本进程内最多告警一次，重启或 Telegram 重新投递后的去重不持久化。无回复消息也按完成时间判断，正常忽略消息仍保留处理状态；Bot 自身发送的消息保留日志但禁止私信告警，避免递归。完成后的总耗时不包含告警发送。

启用私信：

```bash
export SLOW_MESSAGE_ALERT_CHAT_ID=199456161
```

接收者需要先与 Bot 建立私聊。私信仅针对包含普通图片或引用普通图片的消息；纯文字、文档、音视频和贴纸等无普通图片关联的消息仍保留 JSON 计时与慢日志，但不发送私信。私信使用中文分段排版，展示状态、阶段、处理总耗时（含外部等待）、独立的 getFile API 等待和图片读取/下载耗时、UTC 时间、消息链接、定位 ID、file_id 和 dHash；尚无回复或哈希时显示说明文字；旧版日志没有 getFile 计时时明确显示“未记录”，不会把未知值当作零。超过单条消息长度时分段发送。发送使用独立客户端（继承 Bot API 地址及代理配置）和最多 10 秒的总超时；成功记录 `slow_message_alert_sent`，失败记录 `slow_message_alert_failed`，不阻塞业务、不无限重试，也不把告警自身作为业务回复。关闭接收者配置只关闭私信，仍保留所有计时和慢日志。

测试全部使用 loopback 模拟 Telegram API，不向真实用户发送消息。
