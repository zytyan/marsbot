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
