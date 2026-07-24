# Chaoranxin Platform Adapter

A Hermes platform plugin for the **Chaoranxin (超然信)** custom
`rbt_*` WebSocket bot protocol.

---

## 中文：目录插件安装（方式 A）

官方 [NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent) **不包含**本插件。每人需自行安装插件目录，并使用**各自**的 `rbt_*` token 与各自的 Hermes Gateway。

### 本目录需要哪些文件

分发给用户时只需这 7 个文件（不要带 `__pycache__`）：

| 文件 | 作用 |
|------|------|
| `plugin.yaml` | 插件清单；缺少则 Hermes 扫不到 |
| `__init__.py` | 导出 `register` |
| `adapter.py` | 适配器主逻辑 |
| `proto.py` | WS 帧编解码 |
| `multimodal.py` | 入站 Multimodal parts 物化（下载图/音/视/文件） |
| `media.py` | 出站图片上传（`d.xsign.co` objectstorage） |
| `README.md` | 本说明 |

安装后目标路径：

```
~/.hermes/plugins/chaoranxin/
├── __init__.py
├── adapter.py
├── proto.py
├── multimodal.py
├── media.py
├── plugin.yaml
└── README.md
```

### 安装步骤

**1. 安装官方 Hermes**（若尚未安装）：

```bash
curl -fsSL https://raw.githubusercontent.com/NousResearch/hermes-agent/main/scripts/install.sh | bash
```

**2. 放入插件目录**：

从独立仓库只拷内层 `chaoranxin/`（不要拷仓库根目录）：

```bash
mkdir -p ~/.hermes/plugins
cp -R /path/to/hermes-chaoranxin-platform/chaoranxin ~/.hermes/plugins/chaoranxin
ls ~/.hermes/plugins/chaoranxin/   # 应看到上述 6 个文件
```

**3. 启用插件**（用户插件不会自动加载）：

```bash
hermes plugins enable chaoranxin
hermes plugins list    # 期望 chaoranxin 为 enabled
```

**4. 配置 LLM**（用户自备 API Key）：

```bash
hermes setup model
```

**5. 配置超然信凭证**（推荐向导）：

```bash
hermes setup gateway
# 选择 Chaoranxin (超然信)，填入 rbt_* 与 API base
```

或手动写入 `~/.hermes/.env`：

```bash
CHAORANXIN_BOT_TOKEN=rbt_xxxxxxxx
CHAORANXIN_API_BASE=https://api.xsign.co
```

并确认 `~/.hermes/config.yaml`：

```yaml
platforms:
  chaoranxin:
    enabled: true
```

`CHAORANXIN_HOME_CHANNEL` 无需手填 — Gateway 连接后从 `RobotLogin.data.owner` 自动写入。

**6. 启动与验证**：

```bash
hermes gateway start --foreground
hermes gateway status    # 期望: chaoranxin: connected
```

在超然信 App 私聊该机器人，应收到 Hermes 回复。发送 `/help` 可测斜杠命令。

### 你需要从超然信拿到的信息

| 项 | 说明 |
|----|------|
| `CHAORANXIN_BOT_TOKEN` | 官方客户端创建机器人后签发的 `rbt_*`（通常只显示一次） |
| `CHAORANXIN_API_BASE` | IM HTTP 根地址，如 `https://api.xsign.co`（用于节点发现） |
| LLM API Key | Hermes 模型调用，与超然信无关，用户自备 |

### 访问控制（可选）

默认允许所有用户私聊（`CHAORANXIN_ALLOW_ALL_USERS=true`）。白名单：

```bash
CHAORANXIN_ALLOW_ALL_USERS=false
CHAORANXIN_ALLOWED_USERS=uuid1,uuid2
```

机器人创建者（`RobotLogin.data.owner`）始终可发消息。

### 常见问题

| 现象 | 处理 |
|------|------|
| `hermes plugins list` 看不到 chaoranxin | 确认路径为 `~/.hermes/plugins/chaoranxin/plugin.yaml`，不是多嵌套一层或整仓拷贝 |
| 忘记 enable | 执行 `hermes plugins enable chaoranxin` |
| 收不到回复 / `not in allowlist` | 检查 allowlist；确认 token 对应该机器人 |
| `HTTP 401/403/404` | token 或 API base 错误；此类错误不会无限重连 |
| 多人共用一个 Gateway + 同一 token | **不建议** — 每人一个 bot、各自 Gateway |

---

## Status


**v1.1.0 — WebSocket 双向 (two-way) with auto node discovery**

| Capability | Status |
|---|---|
| Auto node discovery (`GET /im/api/v1/robot/servers`) | ✅ with 1× retry on transport / non-2xx |
| Inbound Multimodal (text / voice_url / audio_url / image_url / video_url / file) | ✅ **only** uplink shape — all user→bot messages are Multimodal; legacy Text/Markdown/Voice/Picture inbound is dropped |
| Outbound text messages | ✅ |
| Outbound Picture | ✅ 先上传再发图片消息（与文字同通道） |
| Outbound Article / Url / News | ✅ via `OutboundMsg.set_clazz()` (manual / programmatic) |
| Outbound Video / File / Voice | ❌ — not implemented yet |
| Outbound @-mentions | ✅ via `OutboundMsg.ats` |
| Reply quoting | ⚠️ **off by default** — opt in via `extra.send_quote: true` / `CHAORANXIN_SEND_QUOTE=true` |
| RobotLogin handshake (§3.3) | ✅ (robot uuid + owner captured into `_robot_uuid`) |
| Heartbeat (every 30s) | ✅ |
| Send-receipt matching (`Status` send receipt) | ✅ (legacy `Msg` + content types; server validation errors surfaced) |
| Exponential reconnect (1s → 60s cap) | ✅ |
| Fatal-on-handshake-error (401/403/404) | ✅ |
| HTTP push (`/bot/api/v1/message/push`) | ❌ — not implemented in v1 (out of scope: WS-only mode) |
| HTTP Hook callback (HMAC-SHA256) | ❌ — not implemented in v1 (out of scope: WS-only mode) |
| Tappable approval / clarify buttons | ❌ falls back to text (no interactive UI primitive in spec) |

## Wire Protocol

Authoritative bot protocol: upstream `ROBOT_THIRD_PARTY.md`.

Quick summary:

* The adapter resolves the WS URL on connect:
  - **Auto-discovery (recommended)**: call
    `GET {api_base}/im/api/v1/robot/servers` with the rbt_ token,
    pick the first node's `host` + `inter` + `robot` fields, dial
    `ws://{host}:{inter}{robot}`.
  - **Direct mode**: skip the lookup and dial
    `ws://{CHAORANXIN_HOST}{CHAORANXIN_ROBOT_PATH}` directly.  Use
    this for single-node deployments or to pin a specific node.
* Bearer token in `Authorization: Bearer rbt_*` (or `X-Robot-Token`)
  during the WS upgrade.  **No Login frame** — the server binds the
  token at the HTTP-level WS upgrade.
* Within milliseconds of the WS open, the server pushes a top-level
  `{"type":"RobotLogin","data":{"ok":true,"robot":"<robot-uuid>","owner":"<owner-uuid>","msg":""}}`
  frame (§3.3).  `data.robot` is the uuid every outbound `Msg`'s
  `from` must carry; `data.owner` is auto-written to
  `CHAORANXIN_HOME_CHANNEL` when unset (default cron target).
* Outbound (top-level `type` = content clazz, must match `data.clazz`):
  - Text — `{type:"Markdown", data:{from,to,clazz:"Markdown",content:{text},role,quote,ats,uuid}}`
    (`quote` is **omitted by default**; see `extra.send_quote` below)
  - Picture — upload first (`POST {CHAORANXIN_FILE_BASE}/objectstorage/upload`,
    default `https://d.xsign.co`), then
    `{type:"Picture", data:{clazz:"Picture", content:{smallurl,originurl,...}, ...}}`
    (`smallurl` and `originurl` must be the same `accessUrl`).
  - `Data<Heart>` — `{type:"Heart", data:{time:<ms>}}`
* Inbound:
  - `RobotLogin` — handshake binding (`ok`, `robot`, `owner`, `msg`)
  - `Status` — heartbeat echo / send receipt (`Heart` or content type /
    legacy `Msg` subtype; `status=100` accepted, `status=-1` rejected)
  - `RobotEvent` — user message.  Feishu-schema-2.0 envelope with
    `header.event_type = "im.message.receive_v1"`.
    **All uplink is Multimodal** (`msg_type: "multimodal"`,
    `content.parts` non-empty). Legacy `text` / `markdown` / `voice` /
    `picture` inbound is **not** accepted (logged and dropped).
    Image parts use `image_url`; the remote URL is passed through to
    Hermes (DingTalk-style, no local cache). Gateway text/vision
    routing fetches it. Missing or unsafe URLs get a visible failure
    note so the model does not invent image content. For reliable
    recognition with remote URLs, keep `model.supports_vision: false`
    or set `agent.image_input_mode: text` (native mode treats URLs as
    local paths and skips them).

### Inbound Multimodal parts

| Client capability | Entry | Part | Hermes |
|---|---|---|---|
| Plain text | Composer | `{ "type": "text", "text": "…" }` | user text |
| Hold-to-talk | Mic | `{ "type": "voice_url", "voice_url": { "url": "…", "size": N } }` | STT (`VOICE`) |
| Pick audio file | More panel | `{ "type": "audio_url", "audio_url": { "url": "…" } }` | attachment note, **no** STT |
| Image | More panel | `image_url` | remote URL → gateway vision (text mode) |
| Video / file | More panel | `video_url` / `file` | local path notes |

> **Direction asymmetry:** uplink (user→bot) is always Multimodal parts.
> Downlink (bot→user) still uses content-type frames (`Markdown`,
> `Picture`, …) — see outbound section above.

Example:

```json
{
  "msg_type": "multimodal",
  "content": {
    "parts": [
      { "type": "text", "text": "请分析这些材料" },
      { "type": "voice_url", "voice_url": { "url": "https://…/voice.ogg", "size": 12345 } },
      { "type": "audio_url", "audio_url": { "url": "https://…/a.mp3" } },
      { "type": "image_url", "image_url": { "url": "https://a", "detail": "auto" } },
      { "type": "video_url", "video_url": { "url": "https://c.mp4" } },
      { "type": "file", "file": { "url": "https://d.pdf", "filename": "a.pdf", "mime_type": "application/pdf" } }
    ]
  }
}
```

## Quick Start

The minimum configuration is the rbt_ token and the API base URL.
Everything else (WS host, port, path) is auto-discovered.

```bash
export CHAORANXIN_BOT_TOKEN="rbt_xxxxxxxxxxxxxxxx"
export CHAORANXIN_API_BASE="https://api.example.com"

hermes gateway start --foreground
hermes gateway status   # should show: chaoranxin: connected
```

To pin a specific node (skip the auto-discovery), set
`CHAORANXIN_HOST` instead of (or in addition to) `CHAORANXIN_API_BASE`:

```bash
export CHAORANXIN_BOT_TOKEN="rbt_xxxxxxxxxxxxxxxx"
export CHAORANXIN_HOST="wss://im-node-1.example.com:9000"
export CHAORANXIN_ROBOT_PATH="/robot"        # optional, default /robot
```

To send a cron job to Chaoranxin:

```bash
hermes cron add "daily ping" "0 9 * * *" \
  "good morning" --deliver chaoranxin --target chat_xxx
```

## Config

`config.yaml`:

```yaml
platforms:
  chaoranxin:
    enabled: true
    extra:
      api_base: "https://api.example.com"   # auto-discovery
      # host: "wss://im-node-1.example.com" # OR direct mode
      # robot_path: "/robot"                # only used in direct mode
      bot_token: "rbt_xxxxxxxxxxxxxxxx"
      # file_base: "https://d.xsign.co"         # Picture upload root (optional)
      # send_quote: false                       # omit reply quote by default
      # bot_id: "bot_001"                  # optional, pre-handshake fallback
      # send_quote: false                   # default: do NOT include
                                          # ``quote`` in outbound reply
                                          # frames (so the platform UI
                                          # won't render "Replying to
                                          # <msg_id>"). Set true to
                                          # restore the visual link.
```

`CHAORANXIN_*` env vars override `extra` (env wins).

## Architecture

```
plugins/platforms/chaoranxin/
├── __init__.py          # re-export register()
├── plugin.yaml          # manifest (requires_env, optional_env)
├── adapter.py           # ChaoranxinAdapter + register(ctx) entry
├── proto.py             # OutboundMsg / IncomingFrame / StatusFrame
│                        # / RobotEventFrame / NodeEndpoint / parse_node_list
└── README.md            # this file
tests/gateway/test_chaoranxin_plugin.py
```

## Implementation Notes

* **Auto node discovery** — on connect (and on reconnect after the
  cached node is invalidated by a handshake failure), the adapter
  calls `GET {api_base}/im/api/v1/robot/servers` with the rbt_ token,
  with **1× retry** on transport errors and non-2xx responses (1s
  backoff).  On success the first node is cached for the lifetime of
  the WS connection; a successful RobotLogin then clears the cache so
  a future reconnect re-discovers (catches a node-pool change).
* **No Login frame** — Bearer auth happens at the HTTP-level WS
  upgrade.  After the open, the server pushes a top-level `RobotLogin`
  frame; the adapter binds `data.robot` for subsequent sends and
  seeds `CHAORANXIN_HOME_CHANNEL` from `data.owner` when unset.
* **Robot uuid** — captured from `RobotLogin.data.robot`.  Used as
  `from` on every outbound `Msg`.  During the pre-handshake window,
  `CHAORANXIN_BOT_ID` (or `extra.bot_id`) is used as a fallback.
* **App-layer heartbeats** — WS protocol pings are disabled
  (`ping_interval=None`); a `{"type":"Heart","data":{"time":ms}}`
  frame is sent every `CHAORANXIN_HEARTBEAT_INTERVAL` seconds instead.
  Server idle timeout is 15 minutes; default heartbeat is 30s.
* **Reconnect** — exponential backoff (1s → 60s) with ±20% jitter.
  Handshake rejections (HTTP 401/403/404) are treated as **fatal** —
  they will not improve on retry, so the loop stops and a fatal error
  is reported.  Node discovery errors are NOT fatal — the cluster
  may be coming up, so we keep retrying through the backoff loop.
* **Send receipts** — outbound message frames get a `Status` receipt
  (`type` = legacy `Msg` or content clazz such as `Markdown`).
  `status=100` = server accepted; `status=-1` = server-side validation
  failed (`msg` carries the reason, e.g. `发送方必须为当前登录机器人`).
* **Dedup** — `event_id` keyed with a 24h TTL + 4096-entry hard cap,
  matching the openclaw convention used elsewhere in Hermes.
* **Inbound message classes** — uplink is Multimodal-only. Parts map
  to Hermes types: plain `text` → `TEXT`; sole `image_url` → `PHOTO`
  with the remote URL in `media_urls` (not downloaded by the plugin);
  sole `voice_url` → `VOICE`; mixed / `audio_url` / `video_url` /
  `file` → `TEXT` with media paths and/or attachment notes. Legacy
  `text` / `picture` / `voice` envelopes are dropped. The raw
  `RobotEvent` is kept on `MessageEvent.raw_message["robot_event"]`.

## Troubleshooting

* **Image recognition wrong / empty** — inbound images are remote
  URLs. Set `model.supports_vision: false` and/or
  `agent.image_input_mode: text` so Gateway runs `vision_analyze` on
  the URL. With `supports_vision: true` (native), HTTPS URLs are
  skipped as non-local paths.
* **`Node discovery failed` on startup** — the
  `GET /im/api/v1/robot/servers` call failed twice (after one 1s
  retry).  Check the API base URL, the token, and the network path
  to the cluster.  The adapter keeps retrying through the backoff
  loop, so transient failures recover automatically.
* **`ws not connected` on send** — the WS dropped and is
  reconnecting.  Check `~/.hermes/logs/gateway.log` for the cause.
* **`Chaoranxin handshake rejected: HTTP 401`** — token rejected.
  Check `CHAORANXIN_BOT_TOKEN`.  The adapter stops reconnecting on
  401/403/404 because retrying won't help.
* **`not logged in yet (RobotLogin handshake pending)` on send** —
  the WS is open but the RobotLogin frame hasn't arrived yet.  Either
  wait for the next connect cycle, or set `CHAORANXIN_BOT_ID` for an
  immediate fallback.
* **Inbound messages missing** — by default all users can DM the bot
  (`CHAORANXIN_ALLOW_ALL_USERS=true`).  To restrict, set
  `CHAORANXIN_ALLOW_ALL_USERS=false` and list uuids in
  `CHAORANXIN_ALLOWED_USERS` (the robot creator from `RobotLogin.owner`
  is always allowed when allow-all is off).
* **Cron jobs fail with "No live adapter"** — gateway must be connected
  so `RobotLogin` can populate `CHAORANXIN_HOME_CHANNEL` from `owner`.
  Start the gateway first; standalone cron send also needs the env var
  set (connect once, or set manually only when overriding).
