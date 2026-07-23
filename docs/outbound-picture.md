# 超然信出站图片规范（Hermes 插件）

**状态：** 现行  
**受众：** 插件维护者、二次分发者、对接超然信 IM 的开发者  
**权威协议：** 并列仓库 IM 文档 `im/docs/ROBOT_THIRD_PARTY.md` §6.2 / §6.3  
**实现位置：** [`chaoranxin/`](../chaoranxin/)（`media.py` + `adapter.py`）  
**最后更新：** 2026-07-23

---

## 1. 设计原则（硬性）

### 1.1 与文字同一通道

出站图片与出站文字在 Hermes↔超然信链路上是**同一种投递**：WebSocket `/robot` 上的明文 JSON 帧。

| 内容 | 顶层 `type` / `data.clazz` | `content` |
|------|---------------------------|-----------|
| 文本（推荐） | `Markdown` | `{ "text": "..." }` |
| 图片 | `Picture` | `{ "smallurl", "originurl", "width?", "height?" }` |

图片帧里携带的是 **HTTPS URL**，不是二进制。二进制只出现在**文件服务上传**步骤。

### 1.2 插件自包含，禁止改 Hermes 核心

发图能力**必须**全部落在本仓库 `chaoranxin/` 内：

- 允许改：`media.py`、`adapter.py`、`proto.py`、`plugin.yaml`、`README.md`、本仓库 `docs/`  
- **禁止**为发图去改：上游 Hermes 的 `tools/send_message_tool.py`、`gateway/platforms/base.py` 等核心文件  

理由：目录插件要能单独拷贝到 `~/.hermes/plugins/chaoranxin/`；核心硬编码平台名会破坏分发。

Gateway 会话回复发图走基类钩子 `send_image` / `send_image_file`（插件 override 即可），**不需要**在 `send_message_tool` 白名单里点名 `chaoranxin`。

---

## 2. 端到端流程

```text
本地文件或远程 http(s) URL
        │
        ▼
POST {file_base}/objectstorage/upload
  Authorization: Bearer rbt_*
  bizType=im
  accessLevel=PUBLIC
  file=<image bytes>
        │
        ▼  校验 code=0、accessLevel=PUBLIC、accessUrl 非空
data.accessUrl  (形如 https://d.xsign.co/oss/{uuid})
        │
        ▼
WS 发送 type=Picture，content.smallurl === content.originurl === accessUrl
```

---

## 3. 上传契约

| 项 | 规范值 |
|----|--------|
| 默认文件根 | `https://d.xsign.co` |
| 可覆盖 | `CHAORANXIN_FILE_BASE` 或 `extra.file_base` |
| 路径 | `POST {file_base}/objectstorage/upload` |
| `bizType` | **必须** `im` |
| `accessLevel` | **必须** `PUBLIC` |
| 鉴权 | `Authorization: Bearer rbt_*` |
| 发帧 URL | `smallurl === originurl === accessUrl` |

实现：[`chaoranxin/media.py`](../chaoranxin/media.py)。支持扩展名：`.jpg` `.jpeg` `.png` `.webp` `.gif`。

---

## 4. Adapter 行为

| 方法 | 行为 |
|------|------|
| `send_image_file` | 上传 → Picture 帧 |
| `send_image` | 已是本域 `/oss/...` 则直发，否则下载再上传 |
| caption | 另发 Markdown（Picture 无 caption 字段） |

---

## 5. 与上游 Hermes 开发树同步

开发时常在 Hermes 检出内的 `plugins/platforms/chaoranxin/` 改代码。**每次改完后**必须同步回本仓库并提交：

```bash
SRC=<hermes-agent>/plugins/platforms/chaoranxin
DST=<hermes-chaoranxin-platform>/chaoranxin
cp "$SRC"/{__init__.py,adapter.py,proto.py,media.py,plugin.yaml,README.md} "$DST/"
# 规范文档（若有更新）
cp <hermes-agent>/docs/chaoranxin/outbound-picture.md \
   <hermes-chaoranxin-platform>/docs/outbound-picture.md
# 按需重建 tgz 后 git commit
```

安装时仍只拷贝内层 `chaoranxin/`，不要拷仓库根目录。

---

## 6. 自检清单

- [ ] `bizType=im` + `accessLevel=PUBLIC` + `Bearer rbt_*`
- [ ] `smallurl` 与 `originurl` 同为 `accessUrl`
- [ ] 顶层 `type` 与 `data.clazz` 均为 `Picture`
- [ ] `from` 为 `RobotLogin.data.robot`
- [ ] 未改 Hermes 核心
- [ ] 分发包含 `media.py`
