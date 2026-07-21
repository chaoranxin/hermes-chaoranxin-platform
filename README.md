# hermes-chaoranxin-platform

超然信（Chaoranxin）↔ [Hermes Agent](https://github.com/NousResearch/hermes-agent) 平台插件。

官方 Hermes **不内置**本适配器。本仓库按 **方式 A（目录插件）** 分发：把内层 `chaoranxin/` 放到用户的 `~/.hermes/plugins/chaoranxin/`。

## 快速开始

```bash
# 1a. 只拷内层目录（不要拷本仓库根目录）
mkdir -p ~/.hermes/plugins
cp -R /path/to/hermes-chaoranxin-platform/chaoranxin ~/.hermes/plugins/chaoranxin

# 1b. 或下载仓库根目录的压缩包后解压
# tar xzf chaoranxin-hermes-plugin-1.0.0.tgz -C ~/.hermes/plugins/

# 2. 启用
hermes plugins enable chaoranxin

# 3. 配置（需要超然信签发的 rbt_* + API base）
hermes setup gateway   # 选择 Chaoranxin

# 4. 启动
hermes gateway start --foreground
hermes gateway status  # 期望: chaoranxin: connected
```

详细中文说明见 [`chaoranxin/README.md`](chaoranxin/README.md)。

## 仓库结构

```
hermes-chaoranxin-platform/
├── README.md
├── chaoranxin-hermes-plugin-1.0.0.tgz  # 可直接下载解压安装
└── chaoranxin/                         # ← 安装时只复制这一层
    ├── __init__.py
    ├── adapter.py
    ├── proto.py
    ├── plugin.yaml
    └── README.md
```

## 控制台侧（给每个 Hermes 用户）

1. 在超然信官方客户端为该用户**创建机器人**
2. 复制并安全交付 **`rbt_*` token**（通常只显示一次）
3. 告知 **`CHAORANXIN_API_BASE`**（IM HTTP 根，如 `https://api.xsign.co`）
4. 用户自备 LLM API Key，再按上文安装插件并启动 Gateway

协议细节见 IM 文档：`im/docs/ROBOT_THIRD_PARTY.md`（含 Hermes 专节）。

## 压缩包

仓库根目录跟踪 [`chaoranxin-hermes-plugin-1.0.0.tgz`](chaoranxin-hermes-plugin-1.0.0.tgz)（内含顶层目录 `chaoranxin/`）。发版时重新生成并提交：

```bash
COPYFILE_DISABLE=1 tar czf chaoranxin-hermes-plugin-1.0.0.tgz \
  --exclude='chaoranxin/__pycache__' \
  --exclude='*.pyc' \
  --exclude='.DS_Store' \
  chaoranxin/
```
