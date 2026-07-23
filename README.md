# hermes-chaoranxin-platform

超然信（Chaoranxin）↔ [Hermes Agent](https://github.com/NousResearch/hermes-agent) 平台插件。

官方 Hermes **不内置**本适配器。按 **方式 A（目录插件）** 安装：只把内层 `chaoranxin/` 放到 `~/.hermes/plugins/chaoranxin/`。

本仓库文档为**插件使用说明**（本文件 + [`chaoranxin/README.md`](chaoranxin/README.md)），不放开发/同步类规范。

## 快速开始

```bash
# 1a. 只拷内层目录（不要拷本仓库根目录）
mkdir -p ~/.hermes/plugins
cp -R /path/to/hermes-chaoranxin-platform/chaoranxin ~/.hermes/plugins/chaoranxin

# 1b. 或下载压缩包后解压
# tar xzf chaoranxin-hermes-plugin-1.0.0.tgz -C ~/.hermes/plugins/

# 2. 启用
hermes plugins enable chaoranxin

# 3. 配置（超然信签发的 rbt_* + API base）
hermes setup gateway   # 选择 Chaoranxin

# 4. 启动
hermes gateway start --foreground
hermes gateway status  # 期望: chaoranxin: connected
```

更细的安装、配置、能力说明见 [`chaoranxin/README.md`](chaoranxin/README.md)。

## 仓库结构

```
hermes-chaoranxin-platform/
├── README.md                           # 仓级使用说明
├── chaoranxin-hermes-plugin-1.0.0.tgz  # 可下载解压安装
└── chaoranxin/                         # ← 安装时只复制这一层
    ├── __init__.py
    ├── adapter.py
    ├── media.py
    ├── proto.py
    ├── plugin.yaml
    └── README.md
```

## 你需要准备什么

1. 在超然信官方客户端为该用户**创建机器人**
2. 拿到 **`rbt_*` token**（通常只显示一次）→ `CHAORANXIN_BOT_TOKEN`
3. IM HTTP 根地址 → `CHAORANXIN_API_BASE`（如 `https://api.xsign.co`）
4. 自备 LLM API Key（与超然信无关）

协议细节见 IM 文档：`im/docs/ROBOT_THIRD_PARTY.md`（含 Hermes 专节）。

## 压缩包

根目录 [`chaoranxin-hermes-plugin-1.0.0.tgz`](chaoranxin-hermes-plugin-1.0.0.tgz) 内含顶层目录 `chaoranxin/`。重新打包：

```bash
COPYFILE_DISABLE=1 tar czf chaoranxin-hermes-plugin-1.0.0.tgz \
  --exclude='chaoranxin/__pycache__' \
  --exclude='*.pyc' \
  --exclude='.DS_Store' \
  chaoranxin/
```
