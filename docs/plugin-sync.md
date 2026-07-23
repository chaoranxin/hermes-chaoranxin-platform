# 超然信插件：开发与双仓同步规范

**状态：** 现行（硬性）  
**适用范围：** 凡改动 Hermes 树内 `plugins/platforms/chaoranxin/` 的提交与发版  
**最后更新：** 2026-07-23

---

## 1. 双仓关系

| 角色 | 路径 |
|------|------|
| **开发树**（日常改代码） | Hermes Agent 检出：`plugins/platforms/chaoranxin/` |
| **分发仓**（给用户安装） | [`hermes-chaoranxin-platform`](/Users/mac/Desktop/XsignServer/hermes-chaoranxin-platform) 内层 `chaoranxin/` |

官方 Hermes **不内置**本插件。用户只从分发仓拷贝 `chaoranxin/` 到 `~/.hermes/plugins/chaoranxin/`。  
因此：**只改开发树、不同步分发仓 = 用户拿不到更新。**

```text
Hermes 开发树                          分发仓
plugins/platforms/chaoranxin/   ──同步──►  hermes-chaoranxin-platform/chaoranxin/
docs/chaoranxin/*.md（相关）      ──按需──►  hermes-chaoranxin-platform/docs/
```

---

## 2. 硬性要求

1. **每次**修改开发树中的超然信插件（含 `adapter.py` / `media.py` / `proto.py` / `plugin.yaml` / `README.md` / `__init__.py`）后，**必须**同步到分发仓并在分发仓 **git commit**。
2. 发图等能力须**插件自包含**，禁止为超然信去改 Hermes 核心（见 [`outbound-picture.md`](./outbound-picture.md) §1.2）。
3. 安装说明始终写「只拷内层 `chaoranxin/`」，不要让用户拷分发仓根目录。

分发仓绝对路径（本机约定）：

```text
/Users/mac/Desktop/XsignServer/hermes-chaoranxin-platform
```

远程示例：`https://git.minclouds.com/zhengxin/hermes-chaoranxin-platform.git`

---

## 3. 同步步骤（每次改完执行）

在 Hermes 仓库根目录：

```bash
SRC=plugins/platforms/chaoranxin
DST=/Users/mac/Desktop/XsignServer/hermes-chaoranxin-platform/chaoranxin

cp "$SRC"/{__init__.py,adapter.py,proto.py,media.py,plugin.yaml,README.md} "$DST/"

# 若改了规范文档，一并同步
mkdir -p /Users/mac/Desktop/XsignServer/hermes-chaoranxin-platform/docs
cp docs/chaoranxin/outbound-picture.md \
  /Users/mac/Desktop/XsignServer/hermes-chaoranxin-platform/docs/outbound-picture.md
cp docs/chaoranxin/plugin-sync.md \
  /Users/mac/Desktop/XsignServer/hermes-chaoranxin-platform/docs/plugin-sync.md
```

在分发仓内（可选重建安装包，发版时建议做）：

```bash
cd /Users/mac/Desktop/XsignServer/hermes-chaoranxin-platform

COPYFILE_DISABLE=1 tar czf chaoranxin-hermes-plugin-1.0.0.tgz \
  --exclude='chaoranxin/__pycache__' \
  --exclude='*.pyc' \
  --exclude='.DS_Store' \
  chaoranxin/

git add chaoranxin/ docs/ README.md chaoranxin-hermes-plugin-1.0.0.tgz
git commit -m "sync(chaoranxin): <简述本次从 Hermes 树同步的改动>"
# 需要时再: git push
```

分发仓 `chaoranxin/README.md` 若含指向 Hermes 树的相对链接，同步后应改成指向本仓 `docs/`（见分发仓既有约定）。

---

## 4. 分发文件清单（内层）

| 文件 | 必需 |
|------|------|
| `plugin.yaml` | ✅ |
| `__init__.py` | ✅ |
| `adapter.py` | ✅ |
| `proto.py` | ✅ |
| `media.py` | ✅（发图） |
| `README.md` | ✅ |

不要带 `__pycache__` / `.DS_Store`。

---

## 5. 自检

- [ ] 开发树改动已 `cp` 到分发仓 `chaoranxin/`
- [ ] 相关 `docs/chaoranxin/*` 已按需同步到分发仓 `docs/`
- [ ] 分发仓已 `git commit`（工作区干净或仅剩有意未提交项）
- [ ] 未引入对 Hermes 核心的超然信特判
- [ ] 用户安装路径仍是 `~/.hermes/plugins/chaoranxin/`（仅内层）

---

## 6. 相关文档

- 出站图片规范：[`outbound-picture.md`](./outbound-picture.md)
- 线协议：[`chaoranxin-platform.md`](./chaoranxin-platform.md)
- 插件说明：[`chaoranxin/README.md`](../chaoranxin/README.md)
- 分发仓根说明：[`README.md`](../README.md)
