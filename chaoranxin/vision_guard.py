"""Block redundant vision_analyze on inbound image_cache paths.

When Chaoranxin already downloaded an inbound image into Hermes
``image_cache`` / ``cache/images`` and Gateway attached it natively,
calling ``vision_analyze`` on that same path triggers the native
fast-path multimodal tool-result envelope. On custom OpenAI-compat
providers that drop images from tool messages, that path hallucinates.

This module scopes a ``pre_tool_call`` block to Chaoranxin sessions and
only to local paths under the inbound image cache dirs. Remote URLs and
non-cache paths (browser screenshots, etc.) still run normally.
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Any, Dict, Optional, Set

logger = logging.getLogger(__name__)

_PLATFORM_NAME = "chaoranxin"

_LOCK = threading.Lock()
_SESSION_IDS: Set[str] = set()

_BLOCK_MESSAGE = (
    "This inbound image is already inline in the user message. "
    "Answer directly using built-in vision; do not call vision_analyze "
    "for image_cache paths."
)


def _platform_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip().lower()


def _safe_session_id(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    sid = value.strip()
    if not sid or len(sid) > 512:
        return None
    return sid


def clear_tracked_sessions() -> None:
    """Test helper — wipe the in-process session set."""
    with _LOCK:
        _SESSION_IDS.clear()


def remember_session(session_id: Any) -> None:
    sid = _safe_session_id(session_id)
    if not sid:
        return
    with _LOCK:
        _SESSION_IDS.add(sid)


def forget_session(session_id: Any) -> None:
    sid = _safe_session_id(session_id)
    if not sid:
        return
    with _LOCK:
        _SESSION_IDS.discard(sid)


def is_tracked_session(session_id: Any) -> bool:
    sid = _safe_session_id(session_id)
    if not sid:
        return False
    with _LOCK:
        return sid in _SESSION_IDS


def _remember_if_chaoranxin(**kwargs: Any) -> None:
    if _platform_value(kwargs.get("platform")) != _PLATFORM_NAME:
        return
    remember_session(kwargs.get("session_id"))


def _inbound_image_cache_roots() -> list[Path]:
    """Resolved roots for inbound image caches (current + legacy)."""
    roots: list[Path] = []
    try:
        from gateway.platforms.base import get_image_cache_dir

        roots.append(get_image_cache_dir().resolve())
    except Exception:
        logger.debug("[chaoranxin] get_image_cache_dir failed", exc_info=True)
    try:
        from hermes_constants import get_hermes_home

        legacy = (get_hermes_home() / "image_cache").resolve()
        if legacy not in roots:
            roots.append(legacy)
    except Exception:
        logger.debug("[chaoranxin] legacy image_cache root failed", exc_info=True)
    return roots


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def is_inbound_image_cache_path(image_url: Any) -> bool:
    """True when *image_url* is a local file under an inbound image cache dir."""
    if not isinstance(image_url, str):
        return False
    raw = image_url.strip()
    if not raw:
        return False
    if raw.startswith(("http://", "https://", "data:")):
        return False
    if raw.startswith("file://"):
        raw = raw[len("file://") :]
    try:
        path = Path(os.path.expanduser(raw)).resolve()
    except (OSError, RuntimeError, ValueError):
        return False
    for root in _inbound_image_cache_roots():
        if _is_under(path, root):
            return True
    return False


def on_session_start(**kwargs: Any) -> None:
    _remember_if_chaoranxin(**kwargs)


def on_pre_llm_call(**kwargs: Any) -> None:
    # pre_tool_call has no platform kwarg; refresh tracking each turn.
    _remember_if_chaoranxin(**kwargs)


def on_session_finalize(**kwargs: Any) -> None:
    platform = _platform_value(kwargs.get("platform"))
    # Gateway sometimes omits platform on finalize; still drop our session.
    if platform and platform != _PLATFORM_NAME:
        return
    forget_session(kwargs.get("session_id"))


def on_pre_tool_call(**kwargs: Any) -> Optional[Dict[str, str]]:
    tool_name = str(kwargs.get("tool_name") or "")
    if tool_name != "vision_analyze":
        return None
    if not is_tracked_session(kwargs.get("session_id")):
        return None
    args = kwargs.get("args")
    if not isinstance(args, dict):
        return None
    if not is_inbound_image_cache_path(args.get("image_url")):
        return None
    logger.info(
        "[chaoranxin] blocking vision_analyze for inbound image_cache path "
        "(session=%s)",
        _safe_session_id(kwargs.get("session_id")),
    )
    return {"action": "block", "message": _BLOCK_MESSAGE}


def register_hooks(ctx: Any) -> None:
    """Wire vision-guard hooks onto a plugin context."""
    ctx.register_hook("on_session_start", on_session_start)
    ctx.register_hook("pre_llm_call", on_pre_llm_call)
    ctx.register_hook("pre_tool_call", on_pre_tool_call)
    ctx.register_hook("on_session_finalize", on_session_finalize)


__all__ = [
    "BLOCK_MESSAGE",
    "clear_tracked_sessions",
    "forget_session",
    "is_inbound_image_cache_path",
    "is_tracked_session",
    "on_pre_llm_call",
    "on_pre_tool_call",
    "on_session_finalize",
    "on_session_start",
    "register_hooks",
    "remember_session",
]

# Public alias for tests / README.
BLOCK_MESSAGE = _BLOCK_MESSAGE
