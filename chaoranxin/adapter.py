"""Chaoranxin (超然信) platform adapter (Hermes plugin).

Implements the rbt_* WebSocket bot protocol from
``docs/chaoranxin/ROBOT_THIRD_PARTY.md``.

Quick reference:

* Connect to ``{host}{robot_path}`` with ``Authorization: Bearer <rbt_*>``
  (or ``X-Robot-Token``).  No Login frame is sent — the server binds the
  token at the HTTP-level WS upgrade and confirms with a top-level
  ``{type: "RobotLogin", data: {ok, robot, owner, msg}}`` frame (§3.3).
* ``data.robot`` on RobotLogin is the robot uuid every outbound ``Msg``'s
  ``from`` field must carry.  The adapter captures it on first receive
  and uses it for all subsequent sends.
* Outbound heartbeat is a ``{"type":"Heart","data":{"time":ms}}`` frame
  every 30s.
* User → bot messages arrive as ``RobotEvent`` envelopes with
  ``header.event_type = "im.message.receive_v1"``.
* ``Status(type=Markdown|Msg, status=100)`` is the receipt for each message we send.
  ``status=-1`` carries the reason in ``msg`` (e.g. ``发送方必须为当前登录机器人``).

Configuration (config.yaml)::

    platforms:
      chaoranxin:
        enabled: true
        extra:
          host: "wss://api.example.com"          # or CHAORANXIN_HOST env var
          robot_path: "/robot"                    # default /robot
          bot_id: "bot_001"                      # OPTIONAL fallback pre-handshake
          bot_token: "rbt_xxx"                   # or CHAORANXIN_BOT_TOKEN env var

Environment variables (env wins over ``extra``)::

    CHAORANXIN_BOT_TOKEN              Bearer token (starts with rbt_)
    CHAORANXIN_HOST                   WS host; robot_path appended
    CHAORANXIN_ROBOT_PATH             WS path; default /robot
    CHAORANXIN_BOT_ID                 Optional pre-handshake fallback for ``from``
    CHAORANXIN_ALLOWED_USERS          Allowlist (comma-separated sender IDs)
    CHAORANXIN_ALLOW_ALL_USERS        "true" by default; set "false" to restrict
    CHAORANXIN_HOME_CHANNEL           Default chat_id for cron delivery
    CHAORANXIN_HOME_CHANNEL_NAME      Display name for the home channel
    CHAORANXIN_RECONNECT_MAX_SECONDS  Reconnect backoff cap (default 60)
    CHAORANXIN_HEARTBEAT_INTERVAL     Heartbeat seconds (default 30, 0 = off)
    CHAORANXIN_MAX_MESSAGE_LENGTH     Per-message text cap (default 8000)
    CHAORANXIN_STARTUP_MESSAGE        Send owner a startup DM on connect (default on)
    CHAORANXIN_STARTUP_MESSAGE_TEXT   Custom startup message body
    CHAORANXIN_SEND_QUOTE             Set "true" to include the
                                      ``quote`` field on outbound reply
                                      frames so the platform UI shows
                                      "Replying to <msg_id>". Default
                                      is "false" (no quote).
    CHAORANXIN_FILE_BASE              Object-storage root for Picture
                                      uploads (default https://d.xsign.co).
"""

import asyncio
import json
import logging
import os
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

try:
    import websockets
    import websockets.exceptions

    WEBSOCKETS_AVAILABLE = True
except ImportError:
    websockets = None  # type: ignore[assignment]
    websockets_exceptions = None  # type: ignore[assignment]
    WEBSOCKETS_AVAILABLE = False

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

# Absolute imports (not ``from .proto``) — the plugin's test loader
# registers this module under ``plugin_adapter_chaoranxin`` at the
# top level, so a relative import would fail with
# ``ImportError: attempted relative import with no known parent package``.
from plugins.platforms.chaoranxin.media import (  # noqa: E402
    IMAGE_EXTS,
    build_localfile_content,
    build_localvideo_content,
    build_picture_content,
    build_voice_content,
    classify_media_path,
    is_file_service_oss_url,
    is_image_path,
    resolve_file_base,
    upload_local_file,
    upload_local_image,
)
from plugins.platforms.chaoranxin.proto import (  # noqa: E402
    EVENT_MESSAGE_RECEIVE,
    MSG_CLAZZ_LOCAL_FILE,
    MSG_CLAZZ_LOCAL_VIDEO,
    MSG_CLAZZ_MARKDOWN,
    MSG_CLAZZ_PICTURE,
    MSG_CLAZZ_VOICE,
    TYPE_ROBOT_LOGIN,
    IncomingFrame,
    NodeEndpoint,
    OutboundHeart,
    OutboundMsg,
    RobotEventFrame,
    RobotLoginFrame,
    StatusFrame,
    parse_node_list,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


DEFAULT_RECONNECT_MAX_SECONDS = 60.0
DEFAULT_HEARTBEAT_SECONDS = 30.0
DEFAULT_MAX_MESSAGE_LENGTH = 8000
DEFAULT_ROBOT_PATH = "/robot"
DEFAULT_STARTUP_MESSAGE = "超然信机器人已上线，Hermes Gateway 启动完成。"

# Node discovery — how long to wait for the API call before timing out,
# and how long to back off before the single retry on failure.
NODE_DISCOVERY_TIMEOUT_SECONDS = 8.0
NODE_DISCOVERY_RETRY_DELAY_SECONDS = 1.0

# How long :meth:`ChaoranxinAdapter.connect` waits for the RobotLogin
# handshake before reporting failure to the gateway.  Must stay below the
# gateway's default ``HERMES_GATEWAY_PLATFORM_CONNECT_TIMEOUT`` (30s).
DEFAULT_CONNECT_TIMEOUT_SECONDS = 25.0
# Log a loud warning if RobotLogin has not arrived this many seconds after
# the WebSocket opens — helps diagnose wrong CHAORANXIN_HOST vs API_BASE.
ROBOT_LOGIN_WARN_SECONDS = 15.0

DEDUP_WINDOW_SECONDS = 24 * 60 * 60      # 24h, matches openclaw convention
DEDUP_MAX_SIZE = 4096
INITIAL_BACKOFF_SECONDS = 1.0
BACKOFF_FACTOR = 2.0
BACKOFF_JITTER = 0.2                     # ±20%


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _env_bool(name: str, default: bool = True) -> bool:
    raw = _env(name).lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _coerce_send_quote(value: Any) -> bool:
    """Coerce the ``extra.send_quote`` config value to bool.

    Tolerates booleans, ints, and common truthy strings (``"true"`` /
    ``"1"`` / ``"yes"`` / ``"on"``).  Anything unrecognized defaults to
    ``False`` to match the documented default.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    return False


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if not raw:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _mask_token(token: str) -> str:
    """Return a credential-safe representation of ``token``.

    Format: ``"<prefix>_<first4>…<last4>"`` when the token is long enough,
    ``"<empty>"`` when missing, otherwise the original token in full.
    Used by :func:`_trace` so ``CHAORANXIN_TRACE=1`` never leaks the
    raw rbt_ secret into gateway.log.
    """
    if not token:
        return "<empty>"
    if len(token) <= 12 or "…" in token:
        return token
    return f"{token[:8]}…{token[-4:]}"


_TRACE_FMT = "%H:%M:%S"

# Touchfile fallback: works even when CHAORANXIN_TRACE is not in the
# gateway subprocess env (launchd / systemd / nohup commonly strip it).
# Re-resolved on every call so a live ``touch`` flips trace on at next
# event without restarting the gateway.
_TRACE_FILE = Path.home() / ".hermes" / "chaoranxin.trace"


def _is_trace_enabled() -> bool:
    """Resolve trace toggle at call time (not import time).

    Three independent triggers; any one flips it ON:

    1. ``CHAORANXIN_TRACE=1`` in the process env (re-read each call
       so a late-set env still works after plugin import).
    2. Touchfile exists at ``~/.hermes/chaoranxin.trace`` — covers
       launchd / nohup / systemd where the daemon never inherits
       the user's shell env.
    3. (reserved for future use)

    Returns ``False`` on any ``OSError`` touching the touchfile.
    """
    if os.getenv("CHAORANXIN_TRACE", "").lower() == "1":
        return True
    try:
        return _TRACE_FILE.exists()
    except OSError:
        return False


def _trace(category: str, message: str, **fields: Any) -> None:
    """Emit a structured full-process trace line.

    Opt-in via ``CHAORANXIN_TRACE=1`` *or* by the touchfile at
    ``~/.hermes/chaoranxin.trace``; a no-op otherwise.  Every call
    carries ``[HH:MM:SS.mmm][CATEGORY] message key=value ...`` so the
    stream can be ``grep``-ed and session traces reconstructed by
    interleaving timestamps.

    Category is one of: ``LIFECYCLE``, ``CONFIG``, ``HTTP``, ``WS``,
    ``RX``, ``TX``, ``HEART``, ``RECEIPT``, ``DISPATCH``, ``STATE``,
    ``ERROR``.  Free-form is fine — this is a debug aid, not a contract.
    """
    if not _is_trace_enabled():
        return
    now = datetime.now()
    ts = now.strftime(_TRACE_FMT) + f".{now.microsecond // 1000:03d}"
    parts = [f"[chaoranxin][TRACE {ts}][{category}] {message}"]
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, str):
            v = value
        else:
            v = repr(value)
        if len(v) > 240:
            v = v[:237] + "…"
        parts.append(f"{key}={v}")
    line = " ".join(parts)
    logger.info(line)


def _log_im_rx(raw: str) -> None:
    """Log an inbound WebSocket wire frame at INFO (full JSON body)."""
    if not isinstance(raw, str) or not raw:
        return
    nbytes = len(raw.encode("utf-8"))
    logger.info(
        "[chaoranxin] IM RX raw frame bytes=%d preview=%s",
        nbytes,
        raw,
    )
    preview = raw if len(raw) <= 240 else raw[:237] + "…"
    _trace("RX", "raw frame", bytes=nbytes, preview=preview)


def _log_im_tx(frame_json: str) -> None:
    """Outbound Msg wire frame logging — intentionally a no-op.

    Full-frame TX dumps (INFO + TRACE preview) leaked message bodies into
    gateway logs; keep call sites for potential future debug hooks but do
    not emit content here. Other TRACE categories are unchanged.
    """
    return


def _build_handshake_headers(token: str) -> Dict[str, str]:
    """Authorization header for the WS handshake.

    Per spec the token is sent in either ``Authorization: Bearer rbt_*``
    or ``X-Robot-Token: rbt_*``.  We use the Authorization form because
    it's the more widely-accepted convention.  URL Query is rejected by
    the server with HTTP 400 — never put the token there.
    """
    token = token.strip()
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


# Acceptable URL schemes for the WebSocket connection.
_ALLOWED_HOST_SCHEMES = frozenset({"ws://", "wss://"})


def _is_http_origin(url: str) -> bool:
    """True when ``url`` is an http(s) REST API origin (not a WS URL)."""
    u = (url or "").strip().lower()
    return u.startswith("http://") or u.startswith("https://")


def _ws_to_http_origin(ws_url: str) -> str:
    """Map a ws(s) origin to the matching http(s) REST API origin."""
    parsed = urlparse((ws_url or "").strip().rstrip("/"))
    scheme = (parsed.scheme or "").lower()
    if scheme == "wss":
        http_scheme = "https"
    elif scheme == "ws":
        http_scheme = "http"
    else:
        return (ws_url or "").strip().rstrip("/")
    if not parsed.netloc:
        return (ws_url or "").strip().rstrip("/")
    return f"{http_scheme}://{parsed.netloc}"


def _ws_host_looks_like_rest_api_gateway(host: str) -> bool:
    """True when a ws(s) URL is probably the REST API domain, not an IM node.

    Users routinely paste ``wss://api.example.com`` into ``CHAORANXIN_HOST``.
    Real IM nodes are discovered via ``GET /im/api/v1/robot/servers`` or
  pinned with an explicit non-default port (e.g. ``wss://im-node:9000``).
    """
    u = (host or "").strip().rstrip("/")
    lower = u.lower()
    if not lower.startswith(("ws://", "wss://")):
        return False
    parsed = urlparse(u)
    hostname = (parsed.hostname or "").lower()
    if not hostname:
        return False
    # Explicit port → user likely pinned a real IM node.
    if parsed.port is not None:
        return False
    return hostname.startswith("api.") or hostname == "api"


def _normalize_endpoints(host: str, api_base: str) -> Tuple[str, str]:
    """Coerce REST API origins out of ``CHAORANXIN_HOST`` into ``api_base``.

    Per ROBOT_THIRD_PARTY §2.2, ``https://{your-origin}`` is only for
    ``GET /im/api/v1/robot/servers`` — never dial it as WebSocket.
    Users often paste the REST base into ``CHAORANXIN_HOST`` by mistake,
    including as ``wss://api.example.com`` (same hostname, wrong scheme).
    """
    host = (host or "").strip().rstrip("/")
    api_base = (api_base or "").strip().rstrip("/")

    if host and _is_http_origin(host):
        if not api_base:
            api_base = host
        elif api_base.rstrip("/").lower() != host.rstrip("/").lower():
            logger.warning(
                "[chaoranxin] ignoring CHAORANXIN_HOST=%r (HTTP REST origin); "
                "using CHAORANXIN_API_BASE=%r for node discovery",
                host,
                api_base,
            )
        host = ""

    if host and _ws_host_looks_like_rest_api_gateway(host):
        coerced = _ws_to_http_origin(host)
        if not api_base:
            api_base = coerced
            logger.info(
                "[chaoranxin] treating CHAORANXIN_HOST=%r as REST api_base=%r "
                "(not a direct WebSocket IM node); will call GET "
                "/im/api/v1/robot/servers",
                host,
                api_base,
            )
        elif api_base.rstrip("/").lower() != coerced.rstrip("/").lower():
            logger.warning(
                "[chaoranxin] ignoring CHAORANXIN_HOST=%r (REST API gateway); "
                "using CHAORANXIN_API_BASE=%r for node discovery",
                host,
                api_base,
            )
        else:
            logger.info(
                "[chaoranxin] ignoring CHAORANXIN_HOST=%r — same REST origin "
                "as CHAORANXIN_API_BASE; using node discovery",
                host,
            )
        host = ""

    return host, api_base


def _host_scheme_error(host: str) -> Optional[str]:
    """Return a human-readable error if ``host`` is not a valid WS URL."""
    if not host:
        return "CHAORANXIN_HOST is empty"
    normalized = host.strip().rstrip("/").lower()
    if not (normalized.startswith("ws://") or normalized.startswith("wss://")):
        if _is_http_origin(host):
            return (
                f"CHAORANXIN_HOST must be a WebSocket URL (ws:// or wss://), "
                f"not {host!r}. HTTP origins belong in CHAORANXIN_API_BASE "
                f"for GET /im/api/v1/robot/servers node discovery."
            )
        return (
            f"CHAORANXIN_HOST must start with ws:// or wss://, "
            f"got {host!r}"
        )
    return None


def _api_base_scheme_error(api_base: str) -> Optional[str]:
    """Return a human-readable error if ``api_base`` is not a valid HTTP origin."""
    if not api_base:
        return "CHAORANXIN_API_BASE is empty"
    normalized = api_base.strip().rstrip("/").lower()
    if not (
        normalized.startswith("http://") or normalized.startswith("https://")
    ):
        if normalized.startswith("ws://") or normalized.startswith("wss://"):
            suggestion = api_base.strip().rstrip("/").replace("ws://", "http://", 1).replace("wss://", "https://", 1)
            return (
                f"CHAORANXIN_API_BASE must start with http:// or https://, "
                f"got {api_base!r}. Did you mean {suggestion}?"
            )
        return (
            f"CHAORANXIN_API_BASE must start with http:// or https://, "
            f"got {api_base!r}"
        )
    return None


def _node_list_url(api_base: str) -> str:
    """Build the node-list API URL from an HTTP origin.

    Per spec §2.1 the path is ``/im/api/v1/robot/servers``.  Tolerant of
    origins with or without trailing slashes.
    """
    return f"{api_base.strip().rstrip('/')}/im/api/v1/robot/servers"


async def _fetch_node_list(
    api_base: str,
    token: str,
    *,
    timeout: float = NODE_DISCOVERY_TIMEOUT_SECONDS,
    retry_delay: float = NODE_DISCOVERY_RETRY_DELAY_SECONDS,
) -> Tuple[Optional[List[NodeEndpoint]], Optional[str]]:
    """Call ``GET {api_base}/im/api/v1/robot/servers`` and parse the result.

    Per spec the request uses ``Authorization: Bearer <rbt_*>``.  We
    retry once after ``retry_delay`` seconds on transport errors and
    non-2xx responses — cluster nodes are sometimes slow to come up
    right after a deploy, and a 1-second backoff covers the common
    case without introducing noticeable lag for the user.

    Returns ``(nodes, error)``: ``nodes`` is a non-empty list on
    success or ``None`` on failure; ``error`` is a human-readable
    string when ``nodes`` is ``None``.  Callers treat ``(None, error)``
    as fatal.
    """
    if not HTTPX_AVAILABLE:
        return None, "chaoranxin node discovery requires httpx"
    if not api_base or not token:
        return None, "chaoranxin node discovery: missing api_base or token"
    scheme_err = _api_base_scheme_error(api_base)
    if scheme_err:
        return None, scheme_err

    url = _node_list_url(api_base)
    headers = {"Authorization": f"Bearer {token}"}
    last_error: Optional[str] = None
    logger.info("[chaoranxin] node discovery: GET %s", url)
    _trace(
        "HTTP",
        "GET /im/api/v1/robot/servers enter",
        url=url,
        token=_mask_token(token),
        timeout_s=timeout,
    )

    for attempt in (1, 2):
        _trace("HTTP", "node discovery attempt", attempt=attempt, url=url)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.get(url, headers=headers)
        except Exception as exc:
            last_error = f"node list request failed: {exc}"
            _trace(
                "HTTP",
                "node discovery transport error",
                attempt=attempt,
                error=str(exc),
            )
            if attempt == 1:
                logger.warning(
                    "[chaoranxin] node discovery: %s — retrying in %.1fs",
                    last_error,
                    retry_delay,
                )
                await asyncio.sleep(retry_delay)
                continue
            logger.error("[chaoranxin] node discovery failed: %s", last_error)
            return None, last_error

        _trace(
            "HTTP",
            "node discovery response",
            attempt=attempt,
            http_status=resp.status_code,
        )
        if resp.status_code >= 300:
            body_preview = (resp.text[:200] if resp.text else "<empty body>")
            last_error = (
                f"node list HTTP {resp.status_code}: "
                f"{body_preview}"
            )
            _trace(
                "HTTP",
                "node discovery non-2xx",
                attempt=attempt,
                http_status=resp.status_code,
                body_preview=body_preview,
            )
            if attempt == 1:
                logger.warning(
                    "[chaoranxin] node discovery: %s — retrying in %.1fs",
                    last_error,
                    retry_delay,
                )
                await asyncio.sleep(retry_delay)
                continue
            logger.error("[chaoranxin] node discovery failed: %s", last_error)
            return None, last_error

        try:
            payload = resp.json()
        except Exception as exc:
            err = f"node list response not JSON: {exc}"
            logger.error("[chaoranxin] node discovery failed: %s", err)
            _trace(
                "ERROR",
                "node discovery JSON parse failed",
                attempt=attempt,
                error=str(exc),
            )
            return None, err

        nodes = parse_node_list(payload)
        if not nodes:
            err = "node list response had no usable nodes"
            logger.error("[chaoranxin] node discovery failed: %s", err)
            _trace(
                "ERROR",
                "node discovery returned no usable nodes",
                attempt=attempt,
                payload_preview=str(payload)[:200] if payload else None,
            )
            return None, err

        logger.info(
            "[chaoranxin] node discovery: HTTP %s — %d node(s)",
            resp.status_code,
            len(nodes),
        )
        _trace(
            "HTTP",
            "node discovery ok",
            attempt=attempt,
            http_status=resp.status_code,
            node_count=len(nodes),
        )
        for idx, node in enumerate(nodes[:5]):
            try:
                ws_target = node.ws_url()
            except ValueError:
                ws_target = "(invalid)"
            logger.info(
                "[chaoranxin]   node[%d] host=%s proto=%s port=%s robot=%s → %s",
                idx,
                node.host,
                node.proto,
                node.inter_or_port,
                node.robot,
                ws_target,
            )
            _trace(
                "HTTP",
                "node discovered",
                index=idx,
                host=node.host,
                proto=node.proto,
                port=node.inter_or_port,
                robot=node.robot,
                ws_url=ws_target,
            )
        return nodes, None

    # Defensive — should be unreachable given the loop structure.
    return None, last_error or "node list fetch failed for unknown reason"


def check_requirements() -> bool:
    """Pre-flight gate used by the plugin registry.

    Returns ``True`` only when the minimum viable config is present.
    Two configurations are supported:

    1. **Auto-discovery** (recommended): ``CHAORANXIN_BOT_TOKEN`` +
       ``CHAORANXIN_API_BASE`` — the adapter calls
       ``GET {api_base}/im/api/v1/robot/servers`` on connect and dials
       the first node returned.
    2. **Direct**: ``CHAORANXIN_BOT_TOKEN`` + ``CHAORANXIN_HOST`` —
       skips the API call and dials ``{host}{robot_path}`` directly.
       Use this for single-node deployments or for users who want to
       pin a specific node.

    ``CHAORANXIN_BOT_ID`` is optional in both modes — the real robot
    uuid is acquired from the RobotLogin Status handshake.
    """
    if not WEBSOCKETS_AVAILABLE:
        _trace("CHECK", "websockets package missing")
        return False
    if not _env("CHAORANXIN_BOT_TOKEN"):
        _trace("CHECK", "missing CHAORANXIN_BOT_TOKEN")
        return False
    host, api_base = _normalize_endpoints(
        _env("CHAORANXIN_HOST"), _env("CHAORANXIN_API_BASE")
    )
    if host:
        _trace(
            "CHECK",
            "mode=direct host=present api_base=present" if api_base else "mode=direct host=present",
            host=host,
        )
        if _host_scheme_error(host):
            logger.error("[chaoranxin] %s", _host_scheme_error(host))
            return False
        return True
    if api_base:
        _trace("CHECK", "mode=auto-discovery", api_base=api_base)
        scheme_err = _api_base_scheme_error(api_base)
        if scheme_err:
            logger.error("[chaoranxin] %s", scheme_err)
            return False
        return True
    _trace("CHECK", "missing endpoint (set CHAORANXIN_HOST or CHAORANXIN_API_BASE)")
    return False


def validate_config(config) -> bool:
    """Validate that the configured Chaoranxin platform has the minimum set.

    Both ``extra.host`` (direct) and ``extra.api_base`` (auto-discovery)
    are accepted, as well as the corresponding env-var fallbacks.
    """
    extra = getattr(config, "extra", {}) or {}
    token = extra.get("bot_token") or _env("CHAORANXIN_BOT_TOKEN")
    host, api_base = _normalize_endpoints(
        extra.get("host") or _env("CHAORANXIN_HOST"),
        extra.get("api_base") or _env("CHAORANXIN_API_BASE"),
    )
    if not token:
        _trace("CHECK", "validate_config: missing token")
        return False
    if host:
        err = _host_scheme_error(host)
        _trace("CHECK", "validate_config(host)", ok=err is None, err=err)
        return err is None
    if api_base:
        err = _api_base_scheme_error(api_base)
        _trace("CHECK", "validate_config(api_base)", ok=err is None, err=err)
        return err is None
    _trace("CHECK", "validate_config: missing endpoint")
    return False


def is_connected(config) -> bool:
    """``gateway status`` connectivity check — same shape as ``validate_config``."""
    return validate_config(config)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class ChaoranxinAdapter(BasePlatformAdapter):
    """Chaoranxin rbt_* WebSocket adapter.

    Holds a single persistent WebSocket connection.  Outbound frames are
    ``{"type":"Markdown","data":{...}}`` carrying ``from=<robot-uuid>`` (the
    uuid acquired from the RobotLogin Status handshake).  Inbound frames
    are dispatched by top-level ``type``: ``Status`` updates the
    login / heartbeat / send-receipt state, ``RobotEvent`` delivers user
    messages through :meth:`_dispatch_robot_event`.

    v1 supports the full set of message classes documented in §6 of the
    spec — Text / Picture / At / Article / Url / News.  Inbound
    non-text msg_types (Picture, Article, ...) are forwarded as
    ``MessageType.PHOTO`` / ``MessageType.DOCUMENT`` etc. with the raw
    content dict attached for the agent to inspect.

    Allowlist gating runs *after* dedup but *before* ``handle_message``,
    so an unauthorized sender never wakes the agent.  Echo from the
    server carrying our own robot uuid is dropped at the envelope
    boundary.
    """

    SUPPORTS_MESSAGE_EDITING = False
    MAX_MESSAGE_LENGTH = DEFAULT_MAX_MESSAGE_LENGTH

    def __init__(self, config: PlatformConfig):
        # ``Platform("chaoranxin")`` is resolved by ``Platform._missing_``
        # which scans ``plugins/platforms/`` for bundled plugin names.
        platform = Platform("chaoranxin")
        super().__init__(config=config, platform=platform)

        extra = config.extra or {}
        self._bot_token: str = (
            extra.get("bot_token") or _env("CHAORANXIN_BOT_TOKEN", "")
        )
        raw_host = extra.get("host") or _env("CHAORANXIN_HOST", "")
        raw_api = extra.get("api_base") or _env("CHAORANXIN_API_BASE", "")
        self._host, self._api_base = _normalize_endpoints(raw_host, raw_api)
        if raw_host and not self._host and self._api_base:
            if _is_http_origin(raw_host) or _ws_host_looks_like_rest_api_gateway(raw_host):
                logger.info(
                    "[chaoranxin] endpoint normalized to api_base=%r "
                    "(node discovery via GET /im/api/v1/robot/servers)",
                    self._api_base,
                )
        self._robot_path: str = (
            (extra.get("robot_path") or _env("CHAORANXIN_ROBOT_PATH", DEFAULT_ROBOT_PATH))
            .strip() or DEFAULT_ROBOT_PATH
        )
        if not self._robot_path.startswith("/"):
            self._robot_path = "/" + self._robot_path

        # Optional pre-handshake fallback for ``from``.  The real robot
        # uuid is acquired from the RobotLogin Status; this is only used
        # during the window between connect() and the first RobotLogin
        # receipt, if a send happens to fire in that window.
        self._bot_id_fallback: str = (
            extra.get("bot_id") or _env("CHAORANXIN_BOT_ID", "")
        )

        self._reconnect_max_seconds: float = _env_float(
            "CHAORANXIN_RECONNECT_MAX_SECONDS", DEFAULT_RECONNECT_MAX_SECONDS
        )
        self._heartbeat_interval: float = _env_float(
            "CHAORANXIN_HEARTBEAT_INTERVAL", DEFAULT_HEARTBEAT_SECONDS
        )
        self._max_message_length: int = _env_int(
            "CHAORANXIN_MAX_MESSAGE_LENGTH", DEFAULT_MAX_MESSAGE_LENGTH
        )
        # File service root for Picture uploads (default https://d.xsign.co).
        self._file_base: str = resolve_file_base(extra)

        # Allowlist — open by default; set CHAORANXIN_ALLOW_ALL_USERS=false
        # and/or CHAORANXIN_ALLOWED_USERS to restrict.
        self._allow_all: bool = _env_bool("CHAORANXIN_ALLOW_ALL_USERS", default=True)
        self._allowed_users: List[str] = [
            s.strip() for s in _env("CHAORANXIN_ALLOWED_USERS", "").split(",") if s.strip()
        ]

        # Reply quote — when False (default), the adapter does NOT set the
        # ``quote`` field on outbound Data<Msg> frames, so the platform UI
        # does not render replies as "Replying to <msg_id>". The
        # chaoranxin protocol still supports quoting; users who want the
        # visual link can opt back in via extra.send_quote or
        # CHAORANXIN_SEND_QUOTE.
        send_quote_raw = extra.get("send_quote")
        if send_quote_raw is None:
            self._send_quote: bool = _env_bool("CHAORANXIN_SEND_QUOTE", default=False)
        else:
            self._send_quote = _coerce_send_quote(send_quote_raw)

        # Connection state
        self._ws: Optional[Any] = None
        self._listen_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        # Robot uuid from the RobotLogin Status handshake.  Until this
        # arrives, outbound sends are rejected with ``not_logged_in``.
        self._robot_uuid: str = ""
        # Creator uuid from RobotLogin — always authorized to DM the bot
        # even when CHAORANXIN_ALLOWED_USERS is empty.
        self._robot_owner: str = ""

        # Cached node from the last successful node-list discovery.
        # Reused on reconnect (avoids re-hitting the API on every retry).
        self._cached_node: Optional[NodeEndpoint] = None

        # Pending send receipts — keyed by outgoing msg_id; Status(type=Msg)
        # receipts from the server resolve them.  Used to surface
        # server-side validation errors (status=-1) to the agent.
        self._pending_receipts: "OrderedDict[str, asyncio.Future]" = OrderedDict()

        # Dedup cache: event_id -> last_seen_unix
        self._seen_event_ids: "OrderedDict[str, float]" = OrderedDict()

        # Set by :meth:`connect` and completed by RobotLogin (or fatal error).
        self._handshake_done: Optional[asyncio.Future] = None
        self._login_watchdog_task: Optional[asyncio.Task] = None
        self._startup_message_sent: bool = False
        self._startup_message_task: Optional[asyncio.Task] = None

        _trace(
            "LIFECYCLE",
            "adapter constructed",
            mode=("direct-ws" if self._host else "auto-discovery"),
            host=self._host or None,
            api_base=self._api_base or None,
            robot_path=self._robot_path,
            token=_mask_token(self._bot_token),
            bot_id_fallback=self._bot_id_fallback or None,
            heartbeat_s=self._heartbeat_interval,
            reconnect_max_s=self._reconnect_max_seconds,
            max_msg_len=self._max_message_length,
            allow_all=self._allow_all,
            allowed_users=self._allowed_users or None,
            trace_enabled=_is_trace_enabled(),
        )

        env_raw = os.getenv("CHAORANXIN_TRACE")
        env_str = env_raw.lower() if env_raw else "<unset>"
        try:
            touch_exists = _TRACE_FILE.exists()
        except OSError as exc:
            touch_exists = f"<OSError: {exc}>"
        logger.info(
            "[chaoranxin] trace status: env=%s touchfile=%s → %s",
            env_str,
            touch_exists,
            "ON" if _is_trace_enabled() else "OFF",
        )

    # ----- properties -----

    @property
    def name(self) -> str:
        return "Chaoranxin"

    @staticmethod
    def _ws_is_open(ws) -> bool:
        """True when the WebSocket is in OPEN state and ready to send.

        Compatible with ``websockets>=10`` (which exposes ``ClientConnection.state``
        as a ``State`` enum member) and ``websockets<10`` (which uses the
        ``closed`` boolean on ``WebSocketClientProtocol``).

        Returns ``False`` for any other state (CONNECTING / CLOSING / CLOSED),
        for ``ws is None``, or when neither ``state`` nor ``closed`` exists
        on the object.  This deliberately defaults to "not open" so that a
        future websockets API removal — like the one that broke
        ``getattr(ws, "closed", True)`` when we pinned
        ``websockets==15.0.1`` — never silently lets a closed socket through.
        """
        if ws is None:
            return False
        state = getattr(ws, "state", None)
        if state is not None:
            return getattr(state, "name", None) == "OPEN"
        return not getattr(ws, "closed", True)

    @property
    def is_connected(self) -> bool:
        """Connected when the WS is open, the listener is alive, and
        the RobotLogin handshake has completed (``_robot_uuid`` set)."""
        if not (self._running and self._ws is not None and self._robot_uuid):
            return False
        return self._ws_is_open(self._ws)

    # ----- connection lifecycle -----

    def _connect_timeout_seconds(self) -> float:
        return _env_float(
            "CHAORANXIN_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT_SECONDS
        )

    def _complete_handshake(self, ok: bool) -> None:
        """Resolve the one-shot future :meth:`connect` is waiting on."""
        fut = self._handshake_done
        if fut is not None and not fut.done():
            fut.set_result(ok)

    def _log_startup_config(self) -> None:
        """Emit a one-line summary of which connection mode is active."""
        if self._host:
            logger.info(
                "[chaoranxin] config: mode=direct-ws host=%s path=%s",
                self._host,
                self._robot_path,
            )
        else:
            logger.info(
                "[chaoranxin] config: mode=auto-discovery api_base=%s",
                self._api_base,
            )

    async def connect(self) -> bool:
        """Open the WebSocket, wait for RobotLogin, then return.

        Returns ``True`` only after the server sends a successful RobotLogin
        frame (robot uuid bound).  Until then the gateway must not treat the
        platform as connected — ``is_connected`` also requires ``_robot_uuid``.
        """
        _trace(
            "LIFECYCLE",
            "connect() enter",
            running=self._running,
            has_listener=self._listen_task is not None and not self._listen_task.done(),
        )

        if not WEBSOCKETS_AVAILABLE:
            msg = "Chaoranxin: websockets package not installed"
            self._set_fatal_error("chaoranxin_missing_dep", msg, retryable=True)
            logger.warning("%s. Run: pip install websockets", msg)
            _trace("LIFECYCLE", "connect() exit", ok=False, reason="missing_dep")
            return False

        if not self._bot_token:
            msg = "Chaoranxin: CHAORANXIN_BOT_TOKEN is required"
            self._set_fatal_error("chaoranxin_missing_credentials", msg, retryable=False)
            logger.warning("%s", msg)
            _trace("LIFECYCLE", "connect() exit", ok=False, reason="missing_credentials")
            return False
        if not (self._host or self._api_base):
            msg = (
                "Chaoranxin: set CHAORANXIN_HOST (direct WS) or "
                "CHAORANXIN_API_BASE (auto-discover from "
                "/im/api/v1/robot/servers) — one of the two is required"
            )
            self._set_fatal_error("chaoranxin_missing_endpoint", msg, retryable=False)
            logger.warning("%s", msg)
            _trace("LIFECYCLE", "connect() exit", ok=False, reason="missing_endpoint")
            return False

        self._log_startup_config()

        # Idempotent reconnect — if a background loop is already running,
        # wait on its handshake rather than spawning a duplicate task.
        if self._listen_task is not None and not self._listen_task.done():
            if self._handshake_done is not None and not self._handshake_done.done():
                _trace(
                    "LIFECYCLE",
                    "connect() waiting on in-flight handshake (idempotent)",
                    timeout_s=self._connect_timeout_seconds(),
                )
                try:
                    ok = await asyncio.wait_for(
                        asyncio.shield(self._handshake_done),
                        timeout=self._connect_timeout_seconds(),
                    )
                    _trace("LIFECYCLE", "connect() resolved on in-flight", ok=bool(ok))
                    return bool(ok)
                except asyncio.TimeoutError:
                    logger.error(
                        "[chaoranxin] connect timed out after %.0fs waiting for "
                        "RobotLogin",
                        self._connect_timeout_seconds(),
                    )
                    _trace("LIFECYCLE", "connect() idempotent wait timeout")
                    return False
            _trace(
                "LIFECYCLE",
                "connect() already-running handshake; returning current is_connected",
                ok=self.is_connected,
            )
            return self.is_connected

        self._running = True
        loop = asyncio.get_running_loop()
        self._handshake_done = loop.create_future()
        self._listen_task = asyncio.create_task(
            self._connect_loop(), name="chaoranxin-ws"
        )
        _trace(
            "LIFECYCLE",
            "connect() spawning _connect_loop listener",
            task_name=self._listen_task.get_name(),
            timeout_s=self._connect_timeout_seconds(),
        )
        try:
            ok = await asyncio.wait_for(
                asyncio.shield(self._handshake_done),
                timeout=self._connect_timeout_seconds(),
            )
            if ok:
                logger.info(
                    "[chaoranxin] connect complete — adapter ready "
                    "(robot_id=%s)",
                    self._robot_uuid,
                )
                _trace(
                    "LIFECYCLE",
                    "connect() success",
                    robot_id=self._robot_uuid,
                )
            else:
                logger.error(
                    "[chaoranxin] connect failed — RobotLogin was rejected"
                )
                _trace("LIFECYCLE", "connect() failed (RobotLogin rejected)")
            return bool(ok)
        except asyncio.TimeoutError:
            logger.error(
                "[chaoranxin] connect timed out after %.0fs — no RobotLogin. "
                "Check token, api_base, and gateway.log; if using "
                "CHAORANXIN_HOST on the REST API domain, switch to "
                "CHAORANXIN_API_BASE for node discovery",
                self._connect_timeout_seconds(),
            )
            _trace(
                "LIFECYCLE",
                "connect() timeout (no RobotLogin)",
                timeout_s=self._connect_timeout_seconds(),
                robot_id=self._robot_uuid or None,
            )
            return False

    async def disconnect(self) -> None:
        """Tear down the WS and cancel background tasks."""
        _trace("LIFECYCLE", "disconnect() enter", robot_id=self._robot_uuid or None)
        self._running = False
        self._complete_handshake(False)
        self._mark_disconnected()
        _trace("LIFECYCLE", "disconnect: marked disconnected")
        await self._cancel_task(self._listen_task)
        self._listen_task = None
        await self._cancel_task(self._heartbeat_task)
        self._heartbeat_task = None
        await self._cancel_task(self._login_watchdog_task)
        self._login_watchdog_task = None
        await self._cancel_task(self._startup_message_task)
        self._startup_message_task = None
        if self._ws is not None:
            _trace("WS", "disconnect: closing WebSocket")
            try:
                await self._ws.close()
            except Exception:
                logger.debug("[chaoranxin] ws.close() raised", exc_info=True)
            self._ws = None
        self._robot_uuid = ""
        self._robot_owner = ""
        # Fail any in-flight receipts so senders unblock.
        pending_receipts = len(self._pending_receipts)
        for fut in self._pending_receipts.values():
            if not fut.done():
                fut.set_exception(RuntimeError("chaoranxin disconnected"))
        self._pending_receipts.clear()
        seen_event_count = len(self._seen_event_ids)
        self._seen_event_ids.clear()
        logger.info("[chaoranxin] Disconnected")
        _trace(
            "LIFECYCLE",
            "disconnect() done",
            failed_pending_receipts=pending_receipts,
            cleared_seen_event_ids=seen_event_count,
        )

    async def _cancel_task(self, task: Optional[asyncio.Task]) -> None:
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.debug("[chaoranxin] task raised on cancel", exc_info=True)

    # ----- WS connect loop -----

    async def _resolve_ws_url(self) -> Optional[str]:
        """Decide which WS URL to dial for this connection attempt.

        * ``self._host`` set (ws/wss only) → dial ``{host}{robot_path}``.
        * Otherwise → ``GET {api_base}/im/api/v1/robot/servers`` and
          dial the first node's ``ws://host:inter{robot}`` (§2.2).

        Returns the URL string on success, or ``None`` on failure (the
        error is logged; the caller treats ``None`` as fatal).
        """
        if self._host:
            url = f"{self._host}{self._robot_path}"
            logger.info("[chaoranxin] WS URL (direct): %s", url)
            _trace("WS", "resolve_ws_url direct", url=url, token=_mask_token(self._bot_token))
            return url

        # Auto-discovery.  Reuse the cached node if we have one — that
        # way a transient WS drop doesn't re-hit the API on every retry.
        if self._cached_node is not None:
            try:
                url = self._cached_node.ws_url()
                logger.info("[chaoranxin] WS URL (cached node): %s", url)
                _trace("WS", "resolve_ws_url cached", url=url)
                return url
            except ValueError as exc:
                logger.warning(
                    "[chaoranxin] cached node unusable (%s); re-fetching", exc
                )
                self._cached_node = None

        if not HTTPX_AVAILABLE:
            logger.error(
                "[chaoranxin] node discovery requires httpx (pip install httpx)"
            )
            _trace("WS", "resolve_ws_url: httpx unavailable")
            return None
        if not self._api_base:
            logger.error("[chaoranxin] no CHAORANXIN_API_BASE for node discovery")
            _trace("WS", "resolve_ws_url: no api_base")
            return None

        _trace("HTTP", "resolve_ws_url: calling _fetch_node_list")
        nodes, error = await _fetch_node_list(self._api_base, self._bot_token)
        if error or not nodes:
            logger.error(
                "[chaoranxin] node discovery failed: %s", error or "no nodes"
            )
            _trace("WS", "resolve_ws_url: node discovery failed", error=error)
            return None
        node = nodes[0]
        self._cached_node = node
        try:
            url = node.ws_url()
        except ValueError as exc:
            logger.error("[chaoranxin] discovered node unusable: %s", exc)
            _trace("WS", "resolve_ws_url: discovered node unusable", error=str(exc))
            return None
        logger.info(
            "[chaoranxin] WS URL (discovered): %s (host=%s proto=%s port=%s robot=%s)",
            url,
            node.host,
            node.proto,
            node.inter_or_port,
            node.robot,
        )
        _trace(
            "WS",
            "resolve_ws_url discovered",
            url=url,
            host=node.host,
            proto=node.proto,
            port=node.inter_or_port,
            robot=node.robot,
        )
        return url

    async def _login_watchdog(self, warn_after: float) -> None:
        """Warn loudly when RobotLogin is slow — usually a wrong WS URL."""
        await asyncio.sleep(warn_after)
        if self._robot_uuid or not self._running:
            return
        logger.error(
            "[chaoranxin] still awaiting RobotLogin after %.0fs — check "
            "token and WS endpoint (wrong URL is the usual cause: use "
            "CHAORANXIN_API_BASE for REST node discovery, not "
            "CHAORANXIN_HOST on the API hostname)",
            warn_after,
        )
        _trace(
            "LIFECYCLE",
            "login watchdog fired (no RobotLogin)",
            waited_s=warn_after,
        )

    async def _connect_loop(self) -> None:
        """Outer loop: dial WS → run inner loop → backoff → redial.

        Backoff is exponential with ±20% jitter capped at
        ``self._reconnect_max_seconds``.  Auth / TLS errors are fatal
        (HTTP 401 / 403 / 404 — they will not improve on retry, so we
        report a fatal error and stop the loop).  The server doesn't
        tell us why handshake failed; surface the HTTP status verbatim
        and let the user check their token.

        Node discovery (when ``api_base`` is set) happens once per
        successful resolve; the result is cached and reused on
        reconnect so a transient WS drop doesn't re-hit the API.
        """
        backoff = INITIAL_BACKOFF_SECONDS
        headers = _build_handshake_headers(self._bot_token)
        _trace(
            "LIFECYCLE",
            "_connect_loop enter",
            reconnect_max_s=self._reconnect_max_seconds,
            auth_header=("Bearer " + _mask_token(self._bot_token)),
        )

        attempt = 0
        while self._running:
            attempt += 1
            url = await self._resolve_ws_url()
            if not url:
                # Discovery failed — keep retrying through the backoff
                # loop.  Discovery errors are NOT fatal (the cluster
                # may be coming up); we just wait and try again.
                if not self._running:
                    return
                delay = min(backoff, self._reconnect_max_seconds)
                delay *= 1.0 + (BACKOFF_JITTER * (2 * (time.time() % 1) - 1))
                delay = max(0.5, delay)
                logger.info(
                    "[chaoranxin] Node discovery failed; retrying in %.1fs...",
                    delay,
                )
                _trace(
                    "WS",
                    "node discovery failed; backing off",
                    attempt=attempt,
                    backoff_s=round(delay, 2),
                    next_backoff=round(min(backoff * BACKOFF_FACTOR, self._reconnect_max_seconds), 2),
                )
                await asyncio.sleep(delay)
                backoff = min(
                    backoff * BACKOFF_FACTOR, self._reconnect_max_seconds
                )
                continue

            try:
                logger.info("[chaoranxin] Connecting to %s", url)
                _trace("WS", "dial", attempt=attempt, url=url)
                async with websockets.connect(
                    url,
                    additional_headers=headers,
                    ping_interval=None,        # we drive app-layer heartbeats
                    close_timeout=5,
                    max_size=2 ** 20,          # 1 MiB cap on inbound frames
                ) as ws:
                    self._ws = ws
                    logger.info(
                        "[chaoranxin] WebSocket connected — awaiting RobotLogin"
                    )
                    _trace(
                        "WS",
                        "tcp/tls up; awaiting RobotLogin",
                        url=url,
                        attempt=attempt,
                    )
                    # Successful TCP/TLS handshake — reset backoff and
                    # clear the cached node so a future host change is
                    # picked up by re-discovery.
                    backoff = INITIAL_BACKOFF_SECONDS
                    self._heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(), name="chaoranxin-heartbeat"
                    )
                    self._login_watchdog_task = asyncio.create_task(
                        self._login_watchdog(ROBOT_LOGIN_WARN_SECONDS),
                        name="chaoranxin-login-watchdog",
                    )
                    _trace(
                        "LIFECYCLE",
                        "heartbeat + login watchdog scheduled",
                        heartbeat_s=self._heartbeat_interval,
                        watchdog_after_s=ROBOT_LOGIN_WARN_SECONDS,
                    )
                    try:
                        await self._recv_loop(ws)
                    finally:
                        logger.info(
                            "[chaoranxin] WebSocket session ended"
                        )
                        _trace(
                            "WS",
                            "session ended; resetting state for next connect",
                            cleared_robot_uuid=True,
                            cleared_cached_node=True,
                        )
                        await self._cancel_task(self._heartbeat_task)
                        self._heartbeat_task = None
                        await self._cancel_task(self._login_watchdog_task)
                        self._login_watchdog_task = None
                        self._robot_uuid = ""  # require re-handshake on next connect
                        self._robot_owner = ""
                        self._cached_node = None  # re-discover on next attempt

            except asyncio.CancelledError:
                _trace("LIFECYCLE", "_connect_loop cancelled")
                return
            except websockets.exceptions.InvalidURI as exc:
                suggestion = ""
                if isinstance(exc.args[0], str):
                    bad = exc.args[0]
                    if bad.startswith("http://") or bad.startswith("https://"):
                        suggestion = bad.replace("http://", "ws://", 1).replace(
                            "https://", "wss://", 1
                        )
                msg = f"Chaoranxin URL is invalid: {exc}"
                if suggestion:
                    msg += f" — did you mean {suggestion}?"
                self._set_fatal_error(
                    "chaoranxin_invalid_uri", msg, retryable=False
                )
                logger.error("[chaoranxin] %s — stopping reconnect loop", msg)
                _trace("ERROR", "InvalidURI (fatal)", url=url, error=str(exc))
                self._complete_handshake(False)
                return
            except websockets.exceptions.InvalidStatus as exc:
                # 401 / 403 / 404 — server rejected us; no point retrying.
                status = getattr(exc, "status_code", None) or getattr(exc, "code", None)
                msg = f"Chaoranxin handshake rejected: HTTP {status}"
                self._set_fatal_error(
                    "chaoranxin_handshake_rejected", msg, retryable=False
                )
                logger.error("[chaoranxin] %s — stopping reconnect loop", msg)
                _trace("ERROR", "InvalidStatus (fatal, e.g. 401/403/404)", url=url, http_status=status)
                self._complete_handshake(False)
                return
            except websockets.exceptions.InvalidStatusCode as exc:
                # Some websockets versions raise InvalidStatusCode; cover both.
                msg = f"Chaoranxin handshake rejected: HTTP {exc.status_code}"
                self._set_fatal_error(
                    "chaoranxin_handshake_rejected", msg, retryable=False
                )
                logger.error("[chaoranxin] %s — stopping reconnect loop", msg)
                _trace("ERROR", "InvalidStatusCode (fatal)", url=url, http_status=exc.status_code)
                self._complete_handshake(False)
                return
            except websockets.exceptions.InvalidHeader:
                self._set_fatal_error(
                    "chaoranxin_handshake_rejected",
                    "Chaoranxin handshake rejected: invalid header",
                    retryable=False,
                )
                logger.error(
                    "[chaoranxin] Invalid header during handshake — stopping"
                )
                _trace("ERROR", "InvalidHeader (fatal)", url=url)
                self._complete_handshake(False)
                return
            except OSError as exc:
                logger.warning(
                    "[chaoranxin] Connection error: %s — retrying", exc
                )
                _trace("ERROR", "OSError; will retry", url=url, error=str(exc))
            except Exception as exc:
                logger.exception(
                    "[chaoranxin] Unexpected error in connect loop: %s", exc
                )
                _trace("ERROR", "unexpected connect_loop exception", url=url, error=str(exc))

            if not self._running:
                return

            # Backoff with jitter
            delay = min(backoff, self._reconnect_max_seconds)
            delay *= 1.0 + (BACKOFF_JITTER * (2 * (time.time() % 1) - 1))
            delay = max(0.5, delay)
            logger.info(
                "[chaoranxin] Reconnecting in %.1fs...", delay
            )
            _trace(
                "WS",
                "reconnect backoff",
                attempt=attempt,
                backoff_s=round(delay, 2),
                next_backoff=round(min(backoff * BACKOFF_FACTOR, self._reconnect_max_seconds), 2),
                cap=self._reconnect_max_seconds,
            )
            await asyncio.sleep(delay)
            backoff = min(backoff * BACKOFF_FACTOR, self._reconnect_max_seconds)

    # ----- receive loop -----

    async def _recv_loop(self, ws) -> None:
        """Read frames from the WS until the connection closes.

        Each frame is plaintext JSON.  Top-level ``type`` dispatches:
          * ``RobotLogin`` → bind robot uuid + owner (spec §3.3)
          * ``Status``     → heartbeat echo / send receipt
          * ``RobotEvent`` → forward user message to the agent
        """
        _trace("RX", "_recv_loop enter")
        async for raw in ws:
            if not self._running:
                return
            if isinstance(raw, (bytes, bytearray)):
                try:
                    raw = raw.decode("utf-8")
                except UnicodeDecodeError:
                    logger.warning("[chaoranxin] dropping non-utf8 frame")
                    _trace("RX", "dropping non-utf8 frame")
                    continue
            _log_im_rx(raw)
            frame = IncomingFrame.parse(raw)
            if not frame:
                _trace("RX", "incoming_frame parse -> None")
                continue
            if frame.is_robot_login:
                await self._handle_robot_login(RobotLoginFrame.parse(frame))
            elif frame.is_status:
                await self._handle_status(StatusFrame.parse(frame))
            elif frame.is_robot_event:
                await self._dispatch_robot_event(
                    RobotEventFrame.parse(frame)
                )

    # ----- RobotLogin dispatch (spec §3.3) -----

    async def _handle_robot_login(self, login: RobotLoginFrame) -> None:
        """Bind the robot uuid and owner from a successful RobotLogin frame.

        On success:
          * ``self._robot_uuid = login.robot``
          * ``self._mark_connected()``
          * ``CHAORANXIN_HOME_CHANNEL`` is written to the process env (and
            will be picked up by ``_env_enablement`` / ``_standalone_send``
            / ``hermes cron``) using the ``owner`` uuid.

        On failure (``ok=false``) the reason from ``msg`` is surfaced as a
        fatal error and the reconnect loop is stopped.
        """
        if not login.is_success:
            self._set_fatal_error(
                "chaoranxin_login_rejected",
                f"RobotLogin rejected: {login.msg or 'unknown reason'}",
                retryable=False,
            )
            logger.error(
                "[chaoranxin] RobotLogin rejected: %s", login.msg
            )
            _trace("ERROR", "RobotLogin rejected (fatal)", msg=login.msg or "unknown reason")
            self._complete_handshake(False)
            return
        if not login.robot:
            self._set_fatal_error(
                "chaoranxin_login_malformed",
                "RobotLogin had no robot uuid",
                retryable=False,
            )
            logger.error("[chaoranxin] RobotLogin missing robot uuid")
            _trace("ERROR", "RobotLogin missing robot uuid (fatal)")
            self._complete_handshake(False)
            return

        self._robot_uuid = login.robot
        self._robot_owner = login.owner or ""
        self._mark_connected()

        # Auto-configure HOME_CHANNEL from the bot creator.  The spec
        # returns ``owner`` only when ``ok=True``; it is the
        # ``sys_user`` uuid of the bot's owner and is exactly what the
        # adapter needs as a default cron / notification target.
        if login.owner:
            old = os.environ.get("CHAORANXIN_HOME_CHANNEL")
            if old is None:
                os.environ["CHAORANXIN_HOME_CHANNEL"] = login.owner
        home = os.environ.get("CHAORANXIN_HOME_CHANNEL", "")
        logger.info(
            "[chaoranxin] READY — RobotLogin ok (robot_id=%s, owner=%s%s)",
            self._robot_uuid,
            login.owner or "(none)",
            f", home_channel={home}" if home else "",
        )
        _trace(
            "STATE",
            "RobotLogin bound",
            robot_id=self._robot_uuid,
            owner=login.owner or None,
            home_channel=home or None,
            home_channel_was_set=login.owner is not None,
        )
        self._complete_handshake(True)
        if login.owner:
            self._schedule_startup_message(login.owner)

    def _startup_message_enabled(self) -> bool:
        return _env_bool("CHAORANXIN_STARTUP_MESSAGE", default=True)

    def _startup_message_text(self) -> str:
        custom = _env("CHAORANXIN_STARTUP_MESSAGE_TEXT")
        return custom or DEFAULT_STARTUP_MESSAGE

    def _schedule_startup_message(self, owner: str) -> None:
        """Notify the bot creator once per gateway process after RobotLogin."""
        if self._startup_message_sent or not owner or not self._startup_message_enabled():
            _trace(
                "LIFECYCLE",
                "startup message skipped",
                reason=(
                    "already_sent" if self._startup_message_sent
                    else "no_owner" if not owner
                    else "disabled"
                ),
            )
            return
        self._startup_message_task = asyncio.create_task(
            self._send_startup_message(owner),
            name="chaoranxin-startup-message",
        )
        _trace("LIFECYCLE", "startup message scheduled", owner=owner)

    async def _send_startup_message(self, owner: str) -> None:
        """DM the robot creator that the gateway adapter is online."""
        if self._startup_message_sent or not owner:
            return
        ws = self._ws
        from_robot = self._resolve_from_robot_uuid()
        if not self._ws_is_open(ws) or not from_robot:
            logger.warning(
                "[chaoranxin] startup message skipped (not connected or no robot uuid)"
            )
            _trace(
                "TX",
                "startup message skipped",
                owner=owner,
                ws_open=self._ws_is_open(ws),
                from_robot=from_robot or None,
            )
            return
        text = self._startup_message_text()
        if len(text) > self._max_message_length:
            text = text[: self._max_message_length]
        msg = OutboundMsg(to=owner, text=text, clazz=MSG_CLAZZ_MARKDOWN)
        frame_json = msg.to_json(from_robot)
        _log_im_tx(frame_json)
        try:
            await ws.send(frame_json)
        except Exception as exc:
            logger.warning(
                "[chaoranxin] startup message to owner=%s failed: %s",
                owner,
                exc,
            )
            _trace("ERROR", "startup message raised", owner=owner, error=str(exc))
            return
        self._startup_message_sent = True
        logger.info("[chaoranxin] startup message sent to owner=%s", owner)

    # ----- Status dispatch (Heart / Msg receipts only) -----

    async def _handle_status(self, st: StatusFrame) -> None:
        """Route a Status frame to the right handler.

        Status subtypes we care about:
          * ``Heart`` — heartbeat echo; nothing to do.
          * ``Msg``   — resolve the corresponding pending receipt.

        RobotLogin is handled separately as a top-level frame type.
        """
        if not st:
            return
        if st.is_heart:
            # Per spec: clients don't need to parse the Heart echo's
            # time — receiving one with status=100 confirms liveness.
            logger.debug("[chaoranxin] Heart echo status=%s", st.status)
            _trace("HEART", "heart echo", status=st.status, ok=st.is_ok)
        elif st.is_msg_receipt:
            self._on_msg_receipt(st)
        else:
            logger.debug(
                "[chaoranxin] ignoring Status subtype=%s status=%s",
                st.subtype, st.status,
            )
            _trace("RX", "Status ignored (unknown subtype)", subtype=st.subtype, status=st.status)

    def _on_msg_receipt(self, st: StatusFrame) -> None:
        """Resolve the future for an outbound Msg's Status receipt.

        The server correlates by ``data.value`` (== ``data.uuid`` == our
        ``msg_id``).  ``status=100`` resolves successfully with
        ``message_id``; ``status=-1`` resolves with an error carrying
        the server's reason (e.g. ``发送方必须为当前登录机器人``).
        """
        if not st.uuid and not st.value:
            _trace("RECEIPT", "no msg_id on receipt; ignore", status=st.status)
            return
        msg_id = st.uuid or st.value
        fut = self._pending_receipts.pop(msg_id, None)
        if fut is None or fut.done():
            # Either no waiter (we didn't track it) or already settled.
            if fut is None:
                logger.debug(
                    "[chaoranxin] receipt for unknown msg_id=%s (status=%s)",
                    msg_id, st.status,
                )
                _trace(
                    "RECEIPT",
                    "receipt for unknown/tracked msg_id (no waiter)",
                    msg_id=msg_id,
                    status=st.status,
                )
            return
        if st.is_ok:
            fut.set_result({"message_id": msg_id, "status": st.status})
            _trace(
                "RECEIPT",
                "msg receipt OK",
                msg_id=msg_id,
                status=st.status,
                server_msg=st.msg or None,
            )
        else:
            err_msg = st.msg or "unknown reason"
            fut.set_exception(
                RuntimeError(
                    f"chaoranxin send rejected: {err_msg}"
                )
            )
            _trace(
                "ERROR",
                "msg receipt rejected",
                msg_id=msg_id,
                status=st.status,
                server_msg=err_msg,
            )

    # ----- RobotEvent dispatch -----

    async def _dispatch_robot_event(self, env: RobotEventFrame) -> None:
        """Route a parsed inbound RobotEvent.

        Drop order:
          1. Empty envelope (parse failure) — already filtered upstream
          2. Duplicate ``event_id`` within the dedup window
          3. ``event_type`` outside v1's handled set — log + skip
          4. Schema mismatch (forward-compat: log + continue)
          5. Sender outside allowlist — silent skip (no agent wake)
          6. Otherwise build a ``MessageEvent`` and call ``handle_message``
        """
        if not env.event_id:
            logger.debug("[chaoranxin] envelope missing event_id, skipping")
            _trace("DISPATCH", "drop: missing event_id")
            return

        if self._is_duplicate(env.event_id):
            logger.debug(
                "[chaoranxin] duplicate event_id=%s, skipping", env.event_id
            )
            _trace("DISPATCH", "drop: duplicate event_id", event_id=env.event_id)
            return

        if not env.is_handled:
            logger.debug(
                "[chaoranxin] ignoring event_type=%s", env.event_type
            )
            _trace(
                "DISPATCH",
                "drop: unhandled event_type",
                event_type=env.event_type,
            )
            return

        if not env.schema_ok:
            logger.warning(
                "[chaoranxin] unexpected schema=%s (expected %s) — continuing",
                env.schema_version, EXPECTED_EVENT_SCHEMA,
            )
            _trace(
                "DISPATCH",
                "warn: schema mismatch; continuing",
                schema=env.schema_version,
                expected=EXPECTED_EVENT_SCHEMA,
            )

        # Filter our own echoes — the platform may echo back messages
        # the bot sent.  We compare against the bound robot uuid and,
        # during the pre-handshake window, against the bot_id fallback.
        my_id = self._robot_uuid or self._bot_id_fallback
        if my_id and env.sender_id == my_id:
            _trace(
                "DISPATCH",
                "drop: self-echo (sender == bound robot)",
                sender=env.sender_id,
                robot_id=my_id,
            )
            return

        if not self._is_authorized(env.sender_id):
            logger.info(
                "[chaoranxin] sender %s not in allowlist — dropping",
                env.sender_id,
            )
            _trace(
                "DISPATCH",
                "drop: sender not in allowlist",
                sender=env.sender_id,
                allow_all=self._allow_all,
                allowed=self._allowed_users or None,
            )
            return

        await self._deliver(env)

    def _is_duplicate(self, event_id: str) -> bool:
        """LRU-bounded dedup keyed by ``event_id``.

        Mirrors the openclaw convention: 24h TTL with a hard cap on
        size so a long-running adapter cannot leak memory under
        retry-storm conditions.
        """
        now = time.time()
        cutoff = now - DEDUP_WINDOW_SECONDS
        while self._seen_event_ids:
            oldest_id, oldest_ts = next(iter(self._seen_event_ids.items()))
            if oldest_ts < cutoff or len(self._seen_event_ids) >= DEDUP_MAX_SIZE:
                self._seen_event_ids.popitem(last=False)
            else:
                break
        if event_id in self._seen_event_ids:
            return True
        self._seen_event_ids[event_id] = now
        return False

    def _is_authorized(self, sender_id: str) -> bool:
        """Allowlist gate — mirrors ``gateway.run._is_user_authorized``.

        Open by default (``CHAORANXIN_ALLOW_ALL_USERS`` defaults to true).
        Set ``CHAORANXIN_ALLOW_ALL_USERS=false`` to enforce
        ``CHAORANXIN_ALLOWED_USERS``; the robot creator
        (``RobotLogin.data.owner``) is always allowed when restricted.
        """
        if self._allow_all:
            return True
        if self._robot_owner and sender_id == self._robot_owner:
            return True
        if not self._allowed_users:
            return False
        return sender_id in self._allowed_users

    # ----- MessageType mapping for inbound events -----

    @staticmethod
    def _msg_type_for_clazz(clazz: str) -> MessageType:
        """Map inbound clazz to Hermes MessageType (Multimodal uses parts)."""
        c = (clazz or "").lower()
        if c == "multimodal":
            return MessageType.TEXT
        if c in ("text", "markdown", "at"):
            return MessageType.TEXT
        if c == "picture":
            return MessageType.PHOTO
        if c == "video":
            return MessageType.VIDEO
        if c == "voice":
            return MessageType.VOICE
        return MessageType.DOCUMENT

    async def _deliver(self, env: RobotEventFrame) -> None:
        """Build a ``MessageEvent`` from Multimodal parts and hand to gateway.

        Robot uplink is Multimodal-only (``msg_type=multimodal`` + non-empty
        ``content.parts``). Legacy text/markdown/voice/picture inbound is
        logged and dropped — no compatibility path.
        """
        if not env.is_multimodal:
            logger.info(
                "[chaoranxin] drop non-multimodal inbound msg_type=%s "
                "parts=%d (uplink requires multimodal + parts)",
                env.msg_type or "(empty)",
                len(env.parts),
            )
            _trace(
                "DISPATCH",
                "drop: non-multimodal uplink",
                msg_type=env.msg_type or None,
                parts=len(env.parts),
            )
            return

        from plugins.platforms.chaoranxin.multimodal import materialize_parts

        materialized = await materialize_parts(env.parts)
        text = materialized.final_text
        if not text and not materialized.media_urls:
            logger.info(
                "[chaoranxin] multimodal event %s produced empty payload — drop",
                env.message_id,
            )
            _trace("DISPATCH", "drop: empty multimodal materialization")
            return

        image_parts = sum(
            1
            for p in env.parts
            if str(p.get("type") or "").lower().strip() == "image_url"
        )
        if image_parts and not materialized.media_urls:
            logger.warning(
                "[chaoranxin] multimodal event %s had %d image_url part(s) "
                "but media_urls is empty (missing/unsafe/download failed — see notes)",
                env.message_id,
                image_parts,
            )
        chat_type = "group" if env.chat_type in ("group", "chat", "channel") else "dm"
        source = self.build_source(
            chat_id=env.chat_id,
            chat_name=env.chat_id,
            chat_type=chat_type,
            user_id=env.sender_id,
            user_name=env.sender_name,
        )

        ts = env.create_time_ms / 1000.0 if env.create_time_ms else time.time()
        try:
            timestamp = _epoch_to_dt(ts)
        except (OSError, OverflowError, ValueError):
            timestamp = datetime.now(tz=timezone.utc)

        event = MessageEvent(
            text=text or "",
            message_type=materialized.message_type,
            source=source,
            message_id=env.message_id,
            raw_message={"robot_event": env.raw, "clazz": env.clazz},
            timestamp=timestamp,
            media_urls=list(materialized.media_urls),
            media_types=list(materialized.media_types),
        )
        logger.debug(
            "[chaoranxin] event chat=%s sender=%s multimodal parts=%d "
            "msg_type=%s text=%r media=%d",
            env.chat_id,
            env.sender_id,
            len(env.parts),
            materialized.message_type.value,
            (text[:80] if text else None),
            len(materialized.media_urls),
        )
        await self.handle_message(event)
        _trace(
            "DISPATCH",
            "handle_message returned",
            chat_id=env.chat_id,
            message_id=env.message_id,
        )

    # ----- heartbeat -----

    async def _heartbeat_loop(self) -> None:
        """Send ``{"type":"Heart","data":{"time":ms}}`` every N seconds.

        Per spec the server's idle timeout is 15 minutes; we send
        every 30s (default) — well under the threshold.  The server
        is the source of truth for liveness: silence closes the WS
        and the outer connect loop redials.
        """
        if self._heartbeat_interval <= 0:
            _trace("HEART", "heartbeat disabled (interval<=0)")
            return
        _trace(
            "HEART",
            "heartbeat_loop enter",
            interval_s=self._heartbeat_interval,
        )
        try:
            tick = 0
            while self._running and self._ws is not None:
                await asyncio.sleep(self._heartbeat_interval)
                if not self._running or self._ws is None:
                    return
                tick += 1
                payload = OutboundHeart().to_json()
                try:
                    await self._ws.send(payload)
                    if tick == 1 or tick % 10 == 0:
                        _trace(
                            "HEART",
                            "heart sent",
                            tick=tick,
                            bytes=len(payload) if isinstance(payload, (str, bytes)) else None,
                            preview=payload if isinstance(payload, str) and len(payload) <= 200 else "<truncated>",
                        )
                except Exception as exc:
                    _trace("HEART", "heart send failed; exiting loop", error=str(exc))
                    return
        except asyncio.CancelledError:
            _trace("HEART", "heartbeat_loop cancelled")
            return

    # ----- outbound -----

    def _resolve_from_robot_uuid(self) -> str:
        """Return the robot uuid to use as ``from`` on outbound frames.

        After the RobotLogin handshake this is ``self._robot_uuid``.
        During the brief pre-handshake window we fall back to
        ``CHAORANXIN_BOT_ID`` if configured; otherwise the call is
        rejected with ``error="not logged in yet"``.
        """
        return self._robot_uuid or self._bot_id_fallback

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a ``Data<Msg>`` text frame to ``chat_id``.

        Long messages are truncated with a warning.  Sending while
        disconnected (or before the RobotLogin handshake) returns
        ``success=False`` rather than queueing — the agent layer is the
        source of truth for retry policy.
        """
        ws = self._ws
        if not self._ws_is_open(ws):
            _trace(
                "TX",
                "send() rejected (ws not connected)",
                to=chat_id,
                chars=len(content or ""),
            )
            return SendResult(success=False, error="ws not connected")
        from_robot = self._resolve_from_robot_uuid()
        if not from_robot:
            _trace(
                "TX",
                "send() rejected (handshake pending)",
                to=chat_id,
                chars=len(content or ""),
                bot_id_fallback=self._bot_id_fallback or None,
            )
            return SendResult(
                success=False,
                error="not logged in yet (RobotLogin handshake pending)",
            )

        text = content or ""
        truncated = False
        if len(text) > self._max_message_length:
            logger.warning(
                "[chaoranxin] truncating message from %d to %d chars",
                len(text),
                self._max_message_length,
            )
            text = text[: self._max_message_length]
            truncated = True

        msg = OutboundMsg(
            to=chat_id,
            text=text,
            clazz=MSG_CLAZZ_MARKDOWN,
            quote=(reply_to if self._send_quote else None),
        )
        frame_json = msg.to_json(from_robot)
        _log_im_tx(frame_json)
        if truncated:
            _trace("TX", "send() truncated", to=chat_id, msg_id=msg.msg_id)
        try:
            await ws.send(frame_json)
        except Exception as exc:
            logger.warning("[chaoranxin] send failed: %s", exc)
            _trace(
                "ERROR",
                "send() ws.send raised",
                to=chat_id,
                msg_id=msg.msg_id,
                error=str(exc),
            )
            return SendResult(success=False, error=str(exc))
        return SendResult(success=True, message_id=msg.msg_id)

    async def _send_clazz_frame(
        self,
        chat_id: str,
        clazz: str,
        content: Dict[str, Any],
        *,
        reply_to: Optional[str] = None,
    ) -> SendResult:
        """Send a content-type WS frame (``type`` === ``data.clazz``)."""
        ws = self._ws
        if not self._ws_is_open(ws):
            return SendResult(success=False, error="ws not connected")
        from_robot = self._resolve_from_robot_uuid()
        if not from_robot:
            return SendResult(
                success=False,
                error="not logged in yet (RobotLogin handshake pending)",
            )
        msg = OutboundMsg(to=chat_id, clazz=clazz).set_clazz(clazz, content)
        if reply_to and self._send_quote:
            msg.quote = reply_to
        frame_json = msg.to_json(from_robot)
        _log_im_tx(frame_json)
        try:
            await ws.send(frame_json)
        except Exception as exc:
            logger.warning("[chaoranxin] send_%s failed: %s", clazz.lower(), exc)
            return SendResult(success=False, error=str(exc))
        return SendResult(success=True, message_id=msg.msg_id)

    async def _send_picture_frame(
        self,
        chat_id: str,
        access_url: str,
        *,
        reply_to: Optional[str] = None,
        width: Optional[str] = None,
        height: Optional[str] = None,
    ) -> SendResult:
        """Send a ``type=Picture`` WS frame using an already-uploaded accessUrl."""
        try:
            content = build_picture_content(
                access_url, width=width, height=height
            )
        except ValueError as exc:
            return SendResult(success=False, error=str(exc))
        return await self._send_clazz_frame(
            chat_id, MSG_CLAZZ_PICTURE, content, reply_to=reply_to
        )

    async def _send_caption_then(
        self,
        chat_id: str,
        caption: Optional[str],
        reply_to: Optional[str],
    ) -> Optional[SendResult]:
        """Send caption as Markdown first; return failed SendResult or None."""
        if caption and str(caption).strip():
            cap_result = await self.send(
                chat_id=chat_id,
                content=str(caption).strip(),
                reply_to=reply_to,
            )
            if not cap_result.success:
                return cap_result
        return None

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """Upload a local image to object storage, then send a Picture frame.

        Caption is sent as a separate Markdown message (Picture has no
        caption field in the protocol).
        """
        del metadata, kwargs  # accepted for base-class contract
        safe = self.validate_media_delivery_path(image_path)
        if not safe:
            return SendResult(
                success=False,
                error=f"unsafe or missing image path: {image_path}",
            )
        if not is_image_path(safe):
            return SendResult(
                success=False,
                error=(
                    f"chaoranxin send_image_file: unsupported image type "
                    f"{Path(safe).suffix!r} (supported: {sorted(IMAGE_EXTS)})"
                ),
            )
        if not self._bot_token:
            return SendResult(success=False, error="missing bot token")

        cap_fail = await self._send_caption_then(chat_id, caption, reply_to)
        if cap_fail is not None:
            return cap_fail

        try:
            access_url, width, height = await upload_local_image(
                self._bot_token,
                safe,
                file_base=self._file_base,
            )
        except Exception as exc:
            logger.warning("[chaoranxin] image upload failed: %s", exc)
            return SendResult(success=False, error=str(exc))

        return await self._send_picture_frame(
            chat_id,
            access_url,
            reply_to=reply_to,
            width=width,
            height=height,
        )

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """Upload audio and send a platform ``Voice`` frame (``url`` + ``size``)."""
        del metadata, kwargs
        safe = self.validate_media_delivery_path(audio_path)
        if not safe:
            return SendResult(
                success=False,
                error=f"unsafe or missing audio path: {audio_path}",
            )
        if not self._bot_token:
            return SendResult(success=False, error="missing bot token")

        cap_fail = await self._send_caption_then(chat_id, caption, reply_to)
        if cap_fail is not None:
            return cap_fail

        try:
            access_url, size_bytes = await upload_local_file(
                self._bot_token,
                safe,
                file_base=self._file_base,
            )
            content = build_voice_content(access_url, size_bytes)
        except Exception as exc:
            logger.warning("[chaoranxin] voice upload failed: %s", exc)
            return SendResult(success=False, error=str(exc))

        return await self._send_clazz_frame(
            chat_id, MSG_CLAZZ_VOICE, content, reply_to=reply_to
        )

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """Upload video and send a platform ``LocalVideo`` frame (``video`` + ``cover``).

        Chaoranxin does not use a ``Video`` clazz for IM outbound; the
        client permission / render type is ``LocalVideo``.
        """
        del metadata, kwargs
        safe = self.validate_media_delivery_path(video_path)
        if not safe:
            return SendResult(
                success=False,
                error=f"unsafe or missing video path: {video_path}",
            )
        if not self._bot_token:
            return SendResult(success=False, error="missing bot token")

        cap_fail = await self._send_caption_then(chat_id, caption, reply_to)
        if cap_fail is not None:
            return cap_fail

        try:
            access_url, _size = await upload_local_file(
                self._bot_token,
                safe,
                file_base=self._file_base,
            )
            content = build_localvideo_content(access_url, cover="")
        except Exception as exc:
            logger.warning("[chaoranxin] video upload failed: %s", exc)
            return SendResult(success=False, error=str(exc))

        return await self._send_clazz_frame(
            chat_id, MSG_CLAZZ_LOCAL_VIDEO, content, reply_to=reply_to
        )

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """Upload a file and send a platform ``LocalFile`` frame.

        Content fields: ``fileurl``, ``filename``, ``filesize`` (int).
        """
        del metadata, kwargs
        safe = self.validate_media_delivery_path(file_path)
        if not safe:
            return SendResult(
                success=False,
                error=f"unsafe or missing file path: {file_path}",
            )
        if not self._bot_token:
            return SendResult(success=False, error="missing bot token")

        cap_fail = await self._send_caption_then(chat_id, caption, reply_to)
        if cap_fail is not None:
            return cap_fail

        display_name = (file_name or "").strip() or Path(safe).name
        try:
            access_url, size_bytes = await upload_local_file(
                self._bot_token,
                safe,
                file_base=self._file_base,
                filename=display_name,
            )
            content = build_localfile_content(
                access_url, display_name, size_bytes
            )
        except Exception as exc:
            logger.warning("[chaoranxin] file upload failed: %s", exc)
            return SendResult(success=False, error=str(exc))

        return await self._send_clazz_frame(
            chat_id, MSG_CLAZZ_LOCAL_FILE, content, reply_to=reply_to
        )

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a remote image as a native Picture frame.

        URLs already hosted on the configured file service ``/oss/{id}``
        are sent directly.  Other http(s) URLs are downloaded (SSRF-safe)
        then uploaded via :meth:`send_image_file`.
        """
        url = (image_url or "").strip()
        if not url:
            return SendResult(success=False, error="empty image_url")

        if is_file_service_oss_url(url, self._file_base):
            if caption and str(caption).strip():
                cap_result = await self.send(
                    chat_id=chat_id,
                    content=str(caption).strip(),
                    reply_to=reply_to,
                    metadata=metadata,
                )
                if not cap_result.success:
                    return cap_result
            return await self._send_picture_frame(
                chat_id, url, reply_to=reply_to
            )

        from tools.url_safety import is_safe_url

        if not is_safe_url(url):
            logger.warning(
                "[chaoranxin] Blocked unsafe image URL during send_image"
            )
            return await super().send_image(
                chat_id, image_url, caption, reply_to, metadata=metadata
            )

        try:
            from gateway.platforms.base import cache_image_from_url

            local_path = await cache_image_from_url(url)
        except Exception as exc:
            logger.warning(
                "[chaoranxin] Failed to download image %s: %s", url, exc
            )
            return await super().send_image(
                chat_id, image_url, caption, reply_to, metadata=metadata
            )

        return await self.send_image_file(
            chat_id=chat_id,
            image_path=local_path,
            caption=caption,
            reply_to=reply_to,
            metadata=metadata,
        )

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Chaoranxin v1 has no typing-indicator primitive."""
        return None

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return minimal chat info for the session builder.

        The platform has no chat-enumeration API — best we can do is echo
        the id.  The gateway runner uses this for display only.
        """
        return {"name": chat_id, "type": "dm", "chat_id": chat_id}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _epoch_to_dt(epoch_seconds: float):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


# ---------------------------------------------------------------------------
# Env enablement / standalone send
# ---------------------------------------------------------------------------


def _env_enablement() -> dict | None:
    """Seed ``PlatformConfig.extra`` from env vars at config-load time."""
    if not check_requirements():
        return None
    seed: Dict[str, Any] = {
        "bot_token": _env("CHAORANXIN_BOT_TOKEN"),
    }
    host, api_base = _normalize_endpoints(
        _env("CHAORANXIN_HOST"), _env("CHAORANXIN_API_BASE")
    )
    if host:
        seed["host"] = host
    if api_base:
        seed["api_base"] = api_base
    robot_path = _env("CHAORANXIN_ROBOT_PATH")
    if robot_path:
        seed["robot_path"] = robot_path
    bot_id = _env("CHAORANXIN_BOT_ID")
    if bot_id:
        seed["bot_id"] = bot_id
    home = _env("CHAORANXIN_HOME_CHANNEL")
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": _env("CHAORANXIN_HOME_CHANNEL_NAME") or home,
        }
    return seed


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Out-of-process publish for cron / ``send_message_tool`` fallbacks.

    Used when ``hermes cron`` runs standalone (no gateway runner in
    this process).  Dials a one-shot WebSocket, awaits the RobotLogin
    Status to learn the robot uuid, sends text + optional media frames
    (Picture / Voice / LocalVideo / LocalFile), and closes.

    Resolves the WS URL the same way the adapter does:
    * ``extra.host`` (or ``CHAORANXIN_HOST``) → dial directly
    * otherwise → call ``/im/api/v1/robot/servers`` against
      ``extra.api_base`` (or ``CHAORANXIN_API_BASE``) and dial the
      first node.

    ``media_files`` entries are ``(path, is_voice)`` tuples (or bare
    path strings).  Classification: voice flag → Voice; image → Picture;
    video ext → LocalVideo; else LocalFile.  ``force_document`` forces
    non-image attachments to LocalFile.
    """
    del thread_id  # accepted for registry contract
    _trace("LIFECYCLE", "standalone_send enter", chars=len(message or ""))
    if not WEBSOCKETS_AVAILABLE:
        return {"error": "chaoranxin standalone send: websockets not installed"}

    extra = getattr(pconfig, "extra", {}) or {}
    host = (extra.get("host") or _env("CHAORANXIN_HOST", "")).rstrip("/")
    api_base = (
        extra.get("api_base") or _env("CHAORANXIN_API_BASE", "")
    ).rstrip("/")
    host, api_base = _normalize_endpoints(host, api_base)
    token = extra.get("bot_token") or _env("CHAORANXIN_BOT_TOKEN", "")
    file_base = resolve_file_base(extra)
    robot_path = (
        extra.get("robot_path") or _env("CHAORANXIN_ROBOT_PATH", DEFAULT_ROBOT_PATH)
    ).strip() or DEFAULT_ROBOT_PATH
    if not robot_path.startswith("/"):
        robot_path = "/" + robot_path
    bot_id_fallback = extra.get("bot_id") or _env("CHAORANXIN_BOT_ID", "")
    if not token:
        _trace("ERROR", "standalone_send: missing bot_token")
        return {"error": "chaoranxin standalone send: missing bot_token"}
    if not (host or api_base):
        _trace("ERROR", "standalone_send: missing host/api_base")
        return {
            "error": "chaoranxin standalone send: missing host or api_base "
            "(set CHAORANXIN_HOST or CHAORANXIN_API_BASE)"
        }
    target = chat_id or _env("CHAORANXIN_HOME_CHANNEL")
    if not target:
        _trace("ERROR", "standalone_send: empty target")
        return {
            "error": "chaoranxin standalone send: chat_id and "
            "CHAORANXIN_HOME_CHANNEL are both empty"
        }

    # Normalize media_files → list of (safe_path, kind).
    media_items: List[Tuple[str, str]] = []
    for entry in media_files or []:
        is_voice = False
        if isinstance(entry, (list, tuple)):
            media_path = entry[0] if entry else ""
            if len(entry) > 1:
                is_voice = bool(entry[1])
        else:
            media_path = entry
        media_path = str(media_path or "").strip()
        if not media_path:
            continue
        safe = BasePlatformAdapter.validate_media_delivery_path(media_path)
        if not safe:
            return {"error": f"chaoranxin standalone send: unsafe media path: {media_path}"}
        if not Path(safe).is_file():
            return {"error": f"chaoranxin standalone send: media file not found: {safe}"}
        if force_document and not is_image_path(safe):
            kind = "localfile"
        else:
            kind = classify_media_path(safe, is_voice=is_voice)
        media_items.append((safe, kind))

    if not (message or "").strip() and not media_items:
        return {
            "error": "No deliverable text or media remained after processing MEDIA tags"
        }

    cap = _env_int("CHAORANXIN_MAX_MESSAGE_LENGTH", DEFAULT_MAX_MESSAGE_LENGTH)
    if cap <= 0:
        cap = DEFAULT_MAX_MESSAGE_LENGTH
    truncated = False
    text = message or ""
    if len(text) > cap:
        truncated = True
        text = text[:cap]

    headers = _build_handshake_headers(token)

    _trace(
        "TX",
        "standalone_send prepared",
        target=target,
        truncated=truncated,
        media=len(media_items),
        token=_mask_token(token),
    )

    # Resolve the WS URL: direct host or auto-discovered node.
    if host:
        url = f"{host}{robot_path}"
    else:
        nodes, error = await _fetch_node_list(api_base, token)
        if error or not nodes:
            return {
                "error": f"chaoranxin standalone send node discovery: {error}"
            }
        try:
            url = nodes[0].ws_url()
        except ValueError as exc:
            return {
                "error": f"chaoranxin standalone send: discovered node unusable: {exc}"
            }

    last_msg_id: Optional[str] = None
    _trace("WS", "standalone_send dial", url=url)
    try:
        async with websockets.connect(
            url,
            additional_headers=headers,
            close_timeout=5,
            max_size=2 ** 20,
            ping_interval=None,
        ) as ws:
            # Wait for the RobotLogin frame to learn the real robot uuid
            # (spec §3.3: RobotLogin is a top-level frame with
            # ``data.ok``, ``data.robot``, ``data.owner``).
            robot_uuid = bot_id_fallback
            try:
                first = await asyncio.wait_for(ws.recv(), timeout=10.0)
            except asyncio.TimeoutError:
                first = None
            if first is not None:
                if isinstance(first, (bytes, bytearray)):
                    try:
                        first = first.decode("utf-8")
                    except UnicodeDecodeError:
                        first = None
                if isinstance(first, str):
                    _log_im_rx(first)
                frame = IncomingFrame.parse(first) if first else None
                if frame and frame.is_robot_login:
                    login = RobotLoginFrame.parse(frame)
                    if login.is_success and login.robot:
                        robot_uuid = login.robot
            _trace(
                "RX",
                "standalone_send handshake received",
                bot_uuid_from_login=robot_uuid,
            )
            if not robot_uuid:
                return {
                    "error": "chaoranxin standalone send: no robot uuid "
                    "(handshake did not deliver one and no fallback configured)"
                }

            if text.strip():
                msg = OutboundMsg(
                    to=target, text=text, clazz=MSG_CLAZZ_MARKDOWN
                )
                frame_json = msg.to_json(robot_uuid)
                _log_im_tx(frame_json)
                await ws.send(frame_json)
                last_msg_id = msg.msg_id

            for media_path, kind in media_items:
                try:
                    if kind == "picture":
                        access_url, width, height = await upload_local_image(
                            token, media_path, file_base=file_base
                        )
                        content = build_picture_content(
                            access_url, width=width, height=height
                        )
                        clazz = MSG_CLAZZ_PICTURE
                    else:
                        access_url, size_bytes = await upload_local_file(
                            token, media_path, file_base=file_base
                        )
                        if kind == "voice":
                            content = build_voice_content(access_url, size_bytes)
                            clazz = MSG_CLAZZ_VOICE
                        elif kind == "localvideo":
                            content = build_localvideo_content(
                                access_url, cover=""
                            )
                            clazz = MSG_CLAZZ_LOCAL_VIDEO
                        else:
                            content = build_localfile_content(
                                access_url,
                                Path(media_path).name,
                                size_bytes,
                            )
                            clazz = MSG_CLAZZ_LOCAL_FILE
                except Exception as exc:
                    return {
                        "error": (
                            f"chaoranxin standalone send {kind} "
                            f"upload failed: {exc}"
                        )
                    }
                outbound = OutboundMsg(to=target).set_clazz(clazz, content)
                frame_json = outbound.to_json(robot_uuid)
                _log_im_tx(frame_json)
                await ws.send(frame_json)
                last_msg_id = outbound.msg_id
    except websockets.exceptions.InvalidStatus as exc:
        status = getattr(exc, "status_code", None) or getattr(exc, "code", None)
        _trace("ERROR", "standalone_send: InvalidStatus", url=url, http_status=status)
        return {
            "error": f"chaoranxin standalone send: HTTP {status} from server"
        }
    except websockets.exceptions.InvalidStatusCode as exc:
        _trace("ERROR", "standalone_send: InvalidStatusCode", url=url, http_status=exc.status_code)
        return {
            "error": f"chaoranxin standalone send: HTTP {exc.status_code} from server"
        }
    except Exception as exc:
        _trace("ERROR", "standalone_send exception", url=url, error=str(exc))
        return {"error": f"chaoranxin standalone send failed: {exc}"}
    _trace(
        "TX",
        "standalone_send done",
        target=target,
        msg_id=last_msg_id,
        media=len(media_items),
    )
    return {
        "success": True,
        "platform": "chaoranxin",
        "chat_id": target,
        "message_id": last_msg_id,
    }


# ---------------------------------------------------------------------------
# config.yaml <-> env bridge
# ---------------------------------------------------------------------------


def _apply_yaml_config(yaml_cfg: dict, platform_cfg: dict) -> Optional[dict]:
    """Bridge ``config.yaml`` ``chaoranxin:`` keys into ``CHAORANXIN_*`` env vars."""
    for stray in ("CHAORANXIN_HOST", "CHAORANXIN_API_BASE", "CHAORANXIN_BOT_TOKEN"):
        if stray in yaml_cfg:
            logger.warning(
                "[chaoranxin] config.yaml has misplaced top-level %s — "
                "ignored; use ~/.hermes/.env or platforms.chaoranxin.extra",
                stray,
            )

    raw_host = str(platform_cfg.get("host", "") or "").strip()
    raw_api = str(platform_cfg.get("api_base", "") or "").strip()
    host, api_base = _normalize_endpoints(raw_host, raw_api)

    env_keys = {
        "bot_token": "CHAORANXIN_BOT_TOKEN",
        "robot_path": "CHAORANXIN_ROBOT_PATH",
        "bot_id": "CHAORANXIN_BOT_ID",
        "file_base": "CHAORANXIN_FILE_BASE",
    }
    for yaml_key, env_key in env_keys.items():
        if yaml_key in platform_cfg and not os.getenv(env_key):
            value = str(platform_cfg[yaml_key]).strip()
            if value:
                os.environ[env_key] = value

    if api_base and not os.getenv("CHAORANXIN_API_BASE"):
        os.environ["CHAORANXIN_API_BASE"] = api_base
    elif raw_api and not os.getenv("CHAORANXIN_API_BASE"):
        os.environ["CHAORANXIN_API_BASE"] = raw_api.rstrip("/")

    # Direct WS host is advanced-only — never bridge when api_base is set.
    if host and not api_base and not os.getenv("CHAORANXIN_HOST"):
        os.environ["CHAORANXIN_HOST"] = host

    return None


# ---------------------------------------------------------------------------
# Interactive setup wizard (optional)
# ---------------------------------------------------------------------------


def _save_and_export(key: str, value: str, save_env_value) -> None:
    """Persist an env var via ``save_env_value`` AND update ``os.environ``.

    ``save_env_value`` writes to ``~/.hermes/.env`` but does NOT touch
    the current process's env.  Without mirroring the write into
    ``os.environ`` here, a follow-up ``_env(key)`` check in the same
    setup run would still see the old (empty) value and the wizard
    would re-prompt for the same field.
    """
    save_env_value(key, value)
    os.environ[key] = value


def _clear_env(key: str, save_env_value) -> None:
    """Remove an env var from the current process and ~/.hermes/.env."""
    save_env_value(key, "")
    os.environ.pop(key, None)


def _migrate_rest_host_env(save_env_value) -> None:
    """Drop misplaced REST origins from CHAORANXIN_HOST; keep API_BASE only."""
    from hermes_cli.config import get_env_value

    host = (get_env_value("CHAORANXIN_HOST") or "").strip().rstrip("/")
    api = (get_env_value("CHAORANXIN_API_BASE") or "").strip().rstrip("/")
    if not host:
        return
    new_host, new_api = _normalize_endpoints(host, api)
    if new_api and not api:
        _save_and_export("CHAORANXIN_API_BASE", new_api, save_env_value)
    if not new_host:
        _clear_env("CHAORANXIN_HOST", save_env_value)


def interactive_setup() -> None:
    """Best-effort CLI setup for ``hermes gateway setup``.

    Prompts for token + API base URL (auto-discovery) or direct WS host.
    ``CHAORANXIN_HOME_CHANNEL`` and robot uuid come from ``RobotLogin`` at
    gateway connect — never prompted here.
    """
    try:
        from hermes_cli.cli_output import (
            print_header,
            print_info,
            print_success,
            print_warning,
            prompt,
        )
        from hermes_cli.config import get_env_value, save_env_value, write_platform_config_field
        from hermes_cli.setup import prompt_yes_no
    except ImportError:
        return

    print()
    print_header("Chaoranxin (超然信)")

    _migrate_rest_host_env(save_env_value)

    existing_token = get_env_value("CHAORANXIN_BOT_TOKEN") or ""
    existing_api = get_env_value("CHAORANXIN_API_BASE") or ""
    existing_host = get_env_value("CHAORANXIN_HOST") or ""
    existing_host, existing_api = _normalize_endpoints(existing_host, existing_api)

    reconfigure = False
    if check_requirements():
        detail_parts = []
        if existing_api:
            detail_parts.append(f"api_base={existing_api}")
        if existing_host:
            detail_parts.append(f"host={existing_host}")
        detail = f" ({', '.join(detail_parts)})" if detail_parts else ""
        print_success(f"Chaoranxin is already configured{detail}.")
        print_info(
            "  Robot uuid and CHAORANXIN_HOME_CHANNEL are filled from "
            "RobotLogin when the gateway connects."
        )
        if not prompt_yes_no("  Reconfigure Chaoranxin?", False):
            try:
                write_platform_config_field("chaoranxin", "enabled", True, raw=True)
            except Exception:
                pass
            return
        reconfigure = True
        print()

    # --- bot token ---
    if reconfigure or not existing_token:
        if reconfigure and existing_token:
            print_info(
                "  Bot token is set. Enter a new rbt_* value or leave blank to keep it."
            )
        token = prompt("Chaoranxin bot token (starts with rbt_)", password=True)
        if token.strip():
            _save_and_export("CHAORANXIN_BOT_TOKEN", token.strip(), save_env_value)
            print_success("Chaoranxin bot token saved")
        elif not existing_token:
            print_warning("Bot token is required — skipping Chaoranxin setup")
            return

    # --- endpoint: api_base (auto-discovery) ---
    if reconfigure or not existing_api:
        api_base = prompt(
            "Chaoranxin API base URL (e.g. https://api.example.com — "
            "the adapter calls /im/api/v1/robot/servers here)",
            default=existing_api if reconfigure else None,
            password=False,
        )
        api_base = (api_base or "").strip()
        if api_base:
            scheme_err = _api_base_scheme_error(api_base)
            if scheme_err:
                if api_base.lower().startswith("ws://"):
                    api_base = "http://" + api_base[len("ws://"):]
                    print_info("Auto-corrected scheme: http://")
                elif api_base.lower().startswith("wss://"):
                    api_base = "https://" + api_base[len("wss://"):]
                    print_info("Auto-corrected scheme: https://")
                else:
                    print_warning(scheme_err)
                    api_base = ""
            if api_base:
                _save_and_export("CHAORANXIN_API_BASE", api_base, save_env_value)
                _clear_env("CHAORANXIN_HOST", save_env_value)
                print_success(f"Chaoranxin API base set to {api_base}")

        if not _env("CHAORANXIN_API_BASE"):
            print_warning(
                "API base URL is required — set CHAORANXIN_API_BASE "
                "(e.g. https://api.xsign.co)"
            )
            return

    # robot_path defaults to /robot (DEFAULT_ROBOT_PATH) — never prompted.
    # CHAORANXIN_HOME_CHANNEL is auto-set from RobotLogin.data.owner at
    # WS connect time — never prompted during setup.
    # CHAORANXIN_BOT_ID is optional and comes from RobotLogin — never prompted.

    try:
        write_platform_config_field("chaoranxin", "enabled", True, raw=True)
    except Exception:
        pass
    print_success("🤖 Chaoranxin configured")


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system at startup."""
    if not os.getenv("CHAORANXIN_ALLOW_ALL_USERS"):
        os.environ["CHAORANXIN_ALLOW_ALL_USERS"] = "true"
    try:
        from hermes_cli.config import save_env_value

        _migrate_rest_host_env(save_env_value)
    except Exception:
        logger.debug("[chaoranxin] endpoint env migration skipped", exc_info=True)

    ctx.register_platform(
        name="chaoranxin",
        label="Chaoranxin (超然信 rbt_* WS Bot)",
        adapter_factory=lambda cfg: ChaoranxinAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        # Token is the only strict requirement — the WS endpoint can be
        # either a direct host (``CHAORANXIN_HOST``) or auto-discovered
        # from the API base (``CHAORANXIN_API_BASE``).  check_requirements
        # enforces the "one of the two" rule.
        required_env=["CHAORANXIN_BOT_TOKEN"],
        install_hint="pip install websockets httpx   # both already in hermes-agent[all] extras",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="CHAORANXIN_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="CHAORANXIN_ALLOWED_USERS",
        allow_all_env="CHAORANXIN_ALLOW_ALL_USERS",
        apply_yaml_config_fn=_apply_yaml_config,
        setup_fn=interactive_setup,
        max_message_length=DEFAULT_MAX_MESSAGE_LENGTH,
        emoji="🤖",
        # Chaoranxin sender IDs are opaque platform IDs, not phone numbers
        # or emails — no PII to redact.
        pii_safe=True,
        allow_update_command=True,
        platform_hint=(
            "You are communicating via Chaoranxin (超然信), a custom rbt_* "
            "WebSocket bot.  Configuration: provide "
            "CHAORANXIN_BOT_TOKEN plus EITHER "
            "CHAORANXIN_API_BASE (recommended — adapter auto-discovers "
            "the WS node from GET /im/api/v1/robot/servers) OR "
            "CHAORANXIN_HOST (direct dial, skip the lookup).  "
            "Plaintext JSON frames on the /robot path; the handshake "
            "(RobotLogin frame) is automatic after the Bearer-token WS "
            "upgrade — do not send a Login frame.  Use plain text by "
            "default; media is delivered natively after object-storage "
            "upload (Picture / Voice / LocalVideo / LocalFile frames).  "
            f"Keep responses under {DEFAULT_MAX_MESSAGE_LENGTH} "
            "characters per message; longer text will be truncated."
        ),
    )