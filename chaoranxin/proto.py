"""Chaoranxin (超然信) wire-protocol codec.

Implements the rbt_* WebSocket bot protocol described in
``docs/chaoranxin/ROBOT_THIRD_PARTY.md``.

Top-level frame shapes (always plaintext JSON on the bot channel):

* Outbound — :class:`OutboundMsg`, :class:`OutboundHeart`

      {"type": "Markdown", "data": {"from": ..., "to": ..., "clazz": "Markdown", ...}}
      {"type": "Heart",    "data": {"time": <ms>}}

* Inbound — :class:`IncomingFrame` parses any of these:

      {"type": "Status",     "data": {"type": "Heart|Msg|Markdown|Text|...", "status": 100|-1, ...}}
      {"type": "RobotEvent", "data": {"schema": "2.0", "header": {...}, "event": {...}}}
      {"type": "RobotLogin", "data": {"ok": true, "robot": "...", "owner": "...", ...}}

Status subtypes:
  * ``Heart`` — receipt for our heartbeat; status=100 means the channel
    is healthy.
  * ``Msg`` / content types (``Markdown``, ``Text``, ``Picture``, …) —
    receipt for a sent message. ``status=100`` accepted,
    ``status=-1`` rejected (server-side validation failed; ``msg`` carries
    the reason).

RobotEvent envelopes carry Feishu-schema-2.0-style ``header.event_type``
values. v1 only handles ``im.message.receive_v1`` (user → bot text /
picture / article / etc.); everything else is dropped at the envelope
boundary.

All field accesses go through small helpers so missing / unknown keys
return safe defaults — schema drift never raises into the dispatcher.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union


# ---------------------------------------------------------------------------
# Constants — message classes (clazz) and event types
# ---------------------------------------------------------------------------


#: ``clazz`` values for outbound message frames and inbound msg_type mirrors.
MSG_CLAZZ_MARKDOWN = "Markdown"
MSG_CLAZZ_TEXT = "Text"
MSG_CLAZZ_PICTURE = "Picture"
MSG_CLAZZ_VIDEO = "Video"
MSG_CLAZZ_FILE = "File"
MSG_CLAZZ_VOICE = "Voice"
MSG_CLAZZ_AT = "At"
MSG_CLAZZ_ARTICLE = "Article"
MSG_CLAZZ_URL = "Url"
MSG_CLAZZ_NEWS = "News"

#: All outbound content clazz values (top-level ``type`` must match ``clazz``).
MSG_CONTENT_CLAZZES = frozenset({
    MSG_CLAZZ_MARKDOWN, MSG_CLAZZ_TEXT, MSG_CLAZZ_PICTURE, MSG_CLAZZ_VIDEO,
    MSG_CLAZZ_FILE, MSG_CLAZZ_VOICE, MSG_CLAZZ_AT, MSG_CLAZZ_ARTICLE,
    MSG_CLAZZ_URL, MSG_CLAZZ_NEWS,
})

#: Map msg_type (inbound, lowercase) -> clazz (outbound, PascalCase).
#: Per spec: ``Text``/``Markdown`` -> ``text``/``markdown``, other clazzes
#: -> lowercase class name.
INBOUND_MSG_TYPE_TO_CLAZZ: Dict[str, str] = {
    "text": MSG_CLAZZ_TEXT,
    "markdown": MSG_CLAZZ_MARKDOWN,
    "picture": MSG_CLAZZ_PICTURE,
    "video": MSG_CLAZZ_VIDEO,
    "file": MSG_CLAZZ_FILE,
    "voice": MSG_CLAZZ_VOICE,
    "at": MSG_CLAZZ_AT,
    "article": MSG_CLAZZ_ARTICLE,
    "url": MSG_CLAZZ_URL,
    "news": MSG_CLAZZ_NEWS,
}


#: Handled RobotEvent event_type values. v1 wires only message-receive;
#: other types (bot.added_v1, message_read_v1, etc.) are logged and ignored.
EVENT_MESSAGE_RECEIVE = "im.message.receive_v1"
EVENT_BOT_ADDED = "application.bot.added_v1"
EVENT_MESSAGE_READ = "im.message.message_read_v1"

HANDLED_EVENT_TYPES = frozenset({EVENT_MESSAGE_RECEIVE})

#: Schema version expected on RobotEvent.data.schema. Mismatch is logged
#: but does NOT drop the event — protocol drift is forward-compatible.
EXPECTED_EVENT_SCHEMA = "2.0"


#: Status subtypes we recognize. ``Msg`` receipts and ``Heart`` echoes are
#: matched in the dispatcher; ``RobotLogin`` flips the adapter into
#: "logged in" state.
STATUS_TYPE_ROBOT_LOGIN = "RobotLogin"
STATUS_TYPE_HEART = "Heart"
STATUS_TYPE_MSG = "Msg"

#: Status subtypes that carry send receipts (legacy ``Msg`` + content types).
MSG_RECEIPT_SUBTYPES = frozenset({STATUS_TYPE_MSG}) | MSG_CONTENT_CLAZZES

#: Status codes. 100 = server accepted; -1 = server-side validation
#: failed (see ``msg`` field for human reason).
STATUS_OK = 100
STATUS_FAIL = -1


# ---------------------------------------------------------------------------
# Node discovery — GET /im/api/v1/robot/servers
# ---------------------------------------------------------------------------


@dataclass
class NodeEndpoint:
    """One entry in the ``data[]`` array of the node-list response.

    Legacy spec §2.2 fields:
      * ``host`` + ``port``  — TCP / WebSocket entry port
      * ``inter``            — actual WebSocket port (may differ from ``port``)
      * ``path``             — user IM channel path (e.g. ``/imx``); **not**
        prefixed onto ``robot`` — they are sibling mount paths
      * ``media``            — media / WebRTC path (default ``/media``)
      * ``robot``            — bot channel path from the server list (required)

    Production clusters (xsign) also return ``proto`` (``ws`` / ``wss``).
    """

    host: str
    port: int = 0
    inter: int = 0
    proto: str = "ws"
    path: str = "/"
    media: str = "/media"
    robot: str = ""

    @property
    def inter_or_port(self) -> int:
        """The actual WebSocket port — prefers ``inter``, falls back to ``port``."""
        return self.inter or self.port

    def ws_url(self) -> str:
        """Build the WebSocket URL for the bot channel.

        Dial ``{proto}://{host}[:{port}]{robot}``.  ``robot`` comes from the
        node-list API response and is a top-level mount path — not
        ``{path}{robot}``.  When ``proto`` is ``wss`` on 443 or ``ws`` on 80
        the default port is omitted.
        """
        scheme = (self.proto or "ws").strip().lower()
        if scheme not in ("ws", "wss"):
            scheme = "ws"
        port = self.inter_or_port
        if not port:
            raise ValueError(
                f"NodeEndpoint {self.host!r} has neither `inter` nor `port` set"
            )
        robot = (self.robot or "").strip()
        if not robot:
            raise ValueError(
                f"NodeEndpoint {self.host!r} has no `robot` path from server list"
            )
        if not robot.startswith("/"):
            robot = "/" + robot
        if (scheme == "wss" and port == 443) or (scheme == "ws" and port == 80):
            return f"{scheme}://{self.host}{robot}"
        return f"{scheme}://{self.host}:{port}{robot}"


def parse_node_list(payload: Any) -> List[NodeEndpoint]:
    """Parse the response of ``GET /im/api/v1/robot/servers``.

    Spec response shape::

        {"code": 0, "msg": "success",
         "data": [{"host": ..., "port": ..., "inter": ...,
                   "path": ..., "media": ..., "robot": ...}, ...]}

    Returns an empty list on any parse failure so the caller can fall
    back to its retry / error path.  No exceptions are raised — node
    discovery is best-effort and the adapter treats it as fatal on
    empty result.
    """
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if not isinstance(data, list):
        return []
    out: List[NodeEndpoint] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        host = str(entry.get("host") or "").strip()
        if not host:
            continue
        try:
            port = int(entry.get("port") or 0)
        except (TypeError, ValueError):
            port = 0
        try:
            inter = int(entry.get("inter") or 0)
        except (TypeError, ValueError):
            inter = 0
        proto = str(entry.get("proto") or "ws").strip().lower() or "ws"
        robot_raw = entry.get("robot")
        if robot_raw is None:
            continue
        robot = str(robot_raw).strip()
        if not robot:
            continue
        if not robot.startswith("/"):
            robot = "/" + robot
        out.append(
            NodeEndpoint(
                host=host,
                port=port,
                inter=inter,
                proto=proto,
                path=str(entry.get("path") or "/"),
                media=str(entry.get("media") or "/media"),
                robot=robot,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Outbound — Data<Msg>
# ---------------------------------------------------------------------------


@dataclass
class OutboundMsg:
    """Outbound message frame (text / picture / article / …).

    Per spec, the wire shape is::

        {"type": "Markdown",
         "data": {"uuid": "<id>", "from": "<robot-uuid>",
                  "to": "<user-uuid>", "clazz": "Markdown",
                  "role": "robot", "content": {"text": "..."},
                  "quote": "...", "ats": [...]}}

    Top-level ``type`` **must match** ``data.clazz`` (content type name).
    Legacy ``type=Msg`` is still accepted by the server but deprecated.

    The class encodes the most common case (plain text + optional @ /
    quote). Picture / Article / Url / News / file-style sends are
    constructed by setting ``clazz`` and ``content`` directly via
    :meth:`set_clazz`.

    ``uuid`` is auto-generated and doubles as the idempotency key —
    duplicate ``uuid`` within 24h is treated by the server as already
    sent. Adapter-side we use it to match inbound Status send receipts
    back to our outgoing frame.
    """

    to: str
    text: str = ""
    msg_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    clazz: str = MSG_CLAZZ_MARKDOWN
    role: str = "robot"
    quote: Optional[str] = None
    ats: Optional[List[Dict[str, Any]]] = None
    #: Allow caller to override ``content`` for non-text clazzes.
    content: Optional[Dict[str, Any]] = None

    def set_clazz(self, clazz: str, content: Dict[str, Any]) -> "OutboundMsg":
        """Switch this frame to a non-text clazz (Picture, Article, ...).

        ``content`` becomes the inner Data<Msg>.content JSON. Returns
        self for fluent chaining.
        """
        self.clazz = clazz
        self.content = content
        return self

    def to_frame(self, from_robot_uuid: str) -> Dict[str, Any]:
        """Render the wire frame as a JSON-serializable dict.

        Top-level ``type`` equals ``clazz`` (content type) — e.g.
        ``Markdown``, ``Picture``.  Must stay in sync with ``data.clazz``.
        """
        if not from_robot_uuid:
            raise ValueError(
                "OutboundMsg.to_frame requires a non-empty robot uuid "
                "(from the RobotLogin handshake)"
            )
        content = self.content if self.content is not None else {"text": self.text}
        data: Dict[str, Any] = {
            "uuid": self.msg_id,
            "from": from_robot_uuid,
            "to": self.to,
            "clazz": self.clazz,
            "role": self.role,
            "content": content,
        }
        if self.quote:
            data["quote"] = self.quote
        if self.ats:
            data["ats"] = list(self.ats)
        return {"type": self.clazz, "data": data}

    def to_json(self, from_robot_uuid: str) -> str:
        return json.dumps(
            self.to_frame(from_robot_uuid), ensure_ascii=False
        )


@dataclass
class OutboundHeart:
    """Outbound heartbeat frame.

    Spec: send every 30s (recommended) so the server's 15-minute idle
    timeout never fires. ``time`` is milliseconds since epoch.
    """

    time_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_frame(self) -> Dict[str, Any]:
        return {"type": "Heart", "data": {"time": self.time_ms}}

    def to_json(self) -> str:
        return json.dumps(self.to_frame(), ensure_ascii=False)


# ---------------------------------------------------------------------------
# Inbound — top-level frame
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Top-level frame types
# ---------------------------------------------------------------------------


TYPE_STATUS = "Status"
TYPE_ROBOT_EVENT = "RobotEvent"
TYPE_MSG = "Msg"
TYPE_HEART = "Heart"
TYPE_ROBOT_LOGIN = "RobotLogin"  # standalone per spec §3.3


KNOWN_TOP_LEVEL_TYPES = frozenset(
    {TYPE_STATUS, TYPE_ROBOT_EVENT, TYPE_ROBOT_LOGIN}
)


@dataclass
class IncomingFrame:
    """Parsed top-level inbound frame.

    Use :py:meth:`parse` to construct — bad payloads produce an empty
    frame (``is_empty`` is True) which the dispatcher treats as a no-op.
    """

    top_type: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    # ----- parsing -----

    @classmethod
    def parse(cls, raw: Any) -> "IncomingFrame":
        """Build an ``IncomingFrame`` from a decoded JSON value or string.

        Returns an empty ``IncomingFrame`` when:
          * raw is None / empty / not a dict
          * raw has no recognized ``type`` field

        The adapter's recv loop checks :py:attr:`is_empty` first and
        drops empty frames silently.
        """
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                return cls()
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (TypeError, ValueError):
                return cls()
        if not isinstance(raw, dict):
            return cls()
        top_type = str(raw.get("type") or "")
        if top_type not in KNOWN_TOP_LEVEL_TYPES:
            # Empty frame; downstream will treat as no-op.
            return cls(top_type=top_type, raw=dict(raw))
        return cls(top_type=top_type, raw=dict(raw))

    # ----- helpers -----

    @property
    def data(self) -> Dict[str, Any]:
        """Inner ``data`` object (Status.data / RobotEvent.data)."""
        d = self.raw.get("data")
        return dict(d) if isinstance(d, dict) else {}

    @property
    def is_empty(self) -> bool:
        """True when the frame is unparseable or has no recognized type."""
        return self.top_type not in KNOWN_TOP_LEVEL_TYPES

    @property
    def is_status(self) -> bool:
        return self.top_type == TYPE_STATUS

    @property
    def is_robot_event(self) -> bool:
        return self.top_type == TYPE_ROBOT_EVENT

    @property
    def is_robot_login(self) -> bool:
        return self.top_type == TYPE_ROBOT_LOGIN

    def __bool__(self) -> bool:
        return not self.is_empty


# ---------------------------------------------------------------------------
# Inbound — RobotLogin (standalone, spec §3.3)
# ---------------------------------------------------------------------------


@dataclass
class RobotLoginFrame:
    """Inbound ``{type: "RobotLogin", data: {...}}`` frame.

    Per spec §3.3, this is a top-level frame type (NOT a Status subtype).
    The server pushes it once within ~ms of the WS open, after which:

      * ``ok=True``    → robot uuid and owner are valid, connection bound
      * ``ok=False``   → ``msg`` carries the reason (robot disabled,
                          token mismatch, etc.); ``robot`` and ``owner``
                          are empty

    ``owner`` is the ``sys_user`` uuid of the bot's creator (non-empty
    only when ``ok=True``).  The adapter uses this as the default
    ``CHAORANXIN_HOME_CHANNEL`` so that ``hermes cron --deliver
    chaoranxin`` works without manual setup.
    """

    ok: bool = False
    robot: str = ""
    owner: str = ""
    msg: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, frame: IncomingFrame) -> "RobotLoginFrame":
        if not frame.is_robot_login:
            return cls()
        d = frame.data
        return cls(
            ok=bool(d.get("ok") is True),
            robot=str(d.get("robot") or ""),
            owner=str(d.get("owner") or ""),
            msg=str(d.get("msg") or ""),
            raw=dict(d),
        )

    @property
    def is_success(self) -> bool:
        return self.ok

    @property
    def is_failed(self) -> bool:
        return not self.ok

    def __bool__(self) -> bool:
        return bool(self.robot or self.owner or self.msg)


# ---------------------------------------------------------------------------
# Inbound — Status frames (heartbeat echo / send receipts)
# ---------------------------------------------------------------------------


@dataclass
class StatusFrame:
    """Inbound ``{type: "Status", data: {...}}`` frame.

    ``data.type`` is the Status *subtype* — ``Heart`` or a send-receipt
    content type (``Msg`` legacy, ``Markdown``, ``Text``, ``Picture``, …).
    ``RobotLogin`` is a separate top-level frame type (see
    :class:`RobotLoginFrame`).

    ``data.status`` is the verdict — 100 accepted, -1 failed.
    ``data.value`` and ``data.uuid`` carry the binding identifier
    (sent-message uuid on Msg receipt).
    """

    subtype: str = ""
    status: int = 0
    value: str = ""
    uuid: str = ""
    msg: str = ""
    time: int = 0
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, frame: IncomingFrame) -> "StatusFrame":
        if not frame.is_status:
            return cls()
        d = frame.data
        try:
            status = int(d.get("status") or 0)
        except (TypeError, ValueError):
            status = 0
        try:
            time_ms = int(d.get("time") or 0)
        except (TypeError, ValueError):
            time_ms = 0
        return cls(
            subtype=str(d.get("type") or ""),
            status=status,
            value=str(d.get("value") or ""),
            uuid=str(d.get("uuid") or ""),
            msg=str(d.get("msg") or ""),
            time=time_ms,
            raw=dict(d),
        )

    @property
    def is_ok(self) -> bool:
        return self.status == STATUS_OK

    @property
    def is_failed(self) -> bool:
        return self.status == STATUS_FAIL

    @property
    def is_heart(self) -> bool:
        return self.subtype == STATUS_TYPE_HEART

    @property
    def is_msg_receipt(self) -> bool:
        return self.subtype in MSG_RECEIPT_SUBTYPES

    def __bool__(self) -> bool:
        return bool(self.subtype)


# ---------------------------------------------------------------------------
# Inbound — RobotEvent envelopes
# ---------------------------------------------------------------------------


@dataclass
class RobotEventFrame:
    """Inbound ``{type: "RobotEvent", data: {...}}`` frame.

    Carries a Feishu-schema-2.0-style envelope::

        data.schema   = "2.0"
        data.header   = {event_id, event_type, create_time, robot_id}
        data.event    = {sender, message}

    v1 only handles ``event_type = "im.message.receive_v1"`` — other
    types are returned with ``is_handled = False`` so the dispatcher
    can log + drop.
    """

    header: Dict[str, Any] = field(default_factory=dict)
    event: Dict[str, Any] = field(default_factory=dict)
    schema_version: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, frame: IncomingFrame) -> "RobotEventFrame":
        if not frame.is_robot_event:
            return cls()
        d = frame.data
        header = dict(d.get("header") or {})
        event = dict(d.get("event") or {})
        return cls(
            header=header,
            event=event,
            schema_version=str(d.get("schema") or ""),
            raw=dict(d),
        )

    # ----- envelope accessors -----

    @property
    def event_type(self) -> str:
        return str(self.header.get("event_type") or "")

    @property
    def event_id(self) -> str:
        return str(self.header.get("event_id") or "")

    @property
    def robot_id(self) -> str:
        return str(self.header.get("robot_id") or "")

    @property
    def create_time_ms(self) -> int:
        try:
            return int(self.header.get("create_time") or 0)
        except (TypeError, ValueError):
            return 0

    @property
    def is_handled(self) -> bool:
        """True if this event_type is in v1's handled set."""
        return self.event_type in HANDLED_EVENT_TYPES

    @property
    def schema_ok(self) -> bool:
        """True if the schema field is the expected version (or unset).

        Empty schema is treated as OK for forward compatibility — older
        server deployments may omit the field.
        """
        return not self.schema_version or self.schema_version == EXPECTED_EVENT_SCHEMA

    # ----- message payload -----

    @property
    def message(self) -> Dict[str, Any]:
        msg = self.event.get("message")
        return dict(msg) if isinstance(msg, dict) else {}

    @property
    def msg_type(self) -> str:
        """Inbound ``msg_type`` — lowercase per spec ("text", "picture", ...)."""
        return str(self.message.get("msg_type") or "text")

    @property
    def clazz(self) -> str:
        """Outbound clazz equivalent of :py:attr:`msg_type`."""
        return INBOUND_MSG_TYPE_TO_CLAZZ.get(self.msg_type, self.msg_type)

    @property
    def text(self) -> str:
        """Inner ``content.text`` when msg_type is text — empty otherwise."""
        return str(self.content_field("text") or "")

    @property
    def content(self) -> Dict[str, Any]:
        c = self.message.get("content")
        return dict(c) if isinstance(c, dict) else {}

    def content_field(self, key: str) -> Any:
        """Single key from ``content``, tolerant of stringified content.

        Some clients wrap content as a JSON-encoded string; we accept
        either form and return the requested key.
        """
        c = self.message.get("content")
        if isinstance(c, dict):
            return c.get(key)
        if isinstance(c, str):
            try:
                parsed = json.loads(c)
            except (TypeError, ValueError):
                return None
            if isinstance(parsed, dict):
                return parsed.get(key)
        return None

    @property
    def message_id(self) -> str:
        return str(self.message.get("message_id") or self.event_id)

    @property
    def quote(self) -> str:
        return str(self.message.get("quote") or "")

    # ----- sender / chat -----

    @property
    def sender(self) -> Dict[str, Any]:
        s = self.event.get("sender")
        return dict(s) if isinstance(s, dict) else {}

    @property
    def sender_id(self) -> str:
        s = self.sender
        return str(
            s.get("user_id")
            or s.get("sender_id")
            or s.get("open_id")
            or ""
        )

    @property
    def sender_name(self) -> str:
        s = self.sender
        return str(s.get("name") or s.get("nickname") or "")

    @property
    def chat_id(self) -> str:
        """The user the bot should reply to.

        For single-chat (DM) RobotEvent the platform uses the sender
        user_id as the chat_id — there's no group context in single-
        chat mode. Spec example shows ``event.sender.user_id`` is
        the addressable recipient.
        """
        # Prefer an explicit chat_id if the platform supplies one,
        # else fall back to sender_id (DM convention).
        explicit = self.event.get("chat_id")
        if explicit:
            return str(explicit)
        return self.sender_id

    @property
    def chat_type(self) -> str:
        return str(self.event.get("chat_type") or "dm")

    def __bool__(self) -> bool:
        return bool(self.header or self.event or self.schema_version)


__all__ = [
    # Constants
    "MSG_CLAZZ_MARKDOWN", "MSG_CLAZZ_TEXT", "MSG_CLAZZ_PICTURE", "MSG_CLAZZ_VIDEO",
    "MSG_CLAZZ_FILE", "MSG_CLAZZ_VOICE", "MSG_CLAZZ_AT",
    "MSG_CLAZZ_ARTICLE", "MSG_CLAZZ_URL", "MSG_CLAZZ_NEWS",
    "MSG_CONTENT_CLAZZES", "MSG_RECEIPT_SUBTYPES",
    "INBOUND_MSG_TYPE_TO_CLAZZ",
    "EVENT_MESSAGE_RECEIVE", "EVENT_BOT_ADDED", "EVENT_MESSAGE_READ",
    "HANDLED_EVENT_TYPES", "EXPECTED_EVENT_SCHEMA",
    "STATUS_TYPE_ROBOT_LOGIN", "STATUS_TYPE_HEART", "STATUS_TYPE_MSG",
    "STATUS_OK", "STATUS_FAIL",
    "TYPE_STATUS", "TYPE_ROBOT_EVENT", "TYPE_MSG", "TYPE_HEART",
    "KNOWN_TOP_LEVEL_TYPES",
    # Type literals
    "TYPE_STATUS", "TYPE_ROBOT_EVENT", "TYPE_MSG", "TYPE_HEART",
    "TYPE_ROBOT_LOGIN", "KNOWN_TOP_LEVEL_TYPES",
    # Classes
    "OutboundMsg", "OutboundHeart",
    "IncomingFrame", "RobotLoginFrame", "StatusFrame", "RobotEventFrame",
    "NodeEndpoint", "parse_node_list",
]