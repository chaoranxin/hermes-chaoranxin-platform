"""Chaoranxin (超然信) outbound image upload helpers.

Per ``ROBOT_THIRD_PARTY.md`` §6.2 / §6.3:

1. ``POST {file_base}/objectstorage/upload`` with ``bizType=im``,
   ``accessLevel=PUBLIC``, and ``Authorization: Bearer rbt_*``.
2. Use ``data.accessUrl`` for both ``Picture.content.smallurl`` and
   ``originurl`` (must be identical).
3. Send a WS ``type=Picture`` frame (handled by the adapter).

Absolute imports only — the plugin test loader may register modules
without a package parent.
"""

from __future__ import annotations

import logging
import mimetypes
import os
import struct
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

#: Default minclouds-file public root (authoritative for production).
DEFAULT_FILE_BASE = "https://d.xsign.co"

UPLOAD_PATH = "/objectstorage/upload"
BIZ_TYPE_IM = "im"
ACCESS_LEVEL_PUBLIC = "PUBLIC"

IMAGE_EXTS = frozenset({".jpg", ".jpeg", ".png", ".webp", ".gif"})


def resolve_file_base(
    extra: Optional[Dict[str, Any]] = None,
    *,
    env_name: str = "CHAORANXIN_FILE_BASE",
) -> str:
    """Resolve file-service root from extra / env / default."""
    extra = extra or {}
    raw = (
        str(extra.get("file_base") or "").strip()
        or os.getenv(env_name, "").strip()
        or DEFAULT_FILE_BASE
    )
    return raw.rstrip("/")


def build_picture_content(
    access_url: str,
    *,
    width: Optional[Union[str, int]] = None,
    height: Optional[Union[str, int]] = None,
) -> Dict[str, str]:
    """Build Picture ``content`` with identical ``smallurl`` / ``originurl``."""
    url = (access_url or "").strip()
    if not url:
        raise ValueError("access_url is required for Picture content")
    content: Dict[str, str] = {
        "smallurl": url,
        "originurl": url,
    }
    if width is not None and str(width).strip():
        content["width"] = str(width).strip()
    if height is not None and str(height).strip():
        content["height"] = str(height).strip()
    return content


def is_file_service_oss_url(url: str, file_base: Optional[str] = None) -> bool:
    """Return True when ``url`` is already a stable ``{file_base}/oss/{id}`` link."""
    raw = (url or "").strip()
    if not raw:
        return False
    base = (file_base or DEFAULT_FILE_BASE).rstrip("/")
    try:
        parsed = urlparse(raw)
        base_parsed = urlparse(base)
    except Exception:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    if parsed.netloc.lower() != base_parsed.netloc.lower():
        return False
    path = parsed.path or ""
    # /oss/{uuid} — uuid is opaque hex-ish id; require non-empty segment.
    parts = [p for p in path.split("/") if p]
    return len(parts) >= 2 and parts[0].lower() == "oss" and bool(parts[1])


def is_image_path(path: str) -> bool:
    """True when ``path`` has a known image extension."""
    return Path(path).suffix.lower() in IMAGE_EXTS


def probe_image_size(data: bytes) -> Tuple[Optional[int], Optional[int]]:
    """Best-effort width/height from PNG / JPEG / GIF / WebP headers (stdlib)."""
    if not data or len(data) < 24:
        return None, None
    try:
        # PNG
        if data[:8] == b"\x89PNG\r\n\x1a\n" and len(data) >= 24:
            w, h = struct.unpack(">II", data[16:24])
            return int(w), int(h)
        # GIF
        if data[:6] in (b"GIF87a", b"GIF89a") and len(data) >= 10:
            w, h = struct.unpack("<HH", data[6:10])
            return int(w), int(h)
        # JPEG — scan for SOF0/SOF2
        if data[:2] == b"\xff\xd8":
            i = 2
            while i + 9 < len(data):
                if data[i] != 0xFF:
                    i += 1
                    continue
                marker = data[i + 1]
                if marker in (0xC0, 0xC2):  # SOF0 / SOF2
                    h, w = struct.unpack(">HH", data[i + 5 : i + 9])
                    return int(w), int(h)
                if marker == 0xD9:  # EOI
                    break
                if marker in (0xD8, 0x01) or (0xD0 <= marker <= 0xD7):
                    i += 2
                    continue
                if i + 4 > len(data):
                    break
                seg_len = struct.unpack(">H", data[i + 2 : i + 4])[0]
                i += 2 + seg_len
            return None, None
        # WebP VP8 / VP8L / VP8X
        if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
            chunk = data[12:16]
            if chunk == b"VP8 " and len(data) >= 30:
                w, h = struct.unpack("<HH", data[26:30])
                return int(w & 0x3FFF), int(h & 0x3FFF)
            if chunk == b"VP8L" and len(data) >= 25:
                bits = struct.unpack("<I", data[21:25])[0]
                w = (bits & 0x3FFF) + 1
                h = ((bits >> 14) & 0x3FFF) + 1
                return int(w), int(h)
            if chunk == b"VP8X" and len(data) >= 30:
                w = 1 + int.from_bytes(data[24:27], "little")
                h = 1 + int.from_bytes(data[27:30], "little")
                return int(w), int(h)
    except Exception:
        logger.debug("chaoranxin: image size probe failed", exc_info=True)
    return None, None


def _guess_filename(path: Optional[str], content_type: Optional[str] = None) -> str:
    if path:
        name = Path(path).name
        if name:
            return name
    ext = mimetypes.guess_extension(content_type or "") or ".jpg"
    if ext == ".jpe":
        ext = ".jpg"
    return f"image{ext}"


async def upload_public_image(
    token: str,
    source: Union[str, bytes, bytearray, memoryview],
    *,
    filename: Optional[str] = None,
    file_base: Optional[str] = None,
    content_type: Optional[str] = None,
    timeout: float = 60.0,
) -> str:
    """Upload image bytes/path to object storage; return PUBLIC ``accessUrl``.

    Raises ``RuntimeError`` on transport / protocol failure.
    """
    try:
        import httpx
    except ImportError as exc:
        raise RuntimeError("chaoranxin image upload requires httpx") from exc

    token = (token or "").strip()
    if not token:
        raise RuntimeError("chaoranxin image upload: missing bot token")

    base = (file_base or DEFAULT_FILE_BASE).rstrip("/")
    url = f"{base}{UPLOAD_PATH}"

    if isinstance(source, (bytes, bytearray, memoryview)):
        data = bytes(source)
        name = filename or _guess_filename(None, content_type)
    else:
        path = Path(str(source))
        if not path.is_file():
            raise RuntimeError(f"chaoranxin image upload: file not found: {path}")
        data = path.read_bytes()
        name = filename or path.name

    if not data:
        raise RuntimeError("chaoranxin image upload: empty file")

    mime = content_type or mimetypes.guess_type(name)[0] or "application/octet-stream"
    headers = {"Authorization": f"Bearer {token}"}
    form = {
        "bizType": BIZ_TYPE_IM,
        "accessLevel": ACCESS_LEVEL_PUBLIC,
    }
    files = {"file": (name, data, mime)}

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, headers=headers, data=form, files=files)
    except Exception as exc:
        raise RuntimeError(f"chaoranxin image upload transport error: {exc}") from exc

    try:
        body = resp.json()
    except Exception as exc:
        raise RuntimeError(
            f"chaoranxin image upload: non-JSON response HTTP {resp.status_code}"
        ) from exc

    if resp.status_code >= 400:
        raise RuntimeError(
            f"chaoranxin image upload HTTP {resp.status_code}: "
            f"{body.get('msg') or body}"
        )

    if body.get("code") != 0:
        raise RuntimeError(
            f"chaoranxin image upload failed: {body.get('msg') or body}"
        )

    payload = body.get("data") or {}
    access_url = (payload.get("accessUrl") or "").strip()
    access_level = (payload.get("accessLevel") or "").strip()
    if access_level and access_level != ACCESS_LEVEL_PUBLIC:
        raise RuntimeError(
            f"chaoranxin image upload: expected accessLevel=PUBLIC, got {access_level!r}"
        )
    if not access_url:
        raise RuntimeError(
            "chaoranxin image upload: accessUrl missing; require accessLevel=PUBLIC"
        )
    return access_url


async def upload_local_image(
    token: str,
    image_path: str,
    *,
    file_base: Optional[str] = None,
) -> Tuple[str, Optional[str], Optional[str]]:
    """Upload a local image; return ``(access_url, width, height)`` as strings."""
    path = Path(image_path)
    data = path.read_bytes()
    w, h = probe_image_size(data)
    access_url = await upload_public_image(
        token,
        data,
        filename=path.name,
        file_base=file_base,
    )
    width = str(w) if w else None
    height = str(h) if h else None
    return access_url, width, height
