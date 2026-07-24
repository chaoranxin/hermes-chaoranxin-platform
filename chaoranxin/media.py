"""Chaoranxin (超然信) outbound media upload helpers.

Per ``ROBOT_THIRD_PARTY.md`` §6.2 / §6.3:

1. ``POST {file_base}/objectstorage/upload`` with ``bizType=im``,
   ``accessLevel=PUBLIC``, and ``Authorization: Bearer rbt_*``.
2. Use ``data.accessUrl`` in the platform content object for the clazz.
3. Send a WS content-type frame (``Picture`` / ``Voice`` / ``LocalVideo`` /
   ``LocalFile``) — handled by the adapter.

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
VIDEO_EXTS = frozenset({".mp4", ".mov", ".avi", ".mkv", ".webm", ".3gp"})
VOICE_EXTS = frozenset({".ogg", ".opus", ".mp3", ".wav", ".m4a", ".flac"})


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


def build_voice_content(
    access_url: str,
    size: Union[str, int],
) -> Dict[str, str]:
    """Build Voice ``content`` — ``url`` + ``size`` (byte count as string)."""
    url = (access_url or "").strip()
    if not url:
        raise ValueError("access_url is required for Voice content")
    return {"url": url, "size": str(size)}


def build_localvideo_content(
    access_url: str,
    *,
    cover: str = "",
) -> Dict[str, str]:
    """Build LocalVideo ``content`` — ``video`` + ``cover`` (not Video.url)."""
    url = (access_url or "").strip()
    if not url:
        raise ValueError("access_url is required for LocalVideo content")
    return {"video": url, "cover": str(cover or "")}


def build_localfile_content(
    access_url: str,
    filename: str,
    filesize: Union[str, int],
) -> Dict[str, Any]:
    """Build LocalFile ``content`` — ``fileurl`` / ``filename`` / ``filesize`` int."""
    url = (access_url or "").strip()
    if not url:
        raise ValueError("access_url is required for LocalFile content")
    name = (filename or "").strip() or "file"
    try:
        size_int = int(filesize)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"filesize must be an integer, got {filesize!r}") from exc
    if size_int < 0:
        raise ValueError(f"filesize must be >= 0, got {size_int}")
    return {
        "fileurl": url,
        "filename": name,
        "filesize": size_int,
    }


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


def is_video_path(path: str) -> bool:
    """True when ``path`` has a known video extension."""
    return Path(path).suffix.lower() in VIDEO_EXTS


def is_voice_path(path: str) -> bool:
    """True when ``path`` has a known audio/voice extension."""
    return Path(path).suffix.lower() in VOICE_EXTS


def classify_media_path(path: str, *, is_voice: bool = False) -> str:
    """Classify a local path for outbound clazz routing.

    Returns one of: ``\"picture\"``, ``\"voice\"``, ``\"localvideo\"``, ``\"localfile\"``.
    ``is_voice=True`` wins over extension heuristics (gateway voice flag).
    """
    if is_voice:
        return "voice"
    if is_image_path(path):
        return "picture"
    if is_video_path(path):
        return "localvideo"
    if is_voice_path(path):
        return "voice"
    return "localfile"


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
    ext = mimetypes.guess_extension(content_type or "") or ".bin"
    if ext == ".jpe":
        ext = ".jpg"
    return f"file{ext}"


async def upload_public_file(
    token: str,
    source: Union[str, bytes, bytearray, memoryview],
    *,
    filename: Optional[str] = None,
    file_base: Optional[str] = None,
    content_type: Optional[str] = None,
    timeout: float = 60.0,
) -> str:
    """Upload bytes/path to object storage; return PUBLIC ``accessUrl``.

    Raises ``RuntimeError`` on transport / protocol failure.
    """
    try:
        import httpx
    except ImportError as exc:
        raise RuntimeError("chaoranxin media upload requires httpx") from exc

    token = (token or "").strip()
    if not token:
        raise RuntimeError("chaoranxin media upload: missing bot token")

    base = (file_base or DEFAULT_FILE_BASE).rstrip("/")
    url = f"{base}{UPLOAD_PATH}"

    if isinstance(source, (bytes, bytearray, memoryview)):
        data = bytes(source)
        name = filename or _guess_filename(None, content_type)
    else:
        path = Path(str(source))
        if not path.is_file():
            raise RuntimeError(f"chaoranxin media upload: file not found: {path}")
        data = path.read_bytes()
        name = filename or path.name

    if not data:
        raise RuntimeError("chaoranxin media upload: empty file")

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
        raise RuntimeError(f"chaoranxin media upload transport error: {exc}") from exc

    try:
        body = resp.json()
    except Exception as exc:
        raise RuntimeError(
            f"chaoranxin media upload: non-JSON response HTTP {resp.status_code}"
        ) from exc

    if resp.status_code >= 400:
        raise RuntimeError(
            f"chaoranxin media upload HTTP {resp.status_code}: "
            f"{body.get('msg') or body}"
        )

    if body.get("code") != 0:
        raise RuntimeError(
            f"chaoranxin media upload failed: {body.get('msg') or body}"
        )

    payload = body.get("data") or {}
    access_url = (payload.get("accessUrl") or "").strip()
    access_level = (payload.get("accessLevel") or "").strip()
    if access_level and access_level != ACCESS_LEVEL_PUBLIC:
        raise RuntimeError(
            f"chaoranxin media upload: expected accessLevel=PUBLIC, got {access_level!r}"
        )
    if not access_url:
        raise RuntimeError(
            "chaoranxin media upload: accessUrl missing; require accessLevel=PUBLIC"
        )
    return access_url


async def upload_public_image(
    token: str,
    source: Union[str, bytes, bytearray, memoryview],
    *,
    filename: Optional[str] = None,
    file_base: Optional[str] = None,
    content_type: Optional[str] = None,
    timeout: float = 60.0,
) -> str:
    """Upload image bytes/path; thin wrapper around :func:`upload_public_file`."""
    return await upload_public_file(
        token,
        source,
        filename=filename,
        file_base=file_base,
        content_type=content_type,
        timeout=timeout,
    )


async def upload_local_file(
    token: str,
    file_path: str,
    *,
    file_base: Optional[str] = None,
    filename: Optional[str] = None,
    content_type: Optional[str] = None,
) -> Tuple[str, int]:
    """Upload a local file; return ``(access_url, size_bytes)``."""
    path = Path(file_path)
    data = path.read_bytes()
    access_url = await upload_public_file(
        token,
        data,
        filename=filename or path.name,
        file_base=file_base,
        content_type=content_type,
    )
    return access_url, len(data)


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
