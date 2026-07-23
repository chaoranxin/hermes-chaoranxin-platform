"""Inbound Multimodal part materialization for Chaoranxin.

Robot uplink is Multimodal-only: ``msg_type=multimodal`` + ``content.parts``.
Maps parts onto Hermes ``MessageEvent`` fields (text, media_urls, media_types)
plus attachment context notes for non-STT audio / video / files.
"""

from __future__ import annotations

import logging
import mimetypes
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from gateway.platforms.base import (
    MessageType,
    cache_audio_from_url,
    cache_document_from_bytes,
    cache_image_from_url,
    cache_video_from_bytes,
)
from tools.url_safety import is_safe_url

logger = logging.getLogger(__name__)


@dataclass
class MaterializedMultimodal:
    """Result of downloading/decoding Multimodal parts."""

    text: str = ""
    message_type: MessageType = MessageType.TEXT
    media_urls: List[str] = field(default_factory=list)
    media_types: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    @property
    def final_text(self) -> str:
        body = (self.text or "").strip()
        if not self.notes:
            return body
        prefix = "\n\n".join(self.notes)
        if body:
            return f"{prefix}\n\n{body}"
        return prefix


def _ext_from_url(url: str, default: str = "") -> str:
    path = urlparse(url).path or ""
    ext = os.path.splitext(path)[1].lower()
    return ext if ext else default


def _guess_mime(url: str, fallback: str) -> str:
    ext = _ext_from_url(url)
    if ext:
        guessed, _ = mimetypes.guess_type(f"x{ext}")
        if guessed:
            return guessed
    return fallback


def _display_name(url: str, filename: str = "") -> str:
    if filename:
        return re.sub(r"[^\w.\- ]", "_", os.path.basename(filename))
    path = urlparse(url).path or ""
    base = os.path.basename(path) or "file"
    return re.sub(r"[^\w.\- ]", "_", base)


def _part_type(part: Dict[str, Any]) -> str:
    return str(part.get("type") or "").lower().strip()


def _nested_url(part: Dict[str, Any], key: str) -> Tuple[str, Dict[str, Any]]:
    """Return (url, nested_dict) for keys like image_url / voice_url."""
    nested = part.get(key)
    if isinstance(nested, str):
        return nested.strip(), {}
    if isinstance(nested, dict):
        url = str(nested.get("url") or "").strip()
        return url, nested
    # Flat fallbacks
    url = str(part.get("url") or "").strip()
    return url, {}


async def _download_bytes(url: str) -> bytes:
    from tools.url_safety import safe_url_for_log
    import httpx

    if not is_safe_url(url):
        raise ValueError(f"Blocked unsafe URL (SSRF protection): {safe_url_for_log(url)}")

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; HermesAgent/1.0)"},
        )
        resp.raise_for_status()
        return resp.content


def _audio_file_note(path: str, display: str) -> str:
    return (
        f"[The user sent an audio file attachment: '{display}'. "
        f"It is saved at: {path}. "
        f"Its content is not inlined here. If the user's request involves "
        f"what the audio contains, transcribe or process it yourself — for "
        f"example by passing the path to a transcription or media tool — "
        f"instead of asking the user to describe it. Only ask what to do "
        f"with it if their intent is genuinely unclear.]"
    )


def _video_note(path: str, display: str) -> str:
    return (
        f"[The user sent a video attachment: '{display}'. "
        f"It is saved at: {path}. "
        f"Its content is not inlined here. If the user's request involves "
        f"what the video contains, inspect or process it yourself — for "
        f"example by passing the path to a video analysis or media tool — "
        f"instead of asking the user to describe it. Only ask what to do "
        f"with it if their intent is genuinely unclear.]"
    )


def _document_note(path: str, display: str, mime: str) -> str:
    if mime.startswith("text/"):
        return (
            f"[The user sent a text document: '{display}'. "
            f"It is saved at: {path}.]"
        )
    return (
        f"[The user sent a document: '{display}'. It is saved at: {path}. "
        f"Its content is not inlined here. To read it, extract the document's "
        f"text yourself — for example with the terminal tool or the "
        f"ocr-and-documents skill — before answering, instead of asking the "
        f"user to paste it.]"
    )


def _choose_message_type(
    *,
    has_voice: bool,
    has_image: bool,
    has_audio_file: bool,
    has_video: bool,
    has_file: bool,
) -> MessageType:
    if has_voice and not (has_image or has_audio_file or has_video or has_file):
        return MessageType.VOICE
    if has_image and not (has_voice or has_audio_file or has_video or has_file):
        return MessageType.PHOTO
    return MessageType.TEXT


async def materialize_parts(parts: List[Dict[str, Any]]) -> MaterializedMultimodal:
    """Download / decode Multimodal parts into local paths + notes."""
    text_chunks: List[str] = []
    media_urls: List[str] = []
    media_types: List[str] = []
    notes: List[str] = []
    has_voice = has_image = has_audio_file = has_video = has_file = False

    for part in parts:
        ptype = _part_type(part)
        try:
            if ptype == "text":
                t = part.get("text")
                if t is None and isinstance(part.get("content"), str):
                    t = part.get("content")
                if t is not None and str(t).strip():
                    text_chunks.append(str(t))
                continue

            if ptype == "image_url":
                url, _nested = _nested_url(part, "image_url")
                if not url:
                    logger.warning("[chaoranxin] image_url part missing url")
                    continue
                path = await cache_image_from_url(url)
                mime = _guess_mime(url, "image/jpeg")
                media_urls.append(path)
                media_types.append(mime)
                has_image = True
                continue

            if ptype == "voice_url":
                url, nested = _nested_url(part, "voice_url")
                if not url:
                    logger.warning("[chaoranxin] voice_url part missing url")
                    continue
                declared_size = nested.get("size")
                ext = _ext_from_url(url, ".ogg")
                path = await cache_audio_from_url(url, ext=ext)
                if declared_size is not None:
                    try:
                        expected = int(declared_size)
                        actual = os.path.getsize(path)
                        if expected > 0 and actual != expected:
                            logger.warning(
                                "[chaoranxin] voice_url size mismatch "
                                "declared=%s actual=%s",
                                expected,
                                actual,
                            )
                    except (TypeError, ValueError, OSError):
                        pass
                mime = _guess_mime(url, "audio/ogg")
                if not mime.startswith("audio/"):
                    mime = "audio/ogg"
                media_urls.append(path)
                media_types.append(mime)
                has_voice = True
                continue

            if ptype == "audio_url":
                url, _nested = _nested_url(part, "audio_url")
                if not url:
                    logger.warning("[chaoranxin] audio_url part missing url")
                    continue
                ext = _ext_from_url(url, ".mp3")
                # Download via audio helper then treat as attachment (no STT):
                # keep out of media_urls when message would be VOICE-routed;
                # always inject a note and do not mark has_voice.
                path = await cache_audio_from_url(url, ext=ext)
                display = _display_name(url)
                notes.append(_audio_file_note(path, display))
                has_audio_file = True
                continue

            if ptype == "video_url":
                url, _nested = _nested_url(part, "video_url")
                if not url:
                    logger.warning("[chaoranxin] video_url part missing url")
                    continue
                data = await _download_bytes(url)
                ext = _ext_from_url(url, ".mp4")
                path = cache_video_from_bytes(data, ext=ext)
                mime = _guess_mime(url, "video/mp4")
                media_urls.append(path)
                media_types.append(mime if mime.startswith("video/") else "video/mp4")
                notes.append(_video_note(path, _display_name(url)))
                has_video = True
                continue

            if ptype == "file":
                nested = part.get("file")
                if not isinstance(nested, dict):
                    nested = {}
                url = str(nested.get("url") or part.get("url") or "").strip()
                if not url:
                    logger.warning("[chaoranxin] file part missing url")
                    continue
                filename = str(
                    nested.get("filename") or _display_name(url) or "document"
                )
                mime = str(
                    nested.get("mime_type")
                    or nested.get("mimeType")
                    or _guess_mime(url, "application/octet-stream")
                )
                data = await _download_bytes(url)
                path = cache_document_from_bytes(data, filename)
                notes.append(_document_note(path, _display_name(url, filename), mime))
                has_file = True
                continue

            logger.debug("[chaoranxin] ignoring unknown multimodal part type=%s", ptype)
        except Exception as exc:
            logger.warning(
                "[chaoranxin] multimodal part type=%s failed: %s",
                ptype,
                exc,
            )

    msg_type = _choose_message_type(
        has_voice=has_voice,
        has_image=has_image,
        has_audio_file=has_audio_file,
        has_video=has_video,
        has_file=has_file,
    )
    return MaterializedMultimodal(
        text="\n".join(text_chunks),
        message_type=msg_type,
        media_urls=media_urls,
        media_types=media_types,
        notes=notes,
    )


__all__ = ["MaterializedMultimodal", "materialize_parts"]
