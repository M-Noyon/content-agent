"""Media inspection: type detection, hashing (dedupe), and basic content analytics."""
import hashlib
import mimetypes
from dataclasses import dataclass
from pathlib import Path

TEXT_EXTS = {".txt", ".md", ".csv", ".json", ".html"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
VIDEO_EXTS = {".mp4", ".mov", ".webm", ".mkv"}
AUDIO_EXTS = {".mp3", ".wav", ".m4a", ".ogg"}
SUPPORTED_EXTS = TEXT_EXTS | IMAGE_EXTS | VIDEO_EXTS | AUDIO_EXTS

MIME_OVERRIDES = {
    ".md": "text/markdown", ".webp": "image/webp", ".mkv": "video/x-matroska",
    ".mov": "video/quicktime", ".webm": "video/webm", ".m4a": "audio/mp4", ".ogg": "audio/ogg",
}
MAX_TEXT_CHARS = 30_000


@dataclass
class MediaInput:
    path: Path
    kind: str          # text | image | video | audio
    mime: str
    size_bytes: int
    text: str | None = None


def is_supported(path: Path) -> bool:
    return (path.is_file() and not path.name.startswith(".")
            and path.suffix.lower() in SUPPORTED_EXTS)


def _kind(suffix: str) -> str:
    if suffix in TEXT_EXTS:
        return "text"
    if suffix in IMAGE_EXTS:
        return "image"
    if suffix in VIDEO_EXTS:
        return "video"
    if suffix in AUDIO_EXTS:
        return "audio"
    raise ValueError(f"Unsupported file type: {suffix}")


def inspect_media(path: Path) -> MediaInput:
    if not path.exists():
        raise FileNotFoundError(f"Source file missing: {path}")
    suffix = path.suffix.lower()
    kind = _kind(suffix)
    mime = MIME_OVERRIDES.get(suffix) or mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    text = path.read_text(encoding="utf-8", errors="replace")[:MAX_TEXT_CHARS] if kind == "text" else None
    return MediaInput(path=path, kind=kind, mime=mime, size_bytes=path.stat().st_size, text=text)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def media_stats(media: MediaInput) -> dict:
    stats = {"kind": media.kind, "size_kb": round(media.size_bytes / 1024, 1)}
    if media.text is not None:
        words = len(media.text.split())
        stats["word_count"] = words
        stats["reading_time_min"] = round(words / 200, 1)
    return stats
