"""Publishers. Add a new platform by implementing `async def publish(media, meta) -> PublishResult`."""
import asyncio
import html
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

import httpx

from app.media import MediaInput
from app.models import ContentMetadata, PublishResult
from app.telegram import telegram_request


class Publisher(Protocol):
    async def publish(self, media: MediaInput, meta: ContentMetadata) -> PublishResult: ...


class LocalPublisher:
    """Writes the post as a Markdown file. Zero setup; great for demos and development."""

    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        out_dir.mkdir(parents=True, exist_ok=True)

    async def publish(self, media: MediaInput, meta: ContentMetadata) -> PublishResult:
        post_id = f"{datetime.now(timezone.utc):%Y%m%d%H%M%S}_{media.path.stem}"
        path = self.out_dir / f"{post_id}.md"
        body = (f"# {meta.title}\n\n{meta.caption}\n\n{' '.join(meta.hashtags)}\n\n---\n\n"
                f"**Summary:** {meta.summary}\n\n**Category:** {meta.category}  \n"
                f"**Audience:** {meta.target_audience}  \n**Source:** {media.path.name}\n")
        await asyncio.to_thread(path.write_text, body, "utf-8")
        return PublishResult(platform="local", url=path.resolve().as_uri(), post_id=post_id)


class TelegramChannelPublisher:
    """Publishes to a Telegram channel (bot must be an admin there) and returns the post link."""

    METHODS = {"image": ("sendPhoto", "photo"), "video": ("sendVideo", "video"),
               "audio": ("sendAudio", "audio")}

    def __init__(self, http: httpx.AsyncClient, token: str, channel_id: str):
        self.http, self.token, self.channel_id = http, token, channel_id

    @staticmethod
    def _caption(meta: ContentMetadata, limit: int = 1024) -> str:
        head = f"<b>{html.escape(meta.title[:100])}</b>\n\n{html.escape(meta.caption[:600])}"
        tags = html.escape(" ".join(meta.hashtags))
        full = f"{head}\n\n{tags}"
        return full if len(full) <= limit else head[:limit]

    def _link(self, msg_id: int, chat: dict) -> str:
        if chat.get("username"):
            return f"https://t.me/{chat['username']}/{msg_id}"
        cid = str(chat.get("id", self.channel_id))
        return f"https://t.me/c/{cid[4:]}/{msg_id}" if cid.startswith("-100") else f"telegram:{cid}/{msg_id}"

    async def publish(self, media: MediaInput, meta: ContentMetadata) -> PublishResult:
        if media.kind == "text":
            text = f"{self._caption(meta, 1000)}\n\n{html.escape(meta.summary[:2500])}"
            result = await telegram_request(self.http, self.token, "sendMessage", data={
                "chat_id": self.channel_id, "text": text, "parse_mode": "HTML"})
        else:
            method, field = self.METHODS[media.kind]
            data = await asyncio.to_thread(media.path.read_bytes)  # bytes -> safe to resend on retry
            result = await telegram_request(
                self.http, self.token, method,
                data={"chat_id": self.channel_id, "caption": self._caption(meta), "parse_mode": "HTML"},
                files={field: (media.path.name, data, media.mime)})
        msg_id = result["message_id"]
        return PublishResult(platform="telegram_channel",
                             url=self._link(msg_id, result.get("chat", {})), post_id=str(msg_id))
