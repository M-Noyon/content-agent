"""Telegram Bot API client with rate-limit (429 retry_after) handling, and the phone notifier."""
import logging

import httpx

from app.retry import RetryableError, retry_async

log = logging.getLogger(__name__)
API = "https://api.telegram.org"


async def telegram_request(http: httpx.AsyncClient, token: str, method: str,
                           data: dict | None = None, files: dict | None = None,
                           attempts: int = 5) -> dict:
    url = f"{API}/bot{token}/{method}"

    async def call():
        try:
            resp = await http.post(url, data=data, files=files, timeout=120)
        except httpx.TransportError as exc:
            raise RetryableError(f"Telegram network error: {type(exc).__name__}") from exc
        if resp.status_code == 429:
            retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
            raise RetryableError("Telegram rate limit", retry_after=retry_after)
        if resp.status_code >= 500:
            raise RetryableError(f"Telegram server error {resp.status_code}")
        body = resp.json()
        if not body.get("ok"):
            raise RuntimeError(f"Telegram {method} failed: {body.get('description')}")
        return body["result"]

    return await retry_async(call, attempts=attempts, base_delay=1.0, label=f"telegram.{method}")


class TelegramNotifier:
    """Sends real-time status alerts to your phone. Logs instead if not configured."""

    def __init__(self, http: httpx.AsyncClient, token: str, chat_id: str):
        self.http, self.token, self.chat_id = http, token, chat_id
        self.enabled = bool(token and chat_id)

    async def send(self, text: str) -> None:
        if not self.enabled:
            log.info("[alert, Telegram not configured] %s", text.replace("\n", " | "))
            return
        await telegram_request(self.http, self.token, "sendMessage", data={
            "chat_id": self.chat_id, "text": text[:4096],
            "parse_mode": "HTML", "disable_web_page_preview": "true",
        })
