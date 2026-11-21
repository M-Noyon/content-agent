"""LLM layer: Gemini with structured JSON output, rate limiting and retries.

MockAnalyzer is used automatically when no GEMINI_API_KEY is set, so the whole
pipeline can be demoed (and tested) offline.
"""
import asyncio
import logging
import re
import time

import httpx
from google import genai
from google.genai import errors, types

from app.media import MediaInput
from app.models import ContentMetadata
from app.retry import RateLimiter, RetryableError, retry_async

log = logging.getLogger(__name__)
RETRYABLE_CODES = {408, 429, 500, 502, 503, 504}

SYSTEM_PROMPT = (
    "You are an expert social media strategist and SEO editor. Analyze the provided content "
    "and produce publish-ready metadata. Titles must be specific and compelling (no clickbait), "
    "captions must sound human and invite engagement, hashtags must be genuinely relevant. "
    "Base every claim strictly on the content; never invent facts."
)


class GeminiAnalyzer:
    def __init__(self, api_key: str, model: str, max_rpm: int = 10, max_concurrency: int = 2,
                 inline_limit_bytes: int = 15 * 1024 * 1024, attempts: int = 6):
        self.client = genai.Client(api_key=api_key)
        self.model = model
        self.limiter = RateLimiter(max_rpm)
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.inline_limit = inline_limit_bytes
        self.attempts = attempts

    async def _upload(self, media: MediaInput):
        """Large video/audio: upload via Files API and wait until Gemini has processed it."""
        f = await self.client.aio.files.upload(
            file=str(media.path), config=types.UploadFileConfig(mime_type=media.mime))
        deadline = time.monotonic() + 600
        while f.state and f.state.name == "PROCESSING":
            if time.monotonic() > deadline:
                raise TimeoutError("Gemini file processing timed out")
            await asyncio.sleep(3)
            f = await self.client.aio.files.get(name=f.name)
        if f.state and f.state.name == "FAILED":
            raise RuntimeError(f"Gemini could not process {media.path.name}")
        return f

    async def _build_contents(self, media: MediaInput) -> list:
        instruction = f"Analyze this {media.kind} file named '{media.path.name}' and return the metadata."
        if media.kind == "text":
            return [instruction, media.text or ""]
        if media.size_bytes <= self.inline_limit:
            data = await asyncio.to_thread(media.path.read_bytes)
            return [instruction, types.Part.from_bytes(data=data, mime_type=media.mime)]
        return [instruction, await self._upload(media)]

    async def analyze(self, media: MediaInput) -> tuple[ContentMetadata, dict]:
        contents = await self._build_contents(media)
        config = types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            response_mime_type="application/json",
            response_schema=ContentMetadata,
            temperature=0.7,
        )

        async def call():
            await self.limiter.acquire()          # stay under the RPM quota proactively
            async with self.semaphore:            # cap parallel requests
                try:
                    return await self.client.aio.models.generate_content(
                        model=self.model, contents=contents, config=config)
                except errors.APIError as exc:
                    if exc.code in RETRYABLE_CODES:
                        raise RetryableError(f"Gemini {exc.code}: {exc.message}") from exc
                    raise
                except httpx.TransportError as exc:
                    raise RetryableError(f"Gemini network error: {exc}") from exc

        started = time.perf_counter()
        response = await retry_async(call, attempts=self.attempts, base_delay=2.0, label="gemini")
        latency = time.perf_counter() - started

        parsed = response.parsed
        meta = parsed if isinstance(parsed, ContentMetadata) else ContentMetadata.model_validate_json(response.text)
        usage = response.usage_metadata
        stats = {
            "model": self.model,
            "llm_latency_s": round(latency, 2),
            "prompt_tokens": getattr(usage, "prompt_token_count", None),
            "output_tokens": getattr(usage, "candidates_token_count", None),
            "total_tokens": getattr(usage, "total_token_count", None),
        }
        return meta, stats


class MockAnalyzer:
    """Deterministic offline analyzer for demo mode and tests."""

    async def analyze(self, media: MediaInput) -> tuple[ContentMetadata, dict]:
        await asyncio.sleep(0.1)
        name = re.sub(r"[_\-]+", " ", media.path.stem).strip().title() or "Untitled"
        words = (media.text or name).split()
        keywords = sorted({w.lower().strip(".,!?") for w in words if len(w) > 4})[:8] or [media.kind]
        meta = ContentMetadata(
            title=f"{name}: Key Highlights"[:80],
            summary=" ".join(words[:40]) or f"A {media.kind} file named {media.path.name}.",
            caption=f"Fresh {media.kind} just dropped: {name}. What do you think?",
            hashtags=[f"#{k}" for k in keywords[:6]],
            tags=keywords,
            category="General",
            sentiment="neutral",
            target_audience="General audience",
        )
        return meta, {"model": "mock", "llm_latency_s": 0.1, "total_tokens": 0}
