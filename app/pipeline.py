"""Orchestrator: a checkpointed state machine QUEUED -> ANALYZED -> PUBLISHED -> DONE.

Every stage saves its result to the DB *before* the job advances. If the process is
stopped or crashes, the job resumes from its last checkpoint on restart, so a paid LLM call
or a publish is never repeated for a completed stage.
"""
import asyncio
import html
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

from app.db import JobStore
from app.media import inspect_media, media_stats, sha256_file
from app.models import ContentMetadata, JobStatus

log = logging.getLogger(__name__)
esc = html.escape


def format_success(job: dict) -> str:
    m, p, a = job["metadata"], job["publish"], job["analytics"] or {}
    lines = [
        "✅ <b>Published</b>",
        f"<b>{esc(m['title'])}</b>",
        f"🔗 {esc(p['url'])}",
        "",
        "📊 <b>Analytics</b>",
        f"• Type: {a.get('kind')} ({a.get('size_kb')} KB)",
        f"• Category: {esc(m['category'])} · Sentiment: {esc(m['sentiment'])}",
        f"• Audience: {esc(m['target_audience'])}",
        f"• Hashtags: {esc(' '.join(m['hashtags'][:6]))}",
    ]
    if "word_count" in a:
        lines.append(f"• Words: {a['word_count']} (~{a['reading_time_min']} min read)")
    lines.append(f"• Model: {a.get('model')} · LLM {a.get('llm_latency_s')}s · tokens {a.get('total_tokens')}")
    if "pipeline_seconds" in a:
        lines.append(f"• End-to-end: {a['pipeline_seconds']}s")
    return "\n".join(lines)


class Pipeline:
    def __init__(self, store: JobStore, analyzer, publisher, notifier,
                 outputs_dir: Path, processed_dir: Path,
                 max_attempts: int = 5, retry_base_delay: float = 30.0):
        self.store, self.analyzer, self.publisher, self.notifier = store, analyzer, publisher, notifier
        self.outputs_dir, self.processed_dir = outputs_dir, processed_dir
        self.max_attempts, self.retry_base_delay = max_attempts, retry_base_delay
        outputs_dir.mkdir(parents=True, exist_ok=True)
        processed_dir.mkdir(parents=True, exist_ok=True)
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self._active: set[str] = set()
        self._background: set[asyncio.Task] = set()

    # ---------- intake ----------
    async def submit(self, path: Path) -> tuple[dict, bool]:
        sha = await asyncio.to_thread(sha256_file, path)
        job, created = await self.store.create(str(path), path.name, sha)
        if created:
            log.info("[%s] queued %s", job["id"], path.name)
            await self.queue.put(job["id"])
        else:
            log.info("Duplicate content %s -> existing job %s (%s)", path.name, job["id"], job["status"])
        return job, created

    async def resume(self) -> int:
        """Re-queue every job that was interrupted (called at startup)."""
        ids = await self.store.unfinished()
        for job_id in ids:
            await self.queue.put(job_id)
        if ids:
            log.info("Resuming %d unfinished job(s) from their last checkpoint", len(ids))
        return len(ids)

    async def retry(self, job_id: str) -> dict | None:
        """Manually retry a FAILED job, resuming from the furthest completed stage."""
        job = await self.store.get(job_id)
        if not job or job["status"] != JobStatus.FAILED:
            return job
        status = (JobStatus.PUBLISHED if job["publish"] else
                  JobStatus.ANALYZED if job["metadata"] else JobStatus.QUEUED)
        await self.store.update(job_id, status=status, attempts=0, error=None)
        await self.queue.put(job_id)
        return await self.store.get(job_id)

    # ---------- workers ----------
    async def worker(self, idx: int) -> None:
        log.info("Worker %d started", idx)
        while True:
            job_id = await self.queue.get()
            if job_id in self._active:          # already being processed by another worker
                self.queue.task_done()
                continue
            self._active.add(job_id)
            try:
                await self.process(job_id)
            except Exception:
                log.exception("[%s] unexpected worker error", job_id)
            finally:
                self._active.discard(job_id)
                self.queue.task_done()

    async def process(self, job_id: str) -> None:
        job = await self.store.get(job_id)
        if job is None or job["status"] in (JobStatus.DONE, JobStatus.FAILED):
            return
        try:
            media = await asyncio.to_thread(inspect_media, Path(job["path"]))
            if job["status"] == JobStatus.QUEUED:
                job = await self._analyze(job, media)
            if job["status"] == JobStatus.ANALYZED:
                job = await self._publish(job, media)
            if job["status"] == JobStatus.PUBLISHED:
                await self._finish(job)
        except Exception as exc:
            await self._handle_failure(job_id, exc)

    # ---------- stages ----------
    async def _analyze(self, job: dict, media) -> dict:
        log.info("[%s] analyzing %s with LLM", job["id"], media.path.name)
        meta, llm_stats = await self.analyzer.analyze(media)
        analytics = {**media_stats(media), **llm_stats, "hashtag_count": len(meta.hashtags)}
        await self.store.update(job["id"], status=JobStatus.ANALYZED,
                                metadata=meta.model_dump(), analytics=analytics, error=None)
        job = await self.store.get(job["id"])
        await self._save_output(job)
        return job

    async def _publish(self, job: dict, media) -> dict:
        log.info("[%s] publishing", job["id"])
        meta = ContentMetadata.model_validate(job["metadata"])
        result = await self.publisher.publish(media, meta)
        await self.store.update(job["id"], status=JobStatus.PUBLISHED, publish=result.model_dump(), error=None)
        job = await self.store.get(job["id"])
        await self._save_output(job)
        return job

    async def _finish(self, job: dict) -> None:
        created = datetime.fromisoformat(job["created_at"])
        analytics = {**(job["analytics"] or {}),
                     "pipeline_seconds": round((datetime.now(timezone.utc) - created).total_seconds(), 1)}
        job["analytics"] = analytics
        await self.notifier.send(format_success(job))
        new_path = await asyncio.to_thread(self._archive, Path(job["path"]), job["id"])
        await self.store.update(job["id"], status=JobStatus.DONE, path=str(new_path),
                                analytics=analytics, error=None)
        await self._save_output(await self.store.get(job["id"]))
        log.info("[%s] DONE -> %s", job["id"], job["publish"]["url"])

    # ---------- failure handling ----------
    async def _handle_failure(self, job_id: str, exc: Exception) -> None:
        job = await self.store.get(job_id)
        attempts = job["attempts"] + 1
        msg = f"{type(exc).__name__}: {exc}"
        permanent = isinstance(exc, FileNotFoundError)
        if permanent or attempts >= self.max_attempts:
            await self.store.update(job_id, status=JobStatus.FAILED, attempts=attempts, error=msg)
            log.error("[%s] FAILED permanently: %s", job_id, msg)
            try:
                await self.notifier.send(
                    f"❌ <b>Job failed</b>: {esc(job['filename'])}\n"
                    f"Stage reached: {job['status']}\n<code>{esc(msg[:500])}</code>\n"
                    f"Retry with: POST /jobs/{job_id}/retry")
            except Exception:
                log.exception("Could not send failure alert")
        else:
            delay = self.retry_base_delay * 2 ** (attempts - 1)
            await self.store.update(job_id, attempts=attempts, error=msg)  # status (checkpoint) kept
            log.warning("[%s] attempt %d failed at %s (%s); re-queue in %.0fs",
                        job_id, attempts, job["status"], msg, delay)
            self._spawn(self._requeue_later(job_id, delay))
        await self._save_output(await self.store.get(job_id))

    async def _requeue_later(self, job_id: str, delay: float) -> None:
        await asyncio.sleep(delay)
        await self.queue.put(job_id)

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def shutdown(self) -> None:
        for task in list(self._background):
            task.cancel()
        await asyncio.gather(*self._background, return_exceptions=True)

    # ---------- persistence helpers ----------
    def _archive(self, path: Path, job_id: str) -> Path:
        if not path.exists():
            return path
        dest = self.processed_dir / f"{job_id}_{path.name}"
        shutil.move(str(path), dest)
        return dest

    async def _save_output(self, job: dict) -> None:
        """Mirror each job to data/outputs/<id>.json so results are human-readable on disk."""
        out = self.outputs_dir / f"{job['id']}.json"
        await asyncio.to_thread(out.write_text, json.dumps(job, indent=2, ensure_ascii=False), "utf-8")
