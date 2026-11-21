"""FastAPI entry point: wires everything together and runs workers + watcher in the background."""
import asyncio
import logging
import shutil
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, File, HTTPException, Request, UploadFile

from app.config import settings
from app.db import JobStore
from app.llm import GeminiAnalyzer, MockAnalyzer
from app.media import SUPPORTED_EXTS
from app.pipeline import Pipeline
from app.publisher import LocalPublisher, TelegramChannelPublisher
from app.telegram import TelegramNotifier
from app.watcher import watch_folder

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)  # never log URLs containing the bot token
log = logging.getLogger("agent")


def build_analyzer():
    if settings.gemini_api_key:
        return GeminiAnalyzer(settings.gemini_api_key, settings.gemini_model,
                              settings.llm_max_rpm, settings.llm_max_concurrency,
                              settings.inline_media_limit_mb * 1024 * 1024)
    log.warning("GEMINI_API_KEY not set -> using MockAnalyzer (offline demo mode)")
    return MockAnalyzer()


def build_publisher(http: httpx.AsyncClient):
    if settings.publisher == "telegram_channel":
        if not (settings.telegram_bot_token and settings.telegram_channel_id):
            raise RuntimeError("PUBLISHER=telegram_channel needs TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL_ID")
        return TelegramChannelPublisher(http, settings.telegram_bot_token, settings.telegram_channel_id)
    return LocalPublisher(settings.published_dir)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.ensure_dirs()
    store = JobStore(settings.db_path)
    await store.init()
    http = httpx.AsyncClient()
    notifier = TelegramNotifier(http, settings.telegram_bot_token, settings.telegram_alert_chat_id)
    pipeline = Pipeline(store, build_analyzer(), build_publisher(http), notifier,
                        settings.outputs_dir, settings.processed_dir,
                        settings.max_attempts, settings.retry_base_delay)
    resumed = await pipeline.resume()

    stop = asyncio.Event()
    tasks = [asyncio.create_task(pipeline.worker(i)) for i in range(settings.workers)]
    tasks.append(asyncio.create_task(
        watch_folder(settings.inbox_dir, pipeline, stop, settings.watch_force_polling)))
    app.state.pipeline, app.state.store = pipeline, store
    try:
        await notifier.send(f"🟢 Content agent online. Resumed {resumed} unfinished job(s).")
    except Exception:
        log.exception("Startup alert failed")

    yield

    log.info("Shutting down; unfinished jobs stay saved and resume on next start")
    stop.set()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await pipeline.shutdown()
    await http.aclose()
    await store.close()


app = FastAPI(title="Autonomous Content & Analytics Agent", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health(request: Request):
    return {"status": "ok", "queue_size": request.app.state.pipeline.queue.qsize(),
            "model": settings.gemini_model if settings.gemini_api_key else "mock",
            "publisher": settings.publisher}


@app.post("/upload", status_code=202)
async def upload(request: Request, file: UploadFile = File(...)):
    name = Path(file.filename or "upload").name.replace(" ", "_")
    if Path(name).suffix.lower() not in SUPPORTED_EXTS:
        raise HTTPException(415, f"Unsupported type. Allowed: {sorted(SUPPORTED_EXTS)}")
    dest = settings.uploads_dir / f"{uuid.uuid4().hex[:8]}_{name}"

    def save():
        with dest.open("wb") as out:
            shutil.copyfileobj(file.file, out)

    await asyncio.to_thread(save)
    job, created = await request.app.state.pipeline.submit(dest)
    if not created:
        dest.unlink(missing_ok=True)
    return {"created": created, "job": job}


@app.get("/jobs")
async def list_jobs(request: Request, status: str | None = None, limit: int = 50):
    return await request.app.state.store.list_jobs(limit=limit, status=status)


@app.get("/jobs/{job_id}")
async def get_job(request: Request, job_id: str):
    job = await request.app.state.store.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job


@app.post("/jobs/{job_id}/retry")
async def retry_job(request: Request, job_id: str):
    job = await request.app.state.pipeline.retry(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job
