"""Watches the inbox folder (async, via watchfiles) and submits new files to the pipeline."""
import asyncio
import logging
from pathlib import Path

from watchfiles import Change, awatch

from app.media import is_supported

log = logging.getLogger(__name__)


async def wait_until_stable(path: Path, checks: int = 2, interval: float = 1.0, timeout: float = 300) -> bool:
    """Wait until the file stops growing (it may still be copying)."""
    last, stable, waited = -1, 0, 0.0
    while waited < timeout:
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            return False
        if size == last and size > 0:
            stable += 1
            if stable >= checks:
                return True
        else:
            stable = 0
        last = size
        await asyncio.sleep(interval)
        waited += interval
    return False


async def watch_folder(inbox: Path, pipeline, stop_event: asyncio.Event, force_polling: bool = False) -> None:
    # Catch files that were dropped while the server was off.
    for path in sorted(inbox.iterdir()):
        if is_supported(path):
            await pipeline.submit(path)
    log.info("Watching %s for new files", inbox.resolve())
    async for changes in awatch(inbox, stop_event=stop_event, recursive=False, force_polling=force_polling):
        for change, raw in changes:
            path = Path(raw)
            if change == Change.deleted or not is_supported(path):
                continue
            if await wait_until_stable(path):
                try:
                    await pipeline.submit(path)
                except Exception:
                    log.exception("Could not submit %s", path)
