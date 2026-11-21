import pytest

from app.db import JobStore
from app.llm import MockAnalyzer
from app.models import ContentMetadata, JobStatus, PublishResult
from app.pipeline import Pipeline
from app.publisher import LocalPublisher


class RecordingNotifier:
    def __init__(self):
        self.messages = []

    async def send(self, text):
        self.messages.append(text)


class FlakyPublisher:
    def __init__(self, fail_times):
        self.fail_times, self.calls = fail_times, 0

    async def publish(self, media, meta):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("platform down")
        return PublishResult(platform="fake", url="https://example.com/p/1", post_id="1")


class ExplodingAnalyzer:
    async def analyze(self, media):
        raise AssertionError("LLM must not be called again for a completed stage")


@pytest.fixture
async def store(tmp_path):
    s = JobStore(tmp_path / "jobs.db")
    await s.init()
    yield s
    await s.close()


def make(tmp_path, store, analyzer, publisher, notifier, **kw):
    return Pipeline(store, analyzer, publisher, notifier,
                    outputs_dir=tmp_path / "outputs", processed_dir=tmp_path / "processed", **kw)


def sample(tmp_path, text="FastAPI makes building async Python APIs fast and enjoyable. " * 20):
    f = tmp_path / "my_first_post.txt"
    f.write_text(text)
    return f


async def test_happy_path(tmp_path, store):
    f, notifier = sample(tmp_path), RecordingNotifier()
    p = make(tmp_path, store, MockAnalyzer(), LocalPublisher(tmp_path / "published"), notifier)
    job, created = await p.submit(f)
    assert created
    await p.process(job["id"])

    job = await store.get(job["id"])
    assert job["status"] == JobStatus.DONE
    assert job["metadata"]["title"]
    assert job["analytics"]["word_count"] > 0
    assert (tmp_path / "outputs" / f"{job['id']}.json").exists()
    assert not f.exists()                      # archived to processed/
    assert "Published" in notifier.messages[0]


async def test_duplicate_content_is_not_reprocessed(tmp_path, store):
    f = sample(tmp_path)
    p = make(tmp_path, store, MockAnalyzer(), FlakyPublisher(0), RecordingNotifier())
    first, created1 = await p.submit(f)
    second, created2 = await p.submit(f)
    assert created1 and not created2
    assert first["id"] == second["id"]


async def test_resume_skips_completed_stages(tmp_path, store):
    """Simulates a restart after the LLM step: analysis must not run again."""
    f = sample(tmp_path)
    p = make(tmp_path, store, ExplodingAnalyzer(), FlakyPublisher(0), RecordingNotifier())
    job, _ = await p.submit(f)
    meta = ContentMetadata(title="T", summary="S", caption="C", hashtags=["#a"], tags=["a"],
                           category="Tech", sentiment="neutral", target_audience="devs")
    await store.update(job["id"], status=JobStatus.ANALYZED, metadata=meta.model_dump(),
                       analytics={"kind": "text"})
    assert await p.resume() >= 1
    await p.process(job["id"])
    assert (await store.get(job["id"]))["status"] == JobStatus.DONE


async def test_transient_failure_keeps_checkpoint_and_recovers(tmp_path, store):
    publisher = FlakyPublisher(fail_times=1)
    p = make(tmp_path, store, MockAnalyzer(), publisher, RecordingNotifier(), retry_base_delay=0.01)
    job, _ = await p.submit(sample(tmp_path))

    await p.process(job["id"])
    j = await store.get(job["id"])
    assert j["status"] == JobStatus.ANALYZED and j["attempts"] == 1   # metadata kept

    await p.process(job["id"])
    assert (await store.get(job["id"]))["status"] == JobStatus.DONE
    assert publisher.calls == 2
    await p.shutdown()


async def test_permanent_failure_alerts_and_can_be_retried(tmp_path, store):
    notifier = RecordingNotifier()
    p = make(tmp_path, store, MockAnalyzer(), FlakyPublisher(99), notifier,
             max_attempts=2, retry_base_delay=0.01)
    job, _ = await p.submit(sample(tmp_path))
    await p.process(job["id"])
    await p.process(job["id"])

    j = await store.get(job["id"])
    assert j["status"] == JobStatus.FAILED
    assert any("failed" in m for m in notifier.messages)

    j = await p.retry(job["id"])
    assert j["status"] == JobStatus.ANALYZED and j["attempts"] == 0
    await p.shutdown()
