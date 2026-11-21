"""Pydantic models: the LLM's structured-output schema and job states."""
from enum import Enum

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Each status is a checkpoint. A job resumes from its last checkpoint."""
    QUEUED = "QUEUED"        # registered, waiting for analysis
    ANALYZED = "ANALYZED"    # metadata generated and saved
    PUBLISHED = "PUBLISHED"  # posted, link saved
    DONE = "DONE"            # alert sent, file archived
    FAILED = "FAILED"        # gave up after max attempts (can be retried via API)


UNFINISHED = (JobStatus.QUEUED, JobStatus.ANALYZED, JobStatus.PUBLISHED)


class ContentMetadata(BaseModel):
    """Schema passed to Gemini as response_schema -> guaranteed-valid JSON."""
    title: str = Field(description="Catchy, search-optimized title, at most 80 characters.")
    summary: str = Field(description="Neutral 2-3 sentence summary of the content.")
    caption: str = Field(description="Engaging social media caption, at most 280 characters, no hashtags.")
    hashtags: list[str] = Field(description="5 to 10 relevant hashtags, each starting with #.")
    tags: list[str] = Field(description="5 to 15 lowercase SEO keywords.")
    category: str = Field(description="Single best-fit category, e.g. Education, Tech, Travel, Food.")
    sentiment: str = Field(description="Exactly one of: positive, neutral, negative.")
    target_audience: str = Field(description="Who this content suits best, one short phrase.")


class PublishResult(BaseModel):
    platform: str
    url: str
    post_id: str
