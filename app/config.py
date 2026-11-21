"""Central configuration, loaded from environment variables / .env file."""
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- LLM (Gemini) ---
    gemini_api_key: str = ""                 # empty -> offline MockAnalyzer (demo mode)
    gemini_model: str = "gemini-2.5-flash"   # swap for gemini-2.5-pro for higher quality
    llm_max_rpm: int = 10                    # client-side rate limit (requests per minute)
    llm_max_concurrency: int = 2             # max parallel Gemini calls
    inline_media_limit_mb: int = 15          # bigger files go through the Gemini Files API

    # --- Telegram ---
    telegram_bot_token: str = ""
    telegram_alert_chat_id: str = ""         # your personal chat id (alerts to your phone)
    telegram_channel_id: str = ""            # channel to publish to, e.g. @my_channel

    # --- Pipeline ---
    publisher: str = "local"                 # local | telegram_channel
    workers: int = 2
    max_attempts: int = 5
    retry_base_delay: float = 30.0           # seconds; doubles each failed attempt
    watch_force_polling: bool = False        # set true for Docker on Windows/macOS
    data_dir: Path = Path("data")

    @property
    def inbox_dir(self) -> Path:
        return self.data_dir / "inbox"

    @property
    def uploads_dir(self) -> Path:
        return self.data_dir / "uploads"

    @property
    def processed_dir(self) -> Path:
        return self.data_dir / "processed"

    @property
    def outputs_dir(self) -> Path:
        return self.data_dir / "outputs"

    @property
    def published_dir(self) -> Path:
        return self.data_dir / "published"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "jobs.db"

    def ensure_dirs(self) -> None:
        for d in (self.inbox_dir, self.uploads_dir, self.processed_dir,
                  self.outputs_dir, self.published_dir):
            d.mkdir(parents=True, exist_ok=True)


settings = Settings()
