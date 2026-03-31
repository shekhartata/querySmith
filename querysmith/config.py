from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load `.env` from project root (directory containing `pyproject.toml`), not from the process cwd.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"


def _env_file_config() -> dict[str, str | list[str] | None]:
    """Only attach env_file if present; OS environment variables still apply."""
    cfg: dict[str, str | list[str] | None] = {
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }
    if _ENV_FILE.is_file():
        cfg["env_file"] = str(_ENV_FILE)
    return cfg


class Settings(BaseSettings):
    model_config = SettingsConfigDict(**_env_file_config())

    mongodb_uri: str = Field(
        default="mongodb://localhost:27017",
        validation_alias=AliasChoices("MONGODB_URI", "QUERYSMITH_MONGODB_URI"),
    )
    env: Literal["dev", "uat", "prod"] = Field(default="dev", validation_alias="QUERYSMITH_ENV")
    default_timeout_ms: int = Field(default=30_000, validation_alias="QUERYSMITH_DEFAULT_TIMEOUT_MS")
    sample_size: int = Field(default=80, validation_alias="QUERYSMITH_SAMPLE_SIZE")
    max_pipeline_stages_warn: int = Field(default=25, validation_alias="QUERYSMITH_MAX_PIPELINE_STAGES_WARN")
    view_flatten_timeout_ms: int = Field(default=60_000, validation_alias="QUERYSMITH_VIEW_FLATTEN_TIMEOUT_MS")
    openai_api_key: str | None = Field(default=None, validation_alias=AliasChoices("OPENAI_API_KEY", "QUERYSMITH_OPENAI_API_KEY"))
    openai_base_url: str | None = Field(default=None, validation_alias="QUERYSMITH_OPENAI_BASE_URL")
    llm_model: str = Field(default="gpt-4o-mini", validation_alias="QUERYSMITH_LLM_MODEL")


def load_settings() -> Settings:
    return Settings()
