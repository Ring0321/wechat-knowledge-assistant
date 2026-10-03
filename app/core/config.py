"""Environment-only configuration; credentials are masked in object representations."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="", extra="ignore", env_file=None, hide_input_in_errors=True
    )

    app_env: Literal["development", "test", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    database_url: SecretStr
    redis_url: SecretStr
    health_timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    db_pool_size: int = Field(default=5, ge=1, le=50)
    db_max_overflow: int = Field(default=5, ge=0, le=50)
    wecom_allowed_user_ids: frozenset[str] = frozenset()
    wecom_enabled: bool = False
    wecom_corp_id: str = ""
    wecom_secret: SecretStr | None = Field(default=None, repr=False)
    wecom_callback_token: SecretStr | None = Field(default=None, repr=False)
    wecom_encoding_aes_key: SecretStr | None = Field(default=None, repr=False)
    wecom_open_kfids: frozenset[str] = frozenset()
    connector_database_url: SecretStr | None = None
    wecom_callback_budget_seconds: float = Field(default=3.0, gt=0, le=4)
    wecom_max_callback_bytes: int = Field(default=65536, ge=1024, le=1048576)
    wecom_http_timeout_seconds: float = Field(default=8.0, gt=0, le=30)
    wecom_poll_interval_seconds: float = Field(default=60, ge=30, le=600)
    wecom_retry_base_seconds: float = Field(default=5, ge=1, le=60)
    wecom_max_attempts: int = Field(default=5, ge=1, le=20)
    wecom_max_pages_per_sync: int = Field(default=100, ge=1, le=1000)
    wecom_auto_reply: bool = True
    wecom_reply_text: str = "收到，微信客服连接已就绪。资料整理和知识库问答功能尚未启用。"
    ingestion_enabled: bool = False
    ingestion_max_file_bytes: int = Field(default=20 * 1024 * 1024, ge=1024, le=100 * 1024 * 1024)
    ingestion_max_text_bytes: int = Field(default=2 * 1024 * 1024, ge=1024, le=10 * 1024 * 1024)
    ingestion_max_urls: int = Field(default=10, ge=1, le=20)
    ingestion_job_timeout_seconds: float = Field(default=90, ge=10, le=600)
    ingestion_lease_seconds: int = Field(default=120, ge=30, le=900)
    ingestion_max_attempts: int = Field(default=3, ge=1, le=10)
    s3_endpoint_url: str = ""
    s3_access_key_id: SecretStr | None = Field(default=None, repr=False)
    s3_secret_access_key: SecretStr | None = Field(default=None, repr=False)
    s3_bucket: str = ""
    s3_region: str = "us-east-1"
    parsing_enabled: bool = False
    parser_timeout_seconds: float = Field(default=45, ge=5, le=300)
    parser_max_text_chars: int = Field(default=500_000, ge=1000, le=2_000_000)
    parser_max_pages: int = Field(default=100, ge=1, le=1000)
    parser_max_cells: int = Field(default=50_000, ge=1, le=200_000)
    ocr_command: str = "tesseract"
    ocr_languages: str = "chi_sim+eng"
    browser_ws_url: str = ""
    media_parsing_enabled: bool = False
    openai_api_key: SecretStr | None = Field(default=None, repr=False)
    openai_vision_model: str = ""
    media_timeout_seconds: float = Field(default=240, ge=10, le=500)
    media_max_duration_seconds: float = Field(default=900, gt=0, le=3600)
    media_chunk_seconds: int = Field(default=300, ge=1, le=600)
    media_max_frames: int = Field(default=12, ge=1, le=32)
    media_frame_interval_seconds: float = Field(default=30, ge=5, le=600)
    media_scene_threshold: float = Field(default=0.3, gt=0, lt=1)
    knowledge_enabled: bool = False
    knowledge_http_timeout_seconds: float = Field(default=20, ge=1, le=60)
    knowledge_max_document_bytes: int = Field(default=8 * 1024 * 1024, ge=1024, le=8 * 1024 * 1024)
    knowledge_poll_seconds: int = Field(default=10, ge=1, le=300)
    knowledge_index_timeout_seconds: int = Field(default=3600, ge=60, le=86400)
    agent_enabled: bool = False
    openai_agent_model: str = ""
    agent_http_timeout_seconds: float = Field(default=30, ge=1, le=60)
    agent_job_timeout_seconds: float = Field(default=120, ge=10, le=600)
    agent_lease_seconds: int = Field(default=150, ge=30, le=900)
    agent_max_attempts: int = Field(default=3, ge=1, le=10)
    agent_max_turns: int = Field(default=6, ge=1, le=10)
    agent_max_output_tokens: int = Field(default=2000, ge=128, le=8000)

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr) -> SecretStr:
        try:
            url = make_url(value.get_secret_value())
        except Exception:
            raise ValueError("DATABASE_URL must be a valid PostgreSQL async DSN") from None
        if url.drivername != "postgresql+asyncpg" or not url.host or not url.database:
            raise ValueError("DATABASE_URL requires postgresql+asyncpg and a host/database")
        return value

    @field_validator("redis_url")
    @classmethod
    def validate_redis_url(cls, value: SecretStr) -> SecretStr:
        from urllib.parse import urlsplit

        try:
            url = urlsplit(value.get_secret_value())
            valid = url.scheme in {"redis", "rediss"} and bool(url.hostname)
        except ValueError:
            valid = False
        if not valid:
            raise ValueError("REDIS_URL requires redis:// or rediss:// with a host")
        return value

    @field_validator("wecom_allowed_user_ids", "wecom_open_kfids")
    @classmethod
    def validate_allowlist(cls, values: frozenset[str]) -> frozenset[str]:
        if any(not value or value != value.strip() for value in values):
            raise ValueError("Allowlist entries must be nonempty without surrounding whitespace")
        return values

    @field_validator("wecom_reply_text")
    @classmethod
    def validate_reply_text(cls, value: str) -> str:
        if not value.strip() or len(value.encode("utf-8")) > 2048:
            raise ValueError("WeCom text reply must contain 1 to 2048 UTF-8 bytes")
        return value

    @model_validator(mode="after")
    def validate_wecom_configuration(self) -> "Settings":
        if self.wecom_enabled:
            secrets = (self.wecom_secret, self.wecom_callback_token, self.wecom_encoding_aes_key)
            if not self.wecom_corp_id or not self.wecom_open_kfids:
                raise ValueError("Enabled WeCom requires corp ID and configured customer accounts")
            if any(value is None or not value.get_secret_value() for value in secrets):
                raise ValueError("Enabled WeCom requires all callback and API credentials")
        if self.connector_database_url is not None:
            self.validate_database_url(self.connector_database_url)
        if self.ingestion_enabled:
            if not self.wecom_enabled or not self.s3_endpoint_url or not self.s3_bucket:
                raise ValueError("Ingestion requires WeCom and an explicit S3 endpoint/bucket")
            if any(
                value is None or not value.get_secret_value()
                for value in (self.s3_access_key_id, self.s3_secret_access_key)
            ):
                raise ValueError("Ingestion requires explicit S3 credentials")
            if self.ingestion_lease_seconds < self.ingestion_job_timeout_seconds + 15:
                raise ValueError("Task lease must exceed its timeout by at least 15 seconds")
        if self.parsing_enabled:
            if not self.ingestion_enabled or not self.browser_ws_url:
                raise ValueError("Parsing requires ingestion and an isolated browser endpoint")
            if self.parser_timeout_seconds + 30 > self.ingestion_job_timeout_seconds:
                raise ValueError("Ingestion budget must include parser time plus 30 seconds")
        if self.media_parsing_enabled:
            if not self.ingestion_enabled or not self.openai_api_key:
                raise ValueError("Media parsing requires ingestion and an OpenAI credential")
            if (
                not self.openai_api_key.get_secret_value().strip()
                or not self.openai_vision_model.strip()
            ):
                raise ValueError("Media parsing requires an explicit vision model and credential")
            if self.media_timeout_seconds + 30 > self.ingestion_job_timeout_seconds:
                raise ValueError("Ingestion budget must include media time plus 30 seconds")
        if self.knowledge_enabled:
            if not self.ingestion_enabled or not self.openai_api_key:
                raise ValueError("Knowledge indexing requires ingestion and an OpenAI credential")
            if not self.openai_api_key.get_secret_value().strip():
                raise ValueError("Knowledge indexing requires an explicit credential")
        if self.agent_enabled:
            # The WeCom admission runtime intentionally has no OpenAI key. Its
            # separate Agent runtime also checks knowledge/key before starting.
            if (
                not self.wecom_enabled
                or not self.ingestion_enabled
                or not self.openai_agent_model.strip()
            ):
                raise ValueError("Agent requires WeCom, ingestion and an explicit model")
            if self.agent_lease_seconds < self.agent_job_timeout_seconds + 15:
                raise ValueError("Agent lease must exceed its timeout by at least 15 seconds")
        return self

    def allows_wecom_user(self, external_user_id: str) -> bool:
        """Fail closed; callers must obtain the identity from authenticated WeCom data."""
        return external_user_id in self.wecom_allowed_user_ids


@lru_cache
def get_settings() -> Settings:
    return Settings()
