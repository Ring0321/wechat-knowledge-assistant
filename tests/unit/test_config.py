import pytest
from pydantic import SecretStr, ValidationError

from app.core.config import Settings


def settings(**kwargs: object) -> Settings:
    return Settings(
        database_url=SecretStr("postgresql+asyncpg://app:secret@localhost/db"),
        redis_url=SecretStr("redis://:secret@localhost:6379/0"),
        **kwargs,
    )


def test_default_allowlist_denies_everyone() -> None:
    config = settings()
    assert not config.allows_wecom_user("any-user")
    assert not config.allows_wecom_user("")
    assert "secret" not in repr(config)


def test_parsing_opt_in_and_requires_ingestion_and_isolated_browser() -> None:
    assert not settings().parsing_enabled
    with pytest.raises(ValidationError, match="Parsing requires"):
        settings(parsing_enabled=True)


def test_allowlist_matches_exact_verified_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WECOM_ALLOWED_USER_IDS", '["allowed"]')
    config = settings()
    assert config.allows_wecom_user("allowed")
    assert not config.allows_wecom_user("allowed ")
    assert not config.allows_wecom_user("other")


@pytest.mark.parametrize("value", ["sqlite:///test.db", "postgresql://app:secret@localhost/db"])
def test_wrong_database_driver_rejected(value: str) -> None:
    with pytest.raises(ValidationError):
        Settings(database_url=SecretStr(value), redis_url=SecretStr("redis://localhost/0"))


def test_bad_redis_url_and_blank_identity_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings(
            database_url=SecretStr("postgresql+asyncpg://app:secret@localhost/db"),
            redis_url=SecretStr("http://localhost"),
        )
    with pytest.raises(ValidationError):
        settings(wecom_allowed_user_ids=frozenset({" "}))


def test_health_timeout_is_bounded() -> None:
    with pytest.raises(ValidationError):
        settings(health_timeout_seconds=0)


def test_enabled_wecom_requires_complete_configuration() -> None:
    with pytest.raises(ValidationError):
        settings(wecom_enabled=True)


def test_wecom_reply_limit_counts_utf8_bytes() -> None:
    with pytest.raises(ValidationError):
        settings(wecom_reply_text="中" * 683)
    with pytest.raises(ValidationError):
        settings(wecom_reply_text="   ")
    assert len(settings(wecom_reply_text="中" * 682).wecom_reply_text.encode()) == 2046


def test_invalid_environment_does_not_expose_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://app:sensitive-password@localhost/db")
    monkeypatch.setenv("REDIS_URL", "redis://localhost/0")
    with pytest.raises(ValidationError) as caught:
        Settings()
    assert "sensitive-password" not in str(caught.value)


def test_ingestion_is_opt_in_and_credentials_are_masked() -> None:
    config = settings(
        s3_access_key_id="synthetic-private-access", s3_secret_access_key="synthetic-private-secret"
    )
    assert not config.ingestion_enabled
    assert "synthetic-private" not in repr(config)
    with pytest.raises(ValidationError):
        settings(ingestion_enabled=True)


def test_media_opt_in_masks_key_and_requires_explicit_setup() -> None:
    config = settings(openai_api_key="synthetic-private-openai")
    assert not config.media_parsing_enabled
    assert "synthetic-private-openai" not in repr(config)
    with pytest.raises(ValidationError, match="Media parsing requires"):
        settings(media_parsing_enabled=True)


@pytest.mark.parametrize(
    "change",
    [
        {"openai_api_key": None},
        {"openai_api_key": " "},
        {"openai_vision_model": ""},
        {"ingestion_job_timeout_seconds": 90},
        {"ingestion_lease_seconds": 300},
        {"media_max_frames": 33},
        {"media_frame_interval_seconds": 1},
    ],
)
def test_media_budget_and_configuration_boundaries(change: dict[str, object]) -> None:
    data = {
        "wecom_enabled": True,
        "wecom_corp_id": "synthetic",
        "wecom_open_kfids": ["synthetic"],
        "wecom_secret": "synthetic",
        "wecom_callback_token": "synthetic",
        "wecom_encoding_aes_key": "synthetic",
        "ingestion_enabled": True,
        "s3_endpoint_url": "https://s3.example.com",
        "s3_bucket": "synthetic",
        "s3_access_key_id": "synthetic",
        "s3_secret_access_key": "synthetic",
        "media_parsing_enabled": True,
        "openai_api_key": "synthetic",
        "openai_vision_model": "synthetic-vision",
        "ingestion_job_timeout_seconds": 300,
        "ingestion_lease_seconds": 330,
    }
    assert settings(**data).media_parsing_enabled
    with pytest.raises(ValidationError):
        settings(**{**data, **change})


@pytest.mark.parametrize(
    "changes",
    [
        {"s3_bucket": ""},
        {"s3_access_key_id": None},
        {"ingestion_lease_seconds": 90},
        {"ingestion_max_file_bytes": 101 * 1024 * 1024},
    ],
)
def test_ingestion_rejects_incomplete_or_unbounded_configuration(
    changes: dict[str, object],
) -> None:
    config: dict[str, object] = {
        "wecom_enabled": True,
        "wecom_corp_id": "synthetic-corp",
        "wecom_open_kfids": ["synthetic-kf"],
        "wecom_secret": "synthetic-secret",
        "wecom_callback_token": "synthetic-token",
        "wecom_encoding_aes_key": "synthetic-key",
        "ingestion_enabled": True,
        "s3_endpoint_url": "https://s3.example.com",
        "s3_bucket": "private-bucket",
        "s3_access_key_id": "synthetic-access",
        "s3_secret_access_key": "synthetic-secret",
    }
    assert settings(**config).ingestion_enabled
    with pytest.raises(ValidationError):
        settings(**{**config, **changes})
