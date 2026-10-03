"""Operator preflight is offline, role-specific and never reveals configuration values."""

import json
import sys

import pytest
from pydantic import SecretStr

from app.core.config import Settings
from app.operations import preflight
from app.operations.preflight import Role, configuration_checks
from tests.wecom_helpers import TEST_KEY


def configured() -> Settings:
    return Settings(
        app_env="production",
        database_url=SecretStr("postgresql+asyncpg://pkb_app:private@db/pkb"),
        connector_database_url=SecretStr("postgresql+asyncpg://pkb_connector:private@db/pkb"),
        redis_url=SecretStr("redis://:private@redis/0"),
        wecom_enabled=True,
        wecom_corp_id="private-corp",
        wecom_secret=SecretStr("private-secret"),
        wecom_callback_token=SecretStr("private-token"),
        wecom_encoding_aes_key=SecretStr(TEST_KEY),
        wecom_open_kfids=frozenset({"private-account"}),
        wecom_allowed_user_ids=frozenset({"private-user"}),
        ingestion_enabled=True,
        ingestion_job_timeout_seconds=300,
        ingestion_lease_seconds=330,
        s3_endpoint_url="https://storage.example.com",
        s3_bucket="private-bucket",
        s3_access_key_id=SecretStr("private-key"),
        s3_secret_access_key=SecretStr("private-key-secret"),
        parsing_enabled=True,
        browser_ws_url="ws://browser:3000/",
        media_parsing_enabled=True,
        knowledge_enabled=True,
        agent_enabled=True,
        openai_api_key=SecretStr("private-openai"),
        openai_vision_model="private-vision",
        openai_agent_model="private-agent",
    )


@pytest.mark.parametrize("role", list(Role))
def test_each_full_product_role_passes_without_network(role: Role) -> None:
    checks = configuration_checks(configured(), role)
    assert all(checks.values())
    assert "private" not in json.dumps(checks)


@pytest.mark.parametrize(
    "field,value,check,role",
    [
        ("app_env", "development", "production_environment", Role.API),
        ("wecom_enabled", False, "wecom_enabled", Role.API),
        ("wecom_allowed_user_ids", frozenset(), "allowlist_configured", Role.API),
        ("wecom_open_kfids", frozenset(), "customer_accounts_configured", Role.API),
        ("wecom_encoding_aes_key", SecretStr("bad"), "callback_crypto_valid", Role.API),
        ("connector_database_url", None, "connector_password", Role.WECOM),
        ("wecom_auto_reply", False, "automatic_replies_enabled", Role.WECOM),
        ("agent_enabled", False, "agent_enabled", Role.WECOM),
        ("s3_endpoint_url", "http://storage.example.com", "s3_https_endpoint", Role.WECOM),
        ("s3_endpoint_url", "https://private@storage.example.com", "s3_https_endpoint", Role.WECOM),
        ("knowledge_enabled", False, "knowledge_enabled", Role.INGESTION),
        ("media_parsing_enabled", False, "media_parsing_enabled", Role.INGESTION),
        ("parsing_enabled", False, "document_parsing_enabled", Role.INGESTION),
        ("openai_api_key", None, "openai_credential_configured", Role.AGENT),
    ],
)
def test_incomplete_full_product_configuration_fails_closed(
    field: str, value: object, check: str, role: Role
) -> None:
    settings = configured().model_copy(update={field: value})
    assert configuration_checks(settings, role)[check] is False


def test_api_and_admission_do_not_require_openai_secret() -> None:
    settings = configured().model_copy(update={"openai_api_key": None})
    assert all(configuration_checks(settings, Role.API).values())
    assert all(configuration_checks(settings, Role.WECOM).values())


def test_cli_invalid_environment_never_prints_validation_inputs(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys, "argv", ["preflight", "--role", "api"])
    monkeypatch.setenv("DATABASE_URL", "private-malformed-secret")
    monkeypatch.setenv("REDIS_URL", "private-invalid-redis")
    assert preflight.main() == 1
    output = capsys.readouterr()
    assert "private" not in output.out + output.err
    assert json.loads(output.out)["fields"] == ["database_url", "redis_url"]


def test_cli_success_reports_only_checks(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    settings = configured()
    monkeypatch.setattr(sys, "argv", ["preflight", "--role", "ingestion"])
    monkeypatch.setattr(preflight, "Settings", lambda: settings)
    assert preflight.main() == 0
    output = capsys.readouterr().out
    assert "private" not in output
    assert json.loads(output)["status"] == "configuration_valid"
