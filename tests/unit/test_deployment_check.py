import json
import subprocess
import sys
from typing import Any

import pytest

from scripts import deployment_check
from tests.unit.test_preflight import configured


def complete_config() -> dict[str, Any]:
    settings = configured()
    environment: dict[str, str] = {}
    for name in type(settings).model_fields:
        value = getattr(settings, name)
        if hasattr(value, "get_secret_value"):
            value = value.get_secret_value()
        if isinstance(value, (set, frozenset)):
            value = json.dumps(list(value))
        elif isinstance(value, bool):
            value = str(value).lower()
        elif value is None:
            continue
        environment[name.upper()] = str(value)
    services: dict[str, Any] = {}
    for name in deployment_check.SERVICES:
        env = dict(environment)
        if name in {"api", "worker"}:
            env.update(
                KNOWLEDGE_ENABLED="false", PARSING_ENABLED="false", MEDIA_PARSING_ENABLED="false"
            )
            env.pop("OPENAI_API_KEY")
        services[name] = {"environment": env, "stop_grace_period": "10m50s"}
    services.update({name: {} for name in ("postgres", "redis", "migrate", "browser", "proxy")})
    return {"services": services}


def test_complete_config_keeps_wecom_keyless() -> None:
    results = deployment_check.inspect_configuration(complete_config())
    assert all(all(checks.values()) for checks in results.values())
    assert "private" not in json.dumps(results)


@pytest.mark.parametrize(
    "case", ["no_agent", "no_knowledge", "short_grace", "key_leak", "admin_leak", "empty_allowlist"]
)
def test_full_deployment_rejects_nonworking_or_leaking_combinations(case: str) -> None:
    config = complete_config()
    services = config["services"]
    if case == "no_agent":
        del services["agent-worker"]
    elif case == "no_knowledge":
        services["agent-worker"]["environment"]["KNOWLEDGE_ENABLED"] = "false"
    elif case == "short_grace":
        services["ingestion-worker"]["stop_grace_period"] = "10s"
    elif case == "key_leak":
        services["worker"]["environment"]["OPENAI_API_KEY"] = "private-key"
    elif case == "admin_leak":
        services["api"]["environment"]["DATABASE_ADMIN_URL"] = "private-admin"
    else:
        services["worker"]["environment"]["WECOM_ALLOWED_USER_IDS"] = "[]"
    results = deployment_check.inspect_configuration(config)
    assert not all(all(checks.values()) for checks in results.values())
    assert "private" not in json.dumps(results)


def test_host_credentials_cannot_fill_missing_container_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "private-host-key")
    config = complete_config()
    del config["services"]["agent-worker"]["environment"]["OPENAI_API_KEY"]
    results = deployment_check.inspect_configuration(config)
    assert not all(results["agent-worker"].values())


def test_compose_error_output_never_exposes_rendered_secrets(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys, "argv", ["check", "--env-file", "private-config"])
    monkeypatch.setattr(
        deployment_check.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            [], 1, stdout="private-key", stderr="private-password"
        ),
    )
    assert deployment_check.main() == 1
    output = capsys.readouterr()
    assert "private" not in output.out + output.err
