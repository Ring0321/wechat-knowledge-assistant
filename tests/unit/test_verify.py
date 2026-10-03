"""The full gate must not report success if cleanup or a required check failed."""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts import verify


def rendered_config() -> dict[str, Any]:
    return {
        "services": {
            "api": {"environment": {}},
            "worker": {"environment": {}},
            "ingestion-worker": {"environment": {}},
            "agent-worker": {
                "profiles": ["agent"],
                "environment": {
                    "OPENAI_API_KEY": "synthetic-do-not-log",
                    "OPENAI_AGENT_MODEL": "synthetic",
                    "AGENT_ENABLED": "false",
                    "KNOWLEDGE_ENABLED": "false",
                },
            },
        }
    }


@pytest.mark.parametrize("build_code,cleanup_code,expected", [(0, 0, 0), (0, 1, 1), (1, 0, 1)])
def test_gate_checks_all_profiles_and_requires_cleanup_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    build_code: int,
    cleanup_code: int,
    expected: int,
) -> None:
    commands: list[list[str]] = []

    def run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(args)
        code = cleanup_code if "down" in args else build_code if "build" in args else 0
        return subprocess.CompletedProcess(
            args, code, stdout=json.dumps(rendered_config()), stderr=""
        )

    monkeypatch.setattr(verify, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["verify.py"])
    monkeypatch.setattr(verify.subprocess, "run", run)
    monkeypatch.setattr(verify, "check_production_configuration", lambda env: None)
    assert verify.main() == expected
    config = next(command for command in commands if "config" in command)
    assert config[-8:] == [
        "--profile",
        "ingestion",
        "--profile",
        "agent",
        "--profile",
        "test",
        "config",
        "--quiet",
    ]
    cleanup = commands[-1]
    assert cleanup[cleanup.index("--project-name") + 1].startswith("pkb-m9-check-")
    assert cleanup[-3:] == ["down", "--volumes", "--remove-orphans"]
    assert ("All milestone checks passed" in capsys.readouterr().out) is (expected == 0)


@pytest.mark.parametrize("case", ["profiles", "model_key", "admin_dsn", "missing_key"])
def test_compose_isolation_checks_fail_without_logging_credentials(
    case: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config = rendered_config()
    services = config["services"]
    if case == "profiles":
        services["agent-worker"]["profiles"] = ["wecom", "ingestion", "agent"]
    elif case == "model_key":
        services["api"]["environment"]["OPENAI_API_KEY"] = "synthetic-do-not-log"
    elif case == "admin_dsn":
        services["worker"]["environment"]["DATABASE_ADMIN_URL"] = "synthetic-do-not-log"
    else:
        del services["agent-worker"]["environment"]["OPENAI_API_KEY"]
    monkeypatch.setattr(
        verify.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, stdout=json.dumps(config)),
    )
    with pytest.raises(RuntimeError, match="isolation check failed"):
        verify.check_compose_isolation(["docker", "compose"], {})
    assert "synthetic-do-not-log" not in capsys.readouterr().out
