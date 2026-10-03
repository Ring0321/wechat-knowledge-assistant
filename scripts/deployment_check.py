"""Resolve full Compose configuration privately and reject incomplete deployments."""

import argparse
import json
import re
import subprocess
from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.operations.preflight import Role, configuration_checks

ROOT = Path(__file__).resolve().parents[1]
SERVICES = {
    "api": Role.API,
    "worker": Role.WECOM,
    "ingestion-worker": Role.INGESTION,
    "agent-worker": Role.AGENT,
}


def settings_from_environment(environment: dict[str, str]) -> Settings:
    # Pass every value explicitly, so the host's ambient environment cannot fill a
    # missing container credential and hide an isolation/configuration error.
    values: dict[str, Any] = {}
    for name, field in Settings.model_fields.items():
        raw = environment.get(name.upper())
        if raw is None:
            if field.is_required():
                raise ValueError("missing_container_configuration")
            values[name] = field.get_default(call_default_factory=True)
        elif name in {"wecom_open_kfids", "wecom_allowed_user_ids"}:
            values[name] = json.loads(raw)
        else:
            values[name] = raw
    return Settings(**values)


def duration_seconds(value: object) -> float:
    if not isinstance(value, str) or not re.fullmatch(r"(?:\d+(?:\.\d+)?[hms])+", value):
        return 0
    return sum(
        float(number) * {"h": 3600, "m": 60, "s": 1}[unit]
        for number, unit in re.findall(r"(\d+(?:\.\d+)?)([hms])", value)
    )


def inspect_configuration(config: dict[str, Any]) -> dict[str, dict[str, bool]]:
    services = config.get("services", {})
    result: dict[str, dict[str, bool]] = {}
    for name, role in SERVICES.items():
        if name not in services:
            result[name] = {"service_present": False}
            continue
        service = services[name]
        try:
            environment = service["environment"]
            settings = settings_from_environment(environment)
            checks = configuration_checks(settings, role)
            checks["no_admin_dsn"] = not environment.get("DATABASE_ADMIN_URL")
            if role in {Role.API, Role.WECOM}:
                checks["no_openai_key"] = not environment.get("OPENAI_API_KEY")
            if role != Role.API:
                timeout = (
                    settings.agent_job_timeout_seconds
                    if role == Role.AGENT
                    else settings.ingestion_job_timeout_seconds
                    if role == Role.INGESTION
                    else 30
                )
                checks["shutdown_grace"] = (
                    duration_seconds(service.get("stop_grace_period")) >= timeout + 15
                )
            result[name] = checks
        except Exception:
            result[name] = {"configuration_valid": False}
    result["supporting_services"] = {
        name: name in services for name in ("postgres", "redis", "migrate", "browser", "proxy")
    }
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", required=True, type=Path)
    args = parser.parse_args()
    command = [
        "docker",
        "compose",
        "--env-file",
        str(args.env_file.resolve()),
        "-f",
        "compose.yml",
        "-f",
        "compose.production.yml",
        "--profile",
        "ingestion",
        "--profile",
        "agent",
        "--profile",
        "production",
        "config",
        "--format",
        "json",
    ]
    try:
        rendered = subprocess.run(
            command, cwd=ROOT, capture_output=True, encoding="utf-8", timeout=30, check=False
        )
        if rendered.returncode:
            raise ValueError("compose_configuration_invalid")
        checks = inspect_configuration(json.loads(rendered.stdout))
    except Exception:
        print(json.dumps({"status": "compose_configuration_invalid"}))
        return 1
    valid = all(all(values.values()) for values in checks.values())
    print(
        json.dumps(
            {
                "status": "configuration_valid" if valid else "configuration_incomplete",
                "checks": checks,
            }
        )
    )
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
