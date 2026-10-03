"""Full milestone gate. Docker data cleanup is scoped to a fresh random project."""

import argparse
import json
import os
import secrets
import subprocess
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]


def test_certificate(directory: Path) -> str:
    """Create an ephemeral synthetic certificate, trusted only by this test process."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "proxy")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("proxy")]), critical=False)
        .sign(key, hashes.SHA256())
    )
    pem = certificate.public_bytes(serialization.Encoding.PEM).decode("ascii")
    (directory / "fullchain.pem").write_text(pem, encoding="ascii")
    private = directory / "privkey.pem"
    private.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    # Public synthetic test material must be readable by the non-root test proxy.
    # Never use these permissions for a real production private key.
    directory.chmod(0o755)
    private.chmod(0o644)
    return pem


def check_compose_isolation(compose: list[str], env: dict[str, str]) -> None:
    """Inspect resolved inheritance without putting rendered credentials in logs."""
    result = subprocess.run(
        compose
        + [
            "--profile",
            "ingestion",
            "--profile",
            "agent",
            "--profile",
            "test",
            "config",
            "--format",
            "json",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        encoding="utf-8",
        check=False,
    )
    try:
        if result.returncode:
            raise ValueError
        services = json.loads(result.stdout)["services"]
        if services["agent-worker"]["profiles"] != ["agent"]:
            raise ValueError
        if not {
            "OPENAI_API_KEY",
            "OPENAI_AGENT_MODEL",
            "AGENT_ENABLED",
            "KNOWLEDGE_ENABLED",
        } <= set(services["agent-worker"]["environment"]):
            raise ValueError
        for name in ("api", "worker"):
            if "OPENAI_API_KEY" in services[name]["environment"]:
                raise ValueError
        for name in ("api", "worker", "ingestion-worker", "agent-worker"):
            if "DATABASE_ADMIN_URL" in services[name]["environment"]:
                raise ValueError
    except (ValueError, KeyError, TypeError):
        raise RuntimeError("Compose profile/credential isolation check failed") from None


def check_production_configuration(env: dict[str, str]) -> None:
    """Resolve the documented full deployment with synthetic credentials, offline."""
    # File execution must also work when the current project is not installed.
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from scripts.deployment_check import inspect_configuration

    configured = dict(env)
    configured.update(
        APP_ENV="production",
        WECOM_ENABLED="true",
        WECOM_CORP_ID="synthetic-corp",
        WECOM_SECRET="synthetic-secret",
        WECOM_CALLBACK_TOKEN="synthetic-token",
        WECOM_ENCODING_AES_KEY="abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG",
        WECOM_OPEN_KFIDS='["synthetic-account"]',
        WECOM_ALLOWED_USER_IDS='["synthetic-user"]',
        WECOM_AUTO_REPLY="true",
        INGESTION_ENABLED="true",
        PARSING_ENABLED="true",
        MEDIA_PARSING_ENABLED="true",
        KNOWLEDGE_ENABLED="true",
        AGENT_ENABLED="true",
        OPENAI_API_KEY="synthetic-key",
        OPENAI_AGENT_MODEL="synthetic-agent-model",
        OPENAI_VISION_MODEL="synthetic-vision-model",
        S3_ENDPOINT_URL="https://s3.example.invalid",
        S3_ACCESS_KEY_ID="synthetic-access",
        S3_SECRET_ACCESS_KEY="synthetic-secret",
        S3_BUCKET="synthetic-bucket",
        INGESTION_JOB_TIMEOUT_SECONDS="300",
        INGESTION_LEASE_SECONDS="330",
    )
    result = subprocess.run(
        [
            "docker",
            "compose",
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
        ],
        cwd=ROOT,
        env=configured,
        capture_output=True,
        encoding="utf-8",
        timeout=30,
        check=False,
    )
    try:
        if result.returncode:
            raise ValueError
        checks = inspect_configuration(json.loads(result.stdout))
        if not all(all(values.values()) for values in checks.values()):
            raise ValueError
    except Exception:
        raise RuntimeError("Full production configuration check failed") from None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--official-mirror",
        action="store_true",
        help="Use Docker Official Images on AWS ECR Public",
    )
    options = parser.parse_args()
    output = ROOT / "test-results"
    output.mkdir(exist_ok=True)
    project = "pkb-m9-check-" + uuid4().hex[:12]
    tls = tempfile.TemporaryDirectory(prefix=project + "-tls-", dir=output)
    env = dict(os.environ)
    env.update(
        POSTGRES_DB="pkb_test",
        POSTGRES_USER="pkb_owner",
        POSTGRES_PASSWORD=secrets.token_hex(24),
        APP_DB_PASSWORD=secrets.token_hex(24),
        CONNECTOR_DB_PASSWORD=secrets.token_hex(24),
        REDIS_PASSWORD=secrets.token_hex(24),
        API_PORT="0",
        WECOM_ENABLED="false",
        WECOM_ALLOWED_USER_IDS="[]",
        WECOM_OPEN_KFIDS="[]",
        INGESTION_ENABLED="false",
        PARSING_ENABLED="false",
        MEDIA_PARSING_ENABLED="false",
        KNOWLEDGE_ENABLED="false",
        AGENT_ENABLED="false",
        OPENAI_AGENT_MODEL="",
        OPENAI_API_KEY="",
        OPENAI_VISION_MODEL="",
        PYTHONIOENCODING="utf-8",
        PYTHONUTF8="1",
        TEST_S3_ACCESS_KEY=secrets.token_hex(12),
        TEST_S3_SECRET_KEY=secrets.token_hex(24),
        PKB_TLS_DIR=str(Path(tls.name).resolve()),
        PKB_HTTPS_BIND="127.0.0.1",
        PKB_HTTPS_PORT="0",
        TEST_PROXY_CA=test_certificate(Path(tls.name)),
        TEST_RESTORE_DATABASE="restore_" + uuid4().hex + "_test",
        TEST_RESTORE_USER_A=str(uuid4()),
        TEST_RESTORE_USER_B=str(uuid4()),
    )
    if options.official_mirror:
        env.update(
            PYTHON_IMAGE="public.ecr.aws/docker/library/python:3.12-slim-bookworm",
            POSTGRES_IMAGE="public.ecr.aws/docker/library/postgres:16-bookworm",
            REDIS_IMAGE="public.ecr.aws/docker/library/redis:7.4-bookworm",
            NGINX_IMAGE="public.ecr.aws/docker/library/nginx:stable-bookworm",
        )
    compose = [
        "docker",
        "compose",
        "--project-name",
        project,
        "-f",
        "compose.yml",
        "-f",
        "compose.test.yml",
        "-f",
        "compose.production.yml",
    ]
    with (output / "verification.log").open("w", encoding="utf-8") as log:

        def run(args: list[str], *, check: bool = True) -> int:
            print("Running: " + " ".join(args), flush=True)
            log.write("\n$ " + " ".join(args) + "\n")
            log.flush()
            result = subprocess.run(args, cwd=ROOT, env=env, stdout=log, stderr=log, check=False)
            if check and result.returncode:
                raise RuntimeError(
                    f"Check failed ({result.returncode}); see test-results/verification.log"
                )
            return result.returncode

        outcome = 1
        try:
            run(["uv", "run", "ruff", "check", "."])
            run(["uv", "run", "ruff", "format", "--check", "."])
            run(["uv", "run", "mypy", "app"])
            run(["uv", "run", "pytest", "tests/unit", "--cov=app", "--cov-report=term-missing"])
            run(
                compose
                + [
                    "--profile",
                    "ingestion",
                    "--profile",
                    "agent",
                    "--profile",
                    "test",
                    "config",
                    "--quiet",
                ]
            )
            check_compose_isolation(compose, env)
            log.write("Compose profile and credential isolation passed.\n")
            check_production_configuration(env)
            log.write("Full production configuration passed with synthetic credentials.\n")
            log.flush()
            run(compose + ["build", "api", "tests", "browser"])
            run(compose + ["up", "-d", "--wait", "--wait-timeout", "120", "api", "s3", "browser"])
            # Exercise Debian FFmpeg and real POSIX symlinks even from a Windows host.
            run(
                compose
                + [
                    "run",
                    "--rm",
                    "--no-deps",
                    "tests",
                    "python",
                    "-m",
                    "pytest",
                    "tests/unit/test_ffmpeg.py",
                    "tests/unit/test_openai_media.py",
                    "tests/unit/test_openai_vector.py",
                    "tests/unit/test_s3_delete.py",
                    "tests/unit/test_s3_list.py",
                    "tests/unit/test_openai_responses.py",
                    "tests/unit/test_agent_grounding.py",
                    "tests/unit/test_channels_parser.py",
                    "tests/unit/test_channels_admission.py",
                    "tests/unit/test_wecom_setup.py",
                    "tests/unit/test_operations_status.py",
                    "tests/unit/test_worker_shutdown.py",
                    "-q",
                ]
            )
            run(compose + ["run", "--rm", "--no-deps", "tests"])
            restore_test = compose + [
                "run",
                "--rm",
                "--no-deps",
                "-e",
                "TEST_RESTORE_DATABASE",
                "-e",
                "TEST_RESTORE_USER_A",
                "-e",
                "TEST_RESTORE_USER_B",
                "tests",
                "python",
                "scripts/restore_smoke.py",
            ]
            run(restore_test + ["prepare"])
            run(
                compose
                + [
                    "exec",
                    "-T",
                    "postgres",
                    "sh",
                    "-c",
                    'PGPASSWORD="$POSTGRES_PASSWORD" pg_dump -U "$POSTGRES_USER" '
                    '-d "$POSTGRES_DB" -Fc -f /tmp/pkb-restore-fixture.dump',
                ]
            )
            restore_database = env["TEST_RESTORE_DATABASE"]
            # The name is generated above, never accepted from a user or inferred from a URL.
            run(
                compose
                + [
                    "exec",
                    "-T",
                    "postgres",
                    "sh",
                    "-c",
                    'PGPASSWORD="$POSTGRES_PASSWORD" createdb -U "$POSTGRES_USER" "$1"',
                    "restore",
                    restore_database,
                ]
            )
            run(
                compose
                + [
                    "exec",
                    "-T",
                    "postgres",
                    "sh",
                    "-c",
                    'PGPASSWORD="$POSTGRES_PASSWORD" pg_restore --exit-on-error '
                    '--single-transaction -U "$POSTGRES_USER" -d "$1" '
                    "/tmp/pkb-restore-fixture.dump",
                    "restore",
                    restore_database,
                ]
            )
            run(restore_test + ["check"])
            # The random restore database is removed together with this test project's volume.
            run(compose + ["--profile", "production", "up", "-d", "proxy"])
            run(compose + ["exec", "-T", "proxy", "nginx", "-t"])
            run(
                compose
                + [
                    "run",
                    "--rm",
                    "--no-deps",
                    "-e",
                    "TEST_PROXY_CA",
                    "tests",
                    "python",
                    "scripts/proxy_smoke.py",
                ]
            )
            proxy_log = subprocess.run(
                compose + ["logs", "--no-color", "proxy"],
                cwd=ROOT,
                env=env,
                capture_output=True,
                encoding="utf-8",
                check=False,
            )
            if (
                proxy_log.returncode
                or "synthetic-query-do-not-log" in proxy_log.stdout + proxy_log.stderr
            ):
                raise RuntimeError("Proxy log privacy check failed")
            log.write("Proxy callback query absent from container logs.\n")
            run(
                compose
                + [
                    "exec",
                    "-T",
                    "redis",
                    "sh",
                    "-c",
                    'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli SET m1:persistence yes',
                ]
            )
            run(compose + ["stop", "redis"])
            outage_probe = (
                "import httpx; "
                "assert httpx.get('http://api:8000/health/live').status_code == 200; "
                "assert httpx.get('http://api:8000/health/ready', timeout=10).status_code == 503; "
                "print('Redis outage: live=200, ready=503')"
            )
            run(compose + ["run", "--rm", "--no-deps", "tests", "python", "-c", outage_probe])
            run(compose + ["up", "-d", "--wait", "--wait-timeout", "60", "redis"])
            restored_probe = (
                "import os, httpx, redis; "
                "r=redis.Redis.from_url(os.environ['TEST_REDIS_URL'].rsplit('/',1)[0]+'/0'); "
                "assert r.get('m1:persistence') == b'yes'; "
                "assert httpx.get('http://api:8000/health/ready', timeout=10).status_code == 200; "
                "print('Redis restart: persisted value recovered, ready=200')"
            )
            run(compose + ["run", "--rm", "--no-deps", "tests", "python", "-c", restored_probe])
            outcome = 0
        except (RuntimeError, OSError) as error:
            print(str(error), file=sys.stderr)
            run(compose + ["logs", "--no-color"], check=False)
        finally:
            # Only the named disposable test project and its volumes are removed.
            cleanup = run(
                compose
                + [
                    "--profile",
                    "ingestion",
                    "--profile",
                    "agent",
                    "--profile",
                    "production",
                    "down",
                    "--volumes",
                    "--remove-orphans",
                ],
                check=False,
            )
            if cleanup:
                outcome = 1
                print("Disposable test project cleanup failed; gate not complete.", file=sys.stderr)
            tls.cleanup()
        if outcome == 0:
            print("All milestone checks passed; disposable test project removed.", flush=True)
        return outcome


if __name__ == "__main__":
    raise SystemExit(main())
