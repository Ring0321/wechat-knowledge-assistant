"""Offline full-assistant configuration gate, using each container's own environment."""

import argparse
import json
from enum import StrEnum
from urllib.parse import urlsplit

from pydantic import ValidationError
from sqlalchemy.engine import make_url

from app.connectors.wecom.contracts import CallbackError
from app.connectors.wecom.crypto import WeComCrypto
from app.core.config import Settings


class Role(StrEnum):
    API = "api"
    WECOM = "wecom"
    INGESTION = "ingestion"
    AGENT = "agent"


def configuration_checks(settings: Settings, role: Role) -> dict[str, bool]:
    """Check opt-ins for the full product without disclosing values or contacting APIs."""
    database = make_url(settings.database_url.get_secret_value())
    redis = urlsplit(settings.redis_url.get_secret_value())
    checks = {
        "production_environment": settings.app_env == "production",
        "database_password": bool(database.password),
        "redis_password": bool(redis.password),
        "wecom_enabled": settings.wecom_enabled,
        "customer_accounts_configured": bool(settings.wecom_open_kfids),
        "allowlist_configured": bool(settings.wecom_allowed_user_ids),
    }
    try:
        assert settings.wecom_callback_token is not None
        assert settings.wecom_encoding_aes_key is not None
        WeComCrypto(
            settings.wecom_callback_token.get_secret_value(),
            settings.wecom_encoding_aes_key.get_secret_value(),
            settings.wecom_corp_id,
        )
        checks["callback_crypto_valid"] = True
    except (ValueError, AssertionError, CallbackError):
        checks["callback_crypto_valid"] = False
    if role == Role.API:
        return checks
    connector = settings.connector_database_url
    checks["connector_password"] = bool(
        connector and make_url(connector.get_secret_value()).password
    )
    checks["ingestion_enabled"] = settings.ingestion_enabled
    checks["automatic_replies_enabled"] = settings.wecom_auto_reply
    checks["agent_enabled"] = settings.agent_enabled
    checks["agent_model_configured"] = bool(settings.openai_agent_model.strip())
    endpoint = urlsplit(settings.s3_endpoint_url)
    checks["s3_https_endpoint"] = (
        endpoint.scheme == "https"
        and bool(endpoint.hostname)
        and endpoint.username is None
        and endpoint.password is None
        and not endpoint.query
        and not endpoint.fragment
    )
    if role == Role.WECOM:
        return checks
    checks["knowledge_enabled"] = settings.knowledge_enabled
    checks["openai_credential_configured"] = bool(
        settings.openai_api_key and settings.openai_api_key.get_secret_value().strip()
    )
    if role == Role.INGESTION:
        checks["document_parsing_enabled"] = settings.parsing_enabled
        checks["media_parsing_enabled"] = settings.media_parsing_enabled
        checks["vision_model_configured"] = bool(settings.openai_vision_model.strip())
    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", choices=list(Role), required=True)
    args = parser.parse_args()
    try:
        checks = configuration_checks(Settings(), Role(args.role))
    except ValidationError as error:
        # Never serialize Pydantic's input/context/message, which may contain secrets.
        fields = sorted(
            {
                str(item["loc"][0])
                for item in error.errors(include_input=False, include_context=False)
                if item["loc"] and item["loc"][0] in Settings.model_fields
            }
        )
        print(json.dumps({"status": "configuration_invalid", "fields": fields}))
        return 1
    except Exception:
        print(json.dumps({"status": "configuration_invalid"}))
        return 1
    ready = all(checks.values())
    print(
        json.dumps(
            {
                "status": "configuration_valid" if ready else "configuration_incomplete",
                "checks": checks,
            }
        )
    )
    return 0 if ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
