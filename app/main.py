"""Application assembly. WeCom callback is opt-in; business processing runs in the worker."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import RequestResponseEndpoint

from app.api.health import router
from app.connectors.wecom.callback import CallbackService
from app.connectors.wecom.callback import router as wecom_router
from app.connectors.wecom.crypto import WeComCrypto
from app.connectors.wecom.queue import RedisNotificationQueue
from app.core.config import Settings, get_settings
from app.core.health import HealthProbe
from app.core.logging import configure_logging
from app.core.runtime import infrastructure

logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    health_probe: HealthProbe | None = None,
    callback_service: CallbackService | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging("INFO")
        config = settings or get_settings()
        configure_logging(config.log_level)
        app.state.settings = config
        app.state.wecom_callback = callback_service
        if health_probe is not None:
            app.state.health_probe = health_probe
            yield
        else:
            async with infrastructure(config) as probe:
                app.state.health_probe = probe
                if config.wecom_enabled:
                    assert config.wecom_callback_token is not None
                    assert config.wecom_encoding_aes_key is not None
                    app.state.wecom_callback = CallbackService(
                        WeComCrypto(
                            config.wecom_callback_token.get_secret_value(),
                            config.wecom_encoding_aes_key.get_secret_value(),
                            config.wecom_corp_id,
                        ),
                        RedisNotificationQueue(probe.redis_client, config.wecom_corp_id),
                        config.wecom_open_kfids,
                    )
                logger.info("application_started")
                yield
            logger.info("application_stopped")

    app = FastAPI(title="微信个人知识库 AI 助手", lifespan=lifespan, docs_url=None, redoc_url=None)
    app.include_router(router)
    app.include_router(wecom_router)

    @app.middleware("http")
    async def request_context(request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = str(uuid4())
        start = perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            # Last-resort HTTP boundary: never echo raw exceptions or secrets.
            response = JSONResponse(status_code=500, content={"detail": "Internal server error"})
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "http_request",
            extra={
                "request_id": request_id,
                "status_code": response.status_code,
                "duration_ms": round((perf_counter() - start) * 1000, 2),
                "method": request.method if request.method in {"GET", "POST", "HEAD"} else "OTHER",
            },
        )
        return response

    return app


app = create_app()
