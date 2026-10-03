from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.core.health import check_readiness

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request) -> JSONResponse:
    dependencies = await check_readiness(
        request.app.state.health_probe, request.app.state.settings.health_timeout_seconds
    )
    healthy = all(value == "up" for value in dependencies.values())
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ready" if healthy else "not_ready", "dependencies": dependencies},
    )
