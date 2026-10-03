"""Synthetic TLS/route check inside the disposable test project, never a public probe."""

import os
import ssl

import httpx


def main() -> None:
    context = ssl.create_default_context(cadata=os.environ["TEST_PROXY_CA"])
    with httpx.Client(
        base_url="https://proxy:8443", verify=context, trust_env=False, timeout=5
    ) as client:
        response = client.get("/wecom/callback?echostr=synthetic-query-do-not-log")
        # The verification API intentionally has WeCom disabled. Its request ID proves
        # this exact route reached FastAPI rather than the edge's deny-all location.
        assert response.status_code == 404 and "x-request-id" in response.headers
        for path in ("/health/live", "/health/ready", "/docs", "/openapi.json", "/"):
            denied = client.get(path)
            assert denied.status_code == 404 and "x-request-id" not in denied.headers
        assert client.delete("/wecom/callback").status_code == 403
        assert client.post("/wecom/callback", content=b"x" * 65537).status_code == 413
    print("HTTPS certificate verified; callback-only routing, methods and size limits passed.")


if __name__ == "__main__":
    main()
