"""Official admin setup contracts, separate from the running message adapter interface."""

import re
from dataclasses import dataclass, field
from typing import Protocol, cast
from urllib.parse import urlsplit

from pydantic import JsonValue

from app.connectors.wecom.api import HttpWeComAPI, _identifier
from app.connectors.wecom.contracts import APIError, SyncPage


@dataclass(frozen=True)
class CustomerAccount:
    open_kfid: str = field(repr=False)
    name: str = field(repr=False)
    manage_privilege: bool | None

    def as_json(self) -> dict[str, JsonValue]:
        return {
            "open_kfid": self.open_kfid,
            "name": self.name,
            "manage_privilege": self.manage_privilege,
        }


class SetupAPI(Protocol):
    async def list_accounts(self, *, offset: int, limit: int = 100) -> list[CustomerAccount]: ...

    async def entry_link(self, *, open_kfid: str, scene: str | None = None) -> str: ...

    async def sync_msg(
        self, *, open_kfid: str, cursor: str | None, token: str | None = None
    ) -> SyncPage: ...


class HttpWeComSetupAPI(HttpWeComAPI):
    """Reuse bounded HTTP, token refresh and safe errors; no account mutations."""

    async def list_accounts(self, *, offset: int, limit: int = 100) -> list[CustomerAccount]:
        if type(offset) is not int or not 0 <= offset <= 2**32 - 1:
            raise APIError("wecom_invalid_request")
        if type(limit) is not int or not 1 <= limit <= 100:
            raise APIError("wecom_invalid_request")
        raw = await self._request("account/list", {"offset": offset, "limit": limit}, sending=False)
        rows = raw.get("account_list")
        if not isinstance(rows, list) or len(rows) > limit:
            raise APIError("wecom_invalid_response")
        result: list[CustomerAccount] = []
        for row in rows:
            if not isinstance(row, dict):
                raise APIError("wecom_invalid_response")
            kfid, name, privilege = (
                row.get("open_kfid"),
                row.get("name"),
                row.get("manage_privilege"),
            )
            if (
                not isinstance(kfid, str)
                or not isinstance(name, str)
                or ("manage_privilege" in row and type(privilege) is not bool)
            ):
                raise APIError("wecom_invalid_response")
            try:
                _identifier(kfid, maximum=128)
                _identifier(name, maximum=1024)
            except APIError:
                raise APIError("wecom_invalid_response") from None
            result.append(CustomerAccount(kfid, name, cast(bool | None, privilege)))
        return result

    async def entry_link(self, *, open_kfid: str, scene: str | None = None) -> str:
        _identifier(open_kfid, maximum=128)
        payload: dict[str, JsonValue] = {"open_kfid": open_kfid}
        if scene is not None:
            if not re.fullmatch(r"[0-9a-zA-Z_-]{0,32}", scene):
                raise APIError("wecom_invalid_request")
            payload["scene"] = scene
        raw = await self._request("add_contact_way", payload, sending=True)
        value = raw.get("url")
        if not isinstance(value, str) or len(value) > 8192 or any(ord(c) < 33 for c in value):
            raise APIError("wecom_invalid_response", uncertain=True)
        try:
            url = urlsplit(value)
            valid = (
                url.scheme == "https"
                and url.netloc == "work.weixin.qq.com"
                and url.path.startswith("/kf/")
                and len(url.path) > 4
                and not url.fragment
                and "\\" not in value
            )
        except ValueError:
            valid = False
        if not valid:
            raise APIError("wecom_invalid_response", uncertain=True)
        # Keep the exact official query; do not reconstruct or append scene parameters.
        return value
