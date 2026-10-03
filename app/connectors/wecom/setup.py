"""Explicit operator-only first-use CLI; no server routes, DB access or allowlist writes.

Run with environment credentials and a new private output file. Output contains account
identifiers or a personal identifier: use an administrator-owned private directory. On
Windows the directory ACL supplies privacy; POSIX files are created with mode 0600.
"""

import argparse
import asyncio
import json
import logging
import os
import queue
import secrets
import stat
import sys
import threading
import time
from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Literal, Never
from urllib.parse import urlsplit

import httpx
from pydantic import Field, JsonValue, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio import Redis

from app.connectors.wecom.api import _identifier
from app.connectors.wecom.contracts import APIError
from app.connectors.wecom.normalization import is_setup_challenge_text
from app.connectors.wecom.setup_api import CustomerAccount, HttpWeComSetupAPI, SetupAPI
from app.connectors.wecom.tokens import RedisTokenProvider

_TTL = 600
_SCAN_TIMEOUT = 60
_MAX_PAGES = 100
_ACCOUNT_PAGES = 10


class SetupError(Exception):
    """Fixed local code only; never include exceptions or operator input."""


class SetupSettings(BaseSettings):
    """Bootstrap requires fewer settings; it does not modify runtime Settings."""

    model_config = SettingsConfigDict(
        env_prefix="", extra="ignore", env_file=None, hide_input_in_errors=True
    )
    wecom_corp_id: SecretStr = Field(repr=False)
    wecom_secret: SecretStr = Field(repr=False)
    redis_url: SecretStr = Field(repr=False)
    wecom_open_kfids: frozenset[str] = Field(default=frozenset(), repr=False)
    wecom_http_timeout_seconds: float = Field(default=8, gt=0, le=30)

    @field_validator("wecom_corp_id", "wecom_secret")
    @classmethod
    def required_secret(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if not raw.strip() or len(raw) > 4096 or any(ord(c) < 32 for c in raw):
            raise ValueError("invalid setup credential")
        return value

    @field_validator("redis_url")
    @classmethod
    def redis_address(cls, value: SecretStr) -> SecretStr:
        try:
            url = urlsplit(value.get_secret_value())
            valid = url.scheme in {"redis", "rediss"} and bool(url.hostname)
        except ValueError:
            valid = False
        if not valid:
            raise ValueError("invalid setup Redis address")
        return value

    @field_validator("wecom_open_kfids")
    @classmethod
    def account_ids(cls, value: frozenset[str]) -> frozenset[str]:
        for item in value:
            try:
                _identifier(item, maximum=128)
            except APIError:
                raise ValueError("invalid configured account") from None
            if item != item.strip():
                raise ValueError("invalid configured account")
        return value


@dataclass(frozen=True)
class Challenge:
    text: str = field(repr=False)
    issued_at: float
    deadline: float

    @classmethod
    def create(cls) -> "Challenge":
        return cls("微信验证 " + secrets.token_hex(16), time.time(), time.monotonic() + _TTL)


def select_account(configured: frozenset[str], index: int | None) -> str:
    accounts = sorted(configured)
    if not accounts or (index is None and len(accounts) != 1):
        raise SetupError("setup_select_configured_account")
    selected = 0 if index is None else index
    if type(selected) is not int or not 0 <= selected < len(accounts):
        raise SetupError("setup_select_configured_account")
    return accounts[selected]


async def discover_accounts(api: SetupAPI) -> list[CustomerAccount]:
    accounts: list[CustomerAccount] = []
    seen: set[str] = set()
    try:
        async with asyncio.timeout(_SCAN_TIMEOUT):
            for page_number in range(_ACCOUNT_PAGES):
                page = await api.list_accounts(offset=page_number * 100, limit=100)
                for account in page:
                    if account.open_kfid in seen:
                        raise SetupError("setup_account_page_inconsistent")
                    seen.add(account.open_kfid)
                    accounts.append(account)
                if len(page) < 100:
                    return accounts
    except TimeoutError:
        raise SetupError("setup_scan_timeout") from None
    raise SetupError("setup_account_page_limit")


async def managed_account(api: SetupAPI, configured: frozenset[str], index: int | None) -> str:
    selected = select_account(configured, index)
    accounts = await discover_accounts(api)
    if not any(item.open_kfid == selected and item.manage_privilege is True for item in accounts):
        raise SetupError("setup_account_management_required")
    return selected


def _match(message: dict[str, JsonValue], account: str, challenge: Challenge) -> str | None:
    sent = message.get("send_time")
    body = message.get("text")
    if (
        type(message.get("origin")) is not int
        or message.get("origin") != 3
        or message.get("open_kfid") != account
        or message.get("msgtype") != "text"
        or type(sent) is not int
        or sent < int(challenge.issued_at)
        or sent > min(challenge.issued_at + _TTL, time.time() + 60)
        or not isinstance(body, dict)
        or body.get("content") != challenge.text
    ):
        return None
    user, msgid = message.get("external_userid"), message.get("msgid")
    if not isinstance(user, str) or not isinstance(msgid, str):
        return None
    try:
        _identifier(user)
        _identifier(msgid)
    except APIError:
        return None
    return user


async def identify_user(api: SetupAPI, account: str, challenge: Challenge) -> str:
    """Scan one account independently. Never read/write the worker's cursor or messages."""
    if not is_setup_challenge_text(challenge.text):
        raise SetupError("setup_invalid_challenge")
    budget = min(_SCAN_TIMEOUT, challenge.deadline - time.monotonic())
    if budget <= 0 or time.time() > challenge.issued_at + _TTL:
        raise SetupError("setup_challenge_expired")
    candidates: set[str] = set()
    cursor: str | None = None
    seen_cursors: set[str] = set()
    try:
        async with asyncio.timeout(budget):
            for _ in range(_MAX_PAGES):
                page = await api.sync_msg(open_kfid=account, cursor=cursor)
                if (
                    time.monotonic() >= challenge.deadline
                    or time.time() > challenge.issued_at + _TTL
                ):
                    raise SetupError("setup_challenge_expired")
                for message in page.messages:
                    user = _match(message, account, challenge)
                    if user is not None:
                        candidates.add(user)
                    if len(candidates) > 1:
                        raise SetupError("setup_challenge_ambiguous")
                if not page.has_more:
                    if not candidates:
                        raise SetupError("setup_challenge_not_found")
                    return next(iter(candidates))
                if not page.next_cursor or page.next_cursor in seen_cursors:
                    raise SetupError("setup_cursor_loop")
                seen_cursors.add(page.next_cursor)
                cursor = page.next_cursor
    except TimeoutError:
        raise SetupError("setup_scan_timeout") from None
    raise SetupError("setup_message_page_limit")


async def wait_for_enter(seconds: float) -> None:
    """Daemon input thread avoids executor shutdown hanging after the deadline."""
    answers: queue.Queue[str | None] = queue.Queue(maxsize=1)

    def read() -> None:
        try:
            answers.put(sys.stdin.readline(2))
        except (OSError, ValueError):
            answers.put(None)

    threading.Thread(target=read, daemon=True).start()
    deadline = time.monotonic() + max(0, seconds)
    while time.monotonic() < deadline:
        try:
            answer = answers.get_nowait()
        except queue.Empty:
            await asyncio.sleep(min(0.1, max(0, deadline - time.monotonic())))
            continue
        if answer not in {"\n", "\r\n"}:
            raise SetupError("setup_operator_confirmation_required")
        return
    raise SetupError("setup_challenge_expired")


async def execute(
    api: SetupAPI,
    settings: SetupSettings,
    command: Literal["accounts", "entry-link", "identify-user"],
    *,
    account_index: int | None = None,
    scene: str | None = None,
    output: Callable[[str], None] = print,
    confirm: Callable[[float], Awaitable[None]] = wait_for_enter,
) -> dict[str, JsonValue]:
    if command == "accounts":
        return {"accounts": [account.as_json() for account in await discover_accounts(api)]}
    account = await managed_account(api, settings.wecom_open_kfids, account_index)
    if command == "entry-link":
        url = await api.entry_link(open_kfid=account, scene=scene)
        return {"open_kfid": account, "url": url}
    if command != "identify-user":
        raise SetupError("setup_invalid_command")
    challenge = Challenge.create()
    output("Send the following exact text in the selected WeChat customer-service chat.")
    output(challenge.text)
    output("After sending it, press Enter here within 10 minutes.")
    await confirm(challenge.deadline - time.monotonic())
    user = await identify_user(api, account, challenge)
    # This is an enrollment candidate, never an automatic authorization change.
    return {"open_kfid": account, "external_userid": user, "allowlist_updated": False}


@contextmanager
def private_output(path: Path) -> Iterator[IO[str]]:
    """No-clobber, regular output; POSIX traversal uses directory FDs to prevent races."""
    if not path.is_absolute() or path.name in {"", ".", ".."} or ".." in path.parts:
        raise SetupError("setup_output_requires_absolute_path")
    if any(":" in part or any(ord(c) < 32 for c in part) for part in path.parts[1:]):
        # A Windows alternate data stream is not a new independent private file.
        raise SetupError("setup_output_invalid_path")
    if os.name == "nt" and (
        path.anchor.startswith("\\\\") or any(part.endswith((" ", ".")) for part in path.parts[1:])
    ):
        raise SetupError("setup_output_invalid_path")
    if path.suffix.lower() != ".json":
        raise SetupError("setup_output_requires_json")
    fd: int | None = None
    directory: int | None = None
    try:
        if os.name == "posix":
            no_follow = getattr(os, "O_NOFOLLOW", None)
            directory_flag = getattr(os, "O_DIRECTORY", None)
            if not isinstance(no_follow, int) or not isinstance(directory_flag, int):
                raise SetupError("setup_output_platform_unsupported")
            directory = os.open(path.anchor, os.O_RDONLY | directory_flag | no_follow)
            for part in path.parts[1:-1]:
                next_dir = os.open(part, os.O_RDONLY | directory_flag | no_follow, dir_fd=directory)
                os.close(directory)
                directory = next_dir
            fd = os.open(
                path.name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | no_follow,
                0o600,
                dir_fd=directory,
            )
        elif sys.platform == "win32":
            # Windows privacy is inherited from the operator-owned directory ACL.
            for parent in (*reversed(path.parents), path):
                try:
                    info = parent.lstat()
                except FileNotFoundError:
                    if parent == path:
                        break
                    raise
                if stat.S_ISLNK(info.st_mode) or (
                    getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT
                ):
                    raise SetupError("setup_output_symlink_forbidden")
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_BINARY, 0o600)
        else:
            raise SetupError("setup_output_platform_unsupported")
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise SetupError("setup_output_requires_regular_file")
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            fd = None
            yield handle
            handle.flush()
            os.fsync(handle.fileno())
    except OSError:
        raise SetupError("setup_output_unavailable") from None
    finally:
        if fd is not None:
            os.close(fd)
        if directory is not None:
            os.close(directory)


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        # argparse normally echoes unknown arguments, which could contain secrets.
        raise SetupError("setup_invalid_arguments")


def _arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = _Parser(description="Operator-only WeChat setup; credentials from environment only.")
    commands = parser.add_subparsers(dest="command", required=True, parser_class=_Parser)
    for name in ("accounts", "entry-link", "identify-user"):
        command = commands.add_parser(name)
        command.add_argument(
            "--output", required=True, type=Path, help="new absolute private JSON path"
        )
        if name != "accounts":
            command.add_argument("--account-index", type=int, help="index of sorted configured IDs")
        if name == "entry-link":
            command.add_argument("--scene", default=None)
    return parser.parse_args(argv)


async def _run(arguments: argparse.Namespace, settings: SetupSettings) -> dict[str, JsonValue]:
    async with (
        Redis.from_url(
            settings.redis_url.get_secret_value(),
            socket_connect_timeout=3,
            socket_timeout=3,
            max_connections=2,
        ) as redis,
        httpx.AsyncClient(trust_env=False, follow_redirects=False) as http,
    ):
        tokens = RedisTokenProvider(
            redis,
            http,
            settings.wecom_corp_id.get_secret_value(),
            settings.wecom_secret.get_secret_value(),
        )
        api = HttpWeComSetupAPI(http, tokens, timeout_seconds=settings.wecom_http_timeout_seconds)
        return await execute(
            api,
            settings,
            arguments.command,
            account_index=getattr(arguments, "account_index", None),
            scene=getattr(arguments, "scene", None),
        )


def main(argv: Sequence[str] | None = None) -> int:
    previous_disable = logging.root.manager.disable
    # HTTP/Redis libraries may otherwise expose credential-bearing URLs at DEBUG/INFO.
    logging.disable(sys.maxsize)
    try:
        arguments = _arguments(argv)
        settings = SetupSettings()
        with private_output(arguments.output) as handle:
            result = asyncio.run(_run(arguments, settings))
            json.dump(result, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        print("setup_result_saved_privately")
        return 0
    except SetupError as error:
        print(str(error), file=sys.stderr)
        return 2
    except (Exception, KeyboardInterrupt):
        # No raw API response, identity, URL, configuration or exception traceback.
        print("setup_failed", file=sys.stderr)
        return 2
    finally:
        logging.disable(previous_disable)


if __name__ == "__main__":
    raise SystemExit(main())
