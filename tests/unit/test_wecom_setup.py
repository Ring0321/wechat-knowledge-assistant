import asyncio
import io
import json
import logging
import os
import stat
import time
from pathlib import Path

import httpx
import pytest
from pydantic import JsonValue, SecretStr

from app.connectors.wecom import setup
from app.connectors.wecom.contracts import APIError, SyncPage
from app.connectors.wecom.setup import Challenge, SetupError, SetupSettings
from app.connectors.wecom.setup_api import CustomerAccount, HttpWeComSetupAPI
from app.core.config import Settings


class Tokens:
    def __init__(self) -> None:
        self.invalidated: list[str] = []

    async def get_token(self) -> str:
        return "private-token"

    async def invalidate(self, token: str) -> None:
        self.invalidated.append(token)


def configuration(*, accounts: frozenset[str] = frozenset({"wk-one"})) -> SetupSettings:
    return SetupSettings(
        wecom_corp_id=SecretStr("private-corp"),
        wecom_secret=SecretStr("private-secret"),
        redis_url=SecretStr("redis://localhost/0"),
        wecom_open_kfids=accounts,
    )


def challenge() -> Challenge:
    return Challenge("微信验证 " + "a" * 32, time.time(), time.monotonic() + 600)


def message(code: Challenge, **overrides: JsonValue) -> dict[str, JsonValue]:
    return {
        "msgid": "msg-one",
        "open_kfid": "wk-one",
        "external_userid": "private-user",
        "origin": 3,
        "msgtype": "text",
        "send_time": int(time.time()),
        "text": {"content": code.text},
    } | overrides


class API:
    def __init__(self, pages: list[SyncPage] | None = None) -> None:
        self.pages = pages or []
        self.calls: list[tuple[str, str | None]] = []
        self.accounts = [CustomerAccount("wk-one", "private-name", True)]
        self.link_calls = 0

    async def list_accounts(self, *, offset: int, limit: int = 100) -> list[CustomerAccount]:
        return self.accounts[offset : offset + limit]

    async def entry_link(self, *, open_kfid: str, scene: str | None = None) -> str:
        self.link_calls += 1
        return "https://work.weixin.qq.com/kf/example?enc_scene=private-query"

    async def sync_msg(
        self, *, open_kfid: str, cursor: str | None, token: str | None = None
    ) -> SyncPage:
        assert token is None
        self.calls.append((open_kfid, cursor))
        return self.pages.pop(0)


async def test_official_account_schema_payload_and_no_unknown_fields() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/cgi-bin/kf/account/list"
        assert request.url.params["access_token"] == "private-token"
        assert json.loads(request.content) == {"offset": 100, "limit": 1}
        return httpx.Response(
            200,
            json={
                "errcode": 0,
                "account_list": [
                    {
                        "open_kfid": "wk",
                        "name": "name",
                        "manage_privilege": True,
                        "avatar": "secret",
                    }
                ],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        rows = await HttpWeComSetupAPI(client, Tokens()).list_accounts(offset=100, limit=1)
    assert rows[0].as_json() == {"open_kfid": "wk", "name": "name", "manage_privilege": True}
    assert "wk" not in repr(rows[0])


@pytest.mark.parametrize(
    "offset,limit", [(-1, 1), (2**32, 1), (True, 1), (0, 0), (0, 101), (0, True)]
)
async def test_account_request_limits(offset: int, limit: int) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: pytest.fail("network"))
    ) as http:
        with pytest.raises(APIError, match="wecom_invalid_request"):
            await HttpWeComSetupAPI(http, Tokens()).list_accounts(offset=offset, limit=limit)


@pytest.mark.parametrize(
    "row",
    [
        None,
        {},
        {"open_kfid": "wk", "name": "n", "manage_privilege": 1},
        {"open_kfid": "wk", "name": "n", "manage_privilege": None},
        {"open_kfid": "", "name": "n"},
        {"open_kfid": "wk", "name": 7},
    ],
)
async def test_account_malformed_response(row: JsonValue) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"errcode": 0, "account_list": [row]})
        )
    ) as http:
        with pytest.raises(APIError, match="wecom_invalid_response"):
            await HttpWeComSetupAPI(http, Tokens()).list_accounts(offset=0)


async def test_component_omitted_privilege_is_unknown_not_authorized() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(
                200, json={"errcode": 0, "account_list": [{"open_kfid": "wk", "name": "n"}]}
            )
        )
    ) as http:
        assert (await HttpWeComSetupAPI(http, Tokens()).list_accounts(offset=0))[
            0
        ].manage_privilege is None


@pytest.mark.parametrize("scene", [None, "", "a" * 32, "test-1_2"])
async def test_entry_link_official_payload_query_preserved(scene: str | None) -> None:
    url = "https://work.weixin.qq.com/kf/example?enc_scene=secret%2Bvalue"

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/cgi-bin/kf/add_contact_way"
        assert json.loads(request.content) == (
            {"open_kfid": "wk"} | ({} if scene is None else {"scene": scene})
        )
        return httpx.Response(200, json={"errcode": 0, "url": url})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        assert (
            await HttpWeComSetupAPI(http, Tokens()).entry_link(open_kfid="wk", scene=scene) == url
        )


@pytest.mark.parametrize("scene", ["a" * 33, "中文", "x?query=secret", "a b", "x\n"])
async def test_scene_limits(scene: str) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: pytest.fail("network"))
    ) as http:
        with pytest.raises(APIError, match="wecom_invalid_request"):
            await HttpWeComSetupAPI(http, Tokens()).entry_link(open_kfid="wk", scene=scene)


@pytest.mark.parametrize(
    "url",
    [
        None,
        "http://work.weixin.qq.com/kf/a",
        "https://evil.example/kf/a",
        "https://work.weixin.qq.com@evil.example/kf/a",
        "https://work.weixin.qq.com/kf/a#x",
        "https://work.weixin.qq.com/",
        "https://work.weixin.qq.com/kf/a\n",
    ],
)
async def test_entry_link_rejects_nonofficial_url(url: JsonValue) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"errcode": 0, "url": url})
        )
    ) as http:
        with pytest.raises(APIError, match="wecom_invalid_response"):
            await HttpWeComSetupAPI(http, Tokens()).entry_link(open_kfid="wk")


async def test_adapter_reuses_token_refresh_and_redacts_error() -> None:
    calls = 0
    tokens = Tokens()

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"errcode": 42001, "errmsg": "private-body-token-user"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        with pytest.raises(APIError, match="^wecom_42001$"):
            await HttpWeComSetupAPI(http, tokens).list_accounts(offset=0)
    assert calls == 2
    assert tokens.invalidated == ["private-token"]


@pytest.mark.parametrize("privilege", [False, None])
async def test_configured_account_requires_manage_privilege(privilege: bool | None) -> None:
    api = API()
    api.accounts = [CustomerAccount("wk-one", "name", privilege)]
    with pytest.raises(SetupError, match="setup_account_management_required"):
        await setup.execute(api, configuration(), "entry-link")
    assert api.link_calls == 0
    assert api.calls == []


@pytest.mark.parametrize(
    "configured,index",
    [
        (frozenset(), None),
        (frozenset({"a", "b"}), None),
        (frozenset({"a"}), -1),
        (frozenset({"a"}), 1),
        (frozenset({"a"}), True),
    ],
)
def test_select_account_requires_explicit_configured_choice(
    configured: frozenset[str], index: int | None
) -> None:
    with pytest.raises(SetupError, match="setup_select_configured_account"):
        setup.select_account(configured, index)


def test_sorted_account_selection_and_no_raw_id_argument() -> None:
    assert setup.select_account(frozenset({"b", "a"}), 1) == "b"
    with pytest.raises(SetupError):
        setup._arguments(["identify-user", "--open-kfid", "private", "--output", "x.json"])


async def test_bootstrap_accounts_work_without_customer_account_config() -> None:
    result = await setup.execute(API(), configuration(accounts=frozenset()), "accounts")
    assert result["accounts"] == [
        {"open_kfid": "wk-one", "name": "private-name", "manage_privilege": True}
    ]


def test_setup_configuration_does_not_weaken_runtime_allowlist_or_require_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key in ("DATABASE_URL", "REDIS_URL", "WECOM_OPEN_KFIDS", "WECOM_ALLOWED_USER_IDS"):
        monkeypatch.delenv(key, raising=False)
    config = configuration(accounts=frozenset())
    assert "private" not in repr(config)
    assert config.wecom_open_kfids == frozenset()
    runtime = Settings(
        database_url="postgresql+asyncpg://test:test@localhost/test", redis_url="redis://localhost"
    )
    assert not runtime.allows_wecom_user("private-user")
    assert "wecom_allowed_user_ids" not in SetupSettings.model_fields


async def test_account_pagination_and_duplicate_failure() -> None:
    api = API()
    api.accounts = [CustomerAccount(str(n), "n", True) for n in range(101)]
    assert len(await setup.discover_accounts(api)) == 101
    api.accounts[-1] = api.accounts[0]
    with pytest.raises(SetupError, match="setup_account_page_inconsistent"):
        await setup.discover_accounts(api)


async def test_account_page_limit_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setup, "_ACCOUNT_PAGES", 1)
    api = API()
    api.accounts = [CustomerAccount(str(n), "n", True) for n in range(100)]
    with pytest.raises(SetupError, match="setup_account_page_limit"):
        await setup.discover_accounts(api)


async def test_identify_independent_cursor_and_duplicate_messages_same_identity() -> None:
    code = challenge()
    api = API([SyncPage("next", True, [message(code)]), SyncPage("end", False, [message(code)])])
    assert await setup.identify_user(api, "wk-one", code) == "private-user"
    assert api.calls == [("wk-one", None), ("wk-one", "next")]


@pytest.mark.parametrize(
    "override",
    [
        {"origin": 1},
        {"origin": True},
        {"open_kfid": "another"},
        {"msgtype": "voice"},
        {"send_time": 0},
        {"send_time": 2**40},
        {"send_time": True},
        {"text": {"content": "old-code"}},
        {"text": None},
        {"external_userid": ""},
        {"external_userid": None},
        {"msgid": None},
    ],
)
async def test_identify_rejects_untrusted_stale_future_nontext_or_wrong_account(
    override: dict[str, JsonValue],
) -> None:
    code = challenge()
    api = API([SyncPage("end", False, [message(code, **override)])])
    with pytest.raises(SetupError, match="setup_challenge_not_found"):
        await setup.identify_user(api, "wk-one", code)


async def test_identify_ambiguous_across_pages_fails_not_first_match() -> None:
    code = challenge()
    api = API(
        [
            SyncPage("next", True, [message(code)]),
            SyncPage("end", False, [message(code, external_userid="another-user")]),
        ]
    )
    with pytest.raises(SetupError, match="setup_challenge_ambiguous"):
        await setup.identify_user(api, "wk-one", code)


async def test_identify_empty_page_with_more_continues() -> None:
    code = challenge()
    api = API([SyncPage("next", True, []), SyncPage("end", False, [message(code)])])
    assert await setup.identify_user(api, "wk-one", code) == "private-user"


async def test_identify_expired_challenge_no_network() -> None:
    code = challenge()
    api = API()
    with pytest.raises(SetupError, match="setup_challenge_expired"):
        await setup.identify_user(
            api, "wk-one", Challenge(code.text, code.issued_at, time.monotonic() - 1)
        )
    assert not api.calls


async def test_identify_does_not_reuse_old_challenge() -> None:
    old, fresh = Challenge.create(), Challenge.create()
    assert old.text != fresh.text
    assert len(fresh.text.rsplit(" ", 1)[1]) == 32
    api = API([SyncPage("end", False, [message(old)])])
    with pytest.raises(SetupError, match="setup_challenge_not_found"):
        await setup.identify_user(api, "wk-one", fresh)


async def test_identify_page_limit_does_not_return_partial_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(setup, "_MAX_PAGES", 1)
    code = challenge()
    api = API([SyncPage("next", True, [message(code)])])
    with pytest.raises(SetupError, match="setup_message_page_limit"):
        await setup.identify_user(api, "wk-one", code)


async def test_identify_cursor_loop_fails() -> None:
    api = API([SyncPage("same", True, []), SyncPage("same", True, [])])
    with pytest.raises(SetupError, match="setup_cursor_loop"):
        await setup.identify_user(api, "wk-one", challenge())


async def test_identify_timeout_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    class SlowAPI(API):
        async def sync_msg(
            self, *, open_kfid: str, cursor: str | None, token: str | None = None
        ) -> SyncPage:
            await asyncio.sleep(1)
            return SyncPage("end", False, [])

    monkeypatch.setattr(setup, "_SCAN_TIMEOUT", 0.01)
    with pytest.raises(SetupError, match="setup_scan_timeout"):
        await setup.identify_user(SlowAPI(), "wk-one", challenge())


async def test_execute_challenge_only_stdout_and_no_allowlist_update() -> None:
    api = API()
    printed: list[str] = []

    async def confirm(seconds: float) -> None:
        assert 0 < seconds <= 600
        code = Challenge(printed[1], time.time(), time.monotonic() + 600)
        api.pages = [SyncPage("end", False, [message(code)])]

    result = await setup.execute(
        api, configuration(), "identify-user", output=printed.append, confirm=confirm
    )
    assert result == {
        "open_kfid": "wk-one",
        "external_userid": "private-user",
        "allowlist_updated": False,
    }
    assert all("private" not in line and "wk-one" not in line for line in printed)
    assert len(printed) == 3


@pytest.mark.parametrize("answer", ["", "not-enter\n"])
async def test_enter_requires_real_operator_confirmation(
    monkeypatch: pytest.MonkeyPatch, answer: str
) -> None:
    monkeypatch.setattr(setup.sys, "stdin", io.StringIO(answer))
    with pytest.raises(SetupError, match="setup_operator_confirmation_required"):
        await setup.wait_for_enter(1)


async def test_enter_works_and_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setup.sys, "stdin", io.StringIO("\n"))
    await setup.wait_for_enter(1)
    with pytest.raises(SetupError, match="setup_challenge_expired"):
        await setup.wait_for_enter(0)


def test_private_output_exclusive_and_permissions(tmp_path: Path) -> None:
    target = tmp_path / "result.json"
    with setup.private_output(target) as handle:
        handle.write('{"private":true}')
    assert json.loads(target.read_text(encoding="utf-8")) == {"private": True}
    if os.name == "posix":
        assert stat.S_IMODE(target.stat().st_mode) == 0o600
    with pytest.raises(SetupError, match="setup_output_unavailable"):
        with setup.private_output(target):
            pytest.fail("must not overwrite")
    assert target.read_text(encoding="utf-8") == '{"private":true}'


@pytest.mark.parametrize("path", ["relative.json", "../result.json"])
def test_private_output_requires_absolute_path(path: str) -> None:
    with pytest.raises(SetupError, match="setup_output_requires_absolute_path"):
        with setup.private_output(Path(path)):
            pytest.fail("invalid")


def test_output_rejects_alternate_stream_and_nonjson(tmp_path: Path) -> None:
    with pytest.raises(SetupError, match="setup_output_invalid_path"):
        with setup.private_output(tmp_path / "base.json:stream.json"):
            pytest.fail("invalid")
    with pytest.raises(SetupError, match="setup_output_requires_json"):
        with setup.private_output(tmp_path / "secret.txt"):
            pytest.fail("invalid")


def test_private_output_symlink_forbidden(tmp_path: Path) -> None:
    real = tmp_path / "real.json"
    real.write_text("do-not-change", encoding="utf-8")
    link = tmp_path / "link.json"
    try:
        link.symlink_to(real)
    except OSError:
        pytest.skip("Windows symlink creation privilege unavailable; covered on Linux")
    with pytest.raises(SetupError):
        with setup.private_output(link):
            pytest.fail("invalid")
    assert real.read_text(encoding="utf-8") == "do-not-change"


def test_private_output_parent_symlink_forbidden(tmp_path: Path) -> None:
    target = tmp_path / "real"
    target.mkdir()
    link = tmp_path / "link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("Windows symlink creation privilege unavailable; covered on Linux")
    with pytest.raises(SetupError):
        with setup.private_output(link / "new.json"):
            pytest.fail("invalid")
    assert not (target / "new.json").exists()


def test_main_errors_redacted_and_logs_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(setup, "SetupSettings", configuration)

    async def failed(*args: object) -> dict[str, JsonValue]:
        logging.getLogger("httpx").error("private-token private-user ?secret=query")
        raise RuntimeError("private-token private-user ?secret=query")

    monkeypatch.setattr(setup, "_run", failed)
    previous = logging.root.manager.disable
    assert setup.main(["accounts", "--output", str(tmp_path / "result.json")]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "setup_failed\n"
    assert "private" not in caplog.text
    assert logging.root.manager.disable == previous


def test_main_success_result_private_and_no_credential_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(setup, "SetupSettings", configuration)

    async def result(*args: object) -> dict[str, JsonValue]:
        return {
            "external_userid": "private-user",
            "url": "https://work.weixin.qq.com/kf/a?secret=query",
        }

    monkeypatch.setattr(setup, "_run", result)
    path = tmp_path / "output.json"
    assert setup.main(["accounts", "--output", str(path)]) == 0
    assert capsys.readouterr().out == "setup_result_saved_privately\n"
    assert json.loads(path.read_text(encoding="utf-8"))["external_userid"] == "private-user"
    assert setup.main(["accounts", "--secret", "private-secret", "--output", str(path)]) == 2
    assert capsys.readouterr().err == "setup_invalid_arguments\n"


def test_invalid_scene_before_network_from_adapter() -> None:
    assert "scene" not in setup._arguments(["accounts", "--output", "result.json"])


def test_no_envfile_and_secret_repr() -> None:
    assert SetupSettings.model_config["env_file"] is None
    assert "private" not in repr(configuration())
    assert "wecom_allowed_user_ids" not in setup.__dict__
    assert "private" not in repr(CustomerAccount("private-kfid", "private-name", True))


def test_invalid_config_messages_do_not_echo_secret() -> None:
    with pytest.raises(ValueError) as error:
        SetupSettings(
            wecom_corp_id=SecretStr("private-corp"),
            wecom_secret=SecretStr(""),
            redis_url=SecretStr("private-invalid"),
        )
    assert "private-invalid" not in str(error.value)
    assert "private-corp" not in str(error.value)


async def test_discovered_unconfigured_account_cannot_be_used() -> None:
    api = API()
    with pytest.raises(SetupError, match="setup_account_management_required"):
        await setup.execute(api, configuration(accounts=frozenset({"unknown"})), "entry-link")
    assert api.link_calls == 0


async def test_corrupt_challenge_rejected() -> None:
    api = API()
    with pytest.raises(SetupError, match="setup_invalid_challenge"):
        await setup.identify_user(
            api, "wk-one", Challenge("old-code", time.time(), time.monotonic() + 600)
        )
    assert not api.calls


async def test_matching_message_without_msgid_rejected() -> None:
    code = challenge()
    raw = message(code)
    del raw["msgid"]
    with pytest.raises(SetupError, match="setup_challenge_not_found"):
        await setup.identify_user(API([SyncPage("end", False, [raw])]), "wk-one", code)


async def test_empty_account_list_is_valid() -> None:
    api = API()
    api.accounts = []
    assert await setup.discover_accounts(api) == []


def test_main_existing_output_never_calls_api(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "exists.json"
    path.write_text("keep", encoding="utf-8")
    monkeypatch.setattr(setup, "SetupSettings", configuration)
    monkeypatch.setattr(setup, "_run", lambda *args: pytest.fail("network"))
    assert setup.main(["accounts", "--output", str(path)]) == 2
    assert path.read_text(encoding="utf-8") == "keep"
    assert capsys.readouterr().err == "setup_output_unavailable\n"
