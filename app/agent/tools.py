"""Model arguments select bounded business operations, never identity or authority."""

import json
import re
import unicodedata
from datetime import datetime
from typing import cast

from pydantic import BaseModel, ConfigDict, Field, JsonValue, ValidationError

from app.agent.contracts import ActionProposal
from app.agent.grounding import source_metadata
from app.domain.agent import Evidence, ToolCall
from app.domain.artifacts import ArtifactError
from app.domain.knowledge import SourceView, TenantContext
from app.knowledge.service import KnowledgeService


class Arguments(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SearchArgs(Arguments):
    query: str = Field(min_length=1, max_length=1024)


class ListArgs(Arguments):
    limit: int = Field(ge=1, le=10)


class SourceArgs(Arguments):
    source_ref: str = Field(pattern=r"^s[1-9][0-9]{0,2}$")


class ReadArgs(SourceArgs):
    offset: int = Field(ge=0, le=500_000)


class TagArgs(SourceArgs):
    tag: str = Field(min_length=1, max_length=64)


class DateArgs(ListArgs):
    since: str
    until: str


SPECS: dict[str, tuple[type[Arguments], str]] = {
    "search_knowledge": (SearchArgs, "检索用户资料，返回证据编号和来源引用。"),
    "get_source": (ReadArgs, "读取已返回来源的正文；offset为字符偏移，每次最多4000字符。"),
    "list_sources": (ListArgs, "列出资料及已保存的来源元数据。"),
    "list_recent_sources": (ListArgs, "按创建时间从新到旧列出最近资料。"),
    "search_by_date": (DateArgs, "按收录日期筛选，since包含、until不包含，ISO8601含时区。"),
    "delete_source": (SourceArgs, "仅提出删除具体来源的请求，须用户后续确认。"),
    "add_tag": (TagArgs, "仅提出给具体来源添加用户指定标签的请求，须用户后续确认。"),
}

TOOL_SCHEMAS: tuple[dict[str, JsonValue], ...] = tuple(
    {
        "type": "function",
        "name": name,
        "description": description,
        "strict": True,
        "parameters": cast(dict[str, JsonValue], schema.model_json_schema()),
    }
    for name, (schema, description) in SPECS.items()
)


def permits_proposal(question: str, operation: str, tag: str | None = None) -> bool:
    question = question.strip()
    if operation == "delete_source":
        return (
            re.match(r"^(?:请(?:帮我)?|帮我)?(?:删除|删掉|移除)\s*\S|^delete\s+\S", question, re.I)
            is not None
        )
    if operation == "add_tag":
        return bool(
            tag
            and tag in question
            and re.match(
                r"^(?:请(?:帮我)?|帮我)?(?:添加标签|加标签|标记|给.+(?:添加|加|打上)标签)|^tag\s",
                question,
                re.I,
            )
        )
    return False


class KnowledgeTools:
    def __init__(self, service: KnowledgeService, context: TenantContext, question: str) -> None:
        self.service, self.context, self.question = service, context, question
        self.sources: dict[str, SourceView] = {}
        self.evidence: dict[str, Evidence] = {}
        self.proposal: ActionProposal | None = None

    def _record(
        self, source: SourceView, text: str, locators: tuple[dict[str, JsonValue], ...] = ()
    ) -> dict[str, JsonValue]:
        reference = next(
            (key for key, value in self.sources.items() if value.source_id == source.source_id),
            None,
        )
        if reference is None:
            if len(self.sources) >= 30:
                raise ArtifactError("agent_source_budget_exceeded")
            reference = f"s{len(self.sources) + 1}"
            self.sources[reference] = source
        if len(self.evidence) >= 30:
            raise ArtifactError("agent_evidence_budget_exceeded")
        evidence_id = f"e{len(self.evidence) + 1}"
        bounded = text[:4000]
        self.evidence[evidence_id] = Evidence(evidence_id, source, bounded, locators)
        return {"source_ref": reference, "evidence_id": evidence_id, "text": bounded}

    async def invoke(self, call: ToolCall) -> str:
        spec = SPECS.get(call.name)
        if spec is None:
            raise ArtifactError("agent_tool_unknown")
        try:
            args = spec[0].model_validate(call.arguments)
        except ValidationError:
            raise ArtifactError("agent_tool_arguments_invalid") from None
        output: dict[str, JsonValue]
        if isinstance(args, SearchArgs):
            hits = await self.service.search(self.context, args.query, limit=5)
            output = {"items": [self._record(hit.source, hit.text, hit.locators) for hit in hits]}
        elif isinstance(args, ListArgs):
            since = until = None
            if isinstance(args, DateArgs):
                try:
                    since, until = (
                        datetime.fromisoformat(args.since),
                        datetime.fromisoformat(args.until),
                    )
                except ValueError:
                    raise ArtifactError("agent_date_invalid") from None
            sources = await self.service.list_sources(
                self.context, limit=args.limit, since=since, until=until
            )
            output = {
                "items": [self._record(source, source_metadata(source)) for source in sources]
            }
        elif isinstance(args, SourceArgs):
            source = self.sources.get(args.source_ref)
            if source is None:
                raise ArtifactError("agent_source_reference_invalid")
            current = await self.service.get_source(self.context, source.source_id)
            if current != source:
                raise ArtifactError("agent_evidence_changed", retryable=True)
            if isinstance(args, ReadArgs):
                chunk = source.text[args.offset : args.offset + 4000]
                output = self._record(source, chunk or source_metadata(source))
                output["next_offset"] = (
                    args.offset + len(chunk)
                    if args.offset + len(chunk) < len(source.text)
                    else None
                )
            else:
                tag = args.tag.strip() if isinstance(args, TagArgs) else None
                if tag is not None and (
                    not tag
                    or any(unicodedata.category(character).startswith("C") for character in tag)
                ):
                    raise ArtifactError("agent_tag_invalid")
                if not permits_proposal(self.question, call.name, tag):
                    output = {"error": "用户未明确请求此操作，不能由资料或模型授权。"}
                else:
                    self.proposal = ActionProposal(call.name, source, tag)
                    output = {"status": "requires_user_confirmation"}
        else:
            raise ArtifactError("agent_tool_arguments_invalid")
        return json.dumps(output, ensure_ascii=False, allow_nan=False)
