"""Provider-neutral model turns; identity never comes from a model response."""

from dataclasses import dataclass, field
from typing import Literal, Protocol

from pydantic import JsonValue

from app.domain.knowledge import SourceView


@dataclass(frozen=True)
class ToolCall:
    call_id: str
    name: str
    arguments: dict[str, JsonValue] = field(repr=False)


@dataclass(frozen=True)
class ModelTurn:
    items: tuple[dict[str, JsonValue], ...] = field(repr=False)
    calls: tuple[ToolCall, ...] = ()
    text: str | None = field(default=None, repr=False)
    refused: bool = False


class ResponsesProvider(Protocol):
    async def respond(
        self,
        *,
        instructions: str,
        input_items: tuple[dict[str, JsonValue], ...],
        tools: tuple[dict[str, JsonValue], ...],
        output_schema: dict[str, JsonValue],
        tool_choice: Literal["auto", "none"] = "auto",
    ) -> ModelTurn: ...


@dataclass(frozen=True)
class Evidence:
    evidence_id: str
    source: SourceView
    text: str = field(repr=False)
    locators: tuple[dict[str, JsonValue], ...] = ()
