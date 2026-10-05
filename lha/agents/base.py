"""What every agent shares, which is its per-attempt context and the fake model.

The agent loop, the tool calling and the output validation are all real
Pydantic AI, and only the 'brain' is scripted. Each agent supplies a
`policy`, which is a plain function that looks at the ContextPacket and the
tool results so far and decides the next tool call or the final answer, the
same way an LLM would.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel

from lha.config import CONTEXT_WINDOW_TOKENS, MODEL_ERROR_RATE, OUTPUT_RESERVE_TOKENS
from lha.context import ContextOverflow, estimate_tokens
from lha.faults import roll
from lha.schemas.context import ContextPacket
from lha.tools import ToolBox


@dataclass
class AgentContext:
    """Everything one attempt of one task needs."""

    seed: int
    task_id: UUID
    task_key: str
    task_type: str
    attempt: int
    packet: ContextPacket
    tools: ToolBox
    log: Callable[[str, dict[str, Any]], Awaitable[UUID]]

    @property
    def input(self) -> dict[str, Any]:
        return self.packet.pinned.input


@dataclass
class Call:
    """The model decides to call a tool."""

    tool: str
    args: dict[str, Any]


@dataclass
class Final:
    """The model gives its final, structured answer."""

    output: dict[str, Any]


# policy(packet, tool_results) -> next decision, where tool_results is the
# list of (tool name, returned data) so far in this attempt, in order.
Policy = Callable[[ContextPacket, list[tuple[str, Any]]], Call | Final]
# fabricate(output) -> an output that looks believable but is wrong, or None.
Fabricate = Callable[[dict[str, Any]], dict[str, Any] | None]


def tool_results(messages: list[ModelMessage]) -> list[tuple[str, Any]]:
    """(tool, result) pairs so far, where a fetched pointer counts as the tool it copies."""
    return [
        (_tool_of(part), part.content)
        for msg in messages
        if isinstance(msg, ModelRequest)
        for part in msg.parts
        if isinstance(part, ToolReturnPart)
    ]


def _tool_of(part: ToolReturnPart) -> str:
    if isinstance(part.content, dict) and "_of" in part.content:
        return part.content["_of"]
    return part.tool_name


def prompt_tokens(messages: list[ModelMessage]) -> int:
    """The estimated size of everything the model is shown in this call.

    It counts the content of every part (prompts, tool calls and tool
    results) with the same estimate the context builder uses, so the numbers
    for the system and for the naive baseline can be compared directly.
    """
    return parts_tokens([part for msg in messages for part in msg.parts])


def parts_tokens(parts: list[Any]) -> int:
    return sum(estimate_tokens(part_content(part)) for part in parts)


def part_content(part: Any) -> Any:
    if isinstance(part, ToolCallPart):
        return [part.tool_name, part.args]
    return getattr(part, "content", "")


def scripted_model(ctx: AgentContext, policy: Policy, fabricate: Fabricate) -> FunctionModel:
    """Wrap a policy as a Pydantic AI model, with seeded 'model errors'.

    A small, seeded share of final answers is broken on purpose, to show that
    bad output never reaches the shared state. A 'malformed' answer breaks
    the output schema, so Pydantic AI rejects it. A 'fabricated' answer keeps
    a valid schema but has the wrong content (a made-up number or host), so
    only the coordinator's source check can catch it.
    """

    async def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        tokens = prompt_tokens(messages)
        if tokens > CONTEXT_WINDOW_TOKENS - OUTPUT_RESERVE_TOKENS:
            # The attempt's own tool results have filled the window, so the
            # task is too big for one attempt and the coordinator splits it.
            raise ContextOverflow(f"the attempt needs {tokens} tokens")
        decision = policy(ctx.packet, tool_results(messages))
        corrupted = None
        if isinstance(decision, Final):
            output = decision.output
            r = roll(ctx.seed, ctx.task_key, ctx.attempt, "model")
            if r < MODEL_ERROR_RATE / 2:
                output, corrupted = _malformed(output), "malformed"
            elif r < MODEL_ERROR_RATE:
                fake = fabricate(output)
                output, corrupted = (fake, "fabricated") if fake else (_malformed(output), "malformed")
            part = ToolCallPart(info.output_tools[0].name, output)
            summary = {"final": True}
        else:
            part = ToolCallPart(decision.tool, decision.args)
            summary = {"tool": decision.tool, "args": decision.args}
        summary["prompt_tokens"] = tokens
        await ctx.log("model_call", {**summary, "corrupted": corrupted})
        return ModelResponse(parts=[part])

    return FunctionModel(respond)


def _malformed(output: dict[str, Any]) -> dict[str, Any]:
    """Drop the first field, so the output no longer matches its schema."""
    return dict(list(output.items())[1:])


async def run_agent(
    ctx: AgentContext,
    output_type: type[BaseModel],
    policy: Policy,
    fabricate: Fabricate,
    tools: list[Callable[..., Awaitable[Any]]] = (),
) -> BaseModel:
    """Run one attempt, which is the agent loop until it gives a final answer.

    The model only ever sees the ContextPacket (as the user prompt) and the
    results of its own tool calls in this attempt.
    """
    agent = Agent(
        scripted_model(ctx, policy, fabricate),
        output_type=output_type,
        retries=0,  # an invalid output fails the attempt, and the coordinator retries it
        tools=list(tools),
    )
    result = await agent.run(ctx.packet.model_dump_json())
    return result.output
