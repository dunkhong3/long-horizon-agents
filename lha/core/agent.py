"""What every agent shares, which is its per-attempt context and its model.

The agent loop, the tool calling and the output validation are all real
Pydantic AI. By default only the 'brain' is scripted, because each agent
supplies a `policy`, which is a plain function that reads the ContextPacket
back out of its prompt, looks at the tool results so far and decides the
next tool call or the final answer, the same way an LLM would. A real model
can take its place with --model, and every call of either kind goes through
the same wrapper, which checks it against the window and logs it as a step.
"""

import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    NativeToolReturnPart,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models import Model, ModelRequestParameters, infer_model
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.wrapper import WrapperModel
from pydantic_ai.settings import ModelSettings

from lha.config import CONTEXT_WINDOW_TOKENS, DEFAULT_REAL_MODEL, FALLBACK_PRICE_PER_MILLION, MODEL_ERROR_RATE
from lha.core.context import ContextOverflow, call_limit, estimate_tokens
from lha.core.schemas import ContextPacket
from lha.core.tools import ToolBox
from lha.faults import roll

FAKE = "fake"  # the deterministic scripted model
AUTO = "auto"  # a real model when a provider key is set, and the fake one otherwise


def resolve_model(choice: str, env: Mapping[str, str] = os.environ) -> str:
    """The model a session runs on, from --model.

    'auto' picks the default real model when ANTHROPIC_API_KEY is set and the
    fake one when it isn't, so switching to a real model is setting a key.
    Tests, CI and the benchmarks ask for 'fake' by name, so a key in the
    environment never costs them anything.
    """
    if choice != AUTO:
        return choice
    return DEFAULT_REAL_MODEL if env.get("ANTHROPIC_API_KEY") else FAKE


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
    model: str = FAKE  # 'fake', or any Pydantic AI model name (see --model)
    instructions: str = ""  # the task type's system prompt
    window: int = CONTEXT_WINDOW_TOKENS  # tokens one model call may use, answer included
    world: str = "mock"  # 'mock', or 'web' for the real web


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

    It counts the instructions and the content of every part (prompts, tool
    calls and tool results) with the same estimate the context builder uses,
    so the numbers for the system and for the naive baseline can be compared
    directly.
    """
    requests = [msg for msg in messages if isinstance(msg, ModelRequest) and msg.instructions]
    instructions = estimate_tokens(requests[-1].instructions) if requests else 0
    return instructions + parts_tokens([part for msg in messages for part in msg.parts])


def parts_tokens(parts: list[Any]) -> int:
    return sum(estimate_tokens(part_content(part)) for part in parts)


def part_content(part: Any) -> Any:
    if isinstance(part, ToolCallPart):
        return [part.tool_name, part.args]
    return getattr(part, "content", "")


class StepModel(WrapperModel):
    """Any model, the fake one or a real one, making the calls of one attempt.

    Before every call it checks the size of everything the model is shown
    against the window, and after it logs the call as a `model_call` event,
    which is a step. For the fake model only, a small, seeded share of final
    answers is broken on purpose, to show that bad output never reaches the
    shared state. A 'malformed' answer breaks the output schema, so Pydantic
    AI rejects it, and a 'fabricated' answer keeps a valid schema but has the
    wrong content (a made-up number or host), so only the coordinator's
    source check can catch it. A real model makes mistakes of its own.
    """

    def __init__(self, wrapped: Model, ctx: AgentContext, fabricate: Fabricate | None = None):
        super().__init__(wrapped)
        self.ctx = ctx
        self.fabricate = fabricate

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        tokens = prompt_tokens(messages)
        if tokens > call_limit(self.ctx.window):
            # The attempt's own tool results have filled the window, so the
            # task is too big for one attempt and the coordinator splits it.
            raise ContextOverflow(f"the attempt needs {tokens} tokens")
        response = await self.wrapped.request(messages, model_settings, model_request_parameters)
        # A real model may give its answer as JSON text rather than through
        # an output tool, as Claude does when it thinks before answering.
        _, params = self.wrapped.prepare_request(model_settings, model_request_parameters)
        text_answer = params.output_mode in ("native", "prompted") and any(
            isinstance(part, TextPart) and part.content.strip() for part in response.parts
        )

        output_tools = {t.name for t in model_request_parameters.output_tools}
        calls = [part for part in response.parts if isinstance(part, ToolCallPart)]
        final = next((part for part in calls if part.tool_name in output_tools), None)
        corrupted = None
        if final is not None and self.fabricate is not None:
            output, corrupted = self._corrupt(final.args_as_dict())
            parts = [replace(part, args=output) if part is final else part for part in response.parts]
            response = replace(response, parts=parts)
        if final is not None or (text_answer and not calls):
            summary: dict[str, Any] = {"final": True}
        elif calls:
            summary = {"tool": calls[0].tool_name, "args": calls[0].args_as_dict()}
            if len(calls) > 1:
                summary["also"] = [{"tool": c.tool_name, "args": c.args_as_dict()} for c in calls[1:]]
        else:
            summary = {"text": True}  # no tool call and no answer, which fails the attempt
        summary["prompt_tokens"] = tokens
        for part in response.parts:
            if isinstance(part, NativeToolReturnPart):
                # A tool the provider ran itself, such as its web search, is
                # logged like any tool call, so a result can cite what it found.
                await self.ctx.log("tool_call", native_call(part))
        if self.fabricate is None:  # a real model, whose calls cost money
            usage = response.usage
            summary["usage"] = {"input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens}
            summary["cost_usd"] = _cost(response)
        await self.ctx.log("model_call", {**summary, "corrupted": corrupted})
        return response

    def _corrupt(self, output: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
        r = roll(self.ctx.seed, self.ctx.task_key, self.ctx.attempt, "model")
        if r < MODEL_ERROR_RATE / 2:
            return _malformed(output), "malformed"
        if r < MODEL_ERROR_RATE:
            fake = self.fabricate(output)
            return (fake, "fabricated") if fake else (_malformed(output), "malformed")
        return output, None


def native_call(part: NativeToolReturnPart) -> dict[str, Any]:
    """A provider's own tool call as a tool_call event, where a search keeps the
    URL and title of every page it found, wherever the provider put them."""
    found: list[dict[str, str]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if isinstance(node.get("url"), str):
                found.append({"url": node["url"], "title": str(node.get("title") or "")})
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(part.content)
    failed = isinstance(part.content, dict) and "error_code" in part.content
    return {"tool": part.tool_name, "path": "native", "ok": not failed, "response": {"results": found}}


def scripted_model(policy: Policy) -> FunctionModel:
    """A policy as a Pydantic AI model, which reads nothing but its prompt.

    The ContextPacket is parsed back out of the user prompt, so the policy
    sees what a real model would be shown and nothing more, and its tool
    results come from the messages of this attempt.
    """

    async def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        decision = policy(packet_in(messages), tool_results(messages))
        if isinstance(decision, Final):
            part = ToolCallPart(info.output_tools[0].name, decision.output)
        else:
            part = ToolCallPart(decision.tool, decision.args)
        return ModelResponse(parts=[part])

    return FunctionModel(respond)


def packet_in(messages: list[ModelMessage]) -> ContextPacket:
    """The ContextPacket an attempt was given, which is its first user prompt."""
    for msg in messages:
        if isinstance(msg, ModelRequest):
            for part in msg.parts:
                if isinstance(part, UserPromptPart) and isinstance(part.content, str):
                    return ContextPacket.model_validate_json(part.content)
    raise ValueError("the prompt holds no ContextPacket")


def model_for(ctx: AgentContext, policy: Policy, fabricate: Fabricate) -> StepModel:
    """The fake model by default, or the real one the session asked for."""
    if ctx.model == FAKE:
        return StepModel(scripted_model(policy), ctx, fabricate)
    return StepModel(infer_model(ctx.model), ctx)


def _cost(response: ModelResponse) -> float:
    """What a real model call cost in US dollars, from Pydantic AI's price
    list, or a high estimate for a model the list doesn't know."""
    try:
        return float(response.cost().total_price)
    except Exception:
        per_input, per_output = FALLBACK_PRICE_PER_MILLION
        usage = response.usage
        return (usage.input_tokens * per_input + usage.output_tokens * per_output) / 1_000_000


def _malformed(output: dict[str, Any]) -> dict[str, Any]:
    """Drop the first field, so the output no longer matches its schema."""
    return dict(list(output.items())[1:])


async def run_agent(
    ctx: AgentContext,
    output_type: type[BaseModel],
    policy: Policy,
    fabricate: Fabricate,
    tools: list[Callable[..., Awaitable[Any]]] = (),
    capabilities: list[Any] = (),
) -> BaseModel:
    """Run one attempt, which is the agent loop until it gives a final answer.

    The model only ever sees the task type's instructions, the ContextPacket
    (as the user prompt) and the results of its own tool calls in this attempt.
    """
    agent = Agent(
        model_for(ctx, policy, fabricate),
        output_type=output_type,
        instructions=ctx.instructions or None,
        retries=0,  # an invalid output fails the attempt, and the coordinator retries it
        tools=list(tools),
        capabilities=list(capabilities) or None,
    )
    result = await agent.run(ctx.packet.model_dump_json())
    return result.output
