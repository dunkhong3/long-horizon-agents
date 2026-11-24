"""Every model, fake or real, sees only its prompt and makes its calls through the same steps."""

from uuid import uuid4

import pytest
from pydantic import BaseModel
from pydantic_ai import Agent, NativeOutput
from pydantic_ai.messages import ModelResponse, TextPart
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.profiles import ModelProfile

from lha.config import DEFAULT_REAL_MODEL
from lha.core.agent import AgentContext, Final, StepModel, resolve_model, run_agent
from lha.core.context import ContextOverflow, estimate_tokens, pack
from lha.core.schemas import FactView, Pinned
from lha.domains import DOMAINS
from lha.run import model_problem


class Out(BaseModel):
    answer: int


def _context(model: str = "fake", instructions: str = "") -> tuple[AgentContext, list]:
    pinned = Pinned(goal="count", task_type="t", task_key="t:1", attempt=1, input={"n": 1})
    facts = [FactView(id="f1", subject="thing:1", key="count", value=3, status="observed")]
    events: list[tuple[str, dict]] = []

    async def log(kind: str, payload: dict):
        events.append((kind, payload))
        return uuid4()

    ctx = AgentContext(
        seed=1, task_id=uuid4(), task_key="t:1", task_type="t", attempt=1,
        packet=pack(pinned, facts, [], []), tools=None, log=log,
        model=model, instructions=instructions,
    )  # fmt: skip
    return ctx, events


async def test_the_fake_model_reads_its_packet_from_the_prompt():
    ctx, events = _context(instructions="Answer with the count.")
    seen = []

    def policy(packet, results):
        seen.append(packet)
        return Final({"answer": packet.facts[0].value})

    out = await run_agent(ctx, Out, policy, lambda output: None)
    assert out.answer == 3
    assert seen == [ctx.packet]  # parsed back out of the prompt, and equal to what was built
    kind, payload = events[0]
    assert kind == "model_call" and payload["final"] is True
    # The prompt is the instructions plus the packet, counted together.
    assert payload["prompt_tokens"] >= estimate_tokens(ctx.packet.model_dump_json()) + estimate_tokens(
        ctx.instructions
    )


async def test_a_real_model_makes_its_calls_through_the_same_steps():
    # Pydantic AI's test model stands in for a real one, so nothing leaves this machine.
    ctx, events = _context(model="test", instructions="Answer with the count.")
    out = await run_agent(ctx, Out, lambda packet, results: Final({"answer": -1}), lambda output: None)
    assert isinstance(out, Out)
    kind, payload = events[-1]
    assert kind == "model_call" and payload["final"] is True and payload["corrupted"] is None
    assert set(payload["usage"]) == {"input_tokens", "output_tokens"}


async def test_an_answer_given_as_json_text_is_logged_as_the_final_answer():
    # A real model that thinks first answers as JSON text, which Pydantic AI
    # calls native output, so that answer is the final one and not a stray text.
    ctx, events = _context(model="test")
    model = FunctionModel(
        lambda messages, info: ModelResponse(parts=[TextPart('{"answer": 3}')]),
        profile=ModelProfile(supports_json_schema_output=True),
    )
    agent = Agent(StepModel(model, ctx), output_type=NativeOutput(Out))
    assert isinstance((await agent.run("count")).output, Out)
    kind, payload = events[-1]
    assert kind == "model_call" and payload.get("final") is True and "text" not in payload


async def test_the_window_check_applies_to_every_model():
    for model in ("fake", "test"):
        ctx, events = _context(model=model, instructions="Answer with the count. " * 400)
        with pytest.raises(ContextOverflow):
            await run_agent(ctx, Out, lambda packet, results: Final({"answer": 1}), lambda output: None)
        assert events == []  # an overflowing call is never made, so it is not a step


def test_auto_picks_a_real_model_only_when_a_key_is_set():
    assert resolve_model("auto", {}) == "fake"
    assert resolve_model("auto", {"ANTHROPIC_API_KEY": "sk-test"}) == DEFAULT_REAL_MODEL
    assert resolve_model("fake", {"ANTHROPIC_API_KEY": "sk-test"}) == "fake"  # tests and CI ask by name
    assert resolve_model("openai:gpt-5", {}) == "openai:gpt-5"


def test_a_model_that_cannot_be_built_is_caught_before_the_run():
    assert model_problem("fake") is None
    assert model_problem("test") is None
    assert "no-such-provider" in model_problem("no-such-provider:some-model")


def test_no_output_has_a_field_a_strict_schema_would_empty():
    # Claude's native output uses a strict JSON schema, which turns a dict
    # field into {}, so an answer in that field could never be given.
    def free_form(node) -> bool:
        if isinstance(node, dict):
            extra = node.get("additionalProperties")
            return isinstance(extra, dict) or extra is True or any(map(free_form, node.values()))
        return isinstance(node, list) and any(map(free_form, node))

    for domain in DOMAINS.values():
        for name, task_type in domain.task_types.items():
            assert not free_form(task_type.output.model_json_schema()), (domain.name, name)
