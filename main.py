"""
Dovilo webhooks <-> agentic LangGraph workflow.

Architecture:
  Dovilo ──signed HTTPS webhook──▶ FastAPI :8000/webhook   (incoming)
     ▲
     └── Agents MCP ── https://dovilo.app/mcp/agents over Streamable HTTP,
                       authenticated with the agent's key. Each agent run
                       opens a connection and exposes its tool schemas to
                       Claude as `tools`. Claude calls them itself via
                       tool_use.

The orchestrator's only job is to receive (verified) webhooks and wake the
agent when a task opens to it (status `queued`). The agent owns the task
lifecycle: it reads the task, claims it, writes steps and notes, and
finalizes it with update_status. The host process does not intervene.

Webhook security follows the Dovilo spec: HMAC-SHA256 over `{t}.{rawBody}`,
5-minute replay window, deduplication via X-Dovilo-Delivery.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from collections import deque
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any, Optional, TypedDict

import httpx2
import uvicorn
from anthropic import AsyncAnthropic
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from langgraph.graph import END, StateGraph
from mcp import Client, MCPError
from mcp.client.streamable_http import streamable_http_client
from mcp.types import CallToolResult

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("dovilo-demo")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
# The agent's key: created in Dovilo (AI Agents > New agent), shown once.
DOVILO_AGENT_KEY = os.environ.get("DOVILO_AGENT_KEY")
DOVILO_MCP_URL = os.environ.get("DOVILO_MCP_URL", "https://dovilo.app/mcp/agents")
# Signing secret of the project's webhook, from the Dovilo UI. If unset,
# every webhook is rejected: the receiver is meant to be reachable publicly.
DOVILO_WEBHOOK_SECRET = os.environ.get("DOVILO_WEBHOOK_SECRET")

MODEL = "claude-sonnet-5-5"
MAX_AGENT_TURNS = 25
REPLAY_WINDOW_MS = 5 * 60 * 1000  # 5 minutes, per Dovilo spec
DEDUP_CAP = 1000

if not ANTHROPIC_API_KEY:
    raise RuntimeError("ANTHROPIC_API_KEY is required (set it in .env)")
if not DOVILO_AGENT_KEY:
    raise RuntimeError("DOVILO_AGENT_KEY is required (set it in .env)")
if not DOVILO_WEBHOOK_SECRET:
    log.warning(
        "DOVILO_WEBHOOK_SECRET is not set - every webhook will be rejected "
        "with 401. Queued tasks are still picked up at startup."
    )

anthropic = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)


# ---------------------------------------------------------------------------
# Dovilo Agents MCP client
# ---------------------------------------------------------------------------
class DoviloAuthError(RuntimeError):
    """The Agents MCP rejected the key. The message is Dovilo's, meant for the user as-is."""


class DoviloMCP:
    """One connection to the Dovilo Agents MCP.

    The endpoint is stateless Streamable HTTP: every request is a POST answered
    with JSON, and there is no SSE stream to hold open. So nothing is shared
    between agent runs; each run opens its own connection.

    A missing, wrong or revoked key gets HTTP 401 with a JSON body whose
    `message` tells the user what to do. The MCP SDK only reports a generic
    error, so a response hook keeps that message and it is raised as
    DoviloAuthError.
    """

    def __init__(self) -> None:
        self._stack = AsyncExitStack()
        self._client: Optional[Client] = None
        self._rejected: Optional[str] = None

    async def __aenter__(self) -> DoviloMCP:
        try:
            http = await self._stack.enter_async_context(httpx2.AsyncClient(
                headers={"Authorization": f"Bearer {DOVILO_AGENT_KEY}"},
                event_hooks={"response": [self._keep_401_message]},
                timeout=30.0,
            ))
            transport = streamable_http_client(DOVILO_MCP_URL, http_client=http)
            # The endpoint speaks the initialize handshake, not 2026 discovery.
            self._client = await self._stack.enter_async_context(Client(transport, mode="legacy"))
        except BaseException as exc:  # incl. cancellation: close what was opened
            await self._stack.aclose()
            self._raise_if_rejected(exc)
            raise
        return self

    async def __aexit__(self, *exc_info: Any) -> Optional[bool]:
        return await self._stack.__aexit__(*exc_info)

    async def _keep_401_message(self, response: httpx2.Response) -> None:
        if response.status_code != 401:
            return
        await response.aread()
        try:
            self._rejected = response.json()["message"]
        except (ValueError, KeyError, TypeError):
            self._rejected = f"HTTP 401 from {DOVILO_MCP_URL}"

    def _raise_if_rejected(self, exc: BaseException) -> None:
        if self._rejected:
            raise DoviloAuthError(self._rejected) from exc

    @property
    def instructions(self) -> str:
        """The Dovilo agent protocol, as the server sends it on initialize."""
        assert self._client is not None, "MCP connection not open"
        return self._client.instructions or ""

    async def tool_schemas(self) -> list[dict[str, Any]]:
        """The server's tools in Anthropic `tools` format."""
        assert self._client is not None, "MCP connection not open"
        try:
            listed = await self._client.list_tools()
        except Exception as exc:
            self._raise_if_rejected(exc)
            raise
        return [
            {"name": t.name, "description": t.description or "", "input_schema": t.input_schema}
            for t in listed.tools
        ]

    async def call_tool(self, name: str, args: dict[str, Any]) -> CallToolResult:
        """Call one tool. A Dovilo domain error raises MCPError with `data.domainCode`."""
        assert self._client is not None, "MCP connection not open"
        try:
            return await self._client.call_tool(name, args)
        except Exception as exc:
            self._raise_if_rejected(exc)
            raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _fmt_ms(ms: Optional[int]) -> str:
    if not ms:
        return "—"
    return time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ms / 1000))


def _result_text(result: CallToolResult) -> str:
    text = "\n".join(getattr(c, "text", str(c)) for c in result.content)
    if not text and result.structured_content is not None:
        text = json.dumps(result.structured_content)
    return text or "(no content)"


# ---------------------------------------------------------------------------
# The contract
# ---------------------------------------------------------------------------
# Framing for this host. The protocol itself (read, claim, steps, notes,
# finish) comes from the server's `instructions`, so it stays in step with
# Dovilo instead of being copied here.
_SYSTEM_PROMPT = (
    "You are an agent working one Dovilo AI task. Dovilo is a task app: the "
    "user writes a task in a project, and an agent like you works it through "
    "the Dovilo MCP tools. You own the task's lifecycle; the host process "
    "will not intervene.\n\n"
    "The user message names your task and the slim fields its webhook "
    "carried. Skip step 0 of the Dovilo agent protocol below and start at "
    "step 1 with that task id. Read before you claim, and do not claim a "
    "task you will not work.\n\n"
    "Rules: be concise. Your only tools are Dovilo's, so don't claim to have "
    "browsed, run code or checked sources. If the task needs that, do what "
    "you can from what you know, say plainly in a note what you could not "
    "do, and finish with the status that fits. Never leave the task in "
    "`running`."
)


def _build_user_message(task: dict[str, Any], project_name: Optional[str]) -> str:
    """Render the slim webhook task into a readable prompt."""
    lines = [
        f"Task ID: {task.get('id')}",
        f"Title: {task.get('text', '')}",
        f"Current status: {task.get('status', '')}",
    ]
    if task.get("priority"):
        lines.append(f"Priority: {task['priority']}")
    if task.get("importance"):
        lines.append("Starred: yes")
    if task.get("isRecurring"):
        lines.append(f"Repeats: {task.get('recurrenceType') or 'yes'}")
    if project_name:
        lines.append(f"Project: {project_name}")
    lines.append("")
    lines.append("Start with `read_context` for this task id. Begin.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LangGraph workflow (single node — the agent owns the flow)
# ---------------------------------------------------------------------------
class WorkflowState(TypedDict):
    task: dict[str, Any]
    project_name: Optional[str]


async def agent_node(state: WorkflowState) -> dict[str, Any]:
    """Run Claude in a tool_use loop over its own Agents MCP connection."""
    task = state["task"]
    async with DoviloMCP() as dovilo:
        # cache_control on the last tool and the last system block caches both.
        tools = await dovilo.tool_schemas()
        if tools:
            tools[-1] = {**tools[-1], "cache_control": {"type": "ephemeral"}}
        system: list[dict[str, Any]] = [{"type": "text", "text": _SYSTEM_PROMPT}]
        if dovilo.instructions:
            system.append({"type": "text", "text": dovilo.instructions})
        else:
            log.warning("[agent] %s sent no agent protocol; the prompt refers to one", DOVILO_MCP_URL)
        system[-1] = {**system[-1], "cache_control": {"type": "ephemeral"}}
        messages: list[dict[str, Any]] = [
            {"role": "user", "content": _build_user_message(task, state.get("project_name"))}
        ]

        for turn in range(MAX_AGENT_TURNS):
            resp = await anthropic.beta.messages.create(
                model=MODEL,
                # Thinking is on by default and counts toward max_tokens.
                max_tokens=16000,
                system=system,
                tools=tools,
                messages=messages,
                # For some refusal categories the API reruns the request on a
                # fallback model instead of stopping.
                betas=["server-side-fallback-2026-07-01"],
                fallbacks="default",
            )
            log.info(
                "[agent] turn %d stop=%s in=%d out=%d cache_r=%d cache_w=%d",
                turn + 1,
                resp.stop_reason,
                resp.usage.input_tokens,
                resp.usage.output_tokens,
                getattr(resp.usage, "cache_read_input_tokens", 0) or 0,
                getattr(resp.usage, "cache_creation_input_tokens", 0) or 0,
            )
            messages.append({"role": "assistant", "content": resp.content})

            if resp.stop_reason != "tool_use":
                if resp.stop_reason != "end_turn":
                    log.warning("[agent] stopped early (%s) on task %s", resp.stop_reason, task.get("id"))
                break

            tool_results: list[dict[str, Any]] = []
            for block in resp.content:
                if getattr(block, "type", None) != "tool_use":
                    continue
                log.info("[agent] → %s(%s)", block.name, json.dumps(block.input))
                try:
                    result = await dovilo.call_tool(block.name, block.input)
                    content, is_error = _result_text(result), result.is_error
                    if block.name == "update_status" and not is_error:
                        _remember_own_status(block.input)
                except DoviloAuthError:
                    raise
                except MCPError as exc:
                    # Dovilo's domain errors (TASK_ALREADY_CLAIMED,
                    # FORBIDDEN_OPERATION, ...) carry `data.domainCode`.
                    log.info("[agent] %s: %s %s", block.name, exc.message, exc.data)
                    content, is_error = json.dumps({"error": exc.message, "data": exc.data}), True
                except Exception as exc:  # noqa: BLE001 - surface to the model as a tool error
                    log.exception("[agent] tool %s raised", block.name)
                    content, is_error = f"Tool error: {exc}", True
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": content,
                    "is_error": is_error,
                })
            messages.append({"role": "user", "content": tool_results})
        else:
            log.warning("[agent] hit MAX_AGENT_TURNS=%d for task %s", MAX_AGENT_TURNS, task.get("id"))

    return {}


def _build_graph():
    g = StateGraph(WorkflowState)
    g.add_node("agent", agent_node)
    g.set_entry_point("agent")
    g.add_edge("agent", END)
    return g.compile()


GRAPH = _build_graph()

# In-memory mirror of the latest status, updated by inbound webhooks only.
TASK_STATE: dict[str, str] = {}
# The status an agent run last set, by task id. The webhook that echoes it
# back must not be read as the user's doing: an agent handing a task back
# (`queued`) is not a new task to pick up, and its own cancel is not a
# reason to stop its run.
_OWN_STATUS: dict[str, str] = {}


def _remember_own_status(args: dict[str, Any]) -> None:
    status = args.get("status")
    # `cancelled` is stored as `error` with `cancelledAt`.
    _OWN_STATUS[args.get("id", "")] = "error" if status == "cancelled" else status
# Tool names seen at startup, for /health.
MCP_TOOLS: list[str] = []


async def run_workflow(task: dict[str, Any], project_name: Optional[str]) -> None:
    """Wake the agent for one task. Crashes are logged; no recovery."""
    try:
        await GRAPH.ainvoke({"task": task, "project_name": project_name})
    except asyncio.CancelledError:
        log.info("[workflow] stopped for task %s", task.get("id"))
        raise
    except DoviloAuthError as exc:
        log.error("[workflow] Dovilo rejected the agent key: %s", exc)
    except Exception:
        log.exception("[workflow] crashed for task %s", task.get("id"))


# ---------------------------------------------------------------------------
# Agent runs: one per task, stopped when the task is cancelled
# ---------------------------------------------------------------------------
_RUNS: dict[str, asyncio.Task[None]] = {}
# Tasks that went back to `queued` while their previous run was still ending
# (e.g. the user answered the agent's question within seconds).
_RERUN: dict[str, tuple[dict[str, Any], Optional[str]]] = {}


def start_run(task: dict[str, Any], project_name: Optional[str], *, rerun_if_busy: bool) -> None:
    task_id = task["id"]
    if task_id in _RUNS:
        if rerun_if_busy:
            _RERUN[task_id] = (task, project_name)
        log.info("[runs] task %s already has a run in flight", task_id)
        return
    run = asyncio.create_task(run_workflow(task, project_name))
    _RUNS[task_id] = run
    run.add_done_callback(lambda _: _run_finished(task_id))


def _run_finished(task_id: str) -> None:
    _RUNS.pop(task_id, None)
    pending = _RERUN.pop(task_id, None)
    if pending:
        start_run(*pending, rerun_if_busy=False)


def cancel_run(task_id: str) -> None:
    _RERUN.pop(task_id, None)
    run = _RUNS.get(task_id)
    if run is not None:
        log.info("[runs] stopping the agent run for task %s", task_id)
        run.cancel()


# ---------------------------------------------------------------------------
# Startup sync: check the key and fetch queued tasks once at boot
# ---------------------------------------------------------------------------
async def _bootstrap_sync() -> None:
    """Connect once: fail fast on a bad key, then queue each queued task for the agent."""
    async with DoviloMCP() as dovilo:
        MCP_TOOLS[:] = [t["name"] for t in await dovilo.tool_schemas()]
        log.info("[dovilo-mcp] %s - tools: %s", DOVILO_MCP_URL, MCP_TOOLS)
        result = await dovilo.call_tool("list_tasks", {"status": "queued"})

    if result.is_error:
        log.warning("[bootstrap] list_tasks returned an error: %s", _result_text(result)[:300])
        return
    data = result.structured_content
    if data is None:
        data = json.loads(_result_text(result))
    tasks = data.get("tasks") or []
    projects = {p["id"]: p.get("name") for p in data.get("scopedProjects") or []}
    log.info("[bootstrap] %d queued task(s) in %d project(s)", len(tasks), len(projects))

    for task in tasks:
        TASK_STATE[task["id"]] = task.get("status") or "queued"
        start_run(task, projects.get(task.get("listId")), rerun_if_busy=False)


# ---------------------------------------------------------------------------
# Webhook security: HMAC-SHA256 over `{t}.{rawBody}` + replay window
# ---------------------------------------------------------------------------
def verify_signature(sig_header: str, raw_body: bytes) -> bool:
    """Validate X-Dovilo-Signature. Without a secret nothing can be verified, so nothing passes."""
    if not DOVILO_WEBHOOK_SECRET:
        return False

    parts: dict[str, str] = {}
    for piece in sig_header.split(","):
        if "=" not in piece:
            continue
        k, _, v = piece.partition("=")
        parts[k.strip()] = v.strip()

    try:
        t = int(parts["t"])
        v1 = parts["v1"]
    except (KeyError, ValueError):
        return False

    now_ms = int(time.time() * 1000)
    if abs(now_ms - t) > REPLAY_WINDOW_MS:
        return False

    mac = hmac.new(
        DOVILO_WEBHOOK_SECRET.encode("utf-8"),
        f"{t}.".encode("utf-8") + raw_body,
        hashlib.sha256,
    )
    return hmac.compare_digest(mac.hexdigest(), v1)


# ---------------------------------------------------------------------------
# Idempotency: dedup by X-Dovilo-Delivery (retries reuse the same id)
# ---------------------------------------------------------------------------
_seen_deliveries: deque[str] = deque(maxlen=DEDUP_CAP)
_seen_set: set[str] = set()


def remember_delivery(delivery_id: str) -> bool:
    """Return True if this delivery is new; False if it's a duplicate."""
    if delivery_id in _seen_set:
        return False
    if len(_seen_deliveries) == DEDUP_CAP:
        evicted = _seen_deliveries[0]
        _seen_set.discard(evicted)
    _seen_deliveries.append(delivery_id)
    _seen_set.add(delivery_id)
    return True


# ---------------------------------------------------------------------------
# FastAPI webhook listener
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        await _bootstrap_sync()
    except DoviloAuthError as exc:
        # A configuration error: stop with Dovilo's message, not the exception chain.
        log.error("[dovilo-mcp] %s rejected the agent key: %s", DOVILO_MCP_URL, exc)
        raise SystemExit(1) from None
    except Exception:
        # Anything else (network, server) should not take the receiver down.
        log.exception("[bootstrap] startup sync failed; webhooks are still served")
    yield


app = FastAPI(title="Dovilo LangGraph Demo", lifespan=lifespan)


@app.post("/webhook")
async def webhook(req: Request):
    raw_body = await req.body()

    # 1) Verify signature on the raw bytes BEFORE parsing JSON. A bad
    #    signature gets 401, never 410 (410 disables the webhook).
    if not verify_signature(req.headers.get("x-dovilo-signature", ""), raw_body):
        log.warning(
            "[webhook] rejected: invalid signature (delivery=%s)",
            req.headers.get("x-dovilo-delivery"),
        )
        return Response(status_code=401, content="invalid signature")

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        return Response(status_code=400, content="invalid json")

    # Prefer the headers (cheap, no body parse needed for routing).
    event_type = req.headers.get("x-dovilo-event") or payload.get("type")
    delivery_id = req.headers.get("x-dovilo-delivery") or payload.get("id") or ""

    # 2) Idempotency: same delivery id across retries — process exactly once.
    if delivery_id and not remember_delivery(delivery_id):
        log.info("[webhook] duplicate delivery %s skipped", delivery_id)
        return {"ok": True, "duplicate": True}

    if event_type == "webhook.ping":
        log.info("[webhook] ping project=%s evt=%s", payload.get("projectId"), delivery_id)
        return {"ok": True, "pong": True}

    if event_type not in ("task.created", "task.status_changed"):
        log.warning("[webhook] unknown event: %s", event_type)
        return {"ok": False, "reason": f"unknown event: {event_type}"}

    task = payload.get("task") or {}
    task_id = task.get("id")
    if not task_id:
        return {"ok": False, "reason": "missing task.id"}
    status = task.get("status")
    previous = payload.get("previousStatus")
    project_name = payload.get("projectName")
    TASK_STATE[task_id] = status or "unknown"

    if event_type == "task.created":
        log.info(
            "[webhook] task.created id=%s status=%s text=%r priority=%s",
            task_id, status, task.get("text"), task.get("priority"),
        )
        if status == "queued":
            # Process async: keep the webhook response fast.
            start_run(task, project_name, rerun_if_busy=False)
            return JSONResponse(status_code=202, content={"ok": True, "queued": task_id})
        if status == "scheduled":
            # The agent cannot read a scheduled task; Dovilo queues it on time.
            log.info("[webhook] task %s opens to the agent at %s", task_id, _fmt_ms(task.get("scheduledAt")))
        return {"ok": True, "task_id": task_id, "status": status}

    own = _OWN_STATUS.get(task_id) == status
    if own:
        del _OWN_STATUS[task_id]
    log.info("[webhook] task.status_changed id=%s %s -> %s%s", task_id, previous, status, " (agent)" if own else "")

    # A task opens to the agent when it becomes `queued`: from `scheduled` when
    # its time comes, from `awaiting-user` when the user answers or approves
    # the plan, after Retry or Release in the app. Not when the agent itself
    # handed it back: picking it straight up again could loop.
    if status == "queued" and not own:
        start_run(task, project_name, rerun_if_busy=True)
        return JSONResponse(status_code=202, content={"ok": True, "queued": task_id})

    # There is no `cancelled` status: a cancelled task is `error` + `cancelledAt`.
    if status == "error" and task.get("cancelledAt") and not own:
        log.info("[webhook] task %s was cancelled", task_id)
        cancel_run(task_id)

    return {"ok": True, "task_id": task_id, "status": status, "previous": previous}


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "mcp_url": DOVILO_MCP_URL,
        "mcp_tools": MCP_TOOLS,
        "tracked_tasks": TASK_STATE,
        "agent_runs": sorted(_RUNS),
        "signature_verification": bool(DOVILO_WEBHOOK_SECRET),
        "dedup_size": len(_seen_set),
    }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
