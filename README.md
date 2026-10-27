# Dovilo agentic demo

Minimal LangGraph workflow that receives [Dovilo](https://dovilo.app)
webhooks and drives a Claude agent to work AI tasks through the Dovilo
Agents MCP at `https://dovilo.app/mcp/agents`.

```
Dovilo ──signed HTTPS webhook──▶ FastAPI :8000/webhook
   ▲
   └── Agents MCP over HTTPS, agent key as a Bearer token. The agent reads
       the task, claims it, writes steps and notes, and sets the status
       with the tools it exposes (get_task, read_context, update_status,
       add_step, update_step, append_note, ...).
```

The orchestrator only relays. The agent owns the lifecycle: read
(`read_context`, `get_task`), claim (`update_status='running'`), work
(`add_step`, `update_step`, `append_note`), finish (`update_status='done'`,
`'error'`, `'awaiting-user'` or `'cancelled'`).

Single-file implementation in [main.py](main.py).

> The [walkthrough video](https://www.youtube.com/watch?v=id_7T8qUfpg) shows
> the Dovilo 1.8 setup, with a local MCP bridge on Desktop. It will be
> re-recorded for the setup below.

## What the agent can and cannot do

- It sees only the AI tasks of the projects its agent is assigned to. It
  never sees your personal tasks or other projects.
- It can read those tasks and update them: status, steps, notes and
  telemetry.
- It cannot create or delete tasks. There is no create tool and no delete
  tool.
- It can cancel a task it should not run, with a reason. The task stays in
  the list, shown as Cancelled. Cancelling is not deleting.

## Requirements

- **Python 3.10+** and an [Anthropic API key](https://console.anthropic.com/).
- **Dovilo Pro**: agents and webhooks are part of Pro, which starts with a
  [7-day free trial](https://dovilo.app/downloads/).

## Setup

1. **Install dependencies**
   ```powershell
   pip install -r requirements.txt
   ```

2. **Create an agent** in Dovilo: AI Agents > New agent (in Desktop,
   Connect a new AI). Assign the project whose tasks it should work and copy
   its key. The key is shown only once.

3. **Configure**: copy `.env.example` to `.env` and set `ANTHROPIC_API_KEY`
   and `DOVILO_AGENT_KEY` (the key from step 2). `.env` is in `.gitignore`;
   keep keys out of the repo.

4. **Run**
   ```powershell
   python main.py
   ```
   On startup the demo connects to the Agents MCP and works every task that
   is already queued. If the key is missing, wrong or revoked, it stops and
   prints Dovilo's message.

5. **Add a webhook** so new tasks reach the demo without a restart. Dovilo's
   server delivers to public HTTPS URLs, so expose port 8000 with a tunnel
   (for example cloudflared or ngrok) and use
   `https://<your-tunnel-host>/webhook`. Webhooks are set up in
   [Dovilo Desktop](https://github.com/Dovilo-App/dovilo-desktop-releases/releases/latest)
   2.0 or later: open the project's List settings, find Webhooks,
   paste the URL, tick `task.created` and `task.status_changed`, switch it to
   Enabled and save. Press Generate signing secret (shown once), put it in
   `.env` as `DOVILO_WEBHOOK_SECRET`, restart the demo and press Send test.
   Desktop does not need to stay open for a public URL. A
   `http://localhost:8000/webhook` URL also works, but then Desktop delivers
   the events and has to stay open. The payload and signature spec is at
   [dovilo.app/docs/webhooks](https://dovilo.app/docs/webhooks/).

## How it works

- **Startup**: connects to the Agents MCP, lists its tools, then calls
  `list_tasks` with `status: queued` and starts an agent run for each task.
- **Webhooks** are HMAC-verified (`X-Dovilo-Signature`, 5-minute replay
  window) and deduped by `X-Dovilo-Delivery` before any processing. Invalid
  signatures get `401`, never `410` (a `410` disables the webhook).
  Duplicate deliveries are acknowledged without re-running.
- **A task opens to the agent when it becomes `queued`**:
  - `task.created` with status `queued` starts a run (`202 Accepted`,
    processing happens in the background).
  - `task.status_changed` to `queued` starts a run: a scheduled task whose
    time has come (`scheduled` to `queued`), a question the user answered or
    a plan they approved (`awaiting-user` to `queued`), or a Retry or
    Release in the app. A task the agent itself handed back is not picked
    up again.
  - A task created for a later time arrives as `scheduled`. The demo logs
    it and waits: the agent cannot read a scheduled task until Dovilo
    queues it.
  - A cancelled task arrives as `error` with `cancelledAt`. If the user
    cancelled it while an agent run is working on it, the demo stops the
    run.
  - Other status changes, including the agent's own `update_status` calls
    round-tripping back, are logged.
- **Agent run**: opens its own connection to the Agents MCP (the endpoint
  is stateless) and runs Claude in a tool_use loop with the MCP tools as its
  toolset. The system prompt carries the agent protocol the server sends on
  connect: read the task first, claim it with `update_status`, write steps
  and notes, finish with a status. The agent reads before it claims, so it
  does not claim a task it will not work. The host process never sets
  status itself.

`GET /health` reports the MCP endpoint and its tools, tracked tasks, agent
runs in flight, whether signature verification is active, and the dedup
buffer size.

## Notes

- If `DOVILO_WEBHOOK_SECRET` is missing, every webhook is rejected with
  `401` and a warning is logged at startup. Queued tasks are still picked up
  when the demo starts.
- One receiver checks one signing secret, so it serves one project's
  webhook. If the agent works several projects, the startup sync covers all
  of them; for webhooks from each, run one receiver per project.
- `.env` and `.mcp.json` are no longer tracked. If you cloned an earlier
  version of this demo, move your `.env` aside before you pull, then copy
  `.env.example` to `.env` and add your agent key.
- Statuses: `scheduled`, `queued`, `running`, `awaiting-user`, `done`,
  `done-with-errors`, `error`. There is no `cancelled` status: a cancelled
  task is stored as `error` with `cancelledAt` and shown as Cancelled.
- A task counts against your monthly AI task quota when it opens to the
  agent, not when the agent claims it. Answering the agent's question costs
  no quota.
- `DOVILO_MCP_URL` overrides the endpoint. It defaults to
  `https://dovilo.app/mcp/agents`.

---

[Dovilo](https://dovilo.app) · [AI & MCP integration](https://dovilo.app/ai/) ·
[Webhook docs](https://dovilo.app/docs/webhooks/) ·
[Desktop releases](https://github.com/Dovilo-App/dovilo-desktop-releases)
