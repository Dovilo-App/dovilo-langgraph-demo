# Dovilo Agentic Demo

Minimal LangGraph workflow that listens for [Dovilo](https://dovilo.app)
webhooks and drives a Claude agent to process tasks, reporting progress
through the [Dovilo MCP server](https://dovilo.app/ai/) running locally
(`dovilo-mcp.exe`).

```
Dovilo Desktop ──HTTP webhook──▶ FastAPI :8000/webhook
       ▲
       └── stdio MCP ── this app spawns dovilo-mcp.exe and exposes its
                        tools (get_task, list_tasks, update_status,
                        update_step, append_note, ...) to Claude.
```

The orchestrator only relays. The agent owns the lifecycle: claim (`update_status='running'`), work (`append_note`, `update_step`), finalize (`update_status='done' | 'failed' | 'cancelled'`).

Single-file implementation in [main.py](main.py).

## Requirements

- **Python 3.10+** and an [Anthropic API key](https://console.anthropic.com/).
- **[Dovilo Desktop](https://github.com/slhddn/dovilo-desktop-releases/releases/latest)**
  — it ships the `dovilo-mcp` binary this demo spawns, so there is nothing
  separate to install. Desktop is part of Dovilo Pro, which starts with a
  [7-day free trial](https://dovilo.app/downloads/).

## Setup

1. **Install dependencies**
   ```powershell
   pip install -r requirements.txt
   ```

2. **Configure** — copy `.env.example` to `.env`, set `ANTHROPIC_API_KEY` and
   `DOVILO_WEBHOOK_SECRET` (generate one in Dovilo: Project editor → Webhooks
   → Generate signing secret — plaintext is shown only once; the payload and
   signature spec is at
   [dovilo.app/docs/webhooks](https://dovilo.app/docs/webhooks/)). Confirm
   that [.mcp.json](.mcp.json) points at your local `dovilo-mcp.exe`.

3. **Run** — start the server, then in Dovilo set the webhook URL to
   `http://localhost:8000/webhook` and subscribe to `task.created` and
   `task.status_changed`.
   ```powershell
   python main.py
   ```

## How it works

- **Startup**: spawns `dovilo-mcp.exe` as a stdio subprocess, fetches the
  MCP tool list, then does a one-shot sync — calls `list_tasks` (or the
  tool named by `DOVILO_LIST_TASKS_TOOL`) and queues every `queued` task
  for the agent.
- **Webhooks** are HMAC-verified (`X-Dovilo-Signature`, 5-minute replay
  window) and deduped by `X-Dovilo-Delivery` before any processing. Invalid
  signatures get `401`; duplicate deliveries are acknowledged without
  re-running.
- **`task.created`** → queue the agent for that task (`202 Accepted`,
  processing happens in the background).
- **`task.status_changed`** → log + update the in-memory status map.
  Includes the agent's own `update_status` calls round-tripping back.
- **Agent loop**: Claude runs a tool_use loop with the MCP tools as its
  toolset. It calls `get_task` if it needs the full task body (webhook
  payloads are slim by design), `append_note` for progress, and
  `update_status` to finalize. The host process never sets status itself.

`GET /health` reports tracked tasks, available MCP tools, whether signature
verification is active, and the dedup buffer size.

## Notes

- If `DOVILO_WEBHOOK_SECRET` is missing, signature verification is **off**
  and a loud warning is logged at startup. Fine for local development,
  not for anything else.
- Status values follow Dovilo's lifecycle: `queued → running →
  awaiting-user → running → done | failed | cancelled`.

---

[Dovilo](https://dovilo.app) · [AI & MCP integration](https://dovilo.app/ai/) ·
[Webhook docs](https://dovilo.app/docs/webhooks/) ·
[Desktop releases](https://github.com/slhddn/dovilo-desktop-releases)
