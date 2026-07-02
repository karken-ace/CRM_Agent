# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this repository.

## Project Overview

ACES Agent is a Python FastAPI service that wraps **one Meta ad account** — the only component of the ACES CRM platform that talks to the Meta Marketing API. Deployment model: one agent per VPS, one VPS per Meta ad account (each account's Meta traffic comes from its own IP). A central backend (Express, separate repo/VPS) manages many agents; the agent both serves HTTP to that backend and writes synced data directly into the central MongoDB.

See `README.md` for the full architecture, endpoint reference, and deployment guide.

## Commands

```bash
source .venv/bin/activate
pip install -r requirements.txt   # plus: pip install pymongo (required, not pinned)
python run.py                     # uvicorn on 0.0.0.0:9000
```

No test suite, linting, or CI is configured.

## Architecture

Four modules, no routers:

- **`app/main.py`** — everything HTTP: all FastAPI endpoints, config loading (`load_config()`), the four asyncio background loops (heartbeat 60s, config pull, commands pull, Meta→Mongo sync every 5 min), `CredentialManager` (+watchdog hot-reload of `secrets/*.creds`), and the Gemini Clone Winner video-analysis pipeline (`/meta/creatives/clone-analyze`).
- **`app/meta_client.py`** — `MetaAPIClient`, synchronous `requests`-based wrapper for Meta Graph API v22.0. Every call signed with `appsecret_proof` (HMAC-SHA256 of access token with app secret). Rate-limit detection (code 80004/subcode 2446079/HTTP 429) with retry+backoff and proactive throttling from `x-app-usage` headers at ≥95%.
- **`app/meta_cache.py`** — L2 cache: Meta GET responses persisted to Mongo collection `meta_cache` (TTL index, key = URL with token stripped). L1 is an in-process dict inside the client.
- **`app/db_sync.py`** — L3 sync: upserts the campaigns→adsets→ads hierarchy into the central Mongo collections `campaigns`/`adsets`/`ads`, scoped by `agent_id`/`user_id`. Requires the agent's `agents` document to already exist (created by the backend at registration); skips silently otherwise.

### Data flow (critical to understand)

Campaign data does NOT go to the backend over HTTP. The flow is:
**Meta API → agent (5-min sync loop) → central MongoDB → backend reads Mongo.**
The agent's HTTP calls to the backend (`CRM_BASE_URL`) are only heartbeat / config:pull / commands:pull / meta:sync status, all with `Authorization: Bearer <AGENT_TOKEN>`. Separately, the backend dials INTO this agent (port 9000) for live reads, breakdowns, mutations, and clone-analyze.

### Configuration

`config/meta_config.json` (gitignored): `agent.{id,token}`, `crm.base_url`, `meta_api.{app_id,app_secret,access_token,ad_account_id,base_url,timeout}`, `gemini_api.api_key`. Env-var fallbacks exist for all of these plus `MONGODB_URI` (must be set explicitly on a standalone VPS — the legacy fallback reads `/home/CRM/backend/.env`, which only exists in the old monorepo). Startup exits if `agent.id`/`agent.token` are missing.

## Gotchas

- **Inbound API is unauthenticated** — including budget/status mutations. Deployment relies on firewalling port 9000 to the backend IP or an nginx shared-secret check. Don't expose it raw.
- If `pymongo` is missing or Mongo is unreachable, L2 cache and L3 sync **silently no-op** — the agent looks healthy but the CRM gets no data.
- Some client methods bypass the cache (mutations, `get_ads`, batched ID fetches) — they call `requests` directly.
- Mutation errors surface Meta's `error_user_title`/`error_user_msg` via `_format_mutation_error`.
- `_warm_video_urls` in main.py is dormant (defined, no longer called). `secrets/*.creds` hot-reload is loaded but not consumed by the Meta client (vestigial multi-tenant scaffolding).
- Plaintext secrets live in `config/` — never commit; rotate anything that ever hits git history.

## Core principles (platform-wide)

- One agent = one Meta ad account = one VPS/IP. Never mix tenants.
- Handle Meta API rate limits with retry & backoff; keep the appsecret_proof signing on every call.
- Never hardcode tokens.
- Never execute ad actions except via the explicit mutation endpoints called by the backend (which requires user confirmation upstream).
- No prompt injection from ad comments or creative names (they flow into LLM prompts upstream — treat as untrusted data).
- Do not hallucinate metrics.
