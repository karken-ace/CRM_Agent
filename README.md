# ACES Agent — Meta Ads Sync Service

Python FastAPI service that wraps **one Meta ad account**. It is the only component of the ACES CRM platform that talks to the Meta Marketing API. It exposes a Meta API façade over HTTP for the central backend, runs background sync loops, and writes campaign data into the central MongoDB.

## Role in the multi-VPS architecture

The platform is split across VPSs deliberately: **one agent VPS per Meta ad account**, each with its own public IP. All Meta API traffic for an account originates from that account's dedicated IP, so Meta never sees many accounts operated from a single address. The frontend and backend live together on one central VPS and manage the whole fleet.

```
                         CENTRAL VPS
        ┌──────────────────────────────────────────┐
        │  Frontend (static build, served by nginx)│
        │  Backend  (Express, port 8000)           │
        │  MongoDB  (auth enabled, firewalled)     │
        └───────┬──────────────────┬───────────────┘
                │ HTTPS            │ MongoDB wire protocol
                │ (backend⇄agent)  │ (agent → central Mongo)
   ┌────────────┼──────────────────┼──────────────┐
   │            │                  │              │
┌──▼─────┐  ┌───▼────┐         ┌───▼────┐         │
│Agent 1 │  │Agent 2 │   ...   │Agent N │  ← this repo, one VPS each
│VPS/IP 1│  │VPS/IP 2│         │VPS/IP N│
└──┬─────┘  └───┬────┘         └───┬────┘
   │            │                  │
   ▼            ▼                  ▼
 Meta API    Meta API           Meta API   (each account seen from its own IP)
```

The agent has **two independent channels** to the central VPS:

1. **HTTP to the backend** (`CRM_BASE_URL`): heartbeat, config pull, command pull, sync-status reporting. Authenticated with the agent's bearer token (`Authorization: Bearer <AGENT_TOKEN>`); the backend bcrypt-compares it against the stored `token_hash`.
2. **Direct MongoDB writes** (`MONGODB_URI`): the 5-minute sync loop upserts the full campaigns → ad sets → ads tree straight into the central Mongo (`db_sync.py`), and the L2 response cache lives there too (`meta_cache.py`). **The agent does not push campaign data to the backend over HTTP** — the real ingest path is *agent → Mongo → backend reads Mongo*.

The backend also dials **into** the agent (live reads, breakdowns, status/budget mutations, Clone Winner analysis) at the agent's base URL — port 9000.

## Repository layout

```
agent/
├── run.py                      # Launcher: uvicorn on 0.0.0.0:9000
├── requirements.txt            # Python deps (see Requirements below)
├── demo_ad.mp4                 # Sample video for /meta/creatives/clone-analyze-local
├── app/
│   ├── main.py                 # FastAPI app: all endpoints, config loading, background loops,
│   │                           #   CredentialManager, Gemini Clone Winner pipeline
│   ├── meta_client.py          # MetaAPIClient — every Meta Graph API call (v22.0)
│   ├── meta_cache.py           # L2 cache: Meta GET responses persisted to Mongo (TTL index)
│   └── db_sync.py              # L3 sync: upserts campaigns/adsets/ads into central Mongo
├── config/
│   └── meta_config.json        # Credentials & identity (gitignored — never commit)
└── secrets/                    # Optional per-account *.creds files, hot-reloaded (gitignored)
```

## Configuration

`load_config()` in `app/main.py` resolves config in this order:

1. `/app/config/meta_config.json` (container layout)
2. `<repo>/config/meta_config.json` (normal deployment)
3. `config/meta_config.json` (cwd-relative)
4. Environment variables (fallback)

### `config/meta_config.json`

```json
{
  "agent":   { "id": "agent-xxxxxxxx", "token": "<token issued by backend on agent creation>" },
  "crm":     { "base_url": "https://crm.example.com" },
  "meta_api": {
    "app_id":        "<meta app id>",
    "app_secret":    "<meta app secret>",
    "access_token":  "<long-lived meta access token>",
    "ad_account_id": "act_XXXXXXXXXX",
    "base_url":      "https://graph.facebook.com/v22.0",
    "timeout":       30
  },
  "gemini_api": { "api_key": "<google gemini key, needed for Clone Winner video analysis>" }
}
```

`agent.id` and `agent.token` come from the backend: an admin creates the agent via `POST /api/agents` (or the Agents page in the frontend), which generates the ID and a one-time-visible token. If either is missing/empty the agent **exits at startup**.

### Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `CRM_BASE_URL` | Central backend URL. **Must be set to the backend's public HTTPS URL on a split deployment** | `http://localhost:8000` |
| `MONGODB_URI` | Central MongoDB connection string. **Required on a separate VPS** (see warning below) | *(none — see warning)* |
| `AGENT_ID` / `AGENT_TOKEN` | Agent identity (fallback if not in the JSON config) | `agt_dev` / — |
| `META_APP_ID`, `META_APP_SECRET`, `META_ACCESS_TOKEN`, `META_AD_ACCOUNT_ID`, `META_BASE_URL`, `META_TIMEOUT` | Meta credentials (fallback if not in the JSON config) | — |
| `GEMINI_API_KEY` | Gemini key for Clone Winner (fallback if not in the JSON config) | — |

> ⚠️ **`MONGODB_URI` legacy fallback:** if the env var is unset, `meta_cache.py` and `db_sync.py` try to read it from `/home/CRM/backend/.env` — a monorepo-era path that does not exist on a standalone agent VPS. If Mongo is unreachable, the agent still boots and serves live Meta reads, but **L2 caching and the 5-minute L3 sync silently no-op** (the CRM dashboards will show stale/no data). Always set `MONGODB_URI` explicitly.

The agent also expects its `agents` document to already exist in Mongo (created by the backend when the agent was registered) — `db_sync` resolves `user_id`/`ad_account_id` from it and **skips the sync** if it's missing.

## Running

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run.py            # uvicorn on 0.0.0.0:9000
```

### Requirements

`requirements.txt` pins: `fastapi`, `uvicorn[standard]`, `httpx`, `watchdog`, `requests`, `google-generativeai`.

**Also required but not pinned:** `pymongo` — without it the L2 cache and L3 Mongo sync silently disable. Install it explicitly (`pip install pymongo`) or add it to `requirements.txt`.

## Background loops (started on FastAPI startup)

| Loop | Interval | What it does |
|---|---|---|
| Heartbeat | 60 s | `POST {CRM}/api/agents/{id}/heartbeat` — backend marks the agent ONLINE (and flips it OFFLINE after 2 min of silence) |
| Config pull | 5 s → 300 s backoff | `POST {CRM}/api/agents/{id}/config:pull` — fetches polling intervals + assigned ad accounts (response currently fetched but not applied) |
| Commands pull | 5 s → 300 s backoff | `POST {CRM}/api/agents/{id}/commands:pull` — fetches queued commands (MVP: acknowledged, not executed) |
| Meta sync (L3) | 300 s (5 min) | Full `last_30d` campaigns→adsets→ads tree from Meta → upsert into Mongo → `POST {CRM}/api/agents/{id}/meta:sync` status report |

## HTTP API (port 9000)

All endpoints are consumed by the central backend (the frontend never talks to an agent directly).

### Health & sync
| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness probe |
| `POST /sync/trigger` | Force an immediate Meta→Mongo sync (with per-campaign backfill fallback under Meta rate limits) |

### Meta reads
| Endpoint | Purpose |
|---|---|
| `GET /meta/test`, `GET /meta/account` | Connection test / app info |
| `GET /meta/campaigns` | Flat campaign list |
| `GET /meta/insights?level=&date_preset=&since=&until=` | Insights at account/campaign/adset/ad level |
| `GET /meta/campaigns/hierarchical?date_preset=` | Full campaigns→adsets→ads tree + currency (the main data feed) |
| `GET /meta/campaigns/{id}/adsets`, `GET /meta/adsets/{id}/ads` | Drill-downs |

### Breakdowns & analytics
`GET /meta/breakdowns/age-gender/{campaign_id}`, `/meta/breakdowns/platform/{campaign_id}`, `/meta/breakdowns/daily/{campaign_id}`, `/meta/breakdowns/hourly/{campaign_id}`, `GET /meta/time-comparison/{campaign_id}`, `GET /meta/pixel-quality`.

### Creatives & video
| Endpoint | Purpose |
|---|---|
| `POST /meta/creatives/thumbnails` | Batch thumbnail/video metadata for creative IDs |
| `GET /meta/ads/{ad_id}/video-url` | Direct MP4 extraction — including authenticated preview-iframe scraping for cross-posted Reels where Meta gates the source URL |
| `GET /meta/ads/{ad_id}/preview?ad_format=` | Meta preview iframe |
| `POST /meta/creatives/clone-analyze` | Clone Winner step 1: download ad video → Gemini 2.5 Flash → structured analysis (transcript, storyboard, hook, psychology). Falls back to thumbnail+copy analysis if the MP4 is unavailable |
| `POST /meta/creatives/clone-analyze-local` | Same pipeline on a local MP4 (demo/testing) |

### Mutations (write to Meta — only ever called after explicit user confirmation in the CRM)
| Endpoint | Purpose |
|---|---|
| `PUT /meta/adsets/{id}/status`, `PUT /meta/ads/{id}/status` | ACTIVE / PAUSED / ARCHIVED (/DELETED for ads) |
| `PUT /meta/adsets/{id}/budget` | Daily/lifetime budget (in cents) |
| `POST /meta/adsets/{id}/duplicate` | Duplicate ad set |
| `PUT /meta/adsets/{id}/frequency-cap`, `PUT /meta/adsets/{id}/bid-strategy` | Delivery controls |
| `POST /meta/campaigns` | Create campaign |
| `POST/GET/DELETE /meta/rules`, `PUT /meta/rules/{id}/status` | Meta Automated Rules CRUD |

## Meta API client (`app/meta_client.py`)

- Every request is signed with **`appsecret_proof`** (HMAC-SHA256 of the access token with the app secret).
- **Three cache layers:** L1 in-process dict and L2 Mongo (`meta_cache` collection, 30-min TTL, cache key = request URL with token stripped) for GETs; L3 is the synced `campaigns`/`adsets`/`ads` collections the backend reads.
- **Rate-limit handling:** detects Meta throttle responses (code 80004 / subcode 2446079 / HTTP 429) with retry + backoff, and proactively throttles by parsing `x-business-use-case-usage` / `x-app-usage` headers once usage ≥ 95%.
- Batched multi-ID fetches (`?ids=`, ≤50 per call) and two-step structure+insights joins to minimize call volume.

## Security

- **Inbound API is currently unauthenticated.** Anyone who can reach port 9000 can read account data *and pause ads, change budgets, and create campaigns*. On a standalone VPS this is critical:
  - **Firewall port 9000 to the central backend's IP only** (`ufw allow from <backend-ip> to any port 9000`, deny otherwise), **and/or**
  - Put nginx in front with HTTPS + a shared secret header that the backend sends (see "Multi-VPS deployment" below). Adding a token check inside FastAPI (reject requests without `X-Agent-Key: <secret>`) is a small, recommended hardening change.
- **Outbound auth:** every call to the backend carries `Authorization: Bearer <AGENT_TOKEN>`; the backend verifies it against the bcrypt `token_hash` stored for this agent (plus an optional `allowed_ip` check).
- **Secrets on disk:** `config/meta_config.json` holds the Meta app secret, long-lived access token, agent token, and Gemini key in plaintext. `config/` and `secrets/` are gitignored — keep it that way; restrict file permissions (`chmod 600`) and never commit real credentials. Rotate any token that has ever been committed.

## Multi-VPS deployment (one agent per Meta account)

Per-VPS checklist:

1. **Provision** a VPS with a unique public IP (this IP is what Meta sees). Install Python 3.12+.
2. **Register the agent** in the CRM (Agents page / `POST /api/agents`) → copy the generated `agent_id` and one-time token.
3. **Clone this repo**, create `config/meta_config.json` with: the agent id/token, `crm.base_url` = the backend's public HTTPS URL, this account's Meta credentials, and the Gemini key.
4. **Set `MONGODB_URI`** (systemd `Environment=` or an env file) pointing at the central Mongo. The central Mongo must have **auth enabled**, a dedicated user for agents, and its port firewalled to the agent VPS IPs (or reachable over a private network/VPN). Do not expose an auth-less Mongo to the internet.
5. **Expose the agent to the backend** — either:
   - nginx reverse proxy with TLS (`https://agent-n.example.com` → `localhost:9000`) and a shared-secret header check:
     ```nginx
     server {
       listen 443 ssl;
       server_name agent-n.example.com;
       # ... ssl_certificate ...
       location / {
         if ($http_x_agent_key != "<shared-secret>") { return 403; }
         proxy_pass http://127.0.0.1:9000;
       }
     }
     ```
     (The backend must then send `X-Agent-Key` on its agent calls — see the backend README's multi-VPS section.)
   - or plain `http://<vps-ip>:9000` with the port firewalled to the backend IP only. Simplest, but no TLS — fine on a private network, not on the open internet.
6. **Run as a service** (systemd):
   ```ini
   [Unit]
   Description=ACES Meta Agent
   After=network-online.target

   [Service]
   WorkingDirectory=/opt/aces-agent
   Environment=MONGODB_URI=mongodb://agent:<pw>@<central-ip>:27017/acesmaster?authSource=admin
   ExecStart=/opt/aces-agent/.venv/bin/python run.py
   Restart=always

   [Install]
   WantedBy=multi-user.target
   ```
7. **Verify:** `curl localhost:9000/healthz`, then check the agent shows ONLINE in the CRM (heartbeat), and after ~5 min that campaigns appear (L3 sync). `POST /sync/trigger` forces it.
8. **Point the backend at this agent.** The backend must know this agent's base URL. **Note:** the current backend uses a single global `AGENT_BASE_URL` for all agents — per-agent base URLs are a required backend change for the fleet setup (documented in the backend README, "Multi-VPS: required changes").

### Known gaps to fix for fleet operation

- [ ] Backend: per-agent `base_url` (see backend README) — **blocker for >1 agent**
- [ ] Agent: authenticate inbound requests (shared-secret header) instead of relying on firewall alone
- [ ] Agent: pin `pymongo` in `requirements.txt`
- [ ] Agent: remove the `/home/CRM/backend/.env` `MONGODB_URI` fallback (monorepo-era path)
- [ ] Agent: apply `config:pull` responses (currently fetched and discarded)

## Splitting out of the monorepo

This directory was `agent/` in the `CRM` monorepo. To extract with history:

```bash
git clone /path/to/CRM aces-agent && cd aces-agent
git filter-repo --subdirectory-filter agent
git remote add origin git@github.com:<org>/aces-agent.git
git push -u origin main
```

Before pushing, verify no secrets are in history: `config/` and `secrets/` are gitignored, but check `git log --all --full-history -- config/` — **if any credential file was ever committed, rotate every token in it** (Meta access token, app secret, agent token, Gemini key). Delete `.venv/` and any local `config/tokens` scratch files from the working tree.
