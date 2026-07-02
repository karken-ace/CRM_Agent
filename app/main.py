import asyncio
import os
import json
import logging
import time
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logger = logging.getLogger(__name__)

import httpx
import requests
import urllib.parse
from fastapi import FastAPI
from pydantic import BaseModel

# L2 persistent cache — used to memoize expensive Meta-iframe extraction
# results. Optional: falls back to no-op when pymongo/MONGODB_URI is missing.
try:
    from app import meta_cache as _meta_cache
except ImportError:
    try:
        import meta_cache as _meta_cache
    except ImportError:
        _meta_cache = None

# Handle imports for both standalone and module execution
try:
    from .meta_client import MetaAPIClient
except ImportError:
    # If relative import fails, try absolute import
    sys.path.insert(0, str(Path(__file__).parent))
    from meta_client import MetaAPIClient


# Load configuration from JSON file
def load_config():
    # Try multiple config paths for different execution contexts
    config_paths = [
        '/app/config/meta_config.json',  # Docker
        str(Path(__file__).parent.parent / 'config' / 'meta_config.json'),  # Local development
        'config/meta_config.json',  # Current directory
    ]
    
    for config_path in config_paths:
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                print(f"Loaded config from: {config_path}")
                return config
        except FileNotFoundError:
            continue
    
    # Fallback to environment variables
    print("Config file not found, using environment variables")
    return {
        "meta_api": {
            "app_id": os.getenv("META_APP_ID"),
            "app_secret": os.getenv("META_APP_SECRET"),
            "access_token": os.getenv("META_ACCESS_TOKEN"),
            "ad_account_id": os.getenv("META_AD_ACCOUNT_ID"),
            "base_url": os.getenv("META_BASE_URL", "https://graph.facebook.com/v22.0"),
            "timeout": int(os.getenv("META_TIMEOUT", "30"))
        },
        "agent": {
            "id": os.getenv("AGENT_ID", "agt_dev"),
            "token": os.getenv("AGENT_TOKEN")
        },
        "crm": {
            "base_url": os.getenv("CRM_BASE_URL", "http://localhost:8000"),
            "agent_token": os.getenv("AGENT_TOKEN")
        }
    }

config = load_config()

# Get CRM base URL - prefer config, then env var, then default to localhost
CRM_BASE_URL = config.get("crm", {}).get("base_url") or os.getenv("CRM_BASE_URL", "http://localhost:8000")
AGENT_ID = config.get("agent", {}).get("id") or config.get("crm", {}).get("agent_id") or os.getenv("AGENT_ID", "agt_dev")
AGENT_TOKEN = config.get("agent", {}).get("token") or config.get("crm", {}).get("agent_token") or os.getenv("AGENT_TOKEN")

# Global variables that can be updated when config changes
current_agent_id = AGENT_ID
current_agent_token = AGENT_TOKEN

def reload_config():
    global current_agent_id, current_agent_token
    try:
        new_config = load_config()
        current_agent_id = new_config["agent"]["id"]
        current_agent_token = new_config["agent"]["token"]
        
        # Validate credentials
        if not current_agent_id or not current_agent_token:
            print(f"ERROR: Invalid credentials - agent_id='{current_agent_id}', token={'EMPTY' if not current_agent_token else 'SET'}")
            return False
            
        print(f"Config reloaded: agent_id={current_agent_id}, token={'*' * len(current_agent_token)}")
        return True
    except Exception as e:
        print(f"Failed to reload config: {e}")
        return False


app = FastAPI(title="SM Agent", version="0.1.0")

# Secret management - use /etc/sm-agent in Docker, ./secrets locally
if os.path.exists("/etc/sm-agent"):
    SECRETS_DIR = Path("/etc/sm-agent")
else:
    SECRETS_DIR = Path(__file__).parent.parent / "secrets"
SECRETS_DIR.mkdir(exist_ok=True, parents=True)

class CredentialManager:
    def __init__(self):
        self.credentials = {}
        self.load_all_credentials()
    
    def load_all_credentials(self):
        """Load all credential files from /etc/sm-agent/"""
        for cred_file in SECRETS_DIR.glob("*.creds"):
            account_id = cred_file.stem
            try:
                with open(cred_file, 'r') as f:
                    self.credentials[account_id] = json.load(f)
            except Exception as e:
                print(f"Failed to load credentials for {account_id}: {e}")
    
    def get_credentials(self, account_id: str) -> Dict[str, Any]:
        """Get credentials for a specific account"""
        return self.credentials.get(account_id, {})
    
    def reload_credentials(self, account_id: str):
        """Reload credentials for a specific account"""
        cred_file = SECRETS_DIR / f"{account_id}.creds"
        if cred_file.exists():
            try:
                with open(cred_file, 'r') as f:
                    self.credentials[account_id] = json.load(f)
                print(f"Reloaded credentials for {account_id}")
            except Exception as e:
                print(f"Failed to reload credentials for {account_id}: {e}")

cred_manager = CredentialManager()

# Initialize Meta API client
# Try multiple config paths
config_paths = [
    '/app/config/meta_config.json',  # Docker
    str(Path(__file__).parent.parent / 'config' / 'meta_config.json'),  # Local development
    'config/meta_config.json',  # Current directory
]

meta_config_path = None
for path in config_paths:
    if os.path.exists(path):
        meta_config_path = path
        break

if meta_config_path:
    meta_client = MetaAPIClient(config_path=meta_config_path)
else:
    # Fallback: create client with default path (will use env vars)
    meta_client = MetaAPIClient(config_path="config/meta_config.json")

class CredentialFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.is_file and event.src_path.endswith('.creds'):
            account_id = Path(event.src_path).stem
            cred_manager.reload_credentials(account_id)

# Start file watcher
observer = Observer()
observer.schedule(CredentialFileHandler(), str(SECRETS_DIR), recursive=False)
observer.start()


async def post(client: httpx.AsyncClient, path: str, json: Dict[str, Any] | None = None) -> httpx.Response:
    url = f"{CRM_BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {current_agent_token}"}
    return await client.post(url, json=json or {}, headers=headers, timeout=20.0)


async def heartbeat_loop():
    """Periodically pings the backend so it knows the agent is alive.

    The previous version reloaded `meta_config.json` from disk on every
    heartbeat, which (a) spammed the log with `Config reloaded:` lines
    and (b) was wasted I/O — config rarely changes mid-run, and the
    on_startup hook already validates it. Per-account credentials are
    still hot-reloaded by the CredentialFileHandler watcher on file
    change. If meta_config.json itself changes, the operator restarts
    the agent.
    """
    async with httpx.AsyncClient() as client:
        while True:
            try:
                await post(client, f"/api/agents/{current_agent_id}/heartbeat", {"message": "ok"})
            except Exception as e:
                print(f"Heartbeat error: {e}")
            await asyncio.sleep(60)


async def pull_config_loop():
    async with httpx.AsyncClient() as client:
        backoff = 5
        while True:
            try:
                resp = await post(client, f"/api/agents/{AGENT_ID}/config:pull")
                if resp.is_success:
                    backoff = 5
                else:
                    backoff = min(backoff * 2, 300)
            except Exception:
                backoff = min(backoff * 2, 300)
            await asyncio.sleep(backoff)


async def pull_commands_loop():
    async with httpx.AsyncClient() as client:
        backoff = 5
        while True:
            try:
                resp = await post(client, f"/api/agents/{AGENT_ID}/commands:pull")
                if resp.is_success:
                    _ = resp.json()
                    # In MVP, we do not execute Meta actions; just acknowledge fetch
                    backoff = 5
                else:
                    backoff = min(backoff * 2, 300)
            except Exception:
                backoff = min(backoff * 2, 300)
            await asyncio.sleep(backoff)


def _warm_video_urls(campaigns: List[Dict[str, Any]]) -> None:
    """Pre-extract MP4 URLs for every video ad whose URL isn't already cached.
    Runs as part of each L3 sync so the Top Creatives page renders video
    cards as actual playing videos on first visit, instead of waiting
    20+s per card during user interaction.

    Filters to video creatives only (via the cheap thumbnails batch
    endpoint — already cached by L2) so we don't waste 20s per image ad
    that would extract to None anyway. Skips ads already in cache. Sleeps
    briefly between cold extractions to keep Meta rate-limit pressure low.
    """
    if _meta_cache is None:
        return

    # 1. Collect creative_id → ad_id mapping.
    creative_to_ad: Dict[str, str] = {}
    for c in campaigns or []:
        for a in (c.get("ad_sets") or []):
            for ad in (a.get("ads") or []):
                cid = (ad.get("creative") or {}).get("id")
                ad_id = ad.get("id")
                if cid and ad_id and cid not in creative_to_ad:
                    creative_to_ad[cid] = ad_id

    if not creative_to_ad:
        return

    # 2. Filter to video ads via the cached thumbnails endpoint.
    try:
        metadata = meta_client.get_creative_thumbnails(list(creative_to_ad.keys()))
    except Exception as e:
        logger.warning(f"L3 warm-up: thumbnails fetch failed: {e}")
        return
    video_ad_ids: List[str] = []
    for cid, info in (metadata or {}).items():
        if (info or {}).get("is_video"):
            ad_id = creative_to_ad.get(cid)
            if ad_id:
                video_ad_ids.append(ad_id)

    # 3. Warm each. Skip ads already in cache (positive or negative result).
    warmed = skipped = failed = 0
    for ad_id in video_ad_ids:
        cache_key = f"_xfromp:{ad_id}:any"
        if _meta_cache.get(cache_key) is not None:
            skipped += 1
            continue
        try:
            url = _extract_mp4_url_from_preview(ad_id)
            if url:
                warmed += 1
            else:
                # Cache negative so we don't retry every cycle.
                _meta_cache.set(cache_key, {"url": None}, 1800)
                failed += 1
        except Exception:
            failed += 1
        time.sleep(1)  # bounded rate ~60/min
    print(
        f"L3 warm-up: {len(video_ad_ids)} video ads "
        f"(warmed={warmed} already-fresh={skipped} no-url={failed})"
    )


async def sync_meta_data_loop():
    """Layer 3 hierarchical sync — every 5 min pulls the full campaigns ->
    ad_sets -> ads tree from Meta and writes it to Mongo collections that
    the dashboard reads from. Hits the same Meta endpoint the dashboard
    used to call directly, but now only the agent calls it (once per
    interval, regardless of how many users are clicking)."""
    from app import db_sync as _db_sync

    # Run on a separate thread executor since meta_client is sync (requests).
    import functools
    loop = asyncio.get_event_loop()

    async with httpx.AsyncClient() as client:
        while True:
            try:
                # Pull hierarchical structure (cached by L1/L2 if a recent
                # call already populated it).
                campaigns = await loop.run_in_executor(
                    None,
                    functools.partial(meta_client.get_campaigns_detailed, 100, "last_30d"),
                )
                if campaigns:
                    counts = _db_sync.upsert_hierarchical(current_agent_id, campaigns)
                    print(
                        f"L3 sync: {counts['campaigns']} campaigns, "
                        f"{counts['ad_sets']} ad sets, {counts['ads']} ads -> Mongo"
                    )
                    # Also tell the backend the agent's Meta connection is healthy
                    try:
                        await post(client, f"/api/agents/{AGENT_ID}/meta:sync", {
                            "meta_connected": True,
                            "last_sync": datetime.utcnow().isoformat() + "Z",
                            "synced_counts": counts,
                        })
                    except Exception:
                        pass  # heartbeat-style ping; best-effort

                    # NOTE: video-URL warm-up was removed — it shared the
                    # FastAPI threadpool with foreground extraction requests
                    # and could block hover/auto-fetch responses for minutes
                    # on accounts with many video ads. The frontend's
                    # useQueries fires fetches in parallel on page mount
                    # instead, and the L2 Mongo cache holds results across
                    # sessions so subsequent visits are sub-second.
                else:
                    print("L3 sync: hierarchical fetch returned empty (likely rate-limited); will retry next cycle")

            except Exception as e:
                print(f"L3 sync failed: {e}")

            # Every 5 min. Hits Meta at most once per interval regardless of
            # how many users are loading dashboards — replaces the per-user
            # fan-out that was driving the rate-limit cliff.
            await asyncio.sleep(300)


@app.on_event("startup")
async def on_startup():
    # Validate credentials at startup
    if not reload_config():
        print("ERROR: Invalid credentials at startup. Agent will not start.")
        import sys
        sys.exit(1)  # Exit the entire process
        
    print(f"Agent starting with valid credentials: agent_id={current_agent_id}")
    asyncio.create_task(heartbeat_loop())
    asyncio.create_task(pull_config_loop())
    asyncio.create_task(pull_commands_loop())
    asyncio.create_task(sync_meta_data_loop())


@app.get("/healthz")
def healthz():
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}


@app.post("/sync/trigger")
def sync_trigger():
    """Force an immediate hierarchical sync — bypasses the 5-min loop cadence.
    Used by the dashboard's Refresh button so users can pull truly-current
    Meta state on demand rather than waiting up to 5 min for the next loop.
    Returns the counts that were written (and removed) so the caller can
    show how much state changed.

    Resilience: if the hierarchical fetch comes back with campaigns but no
    nested ad_sets/ads (typical when Meta rate-limited the secondary
    queries), we re-attempt per-campaign ad_set fetches as a fallback.
    Without this, a single rate-limited hierarchical leaves the dashboard
    showing empty even though prior data was valid.
    """
    from app import db_sync as _db_sync
    try:
        campaigns = meta_client.get_campaigns_detailed(100, "last_30d")
        if not campaigns:
            return {"status": "error", "message": "Meta returned no campaigns (likely rate-limited; retry in ~30s)"}

        # Fallback: backfill ad_sets per-campaign when the hierarchical
        # response was partial. Each per-campaign call is cache-aware via L2.
        nested_count = sum(len(c.get("ad_sets") or []) for c in campaigns)
        if nested_count == 0:
            logger.info("Hierarchical returned 0 ad_sets; attempting per-campaign fallback")
            for c in campaigns:
                try:
                    ad_sets = meta_client.get_ad_sets(c["id"], limit=500, date_preset="last_30d")
                    if ad_sets:
                        c["ad_sets"] = ad_sets
                except Exception as e:
                    logger.warning(f"Per-campaign ad_set fallback failed for {c.get('id')}: {e}")

        counts = _db_sync.upsert_hierarchical(current_agent_id, campaigns)
        return {
            "status": "success",
            "synced_at": datetime.utcnow().isoformat() + "Z",
            "counts": counts,
        }
    except Exception as e:
        return {"status": "error", "message": f"Sync failed: {e}"}

@app.get("/meta/test")
def test_meta_connection():
    """Test connection to Meta API"""
    try:
        if meta_client.test_connection():
            return {"status": "success", "message": "Meta API connection successful"}
        else:
            return {"status": "error", "message": "Meta API connection failed"}
    except Exception as e:
        return {"status": "error", "message": f"Meta API error: {str(e)}"}

@app.get("/meta/account")
def get_meta_account():
    """Get Meta app information"""
    try:
        app_info = meta_client.get_app_info()
        return {"status": "success", "data": app_info}
    except Exception as e:
        return {"status": "error", "message": f"Failed to get app info: {str(e)}"}

@app.get("/meta/campaigns")
def get_meta_campaigns():
    """Get Meta campaigns"""
    try:
        campaigns = meta_client.get_campaigns()
        return {"status": "success", "data": campaigns}
    except Exception as e:
        return {"status": "error", "message": f"Failed to get campaigns: {str(e)}"}

@app.get("/meta/insights")
def get_meta_insights(
    date_preset: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    level: Optional[str] = None,
):
    """Get Meta insights/metrics.

    All query params are optional. When none are supplied, behavior is
    identical to the original (account-level summary for ``date_preset=today``),
    returning ``{"status": "success", "data": {<metrics>}}``.

    When ``level`` is supplied as ``campaign|adset|ad``, Meta returns one row
    per object and we return ``data`` as a list instead of a single dict.

    Providing ``since`` and ``until`` (YYYY-MM-DD) overrides ``date_preset``.
    """
    try:
        # Backward-compatible path: no params → exact original behavior.
        if not date_preset and not since and not until and not level:
            insights = meta_client.get_insights()
            return {"status": "success", "data": insights}

        base_fields = "spend,impressions,clicks,ctr,cpc,cpm,reach,frequency"
        level_id_fields = {
            "campaign": ",campaign_id,campaign_name",
            "adset": ",adset_id,adset_name,campaign_id,campaign_name",
            "ad": ",ad_id,ad_name,adset_id,campaign_id",
        }
        lvl = level.lower() if level else None
        if lvl is not None and lvl not in {"account", "campaign", "adset", "ad"}:
            return {"status": "error", "message": "level must be one of: account, campaign, adset, ad"}

        params: Dict[str, str] = {
            "fields": base_fields + level_id_fields.get(lvl or "", ""),
        }
        if since and until:
            params["time_range"] = json.dumps({"since": since, "until": until})
        else:
            params["date_preset"] = date_preset or "today"
        if lvl:
            params["level"] = lvl

        endpoint = f"act_{meta_client.ad_account_id}/insights"
        query = "&".join(f"{k}={urllib.parse.quote_plus(str(v))}" for k, v in params.items())
        response = meta_client._make_request(f"{endpoint}?{query}")
        data = response.get("data", [])

        # For backward compatibility when level is omitted or 'account' (single
        # aggregated row), unwrap to a dict matching the original response
        # shape. Anything else returns the full row list.
        if not lvl or lvl == "account":
            return {"status": "success", "data": (data[0] if data else {})}
        return {"status": "success", "data": data, "count": len(data)}
    except Exception as e:
        return {"status": "error", "message": f"Failed to get insights: {str(e)}"}

@app.get("/meta/campaigns/hierarchical")
def get_hierarchical_campaigns(date_preset: str = "last_30d"):
    """Get campaigns with hierarchical structure (campaigns -> ad sets -> ads)
    
    Args:
        date_preset: Date range for insights (e.g., 'last_30d', 'today', 'yesterday', 'last_7d')
    """
    try:
        campaigns = meta_client.get_campaigns_detailed(limit=100, date_preset=date_preset)

        # The ad account's billing currency drives how Meta returns spend values
        # in `insights`. Surface it so the frontend can render with the correct
        # symbol instead of a hardcoded "$".
        # Ref: https://developers.facebook.com/docs/marketing-api/reference/ad-account/
        currency = None
        try:
            account_info = meta_client.get_ad_account_info() or {}
            currency = account_info.get("currency")
        except Exception as e:
            logger.warning(f"Could not fetch ad account currency: {e}")

        return {
            "status": "success",
            "data": {
                "campaigns": campaigns,
                "currency": currency,
                "summary": {
                    "total_campaigns": len(campaigns),
                    "total_ad_sets": sum(len(campaign.get("ad_sets", [])) for campaign in campaigns),
                    "total_ads": sum(
                        sum(len(ad_set.get("ads", [])) for ad_set in campaign.get("ad_sets", []))
                        for campaign in campaigns
                    )
                },
                "last_updated": datetime.utcnow().isoformat() + "Z"
            }
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to get hierarchical campaigns: {str(e)}"}

@app.get("/meta/test/hierarchical")
def test_hierarchical_structure():
    """Test endpoint to verify Meta API integration with detailed hierarchical display"""
    try:        
        # Get account info
        account_info = meta_client.get_ad_account_info()
        
        # Get campaigns with full hierarchy
        campaigns = meta_client.get_campaigns_detailed(limit=100)
        
        # Create detailed hierarchical display
        hierarchical_display = {
            "status": "success",
            "message": "Meta Marketing API Integration Test - SUCCESS",
            "account_info": account_info,
            "hierarchical_structure": {
                "campaigns": []
            },
            "summary": {
                "total_campaigns": len(campaigns),
                "total_ad_sets": 0,
                "total_ads": 0,
                "active_campaigns": 0,
                "paused_campaigns": 0,
                "archived_campaigns": 0
            }
        }
        
        # Process each campaign
        for campaign in campaigns:
            campaign_data = {
                "id": campaign.get("id"),
                "name": campaign.get("name"),
                "status": campaign.get("status"),
                "effective_status": campaign.get("effective_status"),
                "objective": campaign.get("objective"),
                "daily_budget": campaign.get("daily_budget"),
                "lifetime_budget": campaign.get("lifetime_budget"),
                "performance_metrics": campaign.get("performance_metrics", {}),
                "ad_sets": []
            }
            
            # Count status
            if campaign.get("effective_status") == "ACTIVE":
                hierarchical_display["summary"]["active_campaigns"] += 1
            elif campaign.get("effective_status") == "PAUSED":
                hierarchical_display["summary"]["paused_campaigns"] += 1
            elif campaign.get("effective_status") == "ARCHIVED":
                hierarchical_display["summary"]["archived_campaigns"] += 1
            
            # Process ad sets
            for ad_set in campaign.get("ad_sets", []):
                ad_set_data = {
                    "id": ad_set.get("id"),
                    "name": ad_set.get("name"),
                    "status": ad_set.get("status"),
                    "effective_status": ad_set.get("effective_status"),
                    "daily_budget": ad_set.get("daily_budget"),
                    "lifetime_budget": ad_set.get("lifetime_budget"),
                    "optimization_goal": ad_set.get("optimization_goal"),
                    "performance_metrics": ad_set.get("performance_metrics", {}),
                    "ads": []
                }
                
                hierarchical_display["summary"]["total_ad_sets"] += 1
                
                # Process ads
                for ad in ad_set.get("ads", []):
                    ad_data = {
                        "id": ad.get("id"),
                        "name": ad.get("name"),
                        "status": ad.get("status"),
                        "effective_status": ad.get("effective_status"),
                        "creative": ad.get("creative", {}),
                        "performance_metrics": ad.get("performance_metrics", {})
                    }
                    
                    ad_set_data["ads"].append(ad_data)
                    hierarchical_display["summary"]["total_ads"] += 1
                
                campaign_data["ad_sets"].append(ad_set_data)
            
            hierarchical_display["hierarchical_structure"]["campaigns"].append(campaign_data)
        
        return hierarchical_display
        
    except Exception as e:
        return {
            "status": "error", 
            "message": f"Meta API integration test failed: {str(e)}",
            "error_details": str(e)
        }

@app.get("/meta/test/simple")
def test_simple_campaigns():
    """Simple test endpoint that just shows campaigns without nested data"""
    try:        
        # Get account info
        account_info = meta_client.get_ad_account_info()
        
        # Get campaigns only (no nested data to avoid rate limits)
        campaigns = meta_client.get_campaigns(limit=100)
        
        return {
            "status": "success",
            "message": "Meta Marketing API Integration Test - SUCCESS (Simple)",
            "account_info": account_info,
            "campaigns": campaigns,
            "summary": {
                "total_campaigns": len(campaigns),
                "active_campaigns": len([c for c in campaigns if c.get("status") == "ACTIVE"]),
                "paused_campaigns": len([c for c in campaigns if c.get("status") == "PAUSED"]),
                "archived_campaigns": len([c for c in campaigns if c.get("status") == "ARCHIVED"])
            }
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": f"Meta API integration test failed: {str(e)}",
            "error_details": str(e)
        }

@app.get("/meta/campaigns/{campaign_id}/adsets")
def get_campaign_adsets(campaign_id: str, date_preset: str = "last_30d"):
    """Get ad sets for a specific campaign

    Args:
        campaign_id: The campaign ID
        date_preset: Date range for insights (e.g., 'last_30d', 'last_7d', 'today')
    """
    try:
        # Get ad sets for the specific campaign with performance metrics
        ad_sets = meta_client.get_ad_sets(campaign_id, limit=50, date_preset=date_preset)

        return {
            "status": "success",
            "message": f"Ad sets for campaign {campaign_id}",
            "campaign_id": campaign_id,
            "ad_sets": ad_sets,
            "summary": {
                "total_ad_sets": len(ad_sets),
                "active_ad_sets": len([ads for ads in ad_sets if ads.get("status") == "ACTIVE"]),
                "paused_ad_sets": len([ads for ads in ad_sets if ads.get("status") == "PAUSED"]),
                "archived_ad_sets": len([ads for ads in ad_sets if ads.get("status") == "ARCHIVED"])
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get ad sets for campaign {campaign_id}: {str(e)}",
            "error_details": str(e)
        }

@app.get("/meta/adsets/{adset_id}/ads")
def get_adset_ads(adset_id: str):
    """Get ads for a specific ad set"""
    try:        
        # Get ads for the specific ad set
        ads = meta_client.get_ads(adset_id, limit=50)
        
        return {
            "status": "success",
            "message": f"Ads for ad set {adset_id}",
            "adset_id": adset_id,
            "ads": ads,
            "summary": {
                "total_ads": len(ads),
                "active_ads": len([ad for ad in ads if ad.get("status") == "ACTIVE"]),
                "paused_ads": len([ad for ad in ads if ad.get("status") == "PAUSED"]),
                "archived_ads": len([ad for ad in ads if ad.get("status") == "ARCHIVED"])
            }
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": f"Failed to get ads for ad set {adset_id}: {str(e)}",
            "error_details": str(e)
        }

# ── Breakdown & Analytics Endpoints ──

@app.get("/meta/breakdowns/age-gender/{campaign_id}")
def get_age_gender_breakdown(campaign_id: str, date_preset: str = "last_7d"):
    """Get performance breakdown by age and gender for a campaign"""
    try:
        data = meta_client.get_age_gender_breakdown(campaign_id, date_preset=date_preset)
        return {"status": "success", "campaign_id": campaign_id, "breakdowns": data}
    except Exception as e:
        return {"status": "error", "message": f"Failed to get age/gender breakdown: {str(e)}"}

@app.get("/meta/breakdowns/platform/{campaign_id}")
def get_platform_breakdown_endpoint(campaign_id: str, date_preset: str = "last_7d"):
    """Get performance breakdown by platform for a campaign"""
    try:
        data = meta_client.get_platform_breakdown(campaign_id, date_preset=date_preset)
        return {"status": "success", "campaign_id": campaign_id, "breakdowns": data}
    except Exception as e:
        return {"status": "error", "message": f"Failed to get platform breakdown: {str(e)}"}

@app.get("/meta/breakdowns/daily/{campaign_id}")
def get_daily_breakdown_endpoint(campaign_id: str, date_preset: str = "last_7d"):
    """Get daily performance breakdown for sparkline/trend data"""
    try:
        data = meta_client.get_daily_breakdown(campaign_id, date_preset=date_preset)
        return {"status": "success", "campaign_id": campaign_id, "daily_data": data}
    except Exception as e:
        return {"status": "error", "message": f"Failed to get daily breakdown: {str(e)}"}

@app.get("/meta/breakdowns/hourly/{campaign_id}")
def get_hourly_breakdown_endpoint(campaign_id: str, date_preset: str = "last_7d"):
    """Get hourly performance breakdown for dayparting analysis"""
    try:
        data = meta_client.get_hourly_breakdown(campaign_id, date_preset=date_preset)
        return {"status": "success", "campaign_id": campaign_id, "hourly_data": data}
    except Exception as e:
        return {"status": "error", "message": f"Failed to get hourly breakdown: {str(e)}"}

@app.get("/meta/time-comparison/{campaign_id}")
def get_time_comparison_endpoint(campaign_id: str):
    """Get time comparison insights (last 7d vs last 30d) for trend analysis"""
    try:
        data = meta_client.get_time_comparison_insights(campaign_id)
        return {"status": "success", "campaign_id": campaign_id, "comparison": data}
    except Exception as e:
        return {"status": "error", "message": f"Failed to get time comparison: {str(e)}"}

@app.get("/meta/pixel-quality")
def get_pixel_quality_endpoint():
    """Get pixel health and event quality statistics"""
    try:
        data = meta_client.get_pixel_quality()
        return {"status": "success", "data": data}
    except Exception as e:
        return {"status": "error", "message": f"Failed to get pixel quality: {str(e)}"}


class CreativeThumbnailsRequest(BaseModel):
    creative_ids: List[str]


@app.post("/meta/creatives/thumbnails")
def get_creative_thumbnails_endpoint(request: CreativeThumbnailsRequest):
    """Fetch thumbnail URLs for a list of creative IDs.

    Called by the frontend after getting hierarchical campaign data,
    which only includes creative {id} without thumbnail URLs.
    """
    try:
        creative_ids = request.creative_ids
        if not creative_ids:
            return {"status": "success", "thumbnails": {}}

        thumbnails = meta_client.get_creative_thumbnails(creative_ids)
        # Return id -> { thumbnail_url, video_id, object_type, image_hash }
        # object_type values: "VIDEO", "PHOTO", "SHARE", "STATUS", "INVALID"
        # Ref: https://developers.facebook.com/docs/marketing-api/reference/ad-creative/
        result = {}
        for cid, data in thumbnails.items():
            url = data.get("thumbnail_url") or data.get("image_url")
            result[cid] = {
                "thumbnail_url": url,
                "video_id": data.get("video_id"),
                "object_type": data.get("object_type"),
                "image_hash": data.get("image_hash"),
                "is_video": bool(data.get("is_video")),
            }

        return {"status": "success", "thumbnails": result}
    except Exception as e:
        return {"status": "error", "message": f"Failed to get thumbnails: {str(e)}"}


@app.get("/meta/ads/{ad_id}/video-url")
def get_ad_video_url(ad_id: str):
    """Return a direct MP4 URL for an ad, extracted from the authenticated
    preview iframe. Lets the frontend render a clean <video> tag instead
    of embedding Meta's full preview iframe (which loads ~1MB of FB JS
    and spams the browser console with internal errors like
    `useCometRouterIsPermalink` and unrelated 404s).

    Returns {status, video_url} on success. video_url is a time-limited
    fbcdn signed URL; the caller should use it immediately and not persist.

    Performance: pre-checks the creative's ``object_type``/``video_id``
    via the cached thumbnails path so non-video creatives (PHOTO/STATUS
    cross-posts with no extractable MP4) fail in ~300ms instead of
    burning ~30s on a four-format iframe scrape that can't succeed.
    """
    try:
        # Fast-fail when the creative has no recoverable video. Without an
        # ``expected_video_id`` the iframe scrape can't safely identify the
        # right video (per CLAUDE.md, Meta returns different videos per
        # placement and may swap in synthetic ones), so when the precheck
        # finds no video_id we bail rather than burn ~30s on four iframe
        # fetches that can't succeed. If the precheck itself errors out,
        # we fall through to the original extraction so we don't regress
        # edge cases where the creative metadata is reachable through the
        # iframe but not via the creative endpoint.
        expected_video_id: Optional[str] = None
        precheck_ran = False
        precheck_object_type: Optional[str] = None
        try:
            ad_meta = meta_client._make_request(f"{ad_id}?fields=creative{{id}}")
            creative_id = (ad_meta.get("creative") or {}).get("id")
            if creative_id:
                thumbs = meta_client.get_creative_thumbnails([creative_id])
                info = thumbs.get(creative_id) or {}
                precheck_ran = True
                precheck_object_type = info.get("object_type")
                expected_video_id = info.get("video_id")
        except Exception as precheck_err:
            logger.debug(f"video-url precheck failed for ad {ad_id}: {precheck_err}")

        if precheck_ran and not expected_video_id:
            return {
                "status": "error",
                "message": "No video URL extractable for this ad",
                "reason": "no_video_id",
                "object_type": precheck_object_type,
            }

        url = _extract_mp4_url_from_preview(ad_id, expected_video_id=expected_video_id)
        if not url:
            return {"status": "error", "message": "No video URL extractable for this ad"}
        return {"status": "success", "video_url": url}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/meta/ads/{ad_id}/preview")
def get_ad_preview(ad_id: str, ad_format: str = "MOBILE_FEED_STANDARD"):
    """Get an interactive preview iframe for an ad.

    Uses Meta's Ad Previews API which returns an iframe HTML that renders
    the exact ad as users see it (handles video, image, carousel, dynamic).

    Ref: https://developers.facebook.com/docs/marketing-api/reference/ad/previews/

    Args:
        ad_id: Meta ad ID
        ad_format: Meta ad format (MOBILE_FEED_STANDARD, DESKTOP_FEED_STANDARD,
                   INSTAGRAM_STANDARD, INSTAGRAM_STORY, etc.)

    Returns:
        {status, ad_id, ad_format, iframe_html, iframe_src}
    """
    try:
        # Call Meta's /{ad_id}/previews endpoint
        endpoint = f"{ad_id}/previews?ad_format={ad_format}"
        response = meta_client._make_request(endpoint)

        previews = response.get("data", [])
        if not previews:
            return {"status": "error", "message": "No preview available for this ad"}

        iframe_html = previews[0].get("body", "")

        # Extract the iframe src URL for easier frontend use
        import re
        src_match = re.search(r'src="([^"]+)"', iframe_html)
        iframe_src = src_match.group(1).replace("&amp;", "&") if src_match else None

        return {
            "status": "success",
            "ad_id": ad_id,
            "ad_format": ad_format,
            "iframe_html": iframe_html,
            "iframe_src": iframe_src,
        }
    except Exception as e:
        return {"status": "error", "message": f"Failed to get ad preview: {str(e)}"}


# ── Clone Winner: Gemini Video Analysis ──

def _build_master_analysis_prompt(video_length: Optional[float]) -> str:
    """Build the master analysis prompt, injecting the actual video duration
    when known. The previous static prompt biased Gemini toward a 25-second
    structure (because the example storyboard rows were "0-3s", "3-8s", and
    duration_seconds defaulted to 0 with no anchor on actual length), so every
    analysis came back roughly 25s long regardless of the real video.
    """
    # Decide how many storyboard rows to suggest based on length.
    # ~5s per beat is a reasonable default for short-form ads; longer videos
    # get more beats. Caps prevent Gemini from generating a 60-row storyboard
    # for a 5-minute video.
    if video_length and video_length > 0:
        suggested_rows = max(2, min(12, round(video_length / 5)))
        duration_clause = (
            f"This video is approximately {video_length:.1f} seconds long. "
            f'Set "duration_seconds" to {video_length:.1f} (the true length). '
            f"The storyboard MUST cover the full video from 0s to {video_length:.1f}s — "
            f"do not stop early and do not invent time past the end. "
            f'Each "time" entry uses real seconds, e.g. "0-4s", "4-12s". '
            f"Aim for roughly {suggested_rows} rows, each covering a distinct beat."
        )
    else:
        duration_clause = (
            "Detect the video's actual length yourself by watching to the end. "
            'Set "duration_seconds" to that detected length (a number, in seconds). '
            "The storyboard MUST cover from 0s to the end of the video — do not "
            'stop at 25s if the video is longer. Each "time" entry uses real '
            'seconds from this specific video, e.g. "0-4s", "4-12s". '
            "Use as many rows as needed to capture each distinct beat."
        )

    return f"""Analyze this video advertisement and return ONLY valid JSON.
No preamble, no markdown fences.

{duration_clause}

Exact structure:
{{
  "transcript": "<word-for-word transcription of all spoken audio>",
  "storyboard": [
    {{ "time": "<start>-<end>s", "visual": "<what is on screen>", "audio": "<what is said>" }}
  ],
  "hook": {{
    "type": "problem_statement | bold_claim | question | demonstration | social_proof",
    "script": "<exact words spoken in first 3 seconds>",
    "visual": "<what appears on screen in first 3 seconds>",
    "pain_point_or_desire": "<the specific feeling or problem being triggered>"
  }},
  "text_overlays": [
    {{ "text": "<text shown on screen>", "purpose": "<why this works for sound-off viewers>" }}
  ],
  "cta": {{
    "text": "<exact call to action wording>",
    "timing": "<when does CTA appear>",
    "friction_level": "low | medium | high"
  }},
  "marketing_psychology": {{
    "primary_mechanism": "<the core reason this ad converts>",
    "psychological_triggers": ["urgency", "social_proof"],
    "winning_formula": "<one paragraph explaining why this works>"
  }},
  "format": {{
    "style": "ugc | polished | talking_head | b_roll | animation",
    "pacing": "slow | medium | fast",
    "duration_seconds": <actual length, e.g. 8 or 47.5>,
    "product_first_appears_at": <seconds, e.g. 4>
  }}
}}"""


class CloneAnalyzeRequest(BaseModel):
    creative_id: str
    # Optional: passing the specific ad_id lets the agent fall back to
    # iframe-extraction (Method A) when Meta withholds the direct source URL
    # — which is the common case for cross-posted / Reel videos.
    ad_id: Optional[str] = None


def _extract_mp4_url_from_preview(ad_id: str, expected_video_id: Optional[str] = None) -> Optional[str]:
    """Resolve a downloadable MP4 URL by scraping Meta's authenticated ad
    preview iframe. Used when ``source`` on the video object is gated.

    Critical correctness note: Meta's previews API returns DIFFERENT videos
    for different placements (MOBILE_FEED, INSTAGRAM_STORY, etc.) because
    of dynamic creative optimization. Some ad_formats can even return a
    completely synthetic video that isn't part of the creative at all.
    We therefore extract the embedded ``videoID`` from each iframe's HTML
    and only accept a URL whose video matches ``expected_video_id`` (the
    one ``get_creative_video_url`` resolved from the creative's metadata).

    The result is cached in meta_cache for ~30 min keyed by (ad_id,
    expected_video_id). The iframe HTML fetch is the slowest part of the
    pipeline (~700 KB from business.facebook.com) and isn't cached by
    Layer 2 since it's a direct ``requests.get``. Caching the final
    extracted URL avoids the cost on every hover.
    """
    import re as _re
    import json as _json
    import requests as _req

    # Layer-2-style cache for the final result. fbcdn signed URLs are
    # typically valid for ~24h so a 30-minute cache is conservative.
    _cache_key = f"_xfromp:{ad_id}:{expected_video_id or 'any'}"
    if _meta_cache is not None:
        cached = _meta_cache.get(_cache_key)
        if cached is not None:
            return cached.get("url") if isinstance(cached, dict) else cached

    # Order matters: STORY and INSTAGRAM_STANDARD are most likely to render
    # the original creative video; MOBILE_FEED can substitute a campaign-
    # level optimized variant; DESKTOP_FEED often falls through to image.
    candidate_formats = [
        "INSTAGRAM_STORY",
        "INSTAGRAM_STANDARD",
        "MOBILE_FEED_STANDARD",
        "DESKTOP_FEED_STANDARD",
    ]

    def _grab(html: str, key: str) -> Optional[str]:
        m = _re.search(rf'"{key}"\s*:\s*"([^"]+)"', html)
        if not m:
            return None
        try:
            return _json.loads(f'"{m.group(1)}"')
        except Exception:
            return m.group(1).replace("\\/", "/")

    # Track every videoID we saw across formats — useful for the warning
    # message when none match the expected one.
    seen_video_ids = set()
    fallback_url = None  # used only if expected_video_id is None

    for fmt in candidate_formats:
        try:
            response = meta_client._make_request(f"{ad_id}/previews?ad_format={fmt}")
            previews = response.get("data") or []
            if not previews:
                continue
            body = previews[0].get("body", "")
            src_match = _re.search(r'src="([^"]+)"', body)
            if not src_match:
                continue
            iframe_src = src_match.group(1).replace("&amp;", "&")

            r = _req.get(iframe_src, timeout=30, headers={"User-Agent": "Mozilla/5.0 Chrome/120.0"})
            r.raise_for_status()
            html = r.text

            video_ids_here = _re.findall(r'"videoID"\s*:\s*(\d+)', html)
            seen_video_ids.update(video_ids_here)
            url = _grab(html, "videoURIHD") or _grab(html, "videoURISD")
            if not url:
                continue

            if expected_video_id and str(expected_video_id) in video_ids_here:
                logger.info(f"Preview iframe match for ad {ad_id}: ad_format={fmt}, video_id={expected_video_id}")
                if _meta_cache is not None:
                    _meta_cache.set(_cache_key, {"url": url}, 1800)  # 30 min
                return url

            # When the caller has no expectation (e.g. Top Creatives hover
            # preview), return the first URL we find — no need to iterate
            # through every ad_format. Cuts hot-path latency from ~30s
            # (all 4 iframes fetched) to ~3-7s (one iframe).
            if not expected_video_id:
                if _meta_cache is not None:
                    _meta_cache.set(_cache_key, {"url": url}, 1800)
                return url
        except Exception as e:
            logger.warning(f"Preview-iframe extraction failed for ad {ad_id}, format {fmt}: {e}")
            continue

    if expected_video_id:
        logger.warning(
            f"Preview iframe never rendered the expected video for ad {ad_id}: "
            f"expected={expected_video_id} seen={sorted(seen_video_ids)} — falling back to Mode B"
        )
        return None
    return None


def _analyze_from_copy_and_thumbnail(creative_id: str, video_info: Dict[str, Any]) -> Dict[str, Any]:
    """Fallback analysis when Meta doesn't expose the MP4 source.

    Feeds Gemini the first-frame thumbnail plus the ad's unique copy (bodies,
    titles, CTAs) and asks for the same structured JSON shape. Each ad's copy
    and thumbnail differ, so the output is unique per ad.

    Ref: https://ai.google.dev/gemini-api/docs/vision — Gemini accepts images
    inline for multimodal analysis.
    """
    import requests as req_lib

    thumbnail_url = video_info.get("thumbnail_url")
    bodies = video_info.get("bodies") or []
    titles = video_info.get("titles") or []
    descriptions = video_info.get("descriptions") or []
    ctas = video_info.get("ctas") or []
    creative_name = video_info.get("creative_name", "")

    copy_block = []
    if titles:
        copy_block.append("HEADLINES:\n" + "\n".join(f"- {t}" for t in titles[:5]))
    if bodies:
        copy_block.append("BODY COPY:\n" + "\n".join(f"- {b}" for b in bodies[:5]))
    if descriptions:
        copy_block.append("DESCRIPTIONS:\n" + "\n".join(f"- {d}" for d in descriptions[:3]))
    if ctas:
        copy_block.append("CTAs: " + ", ".join(ctas))
    copy_text = "\n\n".join(copy_block) or "(no copy available)"

    # Use the Meta-reported video length when available so the inferred
    # storyboard and duration_seconds match the real ad. The previous version
    # hard-coded a 4-row, 25-second skeleton, which made every analysis come
    # back at the same length regardless of the source video.
    video_length = video_info.get("video_length")
    if video_length and video_length > 0:
        suggested_rows = max(2, min(8, round(video_length / 5)))
        duration_clause = (
            f"This video is approximately {video_length:.1f} seconds long. "
            f'Set "duration_seconds" to {video_length:.1f}. The storyboard MUST '
            f"cover the full video from 0s to {video_length:.1f}s. Aim for "
            f'roughly {suggested_rows} rows. Each "time" field uses real '
            f'seconds, e.g. "0-4s", "4-12s".'
        )
    else:
        duration_clause = (
            "The MP4 is not accessible and Meta did not expose a length, so the "
            'true duration is unknown. Use phase labels for the "time" field '
            '("opening", "buildup", "demo", "close") instead of inventing '
            'specific seconds. Set "duration_seconds" to null.'
        )

    prompt = f"""Analyze this video ad based on its first-frame thumbnail and its
advertiser-supplied copy. The raw MP4 isn't accessible, so infer visual, audio,
pacing, and storyboard details from the thumbnail plus the copy below.

{duration_clause}

=== AD COPY ===
{copy_text}

=== CREATIVE NAME ===
{creative_name or '(unnamed)'}

Return ONLY valid JSON with this EXACT structure (same as a full video analysis):
{{
  "transcript": "<best-guess VO reconstructed from the copy, 2-4 sentences>",
  "storyboard": [
    {{ "time": "<start>-<end>s OR phase label", "visual": "<inferred shot>", "audio": "<inferred VO>" }}
  ],
  "hook": {{
    "type": "problem_statement | bold_claim | question | demonstration | social_proof",
    "script": "<hook line drawn from the strongest headline/body>",
    "visual": "<what the thumbnail shows in first 3s>",
    "pain_point_or_desire": "<the specific feeling the ad targets>"
  }},
  "text_overlays": [
    {{ "text": "<likely on-screen text from headlines>", "purpose": "<why this works sound-off>" }}
  ],
  "cta": {{
    "text": "<the CTA wording>",
    "timing": "<when CTA likely appears>",
    "friction_level": "low | medium | high"
  }},
  "marketing_psychology": {{
    "primary_mechanism": "<the one core reason this ad converts>",
    "psychological_triggers": ["<trigger 1>", "<trigger 2>"],
    "winning_formula": "<one paragraph on why this works, tailored to THIS copy>"
  }},
  "format": {{
    "style": "ugc | polished | talking_head | b_roll | animation",
    "pacing": "slow | medium | fast",
    "duration_seconds": <true length in seconds, or null if unknown>,
    "product_first_appears_at": <seconds, or null if unknown>
  }}
}}
Tailor every field to the specific copy and thumbnail — do NOT output generic boilerplate."""

    try:
        import google.generativeai as genai
    except ImportError:
        return {"status": "error", "message": "google-generativeai not installed"}

    gemini_key = os.environ.get("GEMINI_API_KEY") or config.get("gemini_api", {}).get("api_key", "")
    if not gemini_key:
        return {"status": "error", "message": "GEMINI_API_KEY not configured"}

    genai.configure(api_key=gemini_key)
    model = genai.GenerativeModel("gemini-2.5-flash")

    parts: List[Any] = [prompt]
    if thumbnail_url:
        try:
            img_resp = req_lib.get(thumbnail_url, timeout=30)
            img_resp.raise_for_status()
            parts.insert(0, {"mime_type": "image/jpeg", "data": img_resp.content})
        except Exception as e:
            logger.warning(f"Thumbnail fetch failed for fallback analysis: {e}")

    try:
        response = model.generate_content(parts)
        response_text = response.text.strip()
        if response_text.startswith("```"):
            response_text = response_text.split("\n", 1)[1] if "\n" in response_text else response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        analysis = json.loads(response_text.strip())
        return {
            "status": "success",
            "creative_id": creative_id,
            "video_id": video_info.get("video_id"),
            "creative_name": creative_name,
            "analysis": analysis,
            "mode": "copy_and_thumbnail",  # signals degraded mode vs full video
        }
    except json.JSONDecodeError as e:
        return {"status": "error", "message": f"Gemini returned invalid JSON: {str(e)}"}
    except Exception as e:
        return {"status": "error", "message": f"Gemini copy-based analysis failed: {str(e)}"}


@app.post("/meta/creatives/clone-analyze")
def clone_analyze_creative(request: CloneAnalyzeRequest):
    """Analyze a video ad creative using Google Gemini 2.5 Flash.

    Downloads the ad video from Meta, uploads to Gemini, and returns
    structured analysis (transcript, storyboard, hook, psychology).

    This is Step 1 of the Clone Winner pipeline.
    """
    creative_id = request.creative_id

    try:
        # Step 1: Get creative metadata (video_id + source URL when available +
        # ad copy as fallback context).
        video_info = meta_client.get_creative_video_url(creative_id)
        if not video_info or not video_info.get("video_id"):
            return {
                "status": "error",
                "message": f"Creative {creative_id} is not a video ad.",
            }

        video_url = video_info.get("source_url")
        video_id = video_info["video_id"]

        # If Meta withheld the direct source URL (the common case for
        # cross-posted Reels / SHARE creatives where the page that owns the
        # video isn't accessible by our token), attempt to recover one by
        # scraping the authenticated preview iframe. We pass the expected
        # video_id so the extractor only accepts a URL whose iframe actually
        # rendered the creative's video — Meta's previews API otherwise
        # returns dynamic-creative-optimized variants that may belong to a
        # different video entirely.
        if not video_url and request.ad_id:
            extracted = _extract_mp4_url_from_preview(request.ad_id, expected_video_id=video_id)
            if extracted:
                logger.info(f"Recovered MP4 URL via preview iframe for ad {request.ad_id} (video_id={video_id})")
                video_url = extracted

        # Still nothing? Fall back to thumbnail + ad copy analysis so each ad
        # still gets a unique Gemini-generated breakdown (Mode B).
        if not video_url:
            return _analyze_from_copy_and_thumbnail(creative_id, video_info)

        # Step 2: Download video to temp file
        import tempfile
        import requests as req_lib

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp_path = tmp.name
            resp = req_lib.get(video_url, timeout=120, stream=True)
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)

        # Check file size (reject > 10 min / ~200MB)
        file_size = os.path.getsize(tmp_path)
        # Spec: Reject videos > 10 minutes (~200MB at typical Meta ad encoding)
        if file_size > 200 * 1024 * 1024:
            os.unlink(tmp_path)
            return {"status": "error", "message": "This video is too long to analyse. Clone Winner works best with ads under 3 minutes."}

        # Step 3: Upload to Gemini and analyze
        try:
            import google.generativeai as genai

            gemini_key = os.environ.get("GEMINI_API_KEY") or config.get("gemini_api", {}).get("api_key", "")
            if not gemini_key:
                os.unlink(tmp_path)
                return {"status": "error", "message": "GEMINI_API_KEY not configured"}

            genai.configure(api_key=gemini_key)

            # Upload video file — retry once after 5s per spec
            video_file = None
            last_upload_error = None
            for attempt in range(2):
                try:
                    video_file = genai.upload_file(tmp_path, mime_type="video/mp4")
                    break
                except Exception as upload_err:
                    last_upload_error = upload_err
                    if attempt == 0:
                        time.sleep(5)
                    else:
                        os.unlink(tmp_path)
                        return {"status": "error", "message": f"We couldn't process this video automatically — you can still generate a brief by describing it manually. ({str(upload_err)})"}

            if video_file is None:
                os.unlink(tmp_path)
                return {"status": "error", "message": f"Video upload failed: {last_upload_error}"}

            # Poll until processed
            max_wait = 120  # seconds
            waited = 0
            while video_file.state.name == "PROCESSING" and waited < max_wait:
                time.sleep(3)
                waited += 3
                video_file = genai.get_file(video_file.name)

            if video_file.state.name == "FAILED":
                os.unlink(tmp_path)
                return {"status": "error", "message": "Gemini failed to process the video"}

            # Generate analysis. Pass the actual video length (when Meta exposed
            # it) so the storyboard scales to the real ad — without this, the
            # default prompt produced 25-second storyboards regardless of length.
            model = genai.GenerativeModel("gemini-2.5-flash")
            analysis_prompt = _build_master_analysis_prompt(video_info.get("video_length"))
            response = model.generate_content([video_file, analysis_prompt])

            # Parse response
            response_text = response.text.strip()
            # Strip markdown fences if present
            if response_text.startswith("```"):
                response_text = response_text.split("\n", 1)[1] if "\n" in response_text else response_text[3:]
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            response_text = response_text.strip()

            analysis = json.loads(response_text)

            # Cleanup
            os.unlink(tmp_path)
            try:
                genai.delete_file(video_file.name)
            except Exception:
                pass

            return {
                "status": "success",
                "creative_id": creative_id,
                "video_id": video_id,
                "creative_name": video_info.get("creative_name", ""),
                "analysis": analysis,
            }

        except ImportError:
            os.unlink(tmp_path)
            return {"status": "error", "message": "google-generativeai package not installed. Run: pip install google-generativeai"}
        except json.JSONDecodeError as e:
            os.unlink(tmp_path)
            return {"status": "error", "message": f"Gemini returned invalid JSON: {str(e)}"}
        except Exception as e:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            return {"status": "error", "message": f"Gemini analysis failed: {str(e)}"}

    except Exception as e:
        return {"status": "error", "message": f"Clone analyze failed: {str(e)}"}


class LocalVideoAnalyzeRequest(BaseModel):
    video_path: str
    creative_name: Optional[str] = "Demo Ad"


@app.post("/meta/creatives/clone-analyze-local")
def clone_analyze_local_video(request: LocalVideoAnalyzeRequest):
    """Analyze a LOCAL video file using Gemini 2.5 Flash.

    For demo/testing: place any MP4 file in the agent directory and pass
    its path to this endpoint. Example: {"video_path": "demo_ad.mp4"}

    Usage:
      1. Place a video file in the agent/ directory (e.g. demo_ad.mp4)
      2. Call: POST /meta/creatives/clone-analyze-local
         Body: {"video_path": "demo_ad.mp4", "creative_name": "My Demo Ad"}
    """
    video_path = request.video_path

    # Resolve relative paths from agent directory
    if not os.path.isabs(video_path):
        video_path = os.path.join(str(Path(__file__).parent.parent), video_path)

    if not os.path.exists(video_path):
        return {"status": "error", "message": f"Video file not found: {video_path}. Place an MP4 file in the agent/ directory."}

    file_size = os.path.getsize(video_path)
    # Spec: Reject videos > 10 minutes (~200MB at typical Meta ad encoding)
    if file_size > 200 * 1024 * 1024:
        return {"status": "error", "message": "This video is too long to analyse. Clone Winner works best with ads under 3 minutes."}

    try:
        import google.generativeai as genai

        gemini_key = os.environ.get("GEMINI_API_KEY") or config.get("gemini_api", {}).get("api_key", "")
        if not gemini_key:
            return {"status": "error", "message": "GEMINI_API_KEY not configured. Set it as an environment variable."}

        genai.configure(api_key=gemini_key)

        # Upload video file — retry once after 5s per spec
        video_file = None
        for attempt in range(2):
            try:
                video_file = genai.upload_file(video_path, mime_type="video/mp4")
                break
            except Exception as upload_err:
                if attempt == 0:
                    time.sleep(5)
                else:
                    return {"status": "error", "message": f"We couldn't process this video automatically — you can still generate a brief by describing it manually. ({str(upload_err)})"}

        max_wait = 120
        waited = 0
        while video_file.state.name == "PROCESSING" and waited < max_wait:
            time.sleep(3)
            waited += 3
            video_file = genai.get_file(video_file.name)

        if video_file.state.name == "FAILED":
            return {"status": "error", "message": "Gemini failed to process the video"}

        # Local video path: we don't have a Meta-reported length, so let
        # Gemini detect the duration itself (the prompt instructs it).
        model = genai.GenerativeModel("gemini-2.5-flash")
        response = model.generate_content([video_file, _build_master_analysis_prompt(None)])

        response_text = response.text.strip()
        if response_text.startswith("```"):
            response_text = response_text.split("\n", 1)[1] if "\n" in response_text else response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        response_text = response_text.strip()

        analysis = json.loads(response_text)

        try:
            genai.delete_file(video_file.name)
        except Exception:
            pass

        return {
            "status": "success",
            "creative_id": "local_demo",
            "video_id": "local",
            "creative_name": request.creative_name,
            "analysis": analysis,
        }

    except ImportError:
        return {"status": "error", "message": "google-generativeai not installed. Run: pip install google-generativeai"}
    except json.JSONDecodeError as e:
        return {"status": "error", "message": f"Gemini returned invalid JSON: {str(e)}"}
    except Exception as e:
        return {"status": "error", "message": f"Gemini analysis failed: {str(e)}"}


class AdSetStatusUpdate(BaseModel):
    status: str

class AdSetBudgetUpdate(BaseModel):
    daily_budget: Optional[int] = None
    lifetime_budget: Optional[int] = None

@app.put("/meta/adsets/{adset_id}/status")
def update_adset_status(adset_id: str, status_data: AdSetStatusUpdate):
    """Update the status of an ad set"""
    try:        
        status = status_data.status
        if not status:
            return {"status": "error", "message": "Status is required"}
        
        if status not in ["ACTIVE", "PAUSED", "ARCHIVED"]:
            return {"status": "error", "message": "Invalid status. Must be ACTIVE, PAUSED, or ARCHIVED"}
        
        # Update the ad set status
        result = meta_client.update_ad_set_status(adset_id, status)
        
        return {
            "status": "success",
            "message": f"Ad set {adset_id} status updated to {status}",
            "adset_id": adset_id,
            "new_status": status,
            "data": result
        }
        
    except Exception as e:
        return _format_mutation_error(e, f"update ad set {adset_id} status")


class AdStatusUpdate(BaseModel):
    status: str


@app.put("/meta/ads/{ad_id}/status")
def update_ad_status_endpoint(ad_id: str, status_data: AdStatusUpdate):
    """Update the status of a single Ad (not an ad set).

    Ref: https://developers.facebook.com/docs/marketing-api/reference/adgroup#Updating
    """
    try:
        status = status_data.status
        if status not in ("ACTIVE", "PAUSED", "ARCHIVED", "DELETED"):
            return {"status": "error", "message": "Invalid status. Must be ACTIVE, PAUSED, ARCHIVED, or DELETED"}

        result = meta_client.update_ad_status(ad_id, status)
        return {
            "status": "success",
            "message": f"Ad {ad_id} status updated to {status}",
            "ad_id": ad_id,
            "new_status": status,
            "data": result,
        }
    except Exception as e:
        return _format_mutation_error(e, f"update ad {ad_id} status")


@app.put("/meta/adsets/{adset_id}/budget")
def update_adset_budget(adset_id: str, budget_data: AdSetBudgetUpdate):
    """Update the budget of an ad set"""
    try:
        if budget_data.daily_budget is None and budget_data.lifetime_budget is None:
            return {"status": "error", "message": "At least one budget type (daily_budget or lifetime_budget) is required"}

        # Update the ad set budget
        result = meta_client.update_ad_set_budget(
            adset_id,
            daily_budget=budget_data.daily_budget,
            lifetime_budget=budget_data.lifetime_budget
        )

        return {
            "status": "success",
            "message": f"Ad set {adset_id} budget updated successfully",
            "adset_id": adset_id,
            "daily_budget": budget_data.daily_budget,
            "lifetime_budget": budget_data.lifetime_budget,
            "data": result
        }

    except Exception as e:
        return _format_mutation_error(e, f"update ad set {adset_id} budget")

@app.post("/meta/adsets/{adset_id}/duplicate")
def duplicate_adset_endpoint(adset_id: str, body: Dict[str, Any] = None):
    """Duplicate an ad set via Meta's /copies endpoint.

    Body (optional):
      - status_option: "PAUSED" (default) | "ACTIVE" | "INHERITED_FROM_SOURCE"
      - deep_copy: bool (default False — Meta caps sync deep copies at <3 sub-objects)
      - rename_suffix: appended to the new ad set's name
    """
    body = body or {}
    try:
        status_option = body.get("status_option", "PAUSED")
        deep_copy = bool(body.get("deep_copy", False))
        rename_options = None
        if body.get("rename_suffix"):
            rename_options = {"rename_strategy": "DEEP_RENAME", "rename_suffix": body["rename_suffix"]}
        result = meta_client.duplicate_ad_set(
            adset_id,
            status_option=status_option,
            deep_copy=deep_copy,
            rename_options=rename_options,
        )
        return {
            "status": "success",
            "message": f"Ad set {adset_id} duplicated. Review and activate in Ads Manager.",
            "adset_id": adset_id,
            "deep_copy": deep_copy,
            "data": result,
        }
    except Exception as e:
        return _format_mutation_error(e, f"duplicate ad set {adset_id}")


@app.put("/meta/adsets/{adset_id}/frequency-cap")
def update_adset_frequency_cap(adset_id: str, body: Dict[str, Any]):
    """Set a frequency cap on an ad set.

    Body:
      - max_frequency: int (required) — max impressions per interval
      - interval_days: int (default 7)
      - event: "IMPRESSIONS" (default) or "REACH"
    """
    try:
        max_frequency = body.get("max_frequency")
        if not max_frequency or int(max_frequency) < 1:
            return {"status": "error", "message": "max_frequency must be >= 1"}
        interval_days = int(body.get("interval_days", 7))
        event = body.get("event", "IMPRESSIONS")
        result = meta_client.update_ad_set_frequency_cap(adset_id, int(max_frequency), interval_days, event)
        return {
            "status": "success",
            "message": f"Ad set {adset_id} frequency cap set to {max_frequency}/{interval_days}d",
            "adset_id": adset_id,
            "data": result,
        }
    except Exception as e:
        return _format_mutation_error(e, f"set frequency cap on ad set {adset_id}")


@app.put("/meta/adsets/{adset_id}/bid-strategy")
def update_adset_bid_strategy(adset_id: str, body: Dict[str, Any]):
    """Change an ad set's bid strategy.

    Body:
      - bid_strategy: "LOWEST_COST_WITHOUT_CAP" | "LOWEST_COST_WITH_BID_CAP" | "COST_CAP" | "LOWEST_COST_WITH_MIN_ROAS"
      - bid_amount: int (cents for cost/bid caps; thousandths for ROAS multiplier)
    """
    try:
        bid_strategy = body.get("bid_strategy")
        bid_amount = body.get("bid_amount")
        if not bid_strategy:
            return {"status": "error", "message": "bid_strategy is required"}
        result = meta_client.update_ad_set_bid_strategy(adset_id, bid_strategy, int(bid_amount) if bid_amount else None)
        return {
            "status": "success",
            "message": f"Ad set {adset_id} bid strategy set to {bid_strategy}",
            "adset_id": adset_id,
            "data": result,
        }
    except Exception as e:
        return _format_mutation_error(e, f"set bid strategy on ad set {adset_id}")


def _format_mutation_error(exc: Exception, what: str) -> Dict[str, Any]:
    """Shared error-shape for mutation endpoints so the backend sees a
    consistent {status, message, error_details} payload.

    When Meta returns ``error_user_title`` / ``error_user_msg`` (the
    human-actionable message + remediation URL, e.g. ToS click-throughs),
    those are surfaced as separate keys so the frontend can render the
    real fix instead of a generic 'Permissions error'.
    """
    error_message = str(exc)
    error_details = error_message
    error_user_title = None
    error_user_msg = None
    error_code = None
    error_subcode = None
    if isinstance(exc, requests.exceptions.HTTPError) and hasattr(exc, "response"):
        try:
            error_response = exc.response.json()
            if "error" in error_response:
                info = error_response["error"]
                error_message = info.get("message", error_message)
                error_code = info.get("code")
                error_subcode = info.get("error_subcode")
                error_user_title = info.get("error_user_title")
                error_user_msg = info.get("error_user_msg")
                error_details = f"Meta API Error {error_code if error_code is not None else ''}: {info.get('message', '')}"
                if error_subcode is not None:
                    error_details += f" (Subcode: {error_subcode})"
        except Exception:
            error_details = exc.response.text if hasattr(exc.response, "text") else error_details
    result: Dict[str, Any] = {
        "status": "error",
        "message": f"Failed to {what}: {error_message}",
        "error_details": error_details,
    }
    if error_code is not None:
        result["error_code"] = error_code
    if error_subcode is not None:
        result["error_subcode"] = error_subcode
    if error_user_title:
        result["error_user_title"] = error_user_title
    if error_user_msg:
        result["error_user_msg"] = error_user_msg
    return result


@app.post("/meta/campaigns")
def create_meta_campaign(campaign_data: Dict[str, Any]):
    """Create a new Meta campaign"""
    try:
        name = campaign_data.get("name")
        objective = campaign_data.get("objective", "OUTCOME_TRAFFIC")
        status = campaign_data.get("status", "PAUSED")

        result = meta_client.create_campaign(name, objective, status)
        return {"status": "success", "data": result}
    except Exception as e:
        return {"status": "error", "message": f"Failed to create campaign: {str(e)}"}


# Meta Automated Rules API endpoints
class AutomatedRuleCreate(BaseModel):
    name: str
    evaluation_spec: Dict[str, Any]
    execution_spec: Dict[str, Any]
    schedule_spec: Optional[Dict[str, Any]] = None
    status: Optional[str] = "ENABLED"


@app.post("/meta/rules")
def create_automated_rule(rule_data: AutomatedRuleCreate):
    """Create an automated rule on Meta's servers

    This creates a rule that Meta will evaluate and execute automatically
    based on the schedule_spec (default: daily).

    IMPORTANT: Meta Automated Rules use filters within evaluation_spec to scope
    which entities the rule applies to. Required filters include:
    - entity_type: "AD", "ADSET", or "CAMPAIGN"
    - time_preset: "LAST_7D", "LAST_14D", "LAST_30D", "LIFETIME", etc.
    - campaign.id or adset.id: with IN operator to scope to specific campaigns/ad sets

    evaluation_spec example:
    {
        "evaluation_type": "SCHEDULE",
        "filters": [
            {"field": "entity_type", "operator": "EQUAL", "value": "ADSET"},
            {"field": "time_preset", "operator": "EQUAL", "value": "LAST_7D"},
            {"field": "campaign.id", "operator": "IN", "value": ["campaign_id_here"]},
            {"field": "spent", "operator": "GREATER_THAN", "value": 50}
        ]
    }

    execution_spec example:
    {
        "execution_type": "PAUSE"  # or "UNPAUSE", "CHANGE_BUDGET", "NOTIFICATION_ONLY"
    }

    Reference: https://developers.facebook.com/docs/marketing-api/reference/ad-rules-library/
    """
    try:
        result = meta_client.create_automated_rule(
            name=rule_data.name,
            evaluation_spec=rule_data.evaluation_spec,
            execution_spec=rule_data.execution_spec,
            schedule_spec=rule_data.schedule_spec,
            status=rule_data.status
        )

        return {
            "status": "success",
            "message": f"Automated rule '{rule_data.name}' created successfully",
            "data": result
        }

    except Exception as e:
        error_message = str(e)
        return {
            "status": "error",
            "message": f"Failed to create automated rule: {error_message}",
            "error_details": error_message
        }


@app.get("/meta/rules")
def get_automated_rules():
    """Get all automated rules for the ad account"""
    try:
        rules = meta_client.get_automated_rules()

        return {
            "status": "success",
            "rules": rules,
            "count": len(rules)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get automated rules: {str(e)}"
        }


@app.delete("/meta/rules/{rule_id}")
def delete_automated_rule(rule_id: str):
    """Delete an automated rule from Meta's servers"""
    try:
        result = meta_client.delete_automated_rule(rule_id)

        return {
            "status": "success",
            "message": f"Automated rule {rule_id} deleted successfully",
            "data": result
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to delete automated rule: {str(e)}"
        }


class RuleStatusUpdate(BaseModel):
    status: str  # ENABLED or DISABLED


@app.put("/meta/rules/{rule_id}/status")
def update_automated_rule_status(rule_id: str, status_data: RuleStatusUpdate):
    """Update the status of an automated rule on Meta's servers"""
    try:
        if status_data.status not in ['ENABLED', 'DISABLED']:
            return {"status": "error", "message": "Status must be ENABLED or DISABLED"}

        result = meta_client.update_automated_rule_status(rule_id, status_data.status)

        return {
            "status": "success",
            "message": f"Automated rule {rule_id} status updated to {status_data.status}",
            "data": result
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to update automated rule status: {str(e)}"
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)


