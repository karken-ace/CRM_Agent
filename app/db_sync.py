"""Layer 3 sync writer: pulls hierarchical Meta data and upserts campaigns,
ad sets, and ads into Mongo so the dashboard can read from there instead of
hitting the agent live on every page load.

Design notes
------------
- One sync run produces: 1 hierarchical fetch from Meta (already cached by
  L1/L2 in meta_client), then 1 upsert per campaign/adset/ad.
- Writes are scoped by ``agent_id`` so multi-tenant queries are clean.
- Uses Meta's ``id`` as the Mongo ``id`` (same identifier; we don't need a
  separate primary key).
- ``last_synced_at`` is bumped on every write so the dashboard can show
  "synced X min ago" and decide when to re-trigger.
- Idempotent — re-running a sync overwrites the previous row and is safe.
- Failures bubble up to the caller (the scheduled loop) which logs them
  and continues — one failed sync doesn't block the next one.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_db = None


def _load_mongo_uri() -> Optional[str]:
    """Same fallback chain as meta_cache: env first, then backend's .env."""
    uri = os.environ.get("MONGODB_URI")
    if uri:
        return uri.strip()
    env_path = "/home/CRM/backend/.env"
    if not os.path.isfile(env_path):
        return None
    try:
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("MONGODB_URI"):
                    _, _, val = line.partition("=")
                    return val.strip().strip('"').strip("'") or None
    except Exception:
        return None
    return None


def _get_db():
    """Lazy-connect to Mongo and return the default database."""
    global _db
    if _db is not None:
        return _db
    try:
        from pymongo import MongoClient
    except ImportError:
        logger.warning("pymongo not installed; db_sync disabled")
        return None
    uri = _load_mongo_uri()
    if not uri:
        logger.warning("MONGODB_URI not configured; db_sync disabled")
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
        _db = client.get_default_database()
        return _db
    except Exception as e:
        logger.warning(f"db_sync: mongo connect failed: {e}")
        return None


def _resolve_agent_record(agent_id: str) -> Optional[Dict[str, Any]]:
    """Look up the Mongo Agent row to get user_id + ad_account_id.

    Sync writes must include user_id so existing auth-scoped reads work.
    """
    db = _get_db()
    if db is None:
        return None
    return db["agents"].find_one({"id": agent_id})


def _parse_iso(ts: Any) -> Optional[datetime]:
    """Convert Meta's ISO timestamps to datetimes; tolerant of missing values."""
    if not ts:
        return None
    try:
        # Meta sometimes returns "2026-04-14T05:54:35-0700"; normalize
        ts = ts.replace("+0000", "+00:00")
        if len(ts) >= 5 and (ts[-5] == "+" or ts[-5] == "-") and ts[-3] != ":":
            ts = ts[:-2] + ":" + ts[-2:]
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def upsert_hierarchical(agent_id: str, campaigns: List[Dict[str, Any]]) -> Dict[str, int]:
    """Upsert a full hierarchical fetch (campaigns -> ad_sets -> ads) into
    Mongo. Returns a dict with counts.

    The shape of `campaigns` matches what /meta/campaigns/hierarchical returns:
        [{ id, name, status, ..., ad_sets: [
            { id, name, status, ..., ads: [{ id, name, status, ... }] }
        ]}]
    """
    db = _get_db()
    if db is None:
        return {"campaigns": 0, "ad_sets": 0, "ads": 0}

    agent = _resolve_agent_record(agent_id)
    if not agent:
        logger.warning(f"db_sync: agent {agent_id} not found in Mongo, skipping sync")
        return {"campaigns": 0, "ad_sets": 0, "ads": 0}

    user_id = agent.get("user_id", "")
    ad_account_id = agent.get("ad_account_id") or ""

    now = datetime.utcnow()
    n_campaigns = n_adsets = n_ads = 0

    for c in campaigns or []:
        cid = c.get("id")
        if not cid:
            continue
        db["campaigns"].update_one(
            {"id": cid},
            {
                "$set": {
                    "id": cid,
                    "agent_id": agent_id,
                    "user_id": user_id,
                    "ad_account_id": ad_account_id,
                    "meta_id": cid,
                    "name": c.get("name") or "",
                    "status": c.get("status") or "",
                    "effective_status": c.get("effective_status"),
                    "objective": c.get("objective"),
                    "daily_budget": str(c.get("daily_budget")) if c.get("daily_budget") is not None else None,
                    "lifetime_budget": str(c.get("lifetime_budget")) if c.get("lifetime_budget") is not None else None,
                    "created_time": _parse_iso(c.get("created_time")),
                    "updated_time": _parse_iso(c.get("updated_time")),
                    "performance_metrics": c.get("performance_metrics") or {},
                    "raw": {k: v for k, v in c.items() if k not in ("ad_sets",)},
                    "last_synced_at": now,
                }
            },
            upsert=True,
        )
        n_campaigns += 1

        for a in c.get("ad_sets") or []:
            aid = a.get("id")
            if not aid:
                continue
            db["adsets"].update_one(
                {"id": aid},
                {
                    "$set": {
                        "id": aid,
                        "agent_id": agent_id,
                        "user_id": user_id,
                        "ad_account_id": ad_account_id,
                        "meta_id": aid,
                        "campaign_id": cid,
                        "name": a.get("name") or "",
                        "status": a.get("status") or "",
                        "effective_status": a.get("effective_status"),
                        "optimization_goal": a.get("optimization_goal"),
                        "daily_budget": str(a.get("daily_budget")) if a.get("daily_budget") is not None else None,
                        "lifetime_budget": str(a.get("lifetime_budget")) if a.get("lifetime_budget") is not None else None,
                        "bid_strategy": a.get("bid_strategy"),
                        "pacing_type": a.get("pacing_type"),
                        "targeting": a.get("targeting"),
                        "created_time": _parse_iso(a.get("created_time")),
                        "updated_time": _parse_iso(a.get("updated_time")),
                        "performance_metrics": a.get("performance_metrics") or {},
                        "raw": {k: v for k, v in a.items() if k not in ("ads",)},
                        "last_synced_at": now,
                    }
                },
                upsert=True,
            )
            n_adsets += 1

            for ad in a.get("ads") or []:
                ad_id = ad.get("id")
                if not ad_id:
                    continue
                db["ads"].update_one(
                    {"id": ad_id},
                    {
                        "$set": {
                            "id": ad_id,
                            "agent_id": agent_id,
                            "user_id": user_id,
                            "ad_account_id": ad_account_id,
                            "meta_id": ad_id,
                            "ad_set_id": aid,
                            "campaign_id": cid,
                            "name": ad.get("name") or "",
                            "status": ad.get("status") or "",
                            "effective_status": ad.get("effective_status"),
                            "creative": ad.get("creative"),
                            "created_time": _parse_iso(ad.get("created_time")),
                            "updated_time": _parse_iso(ad.get("updated_time")),
                            "performance_metrics": ad.get("performance_metrics") or {},
                            "raw": ad,
                            "last_synced_at": now,
                        }
                    },
                    upsert=True,
                )
                n_ads += 1

    # Clean up stale entities — but only for scopes the sync actually
    # populated this cycle. If the hierarchical fetch returned partial data
    # (e.g. campaigns came back but the nested ad_sets/ads fetch was
    # rate-limited and yielded an empty list), we must NOT wipe the
    # existing rows for those scopes — that would erase valid data on every
    # rate-limited sync. Only clean a scope when we wrote at least one fresh
    # row for it, indicating Meta actually returned data at that scope.
    stale = type("", (), {"deleted_count": 0})()
    stale_as = type("", (), {"deleted_count": 0})()
    stale_ads = type("", (), {"deleted_count": 0})()
    if n_campaigns > 0:
        stale = db["campaigns"].delete_many({"agent_id": agent_id, "last_synced_at": {"$lt": now}})
    if n_adsets > 0:
        stale_as = db["adsets"].delete_many({"agent_id": agent_id, "last_synced_at": {"$lt": now}})
    if n_ads > 0:
        stale_ads = db["ads"].delete_many({"agent_id": agent_id, "last_synced_at": {"$lt": now}})

    logger.info(
        f"db_sync: upserted {n_campaigns} campaigns, {n_adsets} ad sets, {n_ads} ads "
        f"for agent {agent_id}; removed stale: campaigns={stale.deleted_count} "
        f"ad_sets={stale_as.deleted_count} ads={stale_ads.deleted_count}"
    )
    return {
        "campaigns": n_campaigns,
        "ad_sets": n_adsets,
        "ads": n_ads,
        "removed": {
            "campaigns": stale.deleted_count,
            "ad_sets": stale_as.deleted_count,
            "ads": stale_ads.deleted_count,
        },
    }


def get_last_sync_for_agent(agent_id: str) -> Optional[datetime]:
    """Return the most recent `last_synced_at` across the agent's collections.

    Used by the read endpoints to decide whether Mongo is fresh enough to
    serve, and by the topbar to render "synced X min ago".
    """
    db = _get_db()
    if db is None:
        return None
    most_recent: Optional[datetime] = None
    for col in ("campaigns", "adsets", "ads"):
        doc = db[col].find_one(
            {"agent_id": agent_id},
            {"last_synced_at": 1},
            sort=[("last_synced_at", -1)],
        )
        if doc and doc.get("last_synced_at"):
            ts = doc["last_synced_at"]
            if most_recent is None or ts > most_recent:
                most_recent = ts
    return most_recent
