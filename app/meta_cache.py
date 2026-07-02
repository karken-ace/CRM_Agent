"""Persistent Mongo-backed cache for Meta Graph API responses (Layer 2).

Why this exists
---------------
The agent originally hit Meta on every dashboard render. Meta enforces a
per-token CPU/time budget; once the budget is exceeded each call adds 5+
seconds of mandatory throttle wait, and the limit doesn't recover while
calls keep coming. This module sits in front of Meta and serves identical
URLs from Mongo for a configurable TTL. First user pays the cost; everyone
else within the TTL window gets a sub-100ms response.

This is L2. The agent's existing in-process dict cache (meta_client.CACHE_TTL)
is L1 — fast same-process path. L2 survives agent restarts, is shared across
processes, and (via Mongo's TTL index) is automatically purged.

Cache key
---------
The full request URL with `access_token` and `appsecret_proof` query params
stripped — those vary per call but represent the same underlying request.

TTL
---
Caller passes the TTL when writing. The module enforces a Mongo TTL index
on `expires_at` so dead entries are automatically removed in the background.
"""
from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Default TTLs by endpoint family. The wiring layer (meta_client) decides
# which one to apply based on the URL path; defaults are conservative.
TTL_SHORT = 5 * 60       # campaigns, ad sets, ads — refresh every 5 min
TTL_MEDIUM = 30 * 60     # breakdowns, pixel quality, insights — 30 min
TTL_LONG = 60 * 60       # creative metadata, account info — 1 hour

_collection = None       # populated lazily by _get_collection()
_index_built = False


def _strip_volatile_params(url: str) -> str:
    """Remove query params that vary per-call but identify the same data.

    `access_token` and `appsecret_proof` are appended by every request and
    rotate independently. They're not part of the cache identity.
    """
    url = re.sub(r"[?&]access_token=[^&]*", "", url)
    url = re.sub(r"[?&]appsecret_proof=[^&]*", "", url)
    # If we removed the first param, the URL might now have ?& or end with ?
    url = url.replace("?&", "?")
    if url.endswith("?") or url.endswith("&"):
        url = url[:-1]
    return url


def _load_mongo_uri() -> Optional[str]:
    """Resolve MONGODB_URI from env or the backend's .env file."""
    uri = os.environ.get("MONGODB_URI")
    if uri:
        return uri.strip()
    # Fall back to backend/.env so we don't have to duplicate config
    env_path = "/home/CRM/backend/.env"
    if not os.path.isfile(env_path):
        return None
    try:
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("MONGODB_URI"):
                    _, _, val = line.partition("=")
                    val = val.strip().strip('"').strip("'")
                    return val or None
    except Exception:
        return None
    return None


def _get_collection():
    """Lazily connect to Mongo and ensure indexes."""
    global _collection, _index_built
    if _collection is not None:
        return _collection
    try:
        from pymongo import MongoClient, ASCENDING
    except ImportError:
        logger.warning("pymongo not installed; meta_cache disabled")
        return None
    uri = _load_mongo_uri()
    if not uri:
        logger.warning("MONGODB_URI not configured; meta_cache disabled")
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=2000)
        # ping to surface auth/connection errors early
        client.admin.command("ping")
        db = client.get_default_database()
        col = db["meta_cache"]
        if not _index_built:
            # Unique on url so writes upsert cleanly
            col.create_index([("url", ASCENDING)], unique=True)
            # TTL index — Mongo purges entries whose expires_at has passed
            col.create_index("expires_at", expireAfterSeconds=0)
            _index_built = True
        _collection = col
        return col
    except Exception as e:
        logger.warning(f"meta_cache: mongo connect failed: {e}")
        return None


def get(url: str) -> Optional[Dict[str, Any]]:
    """Look up a cached response. Returns the parsed JSON dict or None.

    Uses both the Mongo `expires_at` field and a wall-clock check (defensive
    — Mongo's TTL purger only runs every ~60s).
    """
    col = _get_collection()
    if col is None:
        return None
    try:
        key = _strip_volatile_params(url)
        doc = col.find_one({"url": key})
        if not doc:
            return None
        expires = doc.get("expires_at")
        # expires might be naive or tz-aware depending on driver version
        now = datetime.now(timezone.utc) if (expires and expires.tzinfo) else datetime.utcnow()
        if expires and expires <= now:
            return None
        return doc.get("data")
    except Exception as e:
        logger.warning(f"meta_cache.get failed for {url[:80]}: {e}")
        return None


def set(url: str, data: Any, ttl_seconds: int) -> None:
    """Upsert a cache entry with a per-entry TTL."""
    col = _get_collection()
    if col is None:
        return
    try:
        key = _strip_volatile_params(url)
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=ttl_seconds)
        col.update_one(
            {"url": key},
            {
                "$set": {
                    "url": key,
                    "data": data,
                    "fetched_at": now,
                    "expires_at": expires_at,
                    "ttl_seconds": ttl_seconds,
                }
            },
            upsert=True,
        )
    except Exception as e:
        logger.warning(f"meta_cache.set failed for {url[:80]}: {e}")


def ttl_for_endpoint(endpoint: str) -> int:
    """Pick a TTL based on the endpoint shape.

    Short for things that change with active spend; medium for derived
    breakdowns; long for stable creative/account metadata.
    """
    e = endpoint.lower()
    # Stable / slow-changing
    if "?fields=" in e and ("/me" in e or "act_" not in e or "thumbnails" in e or "creative" in e):
        return TTL_LONG
    # Breakdowns and pixel quality
    if "breakdown" in e or "pixel" in e or "insights" in e or "time_comparison" in e or "comparison" in e:
        return TTL_MEDIUM
    # Default to short for live ad-management surfaces
    return TTL_SHORT


def invalidate_prefix(prefix: str) -> int:
    """Remove all cached entries whose URL contains `prefix`.

    Use when a mutation (pause, budget change) makes upstream data stale.
    Returns the number of entries removed.
    """
    col = _get_collection()
    if col is None:
        return 0
    try:
        result = col.delete_many({"url": {"$regex": re.escape(prefix)}})
        return result.deleted_count
    except Exception as e:
        logger.warning(f"meta_cache.invalidate_prefix failed for {prefix}: {e}")
        return 0
