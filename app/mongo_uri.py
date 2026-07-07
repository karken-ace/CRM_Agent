"""Resolve the central MongoDB URI used by the L2 cache and L3 sync.

Resolution order: ``MONGODB_URI`` env var, then ``mongodb.uri`` in
``meta_config.json`` (same search paths as the rest of the agent config).
When neither is set this logs an ERROR — without Mongo the agent still runs,
but the CRM receives no campaign data from it, so the failure must be loud.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_CONFIG_PATHS = [
    "/app/config/meta_config.json",  # Docker
    str(Path(__file__).parent.parent / "config" / "meta_config.json"),  # Local
    "config/meta_config.json",  # Current directory
]


def load_mongo_uri() -> Optional[str]:
    uri = os.environ.get("MONGODB_URI")
    if uri:
        return uri.strip()

    for path in _CONFIG_PATHS:
        try:
            with open(path, "r") as f:
                val = (json.load(f).get("mongodb") or {}).get("uri")
                if val:
                    return str(val).strip()
        except FileNotFoundError:
            continue
        except Exception as e:  # malformed JSON etc. — keep trying other paths
            logger.warning(f"mongo_uri: failed reading {path}: {e}")

    logger.error(
        "MongoDB URI is not configured (set env MONGODB_URI or mongodb.uri in "
        "meta_config.json). Meta cache and Mongo sync are DISABLED — the CRM "
        "will NOT receive campaign data from this agent."
    )
    return None
