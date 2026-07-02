import json
import hmac
import hashlib
import time
import requests
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path

# L2 persistent cache (Mongo). Falls back to no-op silently if pymongo or
# MONGODB_URI isn't available, so the agent still works without it.
try:
    from app import meta_cache as _meta_cache
except ImportError:
    try:
        import meta_cache as _meta_cache  # when imported from agent/app/ directly
    except ImportError:
        _meta_cache = None

logger = logging.getLogger(__name__)

# Rate limit constants
# Meta rate limits are sustained — sleeping 60s+ rarely clears them, it just
# blows past upstream HTTP timeouts and stacks multi-minute hangs on user-
# initiated requests. Fail fast; let the caller (or scheduled sync) retry later.
MAX_RETRIES = 1             # one quick retry, then surface the error
INITIAL_BACKOFF = 5         # seconds between retry attempts
BACKOFF_MULTIPLIER = 2      # 5s then 10s
CACHE_TTL = 1800            # 30 min — Layer 1 band-aid to reduce per-page-load Meta calls; Layer 2 adds the persistent Mongo cache that survives restarts
THROTTLE_THRESHOLD = 95     # only preemptively throttle when truly near the limit

class MetaAPIClient:
    """Client for interacting with Meta's Marketing API"""
    
    def __init__(self, config_path: str = "config/meta_config.json"):
        self.config = self._load_config(config_path)
        self.base_url = self.config["meta_api"]["base_url"]
        self.access_token = self.config["meta_api"]["access_token"]
        self.ad_account_id = self.config["meta_api"]["ad_account_id"]
        self.app_id = self.config["meta_api"]["app_id"]
        self.app_secret = self.config["meta_api"].get("app_secret", "")
        self.timeout = self.config["meta_api"]["timeout"]
        self.appsecret_proof = self._generate_appsecret_proof()
        self._cache: Dict[str, Any] = {}           # {url: {"data": ..., "ts": time.time()}}
        self._usage_pct: float = 0.0                # current API usage percentage from Meta headers

    def _generate_appsecret_proof(self) -> str:
        """Generate appsecret_proof for Meta API requests.

        Meta requires this HMAC-SHA256 hash of the access token using the app secret
        when 'Require App Secret' is enabled in app settings.
        """
        if not self.app_secret:
            return ""
        return hmac.new(
            self.app_secret.encode("utf-8"),
            self.access_token.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            config_file = Path(config_path)
            if not config_file.exists():
                raise FileNotFoundError(f"Config file not found: {config_path}")
            
            with open(config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise
    
    def _make_request(self, endpoint: str, method: str = "GET", data: Optional[Dict] = None) -> Dict[str, Any]:
        """Make a request to Meta's API with retry, backoff, and caching.

        - GET requests are cached for CACHE_TTL seconds to reduce duplicate calls.
        - Rate limit errors (code 80004) trigger exponential backoff retries.
        - Monitors x-business-use-case-usage header to proactively throttle.
        Ref: https://developers.facebook.com/docs/graph-api/overview/rate-limiting#ads-management
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}access_token={self.access_token}"
        if self.appsecret_proof:
            url = f"{url}&appsecret_proof={self.appsecret_proof}"

        # Cache check for GET requests — two layers:
        #   L1: in-process dict (fast; per-process; cleared on restart)
        #   L2: Mongo persistent cache (slower; shared across processes/restarts)
        # Reading L1 first keeps the hot path zero-network. On L1 miss we
        # consult L2; on L2 hit we backfill L1 so subsequent same-process
        # calls are again zero-network.
        if method == "GET":
            cached = self._cache.get(url)
            if cached and (time.time() - cached["ts"]) < CACHE_TTL:
                logger.debug(f"L1 cache hit for {endpoint}")
                return cached["data"]
            if _meta_cache is not None:
                l2 = _meta_cache.get(url)
                if l2 is not None:
                    logger.debug(f"L2 cache hit for {endpoint}")
                    self._cache[url] = {"data": l2, "ts": time.time()}
                    return l2

        # Proactive throttle: if Meta says we're near the limit, wait before calling
        if self._usage_pct >= THROTTLE_THRESHOLD:
            # Brief wait — long enough to let Meta's rolling window decay,
            # short enough not to stall user-facing requests.
            wait = 5
            logger.warning(f"API usage at {self._usage_pct:.0f}%, throttling for {wait}s")
            time.sleep(wait)

        headers = {"Content-Type": "application/json"}
        last_error = None

        for attempt in range(MAX_RETRIES + 1):
            try:
                if method == "GET":
                    response = requests.get(url, headers=headers, timeout=self.timeout)
                elif method == "POST":
                    response = requests.post(url, headers=headers, json=data, timeout=self.timeout)
                elif method == "PUT":
                    response = requests.put(url, headers=headers, json=data, timeout=self.timeout)
                elif method == "DELETE":
                    response = requests.delete(url, headers=headers, timeout=self.timeout)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                # Parse Meta's rate limit usage headers
                self._parse_usage_headers(response)

                # Check for rate limit error before raise_for_status
                if response.status_code in (400, 429):
                    try:
                        body = response.json()
                        error = body.get("error", {})
                        error_code = error.get("code")
                        error_subcode = error.get("error_subcode")

                        if error_code == 80004 or error_subcode == 2446079 or response.status_code == 429:
                            backoff = INITIAL_BACKOFF * (BACKOFF_MULTIPLIER ** attempt)
                            logger.warning(
                                f"Rate limited (code={error_code}, subcode={error_subcode}). "
                                f"Attempt {attempt + 1}/{MAX_RETRIES + 1}. Waiting {backoff}s..."
                            )
                            last_error = Exception(f"Rate limited: {error.get('message', 'Too many calls')}")
                            if attempt < MAX_RETRIES:
                                time.sleep(backoff)
                                continue
                            else:
                                raise last_error
                    except (ValueError, KeyError):
                        pass

                response.raise_for_status()
                result = response.json()

                # Cache successful GET responses to both layers. L2 TTL is
                # picked per endpoint family so frequently-changing surfaces
                # (campaigns, ad sets) refresh quickly while stable metadata
                # (creative, account info) sits longer.
                if method == "GET":
                    self._cache[url] = {"data": result, "ts": time.time()}
                    if _meta_cache is not None:
                        try:
                            ttl = _meta_cache.ttl_for_endpoint(endpoint)
                            _meta_cache.set(url, result, ttl)
                        except Exception as e:
                            logger.warning(f"L2 cache write failed for {endpoint}: {e}")

                return result

            except requests.exceptions.RequestException as e:
                last_error = e
                if attempt < MAX_RETRIES and (
                    isinstance(e, requests.exceptions.ConnectionError)
                    or (hasattr(e, 'response') and e.response is not None and e.response.status_code >= 500)
                ):
                    backoff = INITIAL_BACKOFF * (BACKOFF_MULTIPLIER ** attempt)
                    logger.warning(f"Request failed ({e}). Retrying in {backoff}s (attempt {attempt + 1}/{MAX_RETRIES + 1})")
                    time.sleep(backoff)
                    continue
                logger.error(f"API request failed: {e}")
                raise

        raise last_error or Exception("Max retries exceeded")

    def _parse_usage_headers(self, response: requests.Response) -> None:
        """Parse Meta's rate-limit usage headers.

        x-business-use-case-usage entries:
          - call_count       — raw integer count of calls in the rolling 1-hour window
          - total_cputime    — % of allowed CPU time used (0–100, can briefly exceed 100)
          - total_time       — % of allowed wall time used (0–100)
          - estimated_time_to_regain_access — seconds until back under limit
        Only the *_time fields are percentages. Mixing call_count via max() incorrectly
        treats a count of 125 as "125%" and triggers an endless proactive-throttle loop.
        Ref: https://developers.facebook.com/docs/graph-api/overview/rate-limiting#ads-management
        """
        usage_header = response.headers.get("x-business-use-case-usage") or response.headers.get("x-app-usage")
        if not usage_header:
            return
        try:
            usage_data = json.loads(usage_header)
            if isinstance(usage_data, dict):
                for _key, entries in usage_data.items():
                    if isinstance(entries, list) and entries:
                        entry = entries[0]
                        # Only the percentage fields are meaningful for proactive throttling.
                        cputime_pct = float(entry.get("total_cputime", 0) or 0)
                        time_pct = float(entry.get("total_time", 0) or 0)
                        self._usage_pct = max(cputime_pct, time_pct)
                        if self._usage_pct >= THROTTLE_THRESHOLD:
                            recover = entry.get("estimated_time_to_regain_access", 0)
                            logger.warning(
                                f"Meta API usage: cputime={cputime_pct:.0f}% time={time_pct:.0f}% "
                                f"(threshold {THROTTLE_THRESHOLD}%, recover in ~{recover}s)"
                            )
                        break
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

    def clear_cache(self) -> None:
        """Clear all cached API responses."""
        self._cache.clear()
    
    def get_app_info(self) -> Dict[str, Any]:
        """Get information about the Meta app"""
        endpoint = f"{self.app_id}"
        params = {"fields": "id,name"}
        # params = {"fields": "id,name,category,link,privacy_policy_url,terms_of_service_url"}
        response = self._make_request(f"{endpoint}?{'&'.join([f'{k}={v}' for k, v in params.items()])}")
        return response
    
    def get_ad_account_info(self) -> Dict[str, Any]:
        """Get information about the ad account"""
        endpoint = f"act_{self.ad_account_id}"
        params = {"fields": "id,account_id,currency,account_status,timezone_name"}
        response = self._make_request(f"{endpoint}?{'&'.join([f'{k}={v}' for k, v in params.items()])}")
        return response
    
    def get_campaigns(self, limit: int = 25) -> List[Dict[str, Any]]:
        """Get campaigns from the ad account"""
        endpoint = f"act_{self.ad_account_id}/campaigns"
        params = {"limit": limit, "fields": "id,name,status,objective,created_time,updated_time,daily_budget,lifetime_budget"}
        response = self._make_request(f"{endpoint}?{'&'.join([f'{k}={v}' for k, v in params.items()])}")
        return response.get("data", [])
    
    def get_insights(self, date_preset: str = "today") -> Dict[str, Any]:
        """Get insights/metrics for the ad account"""
        endpoint = f"act_{self.ad_account_id}/insights"
        params = {
            "date_preset": date_preset,
            "fields": "spend,impressions,clicks,ctr,cpc,cpm,reach,frequency"
        }
        response = self._make_request(f"{endpoint}?{'&'.join([f'{k}={v}' for k, v in params.items()])}")
        return response.get("data", [{}])[0] if response.get("data") else {}
    
    def get_ad_sets(self, campaign_id: str, limit: int = 500, date_preset: str = "last_30d") -> List[Dict[str, Any]]:
        """Get ad sets for a specific campaign with performance metrics.

        Uses the two-step pattern per Meta best practices:
        1. Fetch ad set structure (no insights)
        2. Fetch insights directly at adset level for this campaign
        3. Join by adset_id

        Routes both calls through ``_make_request`` (and ``_fetch_paginated``
        for the insights step) so the L1/L2 cache is consulted. The previous
        version called ``requests.get`` directly, bypassing the cache entirely
        — meaning every optimization run hit Meta even if the same data had
        been fetched moments earlier by the dashboard, which is the main
        reason the optimization endpoint returned empty recommendations
        whenever Meta was rate-limited.

        Args:
            campaign_id: The campaign ID to fetch ad sets for
            limit: Maximum number of ad sets to return per page
            date_preset: Date range for insights (e.g., 'last_30d', 'last_7d')

        Returns:
            List of ad sets with their performance_metrics populated
        """
        # Step 1: Fetch ad set structure (cached + paginated)
        fields = (
            "id,name,status,effective_status,daily_budget,lifetime_budget,"
            "optimization_goal,created_time,updated_time,targeting,bid_strategy,pacing_type"
        )
        try:
            ad_sets = self._fetch_paginated(
                f"{campaign_id}/adsets?limit={limit}&fields={fields}"
            )
            for ad_set in ad_sets:
                ad_set["performance_metrics"] = {}

            # Step 2: Fetch adset-level insights for this campaign and join
            try:
                insights_fields = (
                    "spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,"
                    "actions,action_values,cost_per_action_type,"
                    "video_30_sec_watched_actions,video_p25_watched_actions,video_p50_watched_actions,"
                    "video_p75_watched_actions,video_p95_watched_actions,"
                    "inline_link_clicks,outbound_clicks,adset_id"
                )
                insights_endpoint = (
                    f"{campaign_id}/insights"
                    f"?level=adset&date_preset={date_preset}&limit=500&fields={insights_fields}"
                )
                insights_rows = self._fetch_paginated(insights_endpoint)
                insights_map = {r.get("adset_id"): r for r in insights_rows if r.get("adset_id")}

                for ad_set in ad_sets:
                    if ad_set["id"] in insights_map:
                        ad_set["performance_metrics"] = insights_map[ad_set["id"]]
                logger.info(f"Joined {len(insights_map)} insights rows into {len(ad_sets)} ad sets for campaign {campaign_id}")
            except Exception as e:
                logger.warning(f"Failed to fetch ad set insights: {e}")

            return ad_sets

        except Exception as e:
            logger.error(f"Failed to get ad sets for campaign {campaign_id}: {e}")
            raise
    
    def get_ads(self, ad_set_id: str, limit: int = 25) -> List[Dict[str, Any]]:
        """Get ads for a specific ad set"""
        url = f"{self.base_url}/{ad_set_id}/ads"
        params = {
            "access_token": self.access_token,
            "limit": limit,
            "fields": "id,name,status,effective_status,creative,created_time,updated_time"
        }
        if self.appsecret_proof:
            params["appsecret_proof"] = self.appsecret_proof

        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json().get("data", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get ads for ad set {ad_set_id}: {e}")
            raise
    
    def create_campaign(self, name: str, objective: str, status: str = "PAUSED") -> Dict[str, Any]:
        """Create a new campaign"""
        endpoint = f"act_{self.ad_account_id}/campaigns"
        data = {
            "name": name,
            "objective": objective,
            "status": status
        }
        return self._make_request(endpoint, method="POST", data=data)
    
    def _batch_fetch_by_ids(self, ids: List[str], fields: str, batch_size: int = 50, extra_params: str = "") -> Dict[str, Dict[str, Any]]:
        """Batch-fetch multiple objects by IDs using ?ids= parameter.

        Ref: https://developers.facebook.com/docs/graph-api/using-graph-api/#multiid

        Args:
            ids: List of object IDs to fetch
            fields: Comma-separated field names
            batch_size: Max IDs per request (Meta limit is 50)
            extra_params: Additional query params (e.g. "thumbnail_width=400&thumbnail_height=400")

        Returns:
            Dict mapping object_id -> field data
        """
        result: Dict[str, Dict[str, Any]] = {}

        for i in range(0, len(ids), batch_size):
            batch = ids[i:i + batch_size]
            ids_param = ",".join(batch)
            try:
                url = f"{self.base_url}/?ids={ids_param}&fields={fields}"
                if extra_params:
                    url = f"{url}&{extra_params}"
                separator = "&"
                url = f"{url}{separator}access_token={self.access_token}"
                if self.appsecret_proof:
                    url = f"{url}&appsecret_proof={self.appsecret_proof}"

                response = requests.get(url, headers={"Content-Type": "application/json"}, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()

                if isinstance(data, dict):
                    for oid, obj in data.items():
                        if isinstance(obj, dict) and not obj.get("error"):
                            result[oid] = obj
            except Exception as e:
                logger.warning(f"Batch fetch failed (batch {i // batch_size + 1}): {e}")
                # Fallback: fetch individually
                for oid in batch:
                    try:
                        obj = self._make_request(f"{oid}?fields={fields}")
                        if isinstance(obj, dict) and obj.get("id"):
                            result[oid] = obj
                    except Exception:
                        pass

        return result

    def get_video_metadata(self, video_id: str) -> Dict[str, Any]:
        """Get the downloadable source URL and length for a Meta video.

        Ref: https://developers.facebook.com/docs/graph-api/reference/video

        Args:
            video_id: Meta video object ID

        Returns:
            { "source": str|None, "length": float|None } — both fields independent;
            length is returned even when source isn't (Meta exposes length on
            shared/Reels videos where the MP4 download is gated).
        """
        try:
            data = self._make_request(f"{video_id}?fields=source,length")
            src = data.get("source")
            length_raw = data.get("length")
            length: Optional[float] = None
            if length_raw is not None:
                try:
                    length = float(length_raw)
                except (TypeError, ValueError):
                    length = None
            # Meta only returns `source` when the token has download access (typically
            # videos uploaded to THIS ad account). Reels / IG-shared videos return
            # empty. Never fall back to `picture` — that's a JPG thumbnail, not a video.
            return {"source": src if src else None, "length": length}
        except Exception as e:
            logger.error(f"Failed to get video metadata for {video_id}: {e}")
            return {"source": None, "length": None}

    def get_creative_video_url(self, creative_id: str) -> Optional[Dict[str, Any]]:
        """Get the video source URL and metadata for a creative.

        Fetches the creative's video_id, then gets the video source URL.

        Args:
            creative_id: Meta creative ID

        Returns:
            Dict with {video_id, source_url, thumbnail_url} or None
        """
        try:
            fields = (
                "id,name,video_id,thumbnail_url,"
                "object_story_spec{video_data{message,title,call_to_action,video_id,image_url}},"
                "asset_feed_spec{videos{video_id,thumbnail_url},bodies{text},titles{text},"
                "descriptions{text},call_to_action_types}"
            )
            creative = self._make_request(f"{creative_id}?fields={fields}")
            # Check all three possible locations per Meta's AdCreative schema:
            # - top-level video_id (direct video creatives)
            # - object_story_spec.video_data.video_id (video page-post creatives)
            # - asset_feed_spec.videos[*].video_id (Advantage+ / SHARE creatives)
            video_id = creative.get("video_id")
            oss = creative.get("object_story_spec") or {}
            afs = creative.get("asset_feed_spec") or {}
            if not video_id:
                video_id = (oss.get("video_data") or {}).get("video_id")
            if not video_id:
                videos = afs.get("videos") or []
                video_id = next((v.get("video_id") for v in videos if v.get("video_id")), None)

            if not video_id:
                logger.warning(f"Creative {creative_id} has no video_id — may not be a video ad")
                return None

            # Collect ad copy so the analyzer has unique per-ad text even when
            # the MP4 source isn't accessible via our token.
            copy_bodies = [b.get("text") for b in (afs.get("bodies") or []) if b.get("text")]
            copy_titles = [t.get("text") for t in (afs.get("titles") or []) if t.get("text")]
            copy_descriptions = [d.get("text") for d in (afs.get("descriptions") or []) if d.get("text")]
            ctas = afs.get("call_to_action_types") or []
            vd_message = (oss.get("video_data") or {}).get("message")
            vd_title = (oss.get("video_data") or {}).get("title")
            if vd_message:
                copy_bodies.insert(0, vd_message)
            if vd_title:
                copy_titles.insert(0, vd_title)

            # Prefer the creative's primary thumbnail, fall back to the first
            # asset_feed_spec video thumbnail. This is also the first-frame image.
            thumb = creative.get("thumbnail_url")
            if not thumb:
                videos = afs.get("videos") or []
                thumb = next((v.get("thumbnail_url") for v in videos if v.get("thumbnail_url")), None)

            video_meta = self.get_video_metadata(video_id)
            return {
                "video_id": video_id,
                "source_url": video_meta.get("source"),
                "video_length": video_meta.get("length"),
                "thumbnail_url": thumb,
                "creative_name": creative.get("name", ""),
                "bodies": copy_bodies,
                "titles": copy_titles,
                "descriptions": copy_descriptions,
                "ctas": ctas,
            }
        except Exception as e:
            logger.error(f"Failed to get creative video URL for {creative_id}: {e}")
            return None

    def get_creative_thumbnails(self, creative_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Batch-fetch creative thumbnail URLs for a list of creative IDs.

        Uses a 5-step priority chain to get the best available thumbnail:
        1. effective_object_story_id → full_picture (most reliable, ~720px, all ad types)
        2. thumbnail_url with custom dimensions (quick but may be cropped/empty)
        3. object_story_spec deep extraction (video_data, link_data, photo_data)
        4. video_id → picture field (video-specific)
        5. image_hash → adimages URL resolution (for hash-only creatives)

        Reference:
        - https://stackoverflow.com/questions/27801279/how-do-i-retrieve-thumbnail-from-facebook-ad-api
        - https://developers.facebook.com/docs/marketing-api/reference/ad-creative/
        - https://developers.facebook.com/docs/graph-api/reference/video/thumbnails/

        Args:
            creative_ids: List of creative IDs to fetch thumbnails for

        Returns:
            Dict mapping creative_id -> {id, name, thumbnail_url, image_url, video_id, ...}
        """
        result: Dict[str, Dict[str, Any]] = {}

        # ── Step 1: Batch-fetch creative details (1 API call per 50 creatives) ──
        # Use batch ?ids= for speed, then enhance with individual calls only if needed.
        # Ref: https://stackoverflow.com/questions/27801279/how-do-i-retrieve-thumbnail-from-facebook-ad-api
        creative_fields = (
            "id,name,thumbnail_url,image_url,image_hash,video_id,"
            "object_type,effective_object_story_id,object_story_spec,asset_feed_spec"
        )

        # Batch fetch first (fast — 1 call for all creatives)
        result = self._batch_fetch_by_ids(creative_ids, creative_fields, extra_params="thumbnail_width=400&thumbnail_height=400")
        logger.info(f"Creative batch fetch: requested {len(creative_ids)}, got {len(result)}, with thumbnail_url: {sum(1 for d in result.values() if d.get('thumbnail_url'))}")

        # For any creatives missing from batch, try individual fetch
        missing = [cid for cid in creative_ids if cid not in result]
        if missing:
            logger.info(f"Fetching {len(missing)} missing creatives individually")
            for cid in missing:
                try:
                    data = self._make_request(f"{cid}?fields={creative_fields}&thumbnail_width=400&thumbnail_height=400")
                    if isinstance(data, dict) and data.get("id"):
                        result[cid] = data
                except Exception as e:
                    logger.warning(f"Individual creative fetch failed for {cid}: {e}")

        # ── Step 2: Use effective_object_story_id → full_picture (MOST RELIABLE) ──
        story_ids_map: Dict[str, str] = {}  # story_id -> creative_id
        for cid, data in result.items():
            if not self._has_good_thumbnail(data):
                story_id = data.get("effective_object_story_id")
                if story_id:
                    story_ids_map[story_id] = cid

        if story_ids_map:
            logger.info(f"Fetching full_picture for {len(story_ids_map)} creatives via effective_object_story_id")
            posts = self._batch_fetch_by_ids(list(story_ids_map.keys()), "id,full_picture")
            for sid, pdata in posts.items():
                if pdata.get("full_picture"):
                    cid = story_ids_map.get(sid)
                    if cid and cid in result:
                        result[cid]["thumbnail_url"] = pdata["full_picture"]

        # ── Step 3: Extract from object_story_spec ──
        for cid, data in result.items():
            if self._has_good_thumbnail(data):
                continue
            spec = data.get("object_story_spec", {})
            # Video ads
            vid_data = spec.get("video_data", {})
            if vid_data.get("image_url"):
                data["thumbnail_url"] = vid_data["image_url"]
                continue
            # Link ads
            lnk_data = spec.get("link_data", {})
            if lnk_data.get("picture"):
                data["thumbnail_url"] = lnk_data["picture"]
                continue
            if lnk_data.get("image_url"):
                data["thumbnail_url"] = lnk_data["image_url"]
                continue
            # Photo ads
            pho_data = spec.get("photo_data", {})
            if pho_data.get("url"):
                data["thumbnail_url"] = pho_data["url"]
                continue

        # ── Step 4: Fetch video picture for video creatives still missing ──
        video_ids_map: Dict[str, str] = {}
        for cid, data in result.items():
            if self._has_good_thumbnail(data):
                continue
            vid = data.get("video_id")
            if not vid:
                vid = data.get("object_story_spec", {}).get("video_data", {}).get("video_id")
            if vid:
                video_ids_map[vid] = cid

        if video_ids_map:
            logger.info(f"Fetching video picture for {len(video_ids_map)} video creatives")
            vid_results = self._batch_fetch_by_ids(list(video_ids_map.keys()), "id,picture")
            for vid, vdata in vid_results.items():
                if vdata.get("picture"):
                    cid = video_ids_map.get(vid)
                    if cid and cid in result:
                        result[cid]["thumbnail_url"] = vdata["picture"]

        # ── Step 5: Resolve image_hash via adimages endpoint ──
        hashes_map: Dict[str, str] = {}  # hash -> creative_id
        for cid, data in result.items():
            if self._has_good_thumbnail(data):
                continue
            img_hash = data.get("image_hash")
            if not img_hash:
                # Check object_story_spec for image_hash
                spec = data.get("object_story_spec", {})
                img_hash = (
                    spec.get("link_data", {}).get("image_hash") or
                    spec.get("photo_data", {}).get("image_hash") or
                    spec.get("video_data", {}).get("image_hash")
                )
            if img_hash:
                hashes_map[img_hash] = cid

        if hashes_map:
            logger.info(f"Resolving {len(hashes_map)} image hashes via adimages endpoint")
            try:
                hashes_json = json.dumps(list(hashes_map.keys()))
                endpoint = f"act_{self.ad_account_id}/adimages?hashes={hashes_json}&fields=url,url_128,permalink_url,hash"
                img_data = self._make_request(endpoint)
                images = img_data.get("data", {})
                # Response can be {hash: {url, ...}} or {data: [{hash, url, ...}]}
                if isinstance(images, dict):
                    for h, idata in images.items():
                        if isinstance(idata, dict) and (idata.get("url") or idata.get("permalink_url")):
                            cid = hashes_map.get(h)
                            if cid and cid in result:
                                result[cid]["thumbnail_url"] = idata.get("permalink_url") or idata.get("url")
                elif isinstance(images, list):
                    for idata in images:
                        h = idata.get("hash")
                        if h and h in hashes_map:
                            cid = hashes_map[h]
                            if cid in result:
                                result[cid]["thumbnail_url"] = idata.get("permalink_url") or idata.get("url")
            except Exception as e:
                logger.warning(f"Image hash resolution failed: {e}")

        # ── Video detection ──
        # A creative is a video if ANY of these signals are present:
        #   - object_type == "VIDEO"
        #   - top-level video_id
        #   - object_story_spec.video_data (direct video post)
        #   - asset_feed_spec.videos (Advantage+ / dynamic creative video assets)
        # Ref: https://developers.facebook.com/docs/marketing-api/reference/ad-creative/
        for cid, data in result.items():
            oss = data.get("object_story_spec", {}) or {}
            afs = data.get("asset_feed_spec", {}) or {}
            is_video = (
                data.get("object_type") == "VIDEO"
                or bool(data.get("video_id"))
                or bool(oss.get("video_data"))
                or bool(afs.get("videos"))
            )
            data["is_video"] = is_video
            # Backfill video_id from nested specs so downstream Clone Winner can use it
            if not data.get("video_id"):
                nested_vid = (
                    (oss.get("video_data") or {}).get("video_id")
                    or next(
                        (v.get("video_id") for v in (afs.get("videos") or []) if v.get("video_id")),
                        None,
                    )
                )
                if nested_vid:
                    data["video_id"] = nested_vid

        # ── Cleanup ──
        thumb_count = sum(1 for d in result.values() if self._has_good_thumbnail(d))
        video_count = sum(1 for d in result.values() if d.get("is_video"))
        logger.info(f"Thumbnail results: {thumb_count}/{len(result)} thumbnails, {video_count}/{len(result)} videos")
        for cid in result:
            result[cid].pop("object_story_spec", None)
            result[cid].pop("effective_object_story_id", None)
            result[cid].pop("asset_feed_spec", None)

        return result

    def _has_good_thumbnail(self, data: Dict[str, Any]) -> bool:
        """Check if creative data has a usable thumbnail URL."""
        url = data.get("thumbnail_url") or data.get("image_url")
        if not url or not isinstance(url, str):
            return False
        # Reject tiny default thumbnails (64x64)
        if "64x64" in url or len(url) < 20:
            return False
        return True

    def _fetch_paginated(self, endpoint: str, max_pages: int = 20) -> List[Dict[str, Any]]:
        """Fetch a paginated endpoint, following all `paging.next` cursors.

        Uses cursor-based pagination (after=...) instead of full next_url to avoid
        duplicate access_token from _make_request.
        """
        results: List[Dict[str, Any]] = []
        try:
            response = self._make_request(endpoint)
            results.extend(response.get("data", []))

            pages_fetched = 1
            separator = "&" if "?" in endpoint else "?"

            while pages_fetched < max_pages:
                paging = response.get("paging") if isinstance(response.get("paging"), dict) else None
                if not paging:
                    break
                after = paging.get("cursors", {}).get("after") if isinstance(paging.get("cursors"), dict) else None
                if not after:
                    break
                next_endpoint = f"{endpoint}{separator}after={after}"
                response = self._make_request(next_endpoint)
                page_data = response.get("data", [])
                if not page_data:
                    break
                results.extend(page_data)
                pages_fetched += 1
        except Exception as e:
            logger.warning(f"Paginated fetch failed for {endpoint}: {e}")
        return results

    def _fetch_insights_by_level(self, level: str, date_preset: str) -> Dict[str, Dict[str, Any]]:
        """Fetch insights at a specific level (campaign/adset/ad) and return
        a dict keyed by the object ID for easy joining.

        Ref: https://developers.facebook.com/docs/marketing-api/insights/best-practices/
        """
        # Valid video metrics per Meta v22 docs — hook rate uses 'video_view' from actions array
        insight_fields = (
            "spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,"
            "actions,action_values,cost_per_action_type,"
            "video_30_sec_watched_actions,video_p25_watched_actions,video_p50_watched_actions,"
            "video_p75_watched_actions,video_p95_watched_actions,"
            "inline_link_clicks,outbound_clicks,"
            "campaign_id,adset_id,ad_id"
        )
        endpoint = (
            f"act_{self.ad_account_id}/insights"
            f"?level={level}&date_preset={date_preset}&limit=500&fields={insight_fields}"
        )
        rows = self._fetch_paginated(endpoint)

        # Key each row by its native ID for client-side join
        id_field = f"{level}_id"
        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            oid = row.get(id_field)
            if oid:
                result[oid] = row
        logger.info(f"Fetched {len(result)} {level}-level insights rows (date_preset={date_preset})")
        return result

    def get_campaigns_detailed(self, limit: int = 100, date_preset: str = "last_30d") -> List[Dict[str, Any]]:
        """Get campaigns with detailed ad sets and ads using the two-step pattern
        recommended by Meta.

        Step 1: Fetch flat lists of campaigns, ad sets, and ads separately
        Step 2: Fetch insights at each level (campaign/adset/ad) via /insights edge
        Step 3: Assemble hierarchy client-side and join insights by ID
        Step 4: Batch-fetch creative thumbnails

        Ref: https://developers.facebook.com/docs/marketing-api/insights/best-practices/
        """
        try:
            # ── Step 1a: Fetch all campaigns (flat) ──
            camp_fields = "id,name,status,effective_status,objective,created_time,updated_time,daily_budget,lifetime_budget"
            camp_endpoint = f"act_{self.ad_account_id}/campaigns?fields={camp_fields}&limit={limit}"
            campaigns = self._fetch_paginated(camp_endpoint)
            logger.info(f"Fetched {len(campaigns)} campaigns")

            # ── Step 1b: Fetch all ad sets for this account (flat) ──
            adset_fields = (
                "id,name,status,effective_status,campaign_id,"
                "daily_budget,lifetime_budget,optimization_goal,"
                "created_time,updated_time,targeting,bid_strategy,pacing_type"
            )
            adset_endpoint = f"act_{self.ad_account_id}/adsets?fields={adset_fields}&limit=500"
            all_adsets = self._fetch_paginated(adset_endpoint)
            logger.info(f"Fetched {len(all_adsets)} ad sets")

            # ── Step 1c: Fetch all ads for this account (flat) ──
            ad_fields = "id,name,status,effective_status,campaign_id,adset_id,creative,created_time,updated_time"
            ad_endpoint = f"act_{self.ad_account_id}/ads?fields={ad_fields}&limit=500"
            all_ads = self._fetch_paginated(ad_endpoint)
            logger.info(f"Fetched {len(all_ads)} ads")

            # ── Step 2: Fetch insights at each level ──
            campaign_insights = self._fetch_insights_by_level("campaign", date_preset)
            adset_insights = self._fetch_insights_by_level("adset", date_preset)
            ad_insights = self._fetch_insights_by_level("ad", date_preset)

            # ── Step 3: Assemble hierarchy ──
            # Group ads by adset_id
            ads_by_adset: Dict[str, List[Dict[str, Any]]] = {}
            for ad in all_ads:
                aid = ad.get("adset_id")
                if not aid:
                    continue
                ad["performance_metrics"] = ad_insights.get(ad["id"], {})
                # Normalize creative field (may be dict with id, or just id)
                creative = ad.get("creative")
                if isinstance(creative, str):
                    ad["creative"] = {"id": creative}
                elif not isinstance(creative, dict):
                    ad["creative"] = {}
                ads_by_adset.setdefault(aid, []).append(ad)

            # Group adsets by campaign_id and attach ads
            adsets_by_campaign: Dict[str, List[Dict[str, Any]]] = {}
            for ad_set in all_adsets:
                cid = ad_set.get("campaign_id")
                if not cid:
                    continue
                ad_set["performance_metrics"] = adset_insights.get(ad_set["id"], {})
                ad_set["ads"] = ads_by_adset.get(ad_set["id"], [])
                adsets_by_campaign.setdefault(cid, []).append(ad_set)

            # Attach adsets + insights to campaigns
            for campaign in campaigns:
                campaign["performance_metrics"] = campaign_insights.get(campaign["id"], {})
                campaign["ad_sets"] = adsets_by_campaign.get(campaign["id"], [])

            # ── Step 4: Batch-fetch creative thumbnails ──
            try:
                creative_ids = set()
                for campaign in campaigns:
                    for ad_set in campaign.get("ad_sets", []):
                        for ad in ad_set.get("ads", []):
                            cid = ad.get("creative", {}).get("id")
                            if cid:
                                creative_ids.add(cid)

                logger.info(f"Found {len(creative_ids)} unique creative IDs to fetch thumbnails for")

                if creative_ids:
                    thumbnails = self.get_creative_thumbnails(list(creative_ids))
                    merged_count = 0
                    thumb_count = 0
                    for campaign in campaigns:
                        for ad_set in campaign.get("ad_sets", []):
                            for ad in ad_set.get("ads", []):
                                cid = ad.get("creative", {}).get("id")
                                if cid and cid in thumbnails:
                                    thumb_data = thumbnails[cid]
                                    ad["creative"] = {**ad.get("creative", {}), **thumb_data}
                                    merged_count += 1
                                    if thumb_data.get("thumbnail_url"):
                                        thumb_count += 1
                    logger.info(f"Merged thumbnails into {merged_count}/{len(creative_ids)} ads ({thumb_count} have thumbnail_url)")
            except Exception as e:
                logger.error(f"Failed to fetch creative thumbnails: {e}")

            return campaigns

        except Exception as e:
            logger.error(f"Failed to get detailed campaigns: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    def update_ad_status(self, ad_id: str, status: str) -> Dict[str, Any]:
        """Update the status of a single Ad (ACTIVE, PAUSED, ARCHIVED, DELETED).

        Meta endpoint is POST /{ad-id} with form-data status, same pattern as
        ad sets. Ref: https://developers.facebook.com/docs/marketing-api/reference/adgroup#Updating
        """
        return self._post_status_update(ad_id, status, entity_label="ad")

    def update_ad_set_status(self, ad_set_id: str, status: str) -> Dict[str, Any]:
        """Update the status of an ad set (ACTIVE, PAUSED, ARCHIVED).

        According to Meta's Marketing API documentation:
        https://developers.facebook.com/docs/marketing-api/reference/ad-campaign/
        Updates should use POST with form data, not PUT with JSON.
        """
        return self._post_status_update(ad_set_id, status, entity_label="ad set")

    def _post_status_update(self, entity_id: str, status: str, entity_label: str) -> Dict[str, Any]:
        endpoint = entity_id
        # Meta API requires form data, not JSON for updates
        data = {
            "status": status
        }
        
        try:
            # Meta API uses POST for updates, not PUT, and requires form data
            # According to Meta API docs, access_token can be in query params or form data
            url = f"{self.base_url}/{endpoint.lstrip('/')}"
            params = {
                "access_token": self.access_token
            }
            if self.appsecret_proof:
                params["appsecret_proof"] = self.appsecret_proof

            # Use POST with form data (data parameter) instead of PUT with JSON
            # Include access_token in query params as per Meta API documentation examples
            response = requests.post(url, params=params, data=data, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            # If Meta API returns an error, log it and re-raise
            error_msg = f"Meta API error updating {entity_label} {entity_id}: {e}"
            if hasattr(e, 'response') and e.response:
                try:
                    error_data = e.response.json()
                    if 'error' in error_data:
                        error_info = error_data['error']
                        error_msg = f"Meta API Error {error_info.get('code', '')}: {error_info.get('message', str(e))}"
                        logger.error(f"{error_msg} - Full response: {error_data}")
                except:
                    error_msg = f"{error_msg} - Response: {e.response.text}"
            logger.error(error_msg)
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def get_insights_with_breakdowns(
        self, 
        object_id: str, 
        breakdowns: List[str] = None,
        time_increment: str = None,
        date_preset: str = "last_30d"
    ) -> List[Dict[str, Any]]:
        """Get insights with breakdowns (e.g., by platform, time)
        
        Args:
            object_id: Campaign, AdSet, or Ad ID
            breakdowns: List of breakdown types (e.g., ['publisher_platform'], ['hourly_stats_aggregated_by_advertiser_time_zone'])
            time_increment: Time grouping ('1' for daily, 'all_days' for total)
            date_preset: Date range preset
            
        Returns:
            List of insights with breakdown dimensions
        """
        endpoint = f"{object_id}/insights"
        
        params = {
            "date_preset": date_preset,
            "fields": "spend,impressions,clicks,ctr,cpc,actions,action_values,reach,frequency",
        }
        
        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)
        
        if time_increment:
            params["time_increment"] = time_increment
            
        try:
            response = self._make_request(f"{endpoint}?{'&'.join([f'{k}={v}' for k, v in params.items()])}")
            return response.get("data", [])
        except Exception as e:
            logger.error(f"Failed to get insights with breakdowns: {e}")
            return []
    
    def get_platform_breakdown(self, object_id: str, date_preset: str = "last_30d") -> List[Dict[str, Any]]:
        """Get performance breakdown by platform (Facebook, Instagram, Audience Network, Messenger)
        
        Args:
            object_id: Campaign, AdSet, or Ad ID
            date_preset: Date range preset
            
        Returns:
            List of insights broken down by platform
        """
        return self.get_insights_with_breakdowns(
            object_id,
            breakdowns=["publisher_platform"],
            date_preset=date_preset
        )
    
    def get_hourly_breakdown(self, object_id: str, date_preset: str = "last_7d") -> List[Dict[str, Any]]:
        """Get performance breakdown by hour of day
        
        Args:
            object_id: Campaign, AdSet, or Ad ID
            date_preset: Date range preset
            
        Returns:
            List of insights broken down by hour
        """
        return self.get_insights_with_breakdowns(
            object_id,
            breakdowns=["hourly_stats_aggregated_by_advertiser_time_zone"],
            date_preset=date_preset
        )
    
    def get_time_comparison_insights(self, object_id: str) -> Dict[str, Any]:
        """Get insights for different time periods for comparison
        
        Fetches last 7 days and last 30 days for trend analysis
        
        Args:
            object_id: Campaign, AdSet, or Ad ID
            
        Returns:
            Dict with 'last_7d' and 'last_30d' insights
        """
        try:
            last_7d = self.get_insights_with_breakdowns(
                object_id,
                date_preset="last_7d"
            )
            last_30d = self.get_insights_with_breakdowns(
                object_id,
                date_preset="last_30d"
            )
            
            return {
                "last_7d": last_7d[0] if last_7d else {},
                "last_30d": last_30d[0] if last_30d else {}
            }
        except Exception as e:
            logger.error(f"Failed to get time comparison insights: {e}")
            return {"last_7d": {}, "last_30d": {}}
    
    def get_ad_comments(self, ad_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get comments for a specific ad
        
        Args:
            ad_id: Ad ID
            limit: Maximum number of comments to fetch
            
        Returns:
            List of comment objects
        """
        endpoint = f"{ad_id}/comments"
        params = {
            "limit": limit,
            "fields": "id,message,created_time,from,like_count,comment_count"
        }
        
        try:
            response = self._make_request(f"{endpoint}?{'&'.join([f'{k}={v}' for k, v in params.items()])}")
            comments = response.get("data", [])
            
            # Handle pagination for comments
            while "paging" in response and "next" in response["paging"] and len(comments) < limit:
                try:
                    next_url = response["paging"]["next"]
                    if "?" in next_url:
                        query_string = next_url.split("?")[1]
                        response = self._make_request(f"{endpoint}?{query_string}")
                        comments.extend(response.get("data", []))
                    else:
                        break
                except Exception as e:
                    logger.warning(f"Failed to fetch next page of comments: {e}")
                    break
            
            return comments[:limit]
        except Exception as e:
            logger.error(f"Failed to get ad comments: {e}")
            return []

    def update_ad_set_budget(self, ad_set_id: str, daily_budget: int = None, lifetime_budget: int = None) -> Dict[str, Any]:
        """Update the budget of an ad set (values in cents)

        Args:
            ad_set_id: The ID of the ad set to update
            daily_budget: New daily budget in cents (optional)
            lifetime_budget: New lifetime budget in cents (optional)

        Returns:
            Dict with the API response
        """
        url = f"{self.base_url}/{ad_set_id}"
        data = {}

        if daily_budget is not None:
            data["daily_budget"] = daily_budget
        if lifetime_budget is not None:
            data["lifetime_budget"] = lifetime_budget

        if not data:
            raise ValueError("At least one budget type (daily_budget or lifetime_budget) must be provided")

        try:
            params = {"access_token": self.access_token}
            if self.appsecret_proof:
                params["appsecret_proof"] = self.appsecret_proof

            response = requests.post(
                url,
                params=params,
                data=data,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            error_msg = f"Meta API error updating ad set budget {ad_set_id}: {e}"
            if hasattr(e, 'response') and e.response:
                try:
                    error_data = e.response.json()
                    if 'error' in error_data:
                        error_info = error_data['error']
                        error_msg = f"Meta API Error {error_info.get('code', '')}: {error_info.get('message', str(e))}"
                        logger.error(f"{error_msg} - Full response: {error_data}")
                except:
                    error_msg = f"{error_msg} - Response: {e.response.text}"
            logger.error(error_msg)
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def duplicate_ad_set(self, ad_set_id: str, status_option: str = "PAUSED", deep_copy: bool = False, end_time: str = None, rename_options: Dict[str, Any] = None) -> Dict[str, Any]:
        """Duplicate an ad set via Meta's /<adset_id>/copies endpoint.

        Defaults: PAUSED + shallow (no ads copied) — Meta limits sync copies to
        fewer than 3 sub-objects, so deep-copying an ad set with >2 ads fails
        with subcode 1885194. Use deep_copy=True only for tiny ad sets.

        Ref: https://developers.facebook.com/docs/marketing-api/reference/ad-set/copies/

        Args:
            ad_set_id: source ad set
            status_option: "PAUSED" (default) | "ACTIVE" | "INHERITED_FROM_SOURCE"
            deep_copy: True to copy the child ads (subject to Meta's <3 limit)
            end_time: optional end_time for the copy
            rename_options: {"rename_strategy":"DEEP_RENAME","rename_suffix":" - Copy"}

        Returns:
            Dict with copied_adset_ids and any copied ad ids.
        """
        if status_option not in {"PAUSED", "ACTIVE", "INHERITED_FROM_SOURCE"}:
            raise ValueError(f"status_option must be PAUSED, ACTIVE, or INHERITED_FROM_SOURCE; got {status_option}")

        url = f"{self.base_url}/{ad_set_id}/copies"
        params = {"access_token": self.access_token}
        if self.appsecret_proof:
            params["appsecret_proof"] = self.appsecret_proof

        data = {
            "status_option": status_option,
            "deep_copy": "true" if deep_copy else "false",
        }
        if end_time:
            data["end_time"] = end_time
        if rename_options:
            data["rename_options"] = json.dumps(rename_options)

        try:
            response = requests.post(url, params=params, data=data, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            self._raise_meta_error(e, f"duplicating ad set {ad_set_id}")
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def update_ad_set_frequency_cap(self, ad_set_id: str, max_frequency: int, interval_days: int = 7, event: str = "IMPRESSIONS") -> Dict[str, Any]:
        """Apply a frequency cap to an ad set.

        Note: Meta only honors frequency_control_specs on REACH-objective ad sets.
        For non-REACH objectives, this method still POSTs the change but Meta may
        silently ignore it. Returns Meta's raw response so the caller can detect.

        Args:
            ad_set_id: target ad set
            max_frequency: e.g. 2 (max 2 events per interval)
            interval_days: 1..90
            event: "IMPRESSIONS" or "REACH" (default IMPRESSIONS)

        Ref: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign/#fields
        """
        url = f"{self.base_url}/{ad_set_id}"
        spec = [{
            "event": event,
            "interval_days": int(interval_days),
            "max_frequency": int(max_frequency),
        }]
        params = {"access_token": self.access_token}
        if self.appsecret_proof:
            params["appsecret_proof"] = self.appsecret_proof
        data = {"frequency_control_specs": json.dumps(spec)}

        try:
            response = requests.post(url, params=params, data=data, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            self._raise_meta_error(e, f"setting frequency cap on ad set {ad_set_id}")
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def update_ad_set_bid_strategy(self, ad_set_id: str, bid_strategy: str, bid_amount: int = None) -> Dict[str, Any]:
        """Change an ad set's bid strategy. Common values:

        - LOWEST_COST_WITHOUT_CAP (default; spend the budget, lowest cost)
        - LOWEST_COST_WITH_BID_CAP (bid_amount = max bid in cents)
        - COST_CAP (bid_amount = target cost per result in cents)
        - LOWEST_COST_WITH_MIN_ROAS (bid_amount = min ROAS * 1000, e.g. 1.5x = 1500)

        Ref: https://developers.facebook.com/docs/marketing-api/bidding/overview
        """
        allowed = {"LOWEST_COST_WITHOUT_CAP", "LOWEST_COST_WITH_BID_CAP", "COST_CAP", "LOWEST_COST_WITH_MIN_ROAS"}
        if bid_strategy not in allowed:
            raise ValueError(f"bid_strategy must be one of {allowed}")

        url = f"{self.base_url}/{ad_set_id}"
        params = {"access_token": self.access_token}
        if self.appsecret_proof:
            params["appsecret_proof"] = self.appsecret_proof
        data = {"bid_strategy": bid_strategy}
        # bid_amount is required for everything except LOWEST_COST_WITHOUT_CAP
        if bid_amount is not None:
            data["bid_amount"] = int(bid_amount)
        elif bid_strategy != "LOWEST_COST_WITHOUT_CAP":
            raise ValueError(f"bid_amount required for {bid_strategy}")

        try:
            response = requests.post(url, params=params, data=data, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            self._raise_meta_error(e, f"setting bid strategy on ad set {ad_set_id}")
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def _raise_meta_error(self, exc: requests.exceptions.HTTPError, what: str) -> None:
        """Shared HTTPError → Meta error message extractor used by the new mutation
        methods. Mirrors the pattern in update_ad_set_budget."""
        error_msg = f"Meta API error {what}: {exc}"
        if hasattr(exc, "response") and exc.response is not None:
            try:
                error_data = exc.response.json()
                if "error" in error_data:
                    info = error_data["error"]
                    error_msg = f"Meta API Error {info.get('code', '')}: {info.get('message', str(exc))}"
                    sub = info.get("error_subcode")
                    if sub:
                        error_msg += f" (Subcode: {sub})"
                    logger.error(f"{error_msg} - Full response: {error_data}")
            except Exception:
                error_msg = f"{error_msg} - Response: {exc.response.text}"
        logger.error(error_msg)
        raise Exception(error_msg)

    def create_automated_rule(
        self,
        name: str,
        evaluation_spec: Dict[str, Any],
        execution_spec: Dict[str, Any],
        schedule_spec: Dict[str, Any] = None,
        status: str = "ENABLED"
    ) -> Dict[str, Any]:
        """Create an automated rule on Meta's servers

        Meta Automated Rules use filters within evaluation_spec to scope which entities
        the rule applies to. Required filters include:
        - entity_type: "AD", "ADSET", or "CAMPAIGN"
        - time_preset: "LAST_7D", "LAST_14D", "LAST_30D", "LIFETIME", etc.
        - campaign.id or adset.id: with IN operator to scope to specific campaigns/ad sets

        Args:
            name: Rule name
            evaluation_spec: Evaluation specification with filters (must include entity_type,
                            time_preset, and campaign.id/adset.id scoping filters)
            execution_spec: Execution specification with action type
            schedule_spec: Schedule specification (optional, defaults to daily)
            status: Rule status - ENABLED or DISABLED

        Returns:
            Dict with the created rule data including ID

        Reference: https://developers.facebook.com/docs/marketing-api/reference/ad-rules-library/
        """
        url = f"{self.base_url}/act_{self.ad_account_id}/adrules_library"

        # Default schedule: run daily
        if schedule_spec is None:
            schedule_spec = {
                "schedule_type": "DAILY"
            }

        data = {
            "access_token": self.access_token,
            "name": name,
            "evaluation_spec": json.dumps(evaluation_spec) if isinstance(evaluation_spec, dict) else evaluation_spec,
            "execution_spec": json.dumps(execution_spec) if isinstance(execution_spec, dict) else execution_spec,
            "schedule_spec": json.dumps(schedule_spec) if isinstance(schedule_spec, dict) else schedule_spec,
            "status": status
        }
        if self.appsecret_proof:
            data["appsecret_proof"] = self.appsecret_proof

        try:
            response = requests.post(url, data=data, timeout=self.timeout)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            error_msg = f"Meta API error creating automated rule: {e}"
            if hasattr(e, 'response') and e.response:
                try:
                    error_data = e.response.json()
                    if 'error' in error_data:
                        error_info = error_data['error']
                        error_msg = f"Meta API Error {error_info.get('code', '')}: {error_info.get('message', str(e))}"
                        logger.error(f"{error_msg} - Full response: {error_data}")
                except:
                    error_msg = f"{error_msg} - Response: {e.response.text}"
            logger.error(error_msg)
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    def get_automated_rules(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get all automated rules for the ad account"""
        url = f"{self.base_url}/act_{self.ad_account_id}/adrules_library"
        params = {
            "access_token": self.access_token,
            "fields": "id,name,status,evaluation_spec,execution_spec,schedule_spec,created_time,updated_time",
            "limit": limit
        }
        if self.appsecret_proof:
            params["appsecret_proof"] = self.appsecret_proof

        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json().get("data", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get automated rules: {e}")
            raise

    def delete_automated_rule(self, rule_id: str) -> Dict[str, Any]:
        """Delete an automated rule"""
        url = f"{self.base_url}/{rule_id}"
        params = {"access_token": self.access_token}
        if self.appsecret_proof:
            params["appsecret_proof"] = self.appsecret_proof

        try:
            response = requests.delete(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to delete automated rule {rule_id}: {e}")
            raise

    def update_automated_rule_status(self, rule_id: str, status: str) -> Dict[str, Any]:
        """Update the status of an automated rule (ENABLED/DISABLED)

        Args:
            rule_id: The ID of the rule to update
            status: New status - 'ENABLED' or 'DISABLED'

        Returns:
            Dict with the API response
        """
        url = f"{self.base_url}/{rule_id}"
        data = {
            "access_token": self.access_token,
            "status": status
        }
        if self.appsecret_proof:
            data["appsecret_proof"] = self.appsecret_proof

        try:
            response = requests.post(url, data=data, timeout=self.timeout)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            error_msg = f"Meta API error updating rule status: {e}"
            if hasattr(e, 'response') and e.response:
                try:
                    error_data = e.response.json()
                    if 'error' in error_data:
                        error_info = error_data['error']
                        error_msg = f"Meta API Error {error_info.get('code', '')}: {error_info.get('message', str(e))}"
                except:
                    error_msg = f"{error_msg} - Response: {e.response.text}"
            logger.error(error_msg)
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update automated rule status: {e}")
            raise

    def get_age_gender_breakdown(self, object_id: str, date_preset: str = "last_7d") -> List[Dict[str, Any]]:
        """Get performance breakdown by age and gender

        Args:
            object_id: Campaign, AdSet, or Ad ID
            date_preset: Date range preset

        Returns:
            List of insights broken down by age and gender
        """
        return self.get_insights_with_breakdowns(
            object_id,
            breakdowns=["age", "gender"],
            date_preset=date_preset
        )

    def get_daily_breakdown(self, object_id: str, date_preset: str = "last_7d") -> List[Dict[str, Any]]:
        """Get performance breakdown by day for sparkline/trend data

        Args:
            object_id: Campaign, AdSet, or Ad ID
            date_preset: Date range preset

        Returns:
            List of daily insights sorted by date
        """
        return self.get_insights_with_breakdowns(
            object_id,
            time_increment="1",
            date_preset=date_preset
        )

    def get_pixel_quality(self) -> Dict[str, Any]:
        """Get pixel health and event statistics for the ad account

        Fetches AdsPixel data and per-event stats using the Meta Marketing API.
        Reference: https://developers.facebook.com/docs/marketing-api/reference/ads-pixel/
        Reference: https://developers.facebook.com/docs/marketing-api/reference/ads-pixel/stats/

        Returns:
            Dict with pixel info and event stats
        """
        try:
            # Step 1: Get pixels for the ad account
            endpoint = f"act_{self.ad_account_id}/adspixels"
            params = {
                "fields": "id,name,last_fired_time,is_unavailable,enable_automatic_matching,automatic_matching_fields"
            }
            response = self._make_request(f"{endpoint}?{'&'.join([f'{k}={v}' for k, v in params.items()])}")
            pixels = response.get("data", [])

            if not pixels:
                return {"pixels": [], "events": []}

            pixel = pixels[0]  # Primary pixel
            pixel_id = pixel.get("id")

            # Step 2: Get event stats for the pixel (last 7 days)
            import time as _time
            end_time = int(_time.time())
            start_time = end_time - (7 * 24 * 60 * 60)

            stats_endpoint = f"{pixel_id}/stats"
            stats_params = {
                "aggregation": "event",
                "start_time": start_time,
                "end_time": end_time
            }

            try:
                stats_response = self._make_request(f"{stats_endpoint}?{'&'.join([f'{k}={v}' for k, v in stats_params.items()])}")
                event_stats = stats_response.get("data", [])
            except Exception as e:
                logger.warning(f"Failed to get pixel stats: {e}")
                event_stats = []

            return {
                "pixel": pixel,
                "events": event_stats
            }

        except Exception as e:
            logger.error(f"Failed to get pixel quality: {e}")
            return {"pixel": None, "events": []}

    def test_connection(self) -> bool:
        """Test the connection to Meta's API"""
        try:
            self.get_ad_account_info()
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
