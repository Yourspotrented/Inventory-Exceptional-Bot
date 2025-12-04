# =====================================================================
# path: files/facilities.py
# =====================================================================
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx

log = logging.getLogger("app.facilities")

# SpotHero operator facility search endpoint (requires POST)
FACILITIES_URL = "https://spothero.com/api/v1/facilities/operator-facility-search/"

# --------------------------- Public API --------------------------------

async def fetch_all_facilities(flex_auth: str) -> List[Dict[str, Any]]:
    """Return normalized list of {'id','name'} from parking_spots[]."""
    raw_facilities = await _fetch_all_facilities_post(flex_auth)
    out: List[Dict[str, Any]] = []
    for fac in raw_facilities:
        for spot in fac.get("parking_spots", []) or []:
            fid = spot.get("id")
            title = spot.get("title")
            if fid is not None and title:
                out.append({"id": fid, "name": str(title).strip()})
    log.info("facilities: normalized %d items", len(out))
    return out

async def resolve_facility_with_token_mgr(res_facility_name: str, token_mgr) -> Optional[Dict[str, Any]]:
    """Deprecated in new flow; kept for compatibility."""
    headers = await token_mgr.ensure_fresh()
    flex_auth = headers.get("flex_auth")
    if not flex_auth:
        raise RuntimeError("resolve_facility: missing flex_auth")
    return await resolve_facility(res_facility_name, flex_auth)

async def resolve_facility(res_facility_name: str, flex_auth: str) -> Optional[Dict[str, Any]]:
    facilities = await fetch_all_facilities(flex_auth)
    if not facilities:
        log.warning("resolve_facility: no facilities returned")
        return None
    return _best_facility_match(res_facility_name, facilities)

def best_match_from_list(target_name: str, facilities_list: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Match against a provided in-memory list only."""
    if not facilities_list:
        return None
    return _best_facility_match(target_name, facilities_list)

# --------------------------- Network -----------------------------------

async def _fetch_all_facilities_post(flex_auth: str) -> List[Dict[str, Any]]:
    """
    POST to SpotHero operator facility search and return
    the list under data.results.canonical_facilities (raw).
    """
    timeout = httpx.Timeout(30.0)
    headers = {
        "Authorization": flex_auth,
        "Content-Type": "application/json",
    }
    payload = {
        "limit": 5000,
        "offset": 0,
        "filters": {
            "cities": [],
            "status": ["On", "Archived", "Off"],
        },
        "facility_ids": "",
        "operator_email": "",
    }

    async def _post(url: str, json_payload: Dict[str, Any]) -> httpx.Response:
        attempts = 0
        delay = 0.8
        last: Optional[Exception] = None
        async with httpx.AsyncClient(timeout=timeout) as client:
            while attempts < 3:
                attempts += 1
                try:
                    log.info("facilities: HTTP POST %s (attempt=%d)", url, attempts)
                    r = await client.post(url, headers=headers, json=json_payload)
                    log.info("facilities: HTTP POST -> status=%s len=%s", r.status_code, len(r.content or b""))
                    return r
                except Exception as e:
                    last = e
                    log.warning("facilities: POST error (attempt=%d): %s", attempts, e)
                    if attempts >= 3:
                        break
                    await asyncio.sleep(delay)
                    delay *= 2
        assert last is not None
        raise last

    r = await _post(FACILITIES_URL, payload)

    if r.status_code != 200:
        body = r.text[:500] if r.text else ""
        log.warning("facilities: POST failed: %s %s", r.status_code, body)
        return []

    data = _coerce_json(r)
    facilities = (
        ((data or {}).get("data", {}) or {})
        .get("results", {}) or {}
    ).get("canonical_facilities")

    if not isinstance(facilities, list):
        try:
            top_keys = list((data or {}).keys())
        except Exception:
            top_keys = []
        log.info("facilities: unexpected payload shape. top_keys=%s", top_keys[:10])
        return []

    log.info("facilities: POST returned %d canonical facilities", len(facilities))
    return facilities

def _coerce_json(r: httpx.Response) -> Any:
    try:
        return r.json()
    except Exception:
        try:
            return json.loads(r.text)
        except Exception:
            return None

# --------------------------- Matching ---------------------------------

_SUFFIX_RX = re.compile(r"\s*-\s*(?:garage|lot|deck)\b", re.I)
_WS_RX = re.compile(r"\s+")

def _norm(s: str) -> str:
    t = (s or "").strip().lower()
    t = t.replace("–", "-").replace("—", "-").replace("\u00a0", " ")
    t = _SUFFIX_RX.sub("", t)
    t = re.sub(r"[^\w\s&/]", " ", t)
    t = _WS_RX.sub(" ", t).strip()
    return t

def _tokenize(s: str) -> List[str]:
    return [t for t in re.split(r"[^\w]+", s) if t]

def _score(a: str, b: str) -> float:
    """Token-overlap Jaccard + prefix/contains bonuses."""
    na, nb = _norm(a), _norm(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    ta, tb = set(_tokenize(na)), set(_tokenize(nb))
    base = (len(ta & tb) / len(ta | tb)) if (ta and tb) else 0.0
    bonus = 0.0
    if na.startswith(nb) or nb.startswith(na):
        bonus += 0.2
    if na in nb or nb in na:
        bonus += 0.2
    return min(1.0, base + bonus)

def _best_facility_match(target_name: str, facilities: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    tn = _norm(target_name)
    # exact-normalized equality shortcut
    for f in facilities:
        name = f.get("name") or ""
        if _norm(name) == tn:
            return {"id": f.get("id"), "name": name, "score": 1.0}

    best: Tuple[float, int, Dict[str, Any]] | None = None
    for f in facilities:
        name = f.get("name") or ""
        fid = f.get("id")
        if not name:
            continue
        sc = _score(tn, name)
        if sc < 0.45:
            continue
        dist = abs(len(_norm(name)) - len(tn))
        cand = (sc, -dist, {"id": fid, "name": name, "score": round(sc, 3)})
        if (best is None) or (cand > best):
            best = cand
    return best[2] if best else None

