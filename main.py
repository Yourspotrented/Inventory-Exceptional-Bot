# =====================================================================
# path: main.py
# =====================================================================
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time
from typing import Dict, List, Optional

import httpx
from fastapi import FastAPI, Query
from pydantic import BaseModel

# --- Load .env BEFORE reading any os.getenv ---
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(), override=False)
except Exception:
    pass

# ----------------------------- Settings -------------------------------
# ----------------------------- Settings -------------------------------
class Settings(BaseModel):
    # Graph / SharePoint token.json download
    GRAPH_TENANT_ID: str = (os.getenv("GRAPH_TENANT_ID") or "").strip()
    GRAPH_CLIENT_ID: str = (os.getenv("GRAPH_CLIENT_ID") or "").strip()
    GRAPH_CLIENT_SECRET: str = (os.getenv("GRAPH_CLIENT_SECRET") or "").strip()

    SP_SITE_ID: str = (os.getenv("SP_SITE_ID") or "").strip()
    SP_DRIVE_ID: str = (os.getenv("SP_DRIVE_ID") or "").strip()
    SP_TOKEN_JSON_PATH: str = (
        os.getenv("SP_TOKEN_JSON_PATH") or "Business Optimization/Automation/token/token.json"
    ).strip()
    LOCAL_TOKEN_JSON: str = os.getenv("LOCAL_TOKEN_JSON", os.path.join("files", "token.json"))

    TOKEN_REFRESH_HOURS: int = int(os.getenv("TOKEN_REFRESH_HOURS", "12"))
    TOKEN_TTL_SECONDS: int = int(os.getenv("TOKEN_TTL_SECONDS", "900"))  # 15m cache

    # Avoid slamming SpotHero; only refetch if cache older than this.
    FACILITIES_TTL_SECONDS: int = int(os.getenv("FACILITIES_TTL_SECONDS", "600"))  # 10m default

    # Force a fresh Graph download at startup before any SpotHero call
    STARTUP_FORCE_GRAPH_REFRESH: bool = (os.getenv("STARTUP_FORCE_GRAPH_REFRESH", "true").lower() in {"1","true","yes"})

    # Optional: silence INFO logs from files/facilities.py (attempt=1 lines)
    FACILITIES_LOG_INFO: bool = (os.getenv("FACILITIES_LOG_INFO", "true").lower() in {"1","true","yes"})

    # --- SpotHero tiered event updater ---
    SH_UPDATE_ENABLED: bool = (os.getenv("SH_UPDATE_ENABLED", "true").lower() in {"1","true","yes"})
    SH_UPDATE_INTERVAL_SECONDS: int = int(os.getenv("SH_UPDATE_INTERVAL_SECONDS", "14400"))  # 4h
    SH_UPDATE_ONLY_TIERED: bool = (os.getenv("SH_UPDATE_ONLY_TIERED", "true").lower() in {"1","true","yes"})
    SH_UPDATE_DRY_RUN: bool = (os.getenv("SH_UPDATE_DRY_RUN", "false").lower() in {"1","true","yes"})  # live by default
    SH_UPDATE_DEBUG: bool = (os.getenv("SH_UPDATE_DEBUG", "false").lower() in {"1","true","yes"})
    SH_UPDATE_TZ: str = os.getenv("SH_UPDATE_TZ", "America/Chicago")

    # pacing (a little slower + smooth)
    SH_UPDATE_MAX_CONCURRENCY: int = int(os.getenv("SH_UPDATE_MAX_CONCURRENCY", "2"))   # was 3 → 2
    SH_UPDATE_PAUSE_SEC: float = float(os.getenv("SH_UPDATE_PAUSE_SEC", "0.25"))        # was 0.2 → 0.25
    SH_UPDATE_JITTER_SEC: float = float(os.getenv("SH_UPDATE_JITTER_SEC", "0.15"))      # NEW: 0–150ms extra

    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("main")
for noisy in ["httpx", "urllib3", "asyncio"]:
    logging.getLogger(noisy).setLevel(logging.WARNING)

# Optionally quiet the facilities module INFO logs
fac_log = logging.getLogger("app.facilities")
if not settings.FACILITIES_LOG_INFO:
    fac_log.setLevel(logging.WARNING)

def _have_graph_envs() -> bool:
    return all([
        bool(settings.GRAPH_TENANT_ID),
        bool(settings.GRAPH_CLIENT_ID),
        bool(settings.GRAPH_CLIENT_SECRET),
        bool(settings.SP_SITE_ID),
        bool(settings.SP_DRIVE_ID),
        bool(settings.SP_TOKEN_JSON_PATH),
    ])

# ----------------------------- Token Manager --------------------------
class TokenManager:
    """Downloads token.json from SharePoint via Graph and exposes auth headers."""
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._last_loaded: float = 0.0
        self._headers: Optional[Dict[str, str]] = None

    async def ensure_fresh(self) -> Dict[str, str]:
        async with self._lock:
            now = time.time()

            if self._headers and (now - self._last_loaded) < settings.TOKEN_TTL_SECONDS:
                return self._headers

            try:
                stat = os.stat(settings.LOCAL_TOKEN_JSON)
                file_age = now - stat.st_mtime
            except FileNotFoundError:
                file_age = None

            max_file_age = max(3600, settings.TOKEN_REFRESH_HOURS * 3600)  # min 1h
            if file_age is None or file_age > max_file_age:
                await self._download_token_json_with_retry()

            self._headers = self._load_headers_from_file(settings.LOCAL_TOKEN_JSON)
            if not self._headers:
                raise RuntimeError("TokenManager: empty headers after load")
            self._last_loaded = now
            return self._headers

    async def force_refresh(self) -> Dict[str, str]:
        async with self._lock:
            await self._download_token_json_with_retry()
            self._headers = self._load_headers_from_file(settings.LOCAL_TOKEN_JSON)
            if not self._headers:
                raise RuntimeError("TokenManager: empty headers after force refresh")
            self._last_loaded = time.time()
            return self._headers

    async def _download_token_json_with_retry(self) -> None:
        if not _have_graph_envs():
            if os.path.exists(settings.LOCAL_TOKEN_JSON):
                log.info("TokenManager: Graph envs not set; using existing local token.json.")
                return
            raise RuntimeError("GRAPH_* envs required and no local token.json found")

        attempts, backoff = 0, 1.0
        last_err: Optional[Exception] = None
        while attempts < 3:
            attempts += 1
            try:
                await self._download_once()
                return
            except Exception as e:
                last_err = e
                with contextlib.suppress(Exception):
                    if os.path.exists(settings.LOCAL_TOKEN_JSON) and os.path.getsize(settings.LOCAL_TOKEN_JSON) == 0:
                        os.remove(settings.LOCAL_TOKEN_JSON)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 8.0)
        raise RuntimeError(f"TokenManager: failed to download token.json: {last_err}")

    async def _download_once(self) -> None:
        token = await self._get_graph_app_token()
        url = (
            f"https://graph.microsoft.com/v1.0/sites/{settings.SP_SITE_ID}"
            f"/drives/{settings.SP_DRIVE_ID}/root:/{settings.SP_TOKEN_JSON_PATH}:/content"
        )
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), trust_env=True) as client:
            r = await client.get(url, headers=headers, follow_redirects=True)
        if r.status_code != 200:
            raise RuntimeError(f"Graph download failed: {r.status_code} {r.text[:200]}")
        os.makedirs(os.path.dirname(settings.LOCAL_TOKEN_JSON), exist_ok=True)
        with open(settings.LOCAL_TOKEN_JSON, "wb") as f:
            f.write(r.content)
        log.info("TokenManager: token.json saved (%d bytes)", len(r.content))

    async def _get_graph_app_token(self) -> str:
        if not _have_graph_envs():
            raise RuntimeError("GRAPH_* envs required for token.json download")
        url = f"https://login.microsoftonline.com/{settings.GRAPH_TENANT_ID}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": settings.GRAPH_CLIENT_ID,
            "client_secret": settings.GRAPH_CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default",
        }
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), trust_env=True) as client:
            r = await client.post(url, data=data)
        if r.status_code != 200:
            raise RuntimeError(f"Graph token failed: {r.status_code} {r.text[:200]}")
        return r.json()["access_token"]

    @staticmethod
    def _load_headers_from_file(path: str) -> Dict[str, str]:
        with open(path, "r", encoding="utf-8") as f:
            tokens = json.load(f)
        flex = tokens["flex_token_data"]["token_type"] + " " + tokens["flex_token_data"]["access_token"]
        reservation = tokens["reservation_token_data"]["token_type"] + " " + tokens["reservation_token_data"]["access_token"]
        return {"flex_auth": flex, "reservation_auth": reservation}

TOKEN_MGR = TokenManager()

# ----------------------------- Facilities import ----------------------
try:
    from files.facilities import fetch_all_facilities
except ModuleNotFoundError:
    import importlib.util, pathlib, types
    _p = pathlib.Path(__file__).parent / "files" / "facilities.py"
    spec = importlib.util.spec_from_file_location("files.facilities", _p)
    if spec is None or spec.loader is None:
        raise
    _mod = importlib.util.module_from_spec(spec)  # type: types.ModuleType
    spec.loader.exec_module(_mod)
    fetch_all_facilities = _mod.fetch_all_facilities  # type: ignore[attr-defined]

# ----------------------------- SpotHero tool import -------------------
try:
    from files.spothero_tool import run_update_for_facility_all_rules
except ModuleNotFoundError:
    run_update_for_facility_all_rules = None  # guard usage below

# ----------------------------- Facilities cache -----------------------
async def load_facilities_cache(app: FastAPI, force: bool = False) -> List[Dict[str, str]]:
    await app.state.token_ready.wait()

    # TTL guard
    if getattr(app.state, "facilities", None) and not force:
        age = time.time() - (app.state.facilities_loaded_at or 0)
        if age < max(5, settings.FACILITIES_TTL_SECONDS):
            app.state.facilities_last_status = "HIT"
            return app.state.facilities

    headers = await TOKEN_MGR.ensure_fresh()
    flex_auth = headers.get("flex_auth")
    if not flex_auth:
        raise RuntimeError("Missing flex_auth from token.json")

    items = await fetch_all_facilities(flex_auth)
    app.state.facilities = items or []
    app.state.facilities_loaded_at = time.time()
    app.state.facilities_last_status = "MISS"
    log.info("Facilities cache loaded: %d items", len(app.state.facilities))
    return app.state.facilities

# ----------------------------- Background refresh ---------------------
async def refresh_loop(app: FastAPI, stop_evt: asyncio.Event) -> None:
    await app.state.token_ready.wait()

    interval = max(3600, settings.TOKEN_REFRESH_HOURS * 3600)
    while not stop_evt.is_set():
        try:
            # 1) refresh token first
            await TOKEN_MGR.force_refresh()
            if not app.state.token_ready.is_set():
                app.state.token_ready.set()
            log.info("Background: token.json refreshed from Graph.")

            # 2) refresh facilities (respects TTL inside)
            await load_facilities_cache(app, force=False)
            if getattr(app.state, "facilities_last_status", "MISS") == "HIT":
                age = int(time.time() - (app.state.facilities_loaded_at or time.time()))
                log.info("Background: facilities cache still fresh (age=%ss).", age)
            else:
                log.info("Background: facilities cache refreshed.")
        except Exception as e:
            log.warning("Background refresh skipped/failed: %s", e)

        try:
            await asyncio.wait_for(stop_evt.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass

# ------------------ Tiered-event updater (every 4h, threaded) --------
async def sh_update_loop(app: FastAPI, stop_evt: asyncio.Event) -> None:
    """
    Runs every SH_UPDATE_INTERVAL_SECONDS (default 4h).
    Offloads blocking work to threads; bounded concurrency.
    """
    if not settings.SH_UPDATE_ENABLED or run_update_for_facility_all_rules is None:
        return

    await app.state.token_ready.wait()
    interval = max(60, int(settings.SH_UPDATE_INTERVAL_SECONDS))

    # bounded concurrency for thread offloads
    max_conc = max(1, int(settings.SH_UPDATE_MAX_CONCURRENCY))
    sem = asyncio.Semaphore(max_conc)

    # map reused by worker for nicer logs (id -> name)
    name_map: Dict[int, str] = {}

    async def _one(fid: int, token: str) -> bool:
        async with sem:
            try:
                # run the blocking requests client off-thread
                await asyncio.to_thread(
                    run_update_for_facility_all_rules,
                    auth_token=token,           # already "Bearer ..." from token.json
                    facility_id=fid,
                    tz_name=settings.SH_UPDATE_TZ,
                    dry_run=settings.SH_UPDATE_DRY_RUN,
                    debug=settings.SH_UPDATE_DEBUG,
                )
                return True
            except Exception as e:
                fname = name_map.get(fid, "")
                if fname:
                    log.info("SH updater: facility %s (%s) skipped: %s", fid, fname, str(e)[:200])
                else:
                    log.info("SH updater: facility %s skipped: %s", fid, str(e)[:200])
                return False

    while not stop_evt.is_set():
        started = time.time()
        try:
            # fresh token & current facility cache
            headers = await TOKEN_MGR.ensure_fresh()
            flex = headers.get("flex_auth") or ""
            if not flex:
                raise RuntimeError("Updater: missing flex_auth")

            items = await load_facilities_cache(app, force=False)

            # filter: ONLY_TIERED when explicit flag available; otherwise include
            if settings.SH_UPDATE_ONLY_TIERED:
                fsel = [
                    it for it in items
                    if ("eventTieringEnabled" not in it) or bool(it.get("eventTieringEnabled"))
                ]
            else:
                fsel = items

            # (re)build name map for this batch without rebinding the dict
            name_map.clear()
            for it in fsel:
                try:
                    fid_int = int(it.get("id"))
                    name_map[fid_int] = str(it.get("name") or "")
                except Exception:
                    continue

            fids: List[int] = []
            for it in fsel:
                try:
                    fids.append(int(it.get("id")))
                except Exception:
                    continue

            log.info(
                "SH updater: batch start facilities=%d (dry_run=%s debug=%s conc=%d).",
                len(fids), settings.SH_UPDATE_DRY_RUN, settings.SH_UPDATE_DEBUG, max_conc
            )

            # schedule tasks with small pacing
            tasks: List[asyncio.Task] = []
            for fid in fids:
                tasks.append(asyncio.create_task(_one(fid, flex)))
                await asyncio.sleep(max(0.0, settings.SH_UPDATE_PAUSE_SEC))

            done = await asyncio.gather(*tasks, return_exceptions=False)
            ok = sum(1 for r in done if r)
            failed = len(done) - ok
            dur = int(time.time() - started)
            log.info("SH updater: done ok=%d failed=%d duration=%ss", ok, failed, dur)

        except Exception as e:
            log.warning("SH updater: batch failed early: %s", e)

        # sleep until next cycle
        try:
            await asyncio.wait_for(stop_evt.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass

# ----------------------------- FastAPI app ----------------------------
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.stop_evt = asyncio.Event()
    app.state.facilities: List[Dict[str, str]] = []
    app.state.facilities_loaded_at: Optional[float] = None
    app.state.facilities_last_status: str = "MISS"
    app.state.token_ready = asyncio.Event()
    tasks: List[asyncio.Task] = []

    # STARTUP: token first, then facilities
    try:
        if settings.STARTUP_FORCE_GRAPH_REFRESH:
            await TOKEN_MGR.force_refresh()
        else:
            await TOKEN_MGR.ensure_fresh()
        app.state.token_ready.set()
        await load_facilities_cache(app, force=True)
        log.info("Startup warm: token + facilities ready.")
    except Exception as e:
        log.warning("Startup warm failed: %s", e)

    tasks.append(asyncio.create_task(refresh_loop(app, app.state.stop_evt)))
    tasks.append(asyncio.create_task(sh_update_loop(app, app.state.stop_evt)))
    try:
        yield
    finally:
        app.state.stop_evt.set()
        for t in tasks:
            with contextlib.suppress(Exception):
                await t

app = FastAPI(
    title="SpotHero Facilities Minimal",
    version="1.1.1",
    lifespan=lifespan,
)

@app.get("/facilities/all")
async def facilities_all():
    try:
        items = await load_facilities_cache(app)
        return {"ok": True, "count": len(items), "items": items}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/facilities/resolve")
async def facilities_resolve(name: str = Query(..., description="Exact name match only in this minimal app")):
    try:
        items = await load_facilities_cache(app)
        needle = (name or "").strip().lower()
        match = next((f for f in items if (f.get("name","").strip().lower() == needle)), None)
        return {"ok": match is not None, "match": match}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/token/refresh")
async def token_refresh():
    try:
        await TOKEN_MGR.force_refresh()
        if not app.state.token_ready.is_set():
            app.state.token_ready.set()
        await load_facilities_cache(app, force=True)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/healthz")
async def healthz():
    cached = getattr(app.state, "facilities", []) or []
    meta = {
        "has_headers": TOKEN_MGR._headers is not None,
        "headers_age_sec": int(time.time() - TOKEN_MGR._last_loaded) if TOKEN_MGR._last_loaded else None,
        "token_refresh_hours": settings.TOKEN_REFRESH_HOURS,
        "facilities_cached": len(cached),
        "facilities_source": "SpotHero operator-facility-search",
        "graph_envs_present": _have_graph_envs(),
        "facilities_age_sec": (int(time.time() - app.state.facilities_loaded_at) if app.state.facilities_loaded_at else None),
        "facilities_ttl_sec": settings.FACILITIES_TTL_SECONDS,
        "token_ready": app.state.token_ready.is_set(),
        "facilities_cache_status": getattr(app.state, "facilities_last_status", "MISS"),
        "sh_update": {
            "enabled": settings.SH_UPDATE_ENABLED and (run_update_for_facility_all_rules is not None),
            "interval_seconds": settings.SH_UPDATE_INTERVAL_SECONDS,
            "only_tiered": settings.SH_UPDATE_ONLY_TIERED,
            "dry_run": settings.SH_UPDATE_DRY_RUN,
            "debug": settings.SH_UPDATE_DEBUG,
            "tz": settings.SH_UPDATE_TZ,
            "pause_sec": settings.SH_UPDATE_PAUSE_SEC,
            "max_concurrency": settings.SH_UPDATE_MAX_CONCURRENCY,
        },
    }
    return {"ok": True, "meta": meta}

# ----------------------------- Entrypoint -----------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        reload=False,
        access_log=False,
        log_level="warning",
    )
