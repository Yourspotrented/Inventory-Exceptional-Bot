"""
Single-run batch for cron:
1) Ensure fresh SpotHero token from SharePoint (refresh if stale/≥12h).
2) Fetch facilities (from cache unless token refreshed or cache stale).
3) Run tiered-event updates for each facility.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List, Tuple

from files.constants import GET_ALL_FACILITIES_URL
from files.spothero_tool import run_update_for_facility_all_rules
from files.token_manager import TokenManager
from files.facilities_cache import FacilitiesCache  # <-- NEW

# -------- env helpers --------
def _bool_env(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "t", "yes", "y", "on"}

def _float_env(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(v)
    except ValueError:
        return default

def _int_env(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        return default

# -------- args --------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run SpotHero tiered event updates across facilities (cron-safe).")

    # Execution knobs
    p.add_argument("--only-tiered", dest="only_tiered", action="store_true", default=_bool_env("ONLY_TIERED", True))
    p.add_argument("--all-facilities", dest="only_tiered", action="store_false", help="Process all facilities")
    p.add_argument("--dry-run", dest="dry_run", action="store_true", default=_bool_env("DRY_RUN", True))
    p.add_argument("--no-dry-run", dest="dry_run", action="store_false", help="Actually POST updates")
    p.add_argument("--debug", dest="debug", action="store_true", default=_bool_env("DEBUG", True))
    p.add_argument("--no-debug", dest="debug", action="store_false")
    p.add_argument("--pause-sec", type=float, default=_float_env("PAUSE_SEC", 0.2))
    p.add_argument("--tz", default=os.getenv("TZ_NAME", "America/Chicago"))
    p.add_argument("--example-fid", type=int, default=_int_env("EXAMPLE_FID", 72816))
    p.add_argument("--output-json", default=os.getenv("OUTPUT_JSON", "").strip(), help="Optional path to write summaries JSON")
    p.add_argument("--limit", type=int, default=_int_env("FACILITY_LIMIT", 0), help="Optional limit of facilities to process")

    # Optional: allow a one-off direct bearer override (skips SharePoint manager)
    p.add_argument("--auth-token", dest="auth_token",
                   default=(os.getenv("SPOTHERO_FLEX_AUTH") or os.getenv("FLEX_AUTH") or "").strip(),
                   help="Override SpotHero Flex bearer token (skips SharePoint). Not recommended in cron.")
    return p.parse_args()

# -------- main --------
def main() -> int:
    args = parse_args()

    # 1) Ensure fresh token headers (SharePoint-managed) unless a one-off override is provided
    if args.auth_token:
        flex_auth = args.auth_token if args.auth_token.lower().startswith("bearer ") else f"Bearer {args.auth_token}"
        auth_header = flex_auth
        token_refreshed = False   # we didn't manage it, so don't force facilities refresh based on token
        if args.debug:
            print("[auth] using CLI/ENV override bearer (SharePoint bypassed)")
    else:
        tm = TokenManager()
        hdrs, token_refreshed = tm.ensure_fresh(return_refreshed=True)  # <- know if token.json actually changed
        auth_header = hdrs["flex_auth"]
        if args.debug:
            print(f"[auth] using SharePoint-managed token (refreshed={token_refreshed})")

    # 2) Facilities: use cache unless token refreshed or cache stale
    fac_cache = FacilitiesCache(cache_path=os.path.join("files", "facilities.json"), ttl_hours=12.0)
    df_fac = fac_cache.ensure_fresh(
        flex_auth=auth_header,
        facilities_url=GET_ALL_FACILITIES_URL,
        force_refresh=token_refreshed,        # <- refresh if token just refreshed
        debug=args.debug,
    )

    # 2a) Filter: tiered only unless overridden
    if args.only_tiered and "eventTieringEnabled" in df_fac.columns:
        df_fac = df_fac[df_fac["eventTieringEnabled"] == True]

    # 2b) Validate id column
    if "id" not in df_fac.columns:
        raise ValueError("Expected column 'id' in all_facilities DataFrame.")

    fids: List[int] = df_fac["id"].dropna().astype(int).tolist()
    if args.limit and args.limit > 0:
        fids = fids[: args.limit]

    print(
        f"About to process {len(fids)} facilities "
        f"({'tiered only' if args.only_tiered else 'all'}) | dry_run={args.dry_run} debug={args.debug}"
    )

    # 3) Iterate facilities & update
    summaries: Dict[int, Dict[str, Any]] = {}
    failures: List[Tuple[int, str]] = []

    for i, fid in enumerate(fids, start=1):
        title = df_fac.loc[df_fac["id"] == fid, "title"].iloc[0] if "title" in df_fac.columns and not df_fac.loc[df_fac["id"] == fid, "title"].empty else ""
        addr = df_fac.loc[df_fac["id"] == fid, "physicalAddress"].iloc[0] if "physicalAddress" in df_fac.columns and not df_fac.loc[df_fac["id"] == fid, "physicalAddress"].empty else ""
        print(f"[{i}/{len(fids)}] facility {fid} — {title} | {addr}")

        try:
            summaries[fid] = run_update_for_facility_all_rules(
                auth_token=auth_header,
                facility_id=int(fid),
                tz_name=args.tz,
                dry_run=args.dry_run,
                debug=args.debug,
            )
        except Exception as e:
            failures.append((fid, str(e)))
            print(f"  ! failed: {e}")

        time.sleep(args.pause_sec)

    # 4) Done
    print("\n=== run complete ===")
    print(f"ok: {len(summaries)} | failed: {len(failures)}")
    if failures:
        for fid, err in failures[:10]:
            print(f"  - {fid}: {err}")

    if args.example_fid in summaries:
        print("\n--- sample summary keys ---")
        print(list(summaries[args.example_fid].keys()))

    if args.output_json:
        try:
            with open(args.output_json, "w", encoding="utf-8") as f:
                json.dump(summaries, f, indent=2)
            print(f"Wrote summaries to {args.output_json}")
        except Exception as e:
            print(f"Failed to write {args.output_json}: {e}")

    return 1 if failures else 0

if __name__ == "__main__":
    sys.exit(main())
