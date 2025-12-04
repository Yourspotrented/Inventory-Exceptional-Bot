# File: files/spothero_tool.py

from __future__ import annotations

import datetime as dt
import json
import time

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from zoneinfo import ZoneInfo

BASE_URL = "https://spothero.com/api/v1"
DEFAULT_TZ = "America/Chicago"


# ---------- Models (LOCAL time only) ----------
@dataclass
class InventoryRule:
    facility_id: int
    valid_from_local: dt.datetime  # tz-aware in America/Chicago
    valid_to_local: dt.datetime    # tz-aware in America/Chicago
    quantity: int
    raw: Dict[str, Any]


@dataclass
class EventInfo:
    event_id: int
    rule_id: Optional[int]
    event_starts_local: Optional[dt.datetime]  # tz-aware in America/Chicago
    event_ends_local: Optional[dt.datetime]    # tz-aware in America/Chicago
    starts_offset: str
    ends_offset: str
    tiers: List[Dict[str, Any]]
    raw: Dict[str, Any]


# ---------- Client ----------
class SpotHeroClient:
    def __init__(self, token: str, base_url: str = BASE_URL, timeout: int = 20) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self._auth_header(token))
        self.session.headers.update({
            "Accept": "application/json, */*;q=0.1",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "InventoryBot/1.0 (+Render cron)"
        })

    @staticmethod
    def _auth_header(token: str) -> Dict[str, str]:
        v = token.strip()
        if not v.lower().startswith("bearer "):
            v = f"Bearer {v}"
        return {"Authorization": v}

    @staticmethod
    def _fmt_range_starts(d: dt.date) -> str:
        return f"{d.month:02d}/{d.day:02d}/{d.year}T00:00"

    @staticmethod
    def _date_yyyy_mm_dd(d: dt.date) -> str:
        return d.strftime("%Y-%m-%d")

    def _send(
        self,
        method: str,
        url: str,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        expect_status: Tuple[int, ...] = (200, 201, 202, 204),
    ) -> requests.Response:
        RETRY_STATUS = {429, 500, 502, 503, 504}
        backoff = 0.5
        attempts = 0
        last_exc: Optional[Exception] = None

        while attempts < 4:
            attempts += 1
            try:
                r = self.session.request(
                    method=method, url=url, json=payload, params=params, timeout=self.timeout
                )
                if r.status_code in expect_status:
                    return r
                # retry only on transient codes
                if r.status_code not in RETRY_STATUS:
                    msg = (
                        f"{r.status_code} {r.reason} {method} {url} | "
                        f"params={params} body={bool(payload)} | resp={r.text[:400]}"
                    )
                    raise requests.HTTPError(msg, response=r)
            except requests.RequestException as e:
                last_exc = e

            # transient -> backoff and retry
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 6.0)

        if last_exc:
            raise last_exc
        raise requests.HTTPError(f"HTTP failed after retries: {method} {url}")

    # ---------- Logging helper ----------
    @staticmethod
    def _log(debug: bool, *parts: Any) -> None:
        if debug:
            print(*parts, flush=True)

    # --- API: inventory-rules-range (GET) ---
    def get_inventory_rules_range(self, facility_id: int, range_start_date: dt.date, tz_name: str) -> List[InventoryRule]:
        """
        API returns valid_from/valid_to as ISO (often with Z/UTC). We convert ONCE to local tz
        (America/Chicago) and keep local everywhere else.
        """
        tz = ZoneInfo(tz_name)
        params = {"range_starts": self._fmt_range_starts(range_start_date)}
        url = f"{self.base_url}/facilities/{facility_id}/inventory-rules-range/"
        payload = self._send("GET", url, params=params).json()
        items = (payload.get("data", {}) or {}).get("results", []) or []

        rules: List[InventoryRule] = []
        for item in items:
            if int(item.get("facility_id", facility_id)) != facility_id:
                continue
            if item.get("status") != "enabled":
                continue
            if item.get("is_expired"):
                continue

            vf_raw = item.get("valid_from")   # e.g. "2025-12-28T00:00:00Z"
            vt_raw = item.get("valid_to")
            try:
                vf = dt.datetime.fromisoformat(vf_raw.replace("Z", "+00:00"))
                vt = dt.datetime.fromisoformat(vt_raw.replace("Z", "+00:00"))
            except Exception:
                continue
            # convert to local tz and keep local
            vf_local = vf.astimezone(tz)
            vt_local = vt.astimezone(tz)

            qty = int(item.get("quantity") or 0)
            rules.append(
                InventoryRule(
                    facility_id=facility_id,
                    valid_from_local=vf_local,
                    valid_to_local=vt_local,
                    quantity=qty,
                    raw=item
                )
            )
        return rules

    # --- API: upcoming-events (GET, paginated) ---
    def get_upcoming_events_page(
        self,
        facility_id: int,
        from_date: dt.date,
        to_date: dt.date,
        page: int = 1,
        per_page: Optional[int] = None,
        debug: bool = False,
    ) -> Dict[str, Any]:
        params = {
            "from_date": self._date_yyyy_mm_dd(from_date),
            "to_date": self._date_yyyy_mm_dd(to_date),
            "page": page,
        }
        # Accept either spelling if present
        if per_page is not None:
            params["perPage"] = int(per_page)
            params["per_page"] = int(per_page)

        url = f"{self.base_url}/facilities/{facility_id}/upcoming-events/"
        data = self._send("GET", url, params=params).json()

        if debug:
            meta = data.get("meta") or {}
            results = (data.get("data", {}) or {}).get("results", []) or []
            self._log(debug,
                f"[upcoming-events] GET page={page} perPage={per_page} "
                f"| meta.page={meta.get('page')} meta.pages={meta.get('pages')} meta.nextPage={meta.get('nextPage')} "
                f"| returned={len(results)}"
            )
            for row in results[:5]:
                self._log(debug,
                    "    sample:",
                    "event_id=", row.get("event_id"),
                    "rule_id=", row.get("rule_id"),
                    "| event=", row.get("event_starts"), "→", row.get("event_ends"),
                    "| res=", row.get("reservation_starts"), "→", row.get("reservation_ends"),
                    "| tiers_sample=",
                    [(t.get("order"), t.get("price"), t.get("inventory")) for t in (row.get("tiers") or [])]
                )
        return data

    def iter_upcoming_events(
        self,
        facility_id: int,
        from_date: dt.date,
        to_date: dt.date,
        *,
        per_page: int = 25,
        max_pages: int = 30,
        debug: bool = False,
    ) -> Iterable[Dict[str, Any]]:
        """
        Page-walks 1..pages (or until empty) to fetch all events in the date span.
        """
        first = self.get_upcoming_events_page(
            facility_id, from_date, to_date, page=1, per_page=per_page, debug=debug
        )
        meta = first.get("meta") or {}
        results = (first.get("data", {}) or {}).get("results", []) or []
        for ev in results:
            yield ev

        pages = meta.get("pages")
        if isinstance(pages, int) and pages > 1:
            last_page = min(pages, max_pages)
            for p in range(2, last_page + 1):
                data = self.get_upcoming_events_page(
                    facility_id, from_date, to_date, page=p, per_page=per_page, debug=debug
                )
                res = (data.get("data", {}) or {}).get("results", []) or []
                for ev in res:
                    yield ev
                if not res:
                    self._log(debug, f"[upcoming-events] page={p} returned 0; stopping early.")
                    break
            return

        for p in range(2, max_pages + 1):
            data = self.get_upcoming_events_page(
                facility_id, from_date, to_date, page=p, per_page=per_page, debug=debug
            )
            res = (data.get("data", {}) or {}).get("results", []) or []
            if not res:
                self._log(debug, f"[upcoming-events] page={p} returned 0; stopping.")
                break
            for ev in res:
                yield ev

    # --- API: tiered-event-rating-rules (POST) ---
    def post_tiered_event_rating_rules(
        self,
        facility_id: int,
        event_ids: List[int],
        tiers: List[Dict[str, Any]],
        event_starts_offset: str,
        event_ends_offset: str,
        rule_id: Optional[int] = None,
        stop_selling_before_duration: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/facilities/{facility_id}/tiered-event-rating-rules/"
        payload = {
            "event_ids": list(event_ids),
            "tiers": [{"price": int(t["price"]), "order": int(t["order"]), "inventory": int(t["inventory"])} for t in tiers],
            "event_starts_offset": str(event_starts_offset),
            "event_ends_offset": str(event_ends_offset),
            "stop_selling_before_duration": stop_selling_before_duration,
        }
        if rule_id is not None:
            payload["rule_id"] = int(rule_id)
        if dry_run:
            return {"status": "dry_run", "url": url, "payload": payload}

        resp = self._send("POST", url, payload=payload, expect_status=(200, 201, 202, 204))
        ct = (resp.headers.get("Content-Type") or "").lower()
        text = resp.text or ""
        if not text.strip():
            return {"status_code": resp.status_code, "content_type": ct, "text": "", "note": "No JSON body returned."}
        try:
            return resp.json()
        except Exception:
            return {"status_code": resp.status_code, "content_type": ct, "text": text[:2000], "note": "Non-JSON response."}

    # --- Helper: get one day, walking pages (verification) ---
    def get_event_on_day(
        self,
        facility_id: int,
        day: dt.date,
        target_event_id: int,
        per_page: int = 25,
        max_pages: int = 10,
        debug: bool = False,
    ) -> Optional[EventInfo]:
        first = self.get_upcoming_events_page(facility_id, day, day, page=1, per_page=per_page, debug=debug)
        results = (first.get("data", {}) or {}).get("results", []) or []
        for r2 in results:
            if int(r2.get("event_id", -1)) == target_event_id:
                return _parse_event_local(r2, tz_name=DEFAULT_TZ)
        pages = (first.get("meta") or {}).get("pages")
        if isinstance(pages, int) and pages > 1:
            last_page = min(pages, max_pages)
            for p in range(2, last_page + 1):
                data = self.get_upcoming_events_page(facility_id, day, day, page=p, per_page=per_page, debug=debug)
                results = (data.get("data", {}) or {}).get("results", []) or []
                for r2 in results:
                    if int(r2.get("event_id", -1)) == target_event_id:
                        return _parse_event_local(r2, tz_name=DEFAULT_TZ)
        else:
            for p in range(2, max_pages + 1):
                data = self.get_upcoming_events_page(facility_id, day, day, page=p, per_page=per_page, debug=debug)
                results = (data.get("data", {}) or {}).get("results", []) or []
                if not results:
                    break
                for r2 in results:
                    if int(r2.get("event_id", -1)) == target_event_id:
                        return _parse_event_local(r2, tz_name=DEFAULT_TZ)
        return None


# ---------- Helpers (LOCAL parsing & logic) ----------
def _parse_event_local(ev: Dict[str, Any], tz_name: str) -> EventInfo:
    """
    Parse times exactly as API gives (local strings like 'YYYY-MM-DDTHH:MM'),
    attach America/Chicago tz. Prefer reservation_* over event_* for overlap.
    """
    tz = ZoneInfo(tz_name)
    tiers = ev.get("tiers") or []
    start_off = ev.get("event_starts_offset_display") or ev.get("event_starts_offset") or "00:00"
    end_off = ev.get("event_ends_offset_display") or ev.get("event_ends_offset") or "00:00"

    # Reservation-first to match booking windows
    e_starts_s = ev.get("reservation_starts") or ev.get("event_starts")
    e_ends_s   = ev.get("reservation_ends")   or ev.get("event_ends")

    def parse_local(s: Optional[str]) -> Optional[dt.datetime]:
        if not s:
            return None
        try:
            naive = dt.datetime.strptime(s, "%Y-%m-%dT%H:%M")
        except ValueError:
            naive = dt.datetime.fromisoformat(s)
            if naive.tzinfo is not None:
                return naive.astimezone(tz)
        return naive.replace(tzinfo=tz)

    return EventInfo(
        event_id=int(ev["event_id"]),
        rule_id=int(ev["rule_id"]) if ev.get("rule_id") is not None else None,
        event_starts_local=parse_local(e_starts_s),
        event_ends_local=parse_local(e_ends_s),
        starts_offset=str(start_off),
        ends_offset=str(end_off),
        tiers=[{"price": int(t["price"]), "order": int(t["order"]), "inventory": int(t.get("inventory", 0))} for t in tiers],
        raw=ev,
    )


def _tiers_pairs(tiers: List[Dict[str, Any]]) -> List[Tuple[int, int, int]]:
    # (order, price, inventory) sorted by order
    return sorted([(int(t["order"]), int(t["price"]), int(t.get("inventory", 0))) for t in tiers], key=lambda x: x[0])


def _alloc_inventory(tiers: List[Dict[str, Any]], total_qty: int) -> List[Dict[str, Any]]:
    out = [{"price": int(t["price"]), "order": int(t["order"]), "inventory": 0} for t in tiers]
    if total_qty <= 0 or not out:
        return out
    out.sort(key=lambda x: x["order"])
    remain, i = int(total_qty), 0
    while remain > 0:
        out[i]["inventory"] += 1
        remain -= 1
        i = (i + 1) % len(out)
    return out


def _event_within_local(ev: EventInfo, win_from_local: dt.datetime, win_to_local: dt.datetime) -> Tuple[bool, str]:
    # All arguments are tz-aware in America/Chicago
    if not ev.event_starts_local or not ev.event_ends_local:
        return False, "missing start/end"
    if ev.event_ends_local <= win_from_local:
        return False, f"ends({ev.event_ends_local}) <= win_from({win_from_local})"
    if ev.event_starts_local >= win_to_local:
        return False, f"starts({ev.event_starts_local}) >= win_to({win_to_local})"
    return True, "overlaps"


# ---------- Orchestrator ----------
def run_update_for_facility_all_rules(
    auth_token: str,
    facility_id: int,
    tz_name: str = DEFAULT_TZ,
    dry_run: bool = False,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Everything in LOCAL (America/Chicago) time:
      1) GET all inventory rules for today's Chicago window -> convert to LOCAL and keep local.
      2) Build a GLOBAL local-date span that covers all rules.
      3) Fetch ALL upcoming events across that global span with explicit page-walk (cap 30 pages).
      4) For each rule, filter by true LOCAL overlap and POST only when tiers differ.
    """
    tz = ZoneInfo(tz_name)
    today_local = dt.datetime.now(tz).date()
    client = SpotHeroClient(token=auth_token)

    # 1) Get rules (localized)
    rules = client.get_inventory_rules_range(facility_id=facility_id, range_start_date=today_local, tz_name=tz_name)
    if not rules:
        raise RuntimeError(f"No inventory rules for facility {facility_id} on {today_local} (tz={tz_name})")

    client._log(debug, f"[rules] found {len(rules)}")
    for r in (rules if debug else []):
        client._log(debug,
            "   - rule",
            "| qty=", r.quantity,
            "| valid_from_local=", r.valid_from_local,
            "| valid_to_local=", r.valid_to_local
        )

    # 2) Global span (local dates)
    vf_list = [r.valid_from_local.date() for r in rules]
    vt_list = [r.valid_to_local.date() for r in rules]
    global_from = min(vf_list)
    global_to = max(vt_list)
    client._log(debug, f"[global window local] from={global_from} to={global_to}")

    # 3) Fetch ALL events for the global span
    all_events_raw = list(
        client.iter_upcoming_events(
            facility_id,
            global_from,
            global_to,
            per_page=25,
            max_pages=30,
            debug=debug
        )
    )
    all_events = [_parse_event_local(e, tz_name=tz_name) for e in all_events_raw]

    client._log(debug, f"[events] fetched total={len(all_events)} across all pages")
    for ev in (all_events[:5] if debug else []):
        client._log(debug, "   sample-first:", "id=", ev.event_id, "local=", ev.event_starts_local, "→", ev.event_ends_local, "rule_id=", ev.rule_id)
    for ev in (all_events[-5:] if debug else []):
        client._log(debug, "   sample-last:", "id=", ev.event_id, "local=", ev.event_starts_local, "→", ev.event_ends_local, "rule_id=", ev.rule_id)

    summary: Dict[str, Any] = {
        "facility_id": facility_id,
        "tz": tz_name,
        "range_starts": f"{today_local:%m/%d/%Y}T00:00",
        "rules_found": len(rules),
        "global_from": f"{global_from:%Y-%m-%d}",
        "global_to": f"{global_to:%Y-%m-%d}",
        "events_fetched": len(all_events),
        "processed": [],
    }

    # 4) Per rule, filter + update when needed (all LOCAL)
    for rule in rules:
        from_date = rule.valid_from_local.date()
        to_date = max(rule.valid_from_local.date(), rule.valid_to_local.date())

        matched: List[Dict[str, Any]] = []
        overlapped_count = 0
        non_overlap_debug: List[str] = []

        for ev in all_events:
            ok, why = _event_within_local(ev, rule.valid_from_local, rule.valid_to_local)
            if not ok:
                if debug and len(non_overlap_debug) < 10:
                    non_overlap_debug.append(f"event {ev.event_id}: {why}")
                continue

            overlapped_count += 1
            desired = _alloc_inventory(ev.tiers, rule.quantity)
            before_pairs = _tiers_pairs(ev.tiers)
            desired_pairs = _tiers_pairs(desired)
            identical = before_pairs == desired_pairs

            if debug:
                client._log(debug,
                    f"[decision] event_id={ev.event_id} rule_id={ev.rule_id} "
                    f"before={before_pairs} desired={desired_pairs} identical={identical}"
                )

            post_result: Optional[Dict[str, Any]] = None
            verify_after: Optional[List[Tuple[int, int, int]]] = None
            applied: Optional[bool] = None

            if not identical:
                client._log(debug,
                    f"[POST] event_id={ev.event_id} qty={rule.quantity} offsets=({ev.starts_offset},{ev.ends_offset})"
                )
                post_result = client.post_tiered_event_rating_rules(
                    facility_id=facility_id,
                    event_ids=[ev.event_id],
                    tiers=desired,
                    event_starts_offset=ev.starts_offset,
                    event_ends_offset=ev.ends_offset,
                    rule_id=ev.rule_id,
                    stop_selling_before_duration=None,
                    dry_run=dry_run,
                )
                client._log(debug, "[POST][resp]", post_result)

                if not dry_run:
                    # Verify by re-fetching the event on its local start day
                    ev_day = ev.event_starts_local.date() if ev.event_starts_local else from_date
                    chk = client.get_event_on_day(
                        facility_id=facility_id,
                        day=ev_day,
                        target_event_id=ev.event_id,
                        per_page=25,
                        max_pages=10,
                        debug=debug,
                    )
                    if chk:
                        verify_after = _tiers_pairs(chk.tiers)
                        applied = (verify_after == desired_pairs)
                        client._log(debug, f"[VERIFY] event_id={ev.event_id} after={verify_after} applied={applied}")
                        if verify_after != desired_pairs:
                            client._log(debug,
                                "[WARN] Verify-after differs; SpotHero may have blocked reduction (used inventory/constraints)."
                            )
            else:
                client._log(debug, f"[SKIP] event_id={ev.event_id} — tiers already match desired allocation.")

            matched.append({
                "rule_window_local": {
                    "valid_from": rule.valid_from_local.isoformat(),
                    "valid_to": rule.valid_to_local.isoformat(),
                    "quantity": rule.quantity,
                },
                "event": {
                    "event_id": ev.event_id,
                    "rule_id": ev.rule_id,
                    "starts_local": ev.event_starts_local.isoformat() if ev.event_starts_local else None,
                    "ends_local": ev.event_ends_local.isoformat() if ev.event_ends_local else None,
                    "offsets": {"start": ev.starts_offset, "end": ev.ends_offset},
                },
                "tiers_before": before_pairs,
                "tiers_desired": desired_pairs,
                "skipped_no_change": identical,
                "post_result": post_result if dry_run else None,  # keep payload visible only in dry-run
                "tiers_after_verify": verify_after,
                "applied": applied,
            })

        # Clean, single-line rule summary when debug on
        client._log(debug, f"[rule] {from_date}→{to_date} qty={rule.quantity} | overlap_events={overlapped_count}")
        if debug and non_overlap_debug:
            client._log(debug, "   examples of non-overlap reasons:")
            for line in non_overlap_debug:
                client._log(debug, "    -", line)

        summary["processed"].append({
            "rule": {
                "valid_from_local": rule.valid_from_local.isoformat(),
                "valid_to_local": rule.valid_to_local.isoformat(),
                "from_date": f"{from_date:%Y-%m-%d}",
                "to_date": f"{to_date:%Y-%m-%d}",
                "quantity": rule.quantity,
            },
            "events_updated_or_skipped": matched,
        })

    return summary


# ---------- CLI ----------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python files/spothero_tool.py <AUTH_TOKEN> <FACILITY_ID> [--dry-run] [--debug] [TZ='America/Chicago']")
        sys.exit(2)
    token_cli = sys.argv[1]
    facility_cli = int(sys.argv[2])
    dry = False
    debug = False
    tz_cli = DEFAULT_TZ
    for arg in sys.argv[3:]:
        if arg == "--dry-run":
            dry = True
        elif arg == "--debug":
            debug = True
        else:
            tz_cli = arg
    res = run_update_for_facility_all_rules(
        auth_token=token_cli,
        facility_id=facility_cli,
        tz_name=tz_cli,
        dry_run=dry,
        debug=debug
    )
    print(json.dumps(res, indent=2))
