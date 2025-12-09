# files/spothero_tool.py
from __future__ import annotations

import datetime as dt
import json
import os
import time

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from zoneinfo import ZoneInfo

BASE_URL = "https://spothero.com/api/v1"
DEFAULT_TZ = "America/Chicago"

# Teams webhook (optional)
TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL", "").strip()
TEAMS_NOTIFY_DRY_RUN = (os.getenv("TEAMS_NOTIFY_DRY_RUN", "false").lower() in {"1", "true", "yes"})
# Enforce updates only when an inventory *exception* rule is active
ENFORCE_ONLY_IF_INVENTORY_EXCEPTION = (os.getenv("ENFORCE_ONLY_IF_INVENTORY_EXCEPTION", "true").lower() in {"1", "true", "yes"})

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
                if r.status_code not in RETRY_STATUS:
                    msg = (
                        f"{r.status_code} {r.reason} {method} {url} | "
                        f"params={params} body={bool(payload)} | resp={r.text[:400]}"
                    )
                    raise requests.HTTPError(msg, response=r)
            except requests.RequestException as e:
                last_exc = e

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

    # --- API: facility details (best-effort; optional) ---
    def get_facility_details(self, facility_id: int) -> Dict[str, Any]:
        try:
            url = f"{self.base_url}/facilities/{facility_id}/"
            r = self._send("GET", url, expect_status=(200, 204))
            data = r.json() if r.text else {}
            if isinstance(data, dict):
                d = data.get("data") or data
                name = d.get("name") or d.get("title") or d.get("display_name") or ""
                title = d.get("title") or d.get("name") or ""
                return {
                    "name": str(name) if name else "",
                    "title": str(title) if title else "",
                }
        except Exception:
            pass
        return {}

    # --- API: inventory-rules-range (GET) ---
    def get_inventory_rules_range(self, facility_id: int, range_start_date: dt.date, tz_name: str) -> List[InventoryRule]:
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

            vf_raw = item.get("valid_from")
            vt_raw = item.get("valid_to")
            try:
                vf = dt.datetime.fromisoformat(vf_raw.replace("Z", "+00:00"))
                vt = dt.datetime.fromisoformat(vt_raw.replace("Z", "+00:00"))
            except Exception:
                continue
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
    Use ONLY the event's own start/end (no reservation/offset shift).
    """
    tz = ZoneInfo(tz_name)
    tiers = ev.get("tiers") or []
    start_off = ev.get("event_starts_offset_display") or ev.get("event_starts_offset") or "00:00"
    end_off = ev.get("event_ends_offset_display") or ev.get("event_ends_offset") or "00:00"

    # Strictly use event times (not reservation_*). This avoids offset-based windows.
    e_starts_s = ev.get("event_starts")
    e_ends_s   = ev.get("event_ends")

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
    # kept for compatibility (overlap semantics); not used for decisions anymore
    if not ev.event_starts_local or not ev.event_ends_local:
        return False, "missing start/end"
    if ev.event_ends_local <= win_from_local:
        return False, f"ends({ev.event_ends_local}) <= win_from({win_from_local})"
    if ev.event_starts_local >= win_to_local:
        return False, f"starts({ev.event_starts_local}) >= win_to({win_to_local})"
    return True, "overlaps"


def _event_contained_in(ev: EventInfo, win_from_local: dt.datetime, win_to_local: dt.datetime) -> Tuple[bool, str]:
    # require full containment: rule window must fully contain the event window
    if not ev.event_starts_local or not ev.event_ends_local:
        return False, "missing start/end"
    if ev.event_starts_local < win_from_local:
        return False, f"event_starts({ev.event_starts_local}) < rule_from({win_from_local})"
    if ev.event_ends_local > win_to_local:
        return False, f"event_ends({ev.event_ends_local}) > rule_to({win_to_local})"
    return True, "contained"


def _overlap_minutes(a_from: dt.datetime, a_to: dt.datetime, b_from: dt.datetime, b_to: dt.datetime) -> int:
    start = max(a_from, b_from)
    end = min(a_to, b_to)
    return max(0, int((end - start).total_seconds() // 60))


def _is_exact_window_match(ev: EventInfo, r: InventoryRule) -> bool:
    return (ev.event_starts_local == r.valid_from_local) and (ev.event_ends_local == r.valid_to_local)


def _is_rule_exception(raw: Dict[str, Any]) -> bool:
    if not isinstance(raw, dict):
        return False
    if raw.get("is_exception") is True:
        return True
    if raw.get("exception") is True:
        return True
    if str(raw.get("rule_type", "")).lower() == "exception":
        return True
    if raw.get("is_default") is False:
        return True
    return False


def _containing_controller(rules: List[InventoryRule], ev: EventInfo) -> Tuple[Optional[int], Optional[InventoryRule]]:
    """
    Among rules that FULLY CONTAIN the event and are exceptions, pick controller:
      1) Prefer exact time match.
      2) Else choose minimal quantity.
      3) Tie-break by largest overlap minutes, then latest valid_from.
    Returns (target_qty, controller_rule) or (None, None) if none contain.
    """
    contenders: List[Tuple[InventoryRule, int, bool]] = []
    for r in rules:
        if not _is_rule_exception(r.raw):
            continue
        ok, _ = _event_contained_in(ev, r.valid_from_local, r.valid_to_local)
        if not ok:
            continue
        minutes = _overlap_minutes(r.valid_from_local, r.valid_to_local,
                                   ev.event_starts_local, ev.event_ends_local)
        contenders.append((r, minutes, _is_exact_window_match(ev, r)))

    if not contenders:
        return None, None

    exacts = [c for c in contenders if c[2] is True]
    pool = exacts if exacts else contenders

    min_qty = min(c[0].quantity for c in pool)
    pool = [c for c in pool if c[0].quantity == min_qty]

    pool.sort(key=lambda c: (c[1], c[0].valid_from_local), reverse=True)
    controller = pool[0][0]
    return controller.quantity, controller


# ---------- Teams notifier (Adaptive Card) ----------
def _post_teams_card(card: Dict[str, Any]) -> None:
    if not TEAMS_WEBHOOK_URL:
        return
    try:
        payload = {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": card
            }]
        }
        requests.post(TEAMS_WEBHOOK_URL, json=payload, timeout=10)
    except Exception:
        pass


def _fmt_local(ts: Optional[dt.datetime]) -> str:
    return ts.strftime("%Y-%m-%d %H:%M %Z") if ts else "—"


def _tier_header_row() -> Dict[str, Any]:
    return {
        "type": "ColumnSet",
        "spacing": "Small",
        "separator": True,
        "columns": [
            {"type": "Column", "width": "auto",
             "items": [{"type": "TextBlock", "text": "Tier", "weight": "Bolder"}]},
            {"type": "Column", "width": "stretch",
             "items": [{"type": "TextBlock", "text": "Price", "weight": "Bolder"}]},
            {"type": "Column", "width": "stretch",
             "items": [{"type": "TextBlock", "text": "Old Inv.", "weight": "Bolder"}]},
            {"type": "Column", "width": "stretch",
             "items": [{"type": "TextBlock", "text": "New Inv.", "weight": "Bolder"}]},
        ],
    }


def _tier_row(order: int, price: int, inv_old: int, inv_new: int) -> Dict[str, Any]:
    return {
        "type": "ColumnSet",
        "spacing": "None",
        "columns": [
            {"type": "Column", "width": "auto",
             "items": [{"type": "TextBlock", "text": str(order)}]},
            {"type": "Column", "width": "stretch",
             "items": [{"type": "TextBlock", "text": f"${price}"}]},
            {"type": "Column", "width": "stretch",
             "items": [{"type": "TextBlock", "text": str(inv_old)}]},
            {"type": "Column", "width": "stretch",
             "items": [{"type": "TextBlock", "text": str(inv_new)}]},
        ],
    }


def _build_change_card(
    *,
    facility_id: int,
    facility_name: str,
    facility_title: str,
    tz_name: str,
    rule_from: dt.datetime,
    rule_to: dt.datetime,
    rule_qty: int,
    event: EventInfo,
    before_pairs: List[Tuple[int, int, int]],
    desired_pairs: List[Tuple[int, int, int]],
    after_pairs: Optional[List[Tuple[int, int, int]]],
    applied: Optional[bool],
    dry_run: bool,
    status: str,
) -> Dict[str, Any]:
    def sum_inv(pairs: List[Tuple[int, int, int]]) -> int:
        return sum(inv for _, _, inv in pairs)

    old_total = sum_inv(before_pairs)
    new_total = sum_inv(desired_pairs)

    subheader = (
        "Inventory Update (Dry-run)" if dry_run else
        "Inventory Verify Mismatch" if status == "verify_mismatch" else
        "Inventory Update Exception" if status == "exception" else
        "Inventory Update — Applied"
    )

    card: Dict[str, Any] = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "msteams": {"width": "Full"},
        "body": [
            {
                "type": "Container",
                "style": "emphasis",
                "bleed": True,
                "items": [
                    {"type": "TextBlock", "text": subheader, "weight": "Bolder", "size": "Large"},
                    {"type": "TextBlock", "text": "SpotHero Inventory Bot", "isSubtle": True, "spacing": "None"},
                ],
            },
            {
                "type": "Container",
                "spacing": "Medium",
                "items": [
                    {"type": "TextBlock", "text": "Summary", "weight": "Bolder", "size": "Medium"},
                    {
                        "type": "ColumnSet",
                        "columns": [
                            {
                                "type": "Column", "width": "stretch",
                                "items": [
                                    {"type": "TextBlock", "text": "Old Inventory", "isSubtle": True},
                                    {"type": "TextBlock", "text": str(old_total), "weight": "Bolder", "size": "Medium"},
                                ],
                            },
                            {
                                "type": "Column", "width": "stretch",
                                "items": [
                                    {"type": "TextBlock", "text": "New Inventory", "isSubtle": True},
                                    {"type": "TextBlock", "text": str(new_total), "weight": "Bolder", "size": "Medium"},
                                ],
                            },
                        ],
                    },
                ],
            },
            {
                "type": "Container",
                "spacing": "Medium",
                "items": [
                    {"type": "TextBlock", "text": "Details", "weight": "Bolder", "size": "Medium"},
                    {
                        "type": "FactSet",
                        "facts": [
                            {"title": "Facility", "value": facility_title or facility_name or str(facility_id)},
                            {"title": "Facility ID", "value": str(facility_id)},
                            {"title": "Rule Window", "value": f"{_fmt_local(rule_from)} -> {_fmt_local(rule_to)} ({tz_name})"},
                            {"title": "Applied", "value": "true" if applied else ("false" if applied is not None else "n/a")},
                            {"title": "Event", "value": f"#{event.event_id}  { _fmt_local(event.event_starts_local) } -> { _fmt_local(event.event_ends_local) }"},
                        ],
                    },
                ],
            },
            {
                "type": "Container",
                "spacing": "Medium",
                "items": [
                    {"type": "TextBlock",
                     "text": "Posted by Flow bot • Values are local to America/Chicago",
                     "isSubtle": True, "size": "Small"}
                ],
            },
        ],
        "actions": [
            {"type": "Action.OpenUrl", "title": "Open SpotHero", "url": "https://spothero.com/operator"}
        ],
    }

    return card


def _build_exception_card(
    *,
    facility_id: int,
    facility_name: str,
    facility_title: str,
    tz_name: str,
    rule_from: dt.datetime,
    rule_to: dt.datetime,
    rule_qty: int,
    event: Optional[EventInfo],
    error_text: str,
) -> Dict[str, Any]:
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "msteams": {"width": "Full"},
        "body": [
            {"type": "TextBlock", "text": "Inventory Update Exception", "weight": "Bolder", "size": "Large"},
            {"type": "TextBlock", "text": f"Facility: {facility_title or facility_name or str(facility_id)}", "wrap": True},
            {"type": "FactSet", "facts": [
                {"title": "Facility ID", "value": str(facility_id)},
                {"title": "Facility Name", "value": facility_name or "—"},
                {"title": "Facility Title", "value": facility_title or "—"},
                {"title": "Rule Window", "value": f"{_fmt_local(rule_from)} → {_fmt_local(rule_to)} ({tz_name})"},
                {"title": "Total Stalls (rule qty)", "value": str(rule_qty)},
            ]},
            {"type": "TextBlock", "text": f"Error: {error_text[:600]}", "wrap": True, "spacing": "Medium"},
            {"type": "TextBlock", "text": (f"Event #{event.event_id}" if event else "Event: n/a"), "isSubtle": True, "wrap": True},
        ],
        "actions": [
            {
                "type": "ActionSet",
                "actions": [
                    { "type": "Action.OpenUrl", "title": "Open SpotHero", "url": "https://spothero.com/operator" }
                ]
            }
        ],
        "style": "attention"
    }


# ---------- Orchestrator ----------
def run_update_for_facility_all_rules(
    auth_token: str,
    facility_id: int,
    tz_name: str = DEFAULT_TZ,
    dry_run: bool = False,
    debug: bool = False,
) -> Dict[str, Any]:
    tz = ZoneInfo(tz_name)
    today_local = dt.datetime.now(tz).date()
    client = SpotHeroClient(token=auth_token)

    fac_detail = client.get_facility_details(facility_id) if TEAMS_WEBHOOK_URL else {}
    facility_name = fac_detail.get("name", "") if isinstance(fac_detail, dict) else ""
    facility_title = fac_detail.get("title", "") if isinstance(fac_detail, dict) else ""

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

    vf_list = [r.valid_from_local.date() for r in rules]
    vt_list = [r.valid_to_local.date() for r in rules]
    global_from = min(vf_list)
    global_to = max(vt_list)
    client._log(debug, f"[global window local] from={global_from} to={global_to}")

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

    # Containment-based logic with a single controller per event.
    for rule in rules:
        from_date = rule.valid_from_local.date()
        to_date = max(rule.valid_from_local.date(), rule.valid_to_local.date())

        rule_is_exception = _is_rule_exception(rule.raw)

        matched: List[Dict[str, Any]] = []
        considered_count = 0
        non_contain_debug: List[str] = []

        for ev in all_events:
            # Require full containment of the event within the rule window (not overlap)
            ok, why = _event_contained_in(ev, rule.valid_from_local, rule.valid_to_local)
            if not ok:
                if debug and len(non_contain_debug) < 10:
                    non_contain_debug.append(f"event {ev.event_id}: {why}")
                continue

            # Must be exception when guard is enabled
            if ENFORCE_ONLY_IF_INVENTORY_EXCEPTION and not rule_is_exception:
                client._log(debug, f"[SKIP] event_id={ev.event_id} — rule is not an exception.")
                continue

            # Determine the *controller* among all containing exception rules
            target_qty, controller = _containing_controller(rules, ev)
            if controller is None:
                client._log(debug, f"[SKIP] event_id={ev.event_id} — no containing exception found.")
                continue
            if controller is not rule:
                client._log(debug, f"[SKIP] event_id={ev.event_id} — another rule controls (qty={target_qty}).")
                continue

            considered_count += 1

            # Allocate using controller's quantity (not offsets)
            desired = _alloc_inventory(ev.tiers, target_qty)
            before_pairs = _tiers_pairs(ev.tiers)
            desired_pairs = _tiers_pairs(desired)
            identical = before_pairs == desired_pairs

            if debug:
                client._log(debug,
                    f"[decision] event_id={ev.event_id} controller_qty={target_qty} "
                    f"before={before_pairs} desired={desired_pairs} identical={identical}"
                )

            post_result: Optional[Dict[str, Any]] = None
            verify_after: Optional[List[Tuple[int, int, int]]] = None
            applied: Optional[bool] = None

            if not identical:
                client._log(debug, f"[POST] event_id={ev.event_id} qty={target_qty}")
                try:
                    # Offsets are *not* used for filtering; they are passed through as-is to satisfy API schema.
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
                except Exception:
                    raise

                if not dry_run:
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
                        if applied:
                            _post_teams_card(_build_change_card(
                                facility_id=facility_id,
                                facility_name=facility_name,
                                facility_title=facility_title,
                                tz_name=tz_name,
                                rule_from=rule.valid_from_local,
                                rule_to=rule.valid_to_local,
                                rule_qty=target_qty,
                                event=ev,
                                before_pairs=before_pairs,
                                desired_pairs=desired_pairs,
                                after_pairs=verify_after,
                                applied=True,
                                dry_run=False,
                                status="applied",
                            ))
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
                "post_result": post_result if dry_run else None,
                "tiers_after_verify": verify_after,
                "applied": applied,
            })

        client._log(debug, f"[rule] {from_date}→{to_date} qty={rule.quantity} | containing_events={considered_count}")
        if debug and non_contain_debug:
            client._log(debug, "   examples of non-containment reasons:")
            for line in non_contain_debug:
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
