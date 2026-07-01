# files/parkwhiz_processor.py
"""Process ParkWhiz reservation emails: parse, match SpotHero events, reduce inventory."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dateutil import parser as date_parser
from zoneinfo import ZoneInfo

from files.facilities import best_match_from_list
from files.gmail_client import (
    GmailClient,
    extract_headers,
    extract_message_text,
    message_received_at,
)
from files.spothero_tool import (
    SpotHeroClient,
    _alloc_inventory_smart,
    _parse_event_local,
    _tiers_pairs,
)

log = logging.getLogger("app.parkwhiz")

DEFAULT_TZ = os.getenv("SH_UPDATE_TZ", "America/Chicago")
PARKWHIZ_TEAMS_WEBHOOK_URL = os.getenv("PARKWHIZ_TEAMS_WEBHOOK_URL", "").strip()
PARKWHIZ_MATCH_TOLERANCE_MINUTES = int(os.getenv("PARKWHIZ_MATCH_TOLERANCE_MINUTES", "20"))
PARKWHIZ_STATE_FILE = os.getenv("PARKWHIZ_STATE_FILE", os.path.join("files", "parkwhiz_processed.json"))
PARKWHIZ_DRY_RUN = os.getenv("PARKWHIZ_DRY_RUN", "false").lower() in {"1", "true", "yes"}
PARKWHIZ_MAX_WORKERS = max(1, int(os.getenv("PARKWHIZ_MAX_WORKERS", "3")))


@dataclass
class ParsedReservation:
    facility_name: str = ""
    event_title: str = ""
    confirmation_number: str = ""
    purchaser: str = ""
    gross_price: str = ""
    reservation_start: Optional[dt.datetime] = None
    reservation_end: Optional[dt.datetime] = None
    raw_time_text: str = ""
    parse_ok: bool = False
    parse_notes: List[str] = field(default_factory=list)


@dataclass
class ProcessResult:
    message_id: str
    subject: str
    parsed: ParsedReservation
    facility_match: Optional[Dict[str, Any]] = None
    event_match: Optional[Dict[str, Any]] = None
    inventory_applied: bool = False
    inventory_before: Optional[int] = None
    inventory_after: Optional[int] = None
    inventory_skipped_reason: str = ""
    error: str = ""
    processed_at: Optional[str] = None


# ---------- Email parsing ----------
_SUBJECT_FACILITY_RE = re.compile(
    r"Reservation\s+Made\s+for\s+(.+?)\s+Confirmation\s*#",
    re.I,
)
_BODY_FACILITY_RE = re.compile(
    r"(?:booking has been made for|made for)\s+(.+?)(?:\s+is now sold out|\s*Confirmation)",
    re.I,
)
_CONFIRMATION_RE = re.compile(r"Confirmation\s*#?\s*(\d+)", re.I)
_PURCHASER_RE = re.compile(r"Purchased\s+by:\s*(.+)", re.I)
_PRICE_RE = re.compile(r"Seller\s+Gross\s+Price:\s*(\$[\d,.]+)", re.I)

_TIME_SINGLE_RE = re.compile(
    r"(\d{1,2}:\d{2}\s*[ap]\.?m\.?)\s+on\s+"
    r"((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+\w+\s+\d{1,2},?\s+\d{4})"
    r"(?:\s*\+(\d+)hr\s+after\s+event)?",
    re.I,
)
_TIME_RANGE_RE = re.compile(
    r"((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+\w+\s+\d{1,2})\s+at\s+(\d{1,2}:\d{2}\s*[ap]\.?m\.?)"
    r"\s+to\s+"
    r"((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+\w+\s+\d{1,2})\s+at\s+(\d{1,2}:\d{2}\s*[ap]\.?m\.?)",
    re.I,
)


def _parse_dt(text: str, tz_name: str, default_year: Optional[int] = None) -> Optional[dt.datetime]:
    try:
        parsed = date_parser.parse(text, fuzzy=True, default=dt.datetime(default_year or dt.date.today().year, 1, 1))
        tz = ZoneInfo(tz_name)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=tz)
        return parsed.astimezone(tz)
    except Exception:
        return None


def _extract_facility(subject: str, body: str) -> str:
    m = _SUBJECT_FACILITY_RE.search(subject or "")
    if m:
        return m.group(1).strip()
    m = _BODY_FACILITY_RE.search(body or "")
    if m:
        return m.group(1).strip()
    return ""


def _find_event_title_near_time(body: str, time_line: str) -> str:
    lines = [ln.strip() for ln in (body or "").splitlines() if ln.strip()]
    for i, ln in enumerate(lines):
        if time_line in ln or _TIME_SINGLE_RE.search(ln) or _TIME_RANGE_RE.search(ln):
            if i > 0:
                prev = lines[i - 1]
                if prev.lower() not in {"weekday", "weekend"} and not _TIME_SINGLE_RE.search(prev):
                    return prev
    return ""


def parse_parkwhiz_email(*, subject: str, body: str, tz_name: str = DEFAULT_TZ) -> ParsedReservation:
    out = ParsedReservation()
    out.facility_name = _extract_facility(subject, body)
    conf_m = _CONFIRMATION_RE.search(subject + "\n" + body)
    out.confirmation_number = conf_m.group(1) if conf_m else ""
    pm = _PURCHASER_RE.search(body or "")
    if pm:
        out.purchaser = pm.group(1).strip()
    pr = _PRICE_RE.search(body or "")
    if pr:
        out.gross_price = pr.group(1).strip()

    text = body or ""
    default_year = dt.date.today().year

    rm = _TIME_RANGE_RE.search(text)
    if rm:
        start_text = f"{rm.group(1)} {rm.group(2)}"
        end_text = f"{rm.group(3)} {rm.group(4)}"
        out.raw_time_text = rm.group(0).strip()
        out.reservation_start = _parse_dt(start_text, tz_name, default_year)
        end_year = out.reservation_start.year if out.reservation_start else default_year
        out.reservation_end = _parse_dt(end_text, tz_name, end_year)
        if out.reservation_end and out.reservation_start and out.reservation_end < out.reservation_start:
            out.reservation_end = out.reservation_end.replace(year=out.reservation_end.year + 1)
        out.event_title = _find_event_title_near_time(text, out.raw_time_text)
        if out.reservation_start and out.reservation_end:
            out.parse_ok = True
        else:
            out.parse_notes.append("Failed to parse range time window")
        return out

    sm = _TIME_SINGLE_RE.search(text)
    if sm:
        out.raw_time_text = sm.group(0).strip()
        when_text = f"{sm.group(1)} on {sm.group(2)}"
        base_dt = _parse_dt(when_text, tz_name)
        offset_hrs = int(sm.group(3)) if sm.group(3) else 0
        if base_dt:
            out.reservation_start = base_dt
            if offset_hrs > 0:
                out.parse_notes.append(f"Event offset hint: +{offset_hrs}hr after event")
            out.parse_ok = True
        else:
            out.parse_notes.append("Failed to parse single datetime")
        out.event_title = _find_event_title_near_time(text, out.raw_time_text)
        return out

    out.parse_notes.append("No recognizable ParkWhiz time format found")
    return out


# ---------- SpotHero matching & inventory ----------
def _parse_api_dt(val: Optional[str], tz_name: str) -> Optional[dt.datetime]:
    if not val:
        return None
    tz = ZoneInfo(tz_name)
    try:
        naive = dt.datetime.strptime(val, "%Y-%m-%dT%H:%M")
    except ValueError:
        naive = dt.datetime.fromisoformat(val.replace("Z", "+00:00"))
        if naive.tzinfo is not None:
            return naive.astimezone(tz)
    return naive.replace(tzinfo=tz)


def _event_time_windows(ev_raw: Dict[str, Any], tz_name: str) -> List[Tuple[str, dt.datetime, dt.datetime]]:
    """Return (label, start, end) pairs for both event and reservation windows."""
    windows: List[Tuple[str, dt.datetime, dt.datetime]] = []
    for label, sk, ek in (
        ("event", "event_starts", "event_ends"),
        ("reservation", "reservation_starts", "reservation_ends"),
    ):
        start = _parse_api_dt(ev_raw.get(sk), tz_name)
        end = _parse_api_dt(ev_raw.get(ek), tz_name)
        if start:
            if not end:
                end = start + dt.timedelta(hours=4)
            windows.append((label, start, end))
    return windows


def _minutes_apart(a: dt.datetime, b: dt.datetime) -> int:
    return int(abs((a - b).total_seconds()) // 60)


def _candidate_start_times(parsed: ParsedReservation) -> List[dt.datetime]:
    """Build candidate reservation start times for matching (raw + offset variants)."""
    if not parsed.reservation_start:
        return []
    starts = [parsed.reservation_start]
    m = _TIME_SINGLE_RE.search(parsed.raw_time_text or "")
    if m and m.group(3):
        offset_hrs = int(m.group(3))
        shifted = parsed.reservation_start + dt.timedelta(hours=offset_hrs)
        if shifted not in starts:
            starts.append(shifted)
    return starts


def _find_matching_event(
    client: SpotHeroClient,
    facility_id: int,
    parsed: ParsedReservation,
    tz_name: str,
    *,
    tolerance_min: int = PARKWHIZ_MATCH_TOLERANCE_MINUTES,
    debug: bool = False,
) -> Tuple[Optional[Dict[str, Any]], str]:
    candidates_starts = _candidate_start_times(parsed)
    if not candidates_starts:
        return None, "missing_reservation_start"

    best_match: Optional[Dict[str, Any]] = None
    best_score = 10**9
    best_why = "no_matching_event_in_tolerance"

    for cand_start in candidates_starts:
        probe = ParsedReservation(
            facility_name=parsed.facility_name,
            reservation_start=cand_start,
            reservation_end=parsed.reservation_end,
            raw_time_text=parsed.raw_time_text,
            parse_ok=True,
        )
        match, why = _find_matching_event_for_start(
            client, facility_id, probe, tz_name, tolerance_min=tolerance_min, debug=debug
        )
        if match and int(match.get("match_score_minutes", 10**9)) < best_score:
            best_match = match
            best_score = int(match["match_score_minutes"])
            best_why = why

    if best_match:
        return best_match, best_why
    return None, best_why


def _find_matching_event_for_start(
    client: SpotHeroClient,
    facility_id: int,
    parsed: ParsedReservation,
    tz_name: str,
    *,
    tolerance_min: int = PARKWHIZ_MATCH_TOLERANCE_MINUTES,
    debug: bool = False,
) -> Tuple[Optional[Dict[str, Any]], str]:
    if not parsed.reservation_start:
        return None, "missing_reservation_start"

    start_day = parsed.reservation_start.date()
    end_day = (parsed.reservation_end or parsed.reservation_start).date()
    if end_day < start_day:
        end_day = start_day

    # widen search by a day on each side for cross-midnight windows
    search_from = start_day - dt.timedelta(days=1)
    search_to = end_day + dt.timedelta(days=1)

    candidates: List[Tuple[int, Dict[str, Any], dt.datetime, dt.datetime, str]] = []
    for ev_raw in client.iter_upcoming_events(facility_id, search_from, search_to, per_page=25, max_pages=30, debug=debug):
        for win_label, win_start, win_end in _event_time_windows(ev_raw, tz_name):
            start_delta = _minutes_apart(win_start, parsed.reservation_start)
            if parsed.reservation_end:
                end_delta = _minutes_apart(win_end, parsed.reservation_end)
                score = start_delta + end_delta
                if start_delta <= tolerance_min and end_delta <= tolerance_min:
                    candidates.append((score, ev_raw, win_start, win_end, win_label))
            elif start_delta <= tolerance_min:
                candidates.append((start_delta, ev_raw, win_start, win_end, win_label))

    if not candidates:
        return None, "no_matching_event_in_tolerance"

    candidates.sort(key=lambda x: x[0])
    best = candidates[0]
    ev_raw = best[1]
    return {
        "event_id": int(ev_raw["event_id"]),
        "rule_id": ev_raw.get("rule_id"),
        "matched_window": best[4],
        "reservation_start": best[2].isoformat(),
        "reservation_end": best[3].isoformat(),
        "event_starts_offset": ev_raw.get("event_starts_offset_display") or ev_raw.get("event_starts_offset") or "00:00",
        "event_ends_offset": ev_raw.get("event_ends_offset_display") or ev_raw.get("event_ends_offset") or "00:00",
        "raw": ev_raw,
        "match_score_minutes": best[0],
    }, "matched"


def reduce_event_inventory_by_one(
    *,
    auth_token: str,
    facility_id: int,
    event_raw: Dict[str, Any],
    tz_name: str = DEFAULT_TZ,
    dry_run: bool = False,
    debug: bool = False,
) -> Dict[str, Any]:
    client = SpotHeroClient(token=auth_token)
    ev = _parse_event_local(event_raw, tz_name=tz_name)
    current_total = sum(int(t.get("inventory", 0)) for t in ev.tiers)
    target_qty = max(0, current_total - 1)

    if current_total <= 0:
        return {"applied": False, "reason": "inventory_already_zero", "before": current_total, "after": current_total}

    current_tier_number = None
    try:
        current_tier_number = int((ev.raw.get("current_tier") or {}).get("current_tier_number"))
    except Exception:
        pass

    desired = _alloc_inventory_smart(ev.tiers, target_qty, current_tier_number)
    before_pairs = _tiers_pairs(ev.tiers)
    desired_pairs = _tiers_pairs(desired)

    if before_pairs == desired_pairs:
        return {"applied": False, "reason": "no_change_needed", "before": current_total, "after": current_total}

    client._log(debug, f"[PARKWHIZ] POST reduce inventory event_id={ev.event_id} {current_total}->{target_qty}")
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

    applied = False
    verify_after = None
    if not dry_run:
        ev_day = ev.event_starts_local.date() if ev.event_starts_local else dt.date.today()
        chk = client.get_event_on_day(facility_id, ev_day, ev.event_id, debug=debug)
        if chk:
            verify_after = _tiers_pairs(chk.tiers)
            applied = verify_after == desired_pairs

    return {
        "applied": applied if not dry_run else False,
        "dry_run": dry_run,
        "before": current_total,
        "after": target_qty,
        "before_pairs": before_pairs,
        "desired_pairs": desired_pairs,
        "after_pairs": verify_after,
        "post_result": post_result,
    }


# ---------- Teams cards (separate webhook) ----------
def _post_parkwhiz_teams_card(card: Dict[str, Any]) -> None:
    if not PARKWHIZ_TEAMS_WEBHOOK_URL:
        return
    try:
        payload = {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": card,
            }],
        }
        requests.post(PARKWHIZ_TEAMS_WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:
        log.warning("ParkWhiz Teams post failed: %s", e)


def _fmt_dt(ts: Optional[dt.datetime]) -> str:
    return ts.strftime("%a, %b %d %Y %I:%M %p %Z") if ts else "—"


def _fmt_dt_short(ts: Optional[dt.datetime]) -> str:
    return ts.strftime("%b %d · %I:%M %p") if ts else "—"


def _sh_event_title(ev_raw: Optional[Dict[str, Any]], fallback: str = "") -> str:
    if not ev_raw:
        return fallback or "—"
    for key in ("event_title", "title", "name", "event_name"):
        val = ev_raw.get(key)
        if val:
            return str(val).strip()
    nested = ev_raw.get("event") or ev_raw.get("canonical_event") or {}
    if isinstance(nested, dict):
        for key in ("title", "name", "event_title"):
            val = nested.get(key)
            if val:
                return str(val).strip()
    return fallback or "—"


def _status_meta(result: ProcessResult) -> Dict[str, str]:
    if result.inventory_applied:
        return {"label": "Inventory Updated", "color": "Good", "icon": "✓"}
    if result.inventory_skipped_reason == "time_parse_failed":
        return {"label": "Review — Time Not Parsed", "color": "Warning", "icon": "!"}
    if result.inventory_skipped_reason in {"facility_not_matched", "no_matching_event_in_tolerance"}:
        return {"label": "Review — No SpotHero Match", "color": "Warning", "icon": "?"}
    return {"label": "Review Required", "color": "Attention", "icon": "!"}


def build_parkwhiz_review_card(
    *,
    result: ProcessResult,
    tz_name: str = DEFAULT_TZ,
) -> Dict[str, Any]:
    p = result.parsed
    fac = result.facility_match or {}
    ev = result.event_match or {}
    ev_raw = ev.get("raw") or {}
    status = _status_meta(result)
    sh_event = _sh_event_title(ev_raw, p.event_title)
    facility_id = str(fac.get("id") or "—")
    event_id = str(ev.get("event_id") or "—")

    inv_before = result.inventory_before
    inv_after = result.inventory_after
    inv_changed = (
        inv_before is not None
        and inv_after is not None
        and inv_before != inv_after
    )

    sh_window_start = ev.get("reservation_start") or "—"
    sh_window_end = ev.get("reservation_end") or "—"
    matched_via = ev.get("matched_window") or "event"

    body: List[Dict[str, Any]] = [
        {
            "type": "Container",
            "style": "emphasis",
            "bleed": True,
            "items": [
                {
                    "type": "ColumnSet",
                    "columns": [
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {"type": "TextBlock", "text": "ParkWhiz → SpotHero", "weight": "Bolder", "size": "Large"},
                                {"type": "TextBlock", "text": "Reservation sync · 2026", "isSubtle": True, "spacing": "None", "size": "Small"},
                            ],
                        },
                        {
                            "type": "Column",
                            "width": "auto",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": f"{status['icon']} {status['label']}",
                                    "color": status["color"],
                                    "weight": "Bolder",
                                    "horizontalAlignment": "Right",
                                    "wrap": True,
                                },
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
                {"type": "TextBlock", "text": "Inventory Change (Control Panel)", "weight": "Bolder", "size": "Medium"},
                {
                    "type": "ColumnSet",
                    "columns": [
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {"type": "TextBlock", "text": "Before", "isSubtle": True, "size": "Small"},
                                {"type": "TextBlock", "text": str(inv_before if inv_before is not None else "—"), "size": "ExtraLarge", "weight": "Bolder"},
                            ],
                        },
                        {
                            "type": "Column",
                            "width": "auto",
                            "items": [{"type": "TextBlock", "text": "→", "size": "ExtraLarge", "horizontalAlignment": "Center"}],
                        },
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {"type": "TextBlock", "text": "After", "isSubtle": True, "size": "Small"},
                                {
                                    "type": "TextBlock",
                                    "text": str(inv_after if inv_after is not None else "—"),
                                    "size": "ExtraLarge",
                                    "weight": "Bolder",
                                    "color": "Good" if inv_changed else "Default",
                                },
                            ],
                        },
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {"type": "TextBlock", "text": "Applied", "isSubtle": True, "size": "Small"},
                                {
                                    "type": "TextBlock",
                                    "text": "Yes" if result.inventory_applied else "No",
                                    "weight": "Bolder",
                                    "color": "Good" if result.inventory_applied else "Warning",
                                },
                            ],
                        },
                    ],
                },
            ],
        },
        {
            "type": "ColumnSet",
            "spacing": "Medium",
            "separator": True,
            "columns": [
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {"type": "TextBlock", "text": "ParkWhiz Reservation", "weight": "Bolder", "size": "Medium"},
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Facility", "value": (p.facility_name or "—")[:120]},
                                {"title": "Confirmation", "value": p.confirmation_number or "—"},
                                {"title": "Purchaser", "value": p.purchaser or "—"},
                                {"title": "Gross Price", "value": p.gross_price or "—"},
                                {"title": "Event (email)", "value": (p.event_title or "—")[:80]},
                                {"title": "Start", "value": _fmt_dt_short(p.reservation_start)},
                                {"title": "End", "value": _fmt_dt_short(p.reservation_end)},
                            ],
                        },
                    ],
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {"type": "TextBlock", "text": "SpotHero Control Panel", "weight": "Bolder", "size": "Medium"},
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "Facility", "value": (fac.get("name") or "—")[:80]},
                                {"title": "SH ID", "value": facility_id},
                                {"title": "Event", "value": sh_event[:80]},
                                {"title": "Event ID", "value": f"#{event_id}"},
                                {"title": "Window", "value": f"{sh_window_start} → {sh_window_end}"[:120]},
                                {"title": "Matched via", "value": matched_via},
                                {"title": "Timezone", "value": tz_name},
                            ],
                        },
                    ],
                },
            ],
        },
    ]

    if result.error or p.parse_notes:
        body.append({
            "type": "Container",
            "spacing": "Small",
            "style": "warning" if not result.inventory_applied else "default",
            "items": [
                {"type": "TextBlock", "text": "Notes", "weight": "Bolder", "size": "Small"},
                {"type": "TextBlock", "text": (result.error or "; ".join(p.parse_notes))[:400], "wrap": True, "isSubtle": True},
            ],
        })

    body.append({
        "type": "TextBlock",
        "text": f"Processed {result.processed_at or '—'} · {result.subject[:100]}",
        "isSubtle": True,
        "size": "Small",
        "wrap": True,
    })

    operator_url = "https://spothero.com/operator"
    if facility_id.isdigit():
        operator_url = f"https://spothero.com/operator/facilities/{facility_id}/"

    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "msteams": {"width": "Full"},
        "body": body,
        "actions": [
            {"type": "Action.OpenUrl", "title": "Open Facility in SpotHero", "url": operator_url},
            {"type": "Action.OpenUrl", "title": "SpotHero Operator", "url": "https://spothero.com/operator"},
        ],
    }


# ---------- State & orchestration ----------
def _load_state(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"processed_ids": []}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("processed_ids"), list):
            return data
    except Exception:
        pass
    return {"processed_ids": []}


def _save_state(path: str, state: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    ids = state.get("processed_ids") or []
    state["processed_ids"] = ids[-500:]
    p.write_text(json.dumps(state, indent=2), encoding="utf-8")


def process_parkwhiz_message(
    *,
    gmail: GmailClient,
    message_id: str,
    auth_token: str,
    facilities: List[Dict[str, Any]],
    tz_name: str = DEFAULT_TZ,
    dry_run: bool = False,
    debug: bool = False,
) -> ProcessResult:
    msg = gmail.get_message(message_id)
    headers = extract_headers(msg)
    subject = headers.get("subject", "")
    body = extract_message_text(msg)
    parsed = parse_parkwhiz_email(subject=subject, body=body, tz_name=tz_name)

    result = ProcessResult(message_id=message_id, subject=subject, parsed=parsed)
    result.processed_at = dt.datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M %Z")

    if not parsed.facility_name:
        result.inventory_skipped_reason = "facility_not_found_in_email"
        result.error = "Could not extract facility name"
        return result

    if not parsed.parse_ok:
        result.inventory_skipped_reason = "time_parse_failed"
        result.error = "; ".join(parsed.parse_notes) or "Time/date not recognized"
        return result

    match = best_match_from_list(parsed.facility_name, facilities)
    if not match:
        result.inventory_skipped_reason = "facility_not_matched"
        result.error = f"No SpotHero facility match for: {parsed.facility_name}"
        return result
    result.facility_match = match

    try:
        facility_id = int(match["id"])
        client = SpotHeroClient(token=auth_token)
        ev_match, why = _find_matching_event(client, facility_id, parsed, tz_name, debug=debug)
        if not ev_match:
            result.inventory_skipped_reason = why
            result.error = f"No SpotHero event matched for time window ({why})"
            return result
        result.event_match = ev_match

        inv = reduce_event_inventory_by_one(
            auth_token=auth_token,
            facility_id=facility_id,
            event_raw=ev_match["raw"],
            tz_name=tz_name,
            dry_run=dry_run,
            debug=debug,
        )
        result.inventory_before = inv.get("before")
        result.inventory_after = inv.get("after")
        result.inventory_applied = bool(inv.get("applied")) or (dry_run and inv.get("reason") not in {"inventory_already_zero"})
        if not result.inventory_applied:
            result.inventory_skipped_reason = inv.get("reason", "inventory_not_applied")
    except Exception as e:
        result.inventory_skipped_reason = "processing_error"
        result.error = str(e)[:400]
        log.exception("ParkWhiz message %s failed", message_id)

    return result


def _process_one_and_notify(
    *,
    gmail: GmailClient,
    message_id: str,
    auth_token: str,
    facilities: List[Dict[str, Any]],
    tz_name: str,
    dry_run: bool,
    debug: bool,
) -> ProcessResult:
    result = process_parkwhiz_message(
        gmail=gmail,
        message_id=message_id,
        auth_token=auth_token,
        facilities=facilities,
        tz_name=tz_name,
        dry_run=dry_run,
        debug=debug,
    )
    _post_parkwhiz_teams_card(build_parkwhiz_review_card(result=result, tz_name=tz_name))
    try:
        gmail.mark_as_read(message_id)
    except Exception as e:
        log.warning("Failed to mark ParkWhiz message %s read: %s", message_id, e)
    return result


def run_parkwhiz_poll(
    *,
    gmail: GmailClient,
    query: str,
    auth_token: str,
    facilities: List[Dict[str, Any]],
    tz_name: str = DEFAULT_TZ,
    dry_run: bool = False,
    debug: bool = False,
    state_file: str = PARKWHIZ_STATE_FILE,
    max_messages: int = 10,
) -> List[ProcessResult]:
    state = _load_state(state_file)
    processed = set(str(x) for x in (state.get("processed_ids") or []))
    results: List[ProcessResult] = []

    message_ids = gmail.list_message_ids(query, max_results=max_messages)
    pending = [mid for mid in message_ids if mid not in processed]
    log.info(
        "ParkWhiz Gmail poll: query=%r candidates=%d pending=%d already_processed=%d workers=%d",
        query, len(message_ids), len(pending), len(processed), PARKWHIZ_MAX_WORKERS,
    )

    if not pending:
        return results

    if len(pending) == 1:
        pending_results = [
            _process_one_and_notify(
                gmail=gmail, message_id=pending[0], auth_token=auth_token,
                facilities=facilities, tz_name=tz_name, dry_run=dry_run, debug=debug,
            )
        ]
    else:
        pending_results = []
        with ThreadPoolExecutor(max_workers=min(PARKWHIZ_MAX_WORKERS, len(pending))) as pool:
            futs = {
                pool.submit(
                    _process_one_and_notify,
                    gmail=gmail,
                    message_id=mid,
                    auth_token=auth_token,
                    facilities=facilities,
                    tz_name=tz_name,
                    dry_run=dry_run,
                    debug=debug,
                ): mid
                for mid in pending
            }
            for fut in as_completed(futs):
                try:
                    pending_results.append(fut.result())
                except Exception as e:
                    mid = futs[fut]
                    log.exception("ParkWhiz parallel process failed for %s: %s", mid, e)

    for result in pending_results:
        results.append(result)
        processed.add(result.message_id)
        log.info(
            "ParkWhiz processed msg=%s facility=%s inv=%s→%s applied=%s reason=%s",
            result.message_id,
            result.parsed.facility_name,
            result.inventory_before,
            result.inventory_after,
            result.inventory_applied,
            result.inventory_skipped_reason or "ok",
        )

    state["processed_ids"] = list(processed)
    _save_state(state_file, state)

    return results
