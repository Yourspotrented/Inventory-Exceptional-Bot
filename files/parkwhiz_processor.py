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
from files.parkwhiz_allowlist import match_allowlist_name
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
PARKWHIZ_MATCH_TOLERANCE_MINUTES = int(os.getenv("PARKWHIZ_MATCH_TOLERANCE_MINUTES", "30"))
PARKWHIZ_MATCH_SAME_DAY_MINUTES = int(os.getenv("PARKWHIZ_MATCH_SAME_DAY_MINUTES", "180"))
PARKWHIZ_STATE_FILE = os.getenv("PARKWHIZ_STATE_FILE", os.path.join("files", "parkwhiz_processed.json"))
PARKWHIZ_DRY_RUN = os.getenv("PARKWHIZ_DRY_RUN", "false").lower() in {"1", "true", "yes"}
PARKWHIZ_MAX_WORKERS = max(1, int(os.getenv("PARKWHIZ_MAX_WORKERS", "3")))
# powerautomate = {"card": ...} for triggerBody()?['card'] in Power Automate / Workflows
# incoming = standard Teams incoming webhook attachments format
PARKWHIZ_TEAMS_WEBHOOK_MODE = (os.getenv("PARKWHIZ_TEAMS_WEBHOOK_MODE") or "powerautomate").strip().lower()
PARKWHIZ_TEAMS_CARD_AS_STRING = os.getenv("PARKWHIZ_TEAMS_CARD_AS_STRING", "false").lower() in {"1", "true", "yes"}


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
    r"((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+\w+\s+\d{1,2}(?:,?\s+\d{4})?)\s+at\s+(\d{1,2}:\d{2}\s*[ap]\.?m\.?)"
    r"\s+to\s+"
    r"((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+\w+\s+\d{1,2}(?:,?\s+\d{4})?)\s+at\s+(\d{1,2}:\d{2}\s*[ap]\.?m\.?)",
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
    """Build candidate start times — includes +1/+2/+3hr for '+Nhr after event' emails."""
    if not parsed.reservation_start:
        return []
    base = parsed.reservation_start
    starts: List[dt.datetime] = [base]
    m = _TIME_SINGLE_RE.search(parsed.raw_time_text or "")
    max_hours = 4
    if m and m.group(3):
        max_hours = max(4, int(m.group(3)) + 2)
    for h in range(1, max_hours + 1):
        t = base + dt.timedelta(hours=h)
        if t not in starts:
            starts.append(t)
    return starts


def _parkwhiz_window(parsed: ParsedReservation) -> Tuple[Optional[dt.datetime], Optional[dt.datetime]]:
    """ParkWhiz reservation window for overlap checks."""
    cands = _candidate_start_times(parsed)
    if not cands:
        return None, None
    start = min(cands)
    if parsed.reservation_end:
        end = parsed.reservation_end
    else:
        end = max(cands) + dt.timedelta(hours=5)
    return start, end


def _overlap_minutes(a0: dt.datetime, a1: dt.datetime, b0: dt.datetime, b1: dt.datetime) -> int:
    s = max(a0, b0)
    e = min(a1, b1)
    if e > s:
        return int((e - s).total_seconds() // 60)
    return 0


def _event_match_payload(ev_raw: Dict[str, Any], win_start: dt.datetime, win_end: dt.datetime, win_label: str, score: int) -> Dict[str, Any]:
    return {
        "event_id": int(ev_raw["event_id"]),
        "rule_id": ev_raw.get("rule_id"),
        "matched_window": win_label,
        "reservation_start": win_start.isoformat(),
        "reservation_end": win_end.isoformat(),
        "event_starts_offset": ev_raw.get("event_starts_offset_display") or ev_raw.get("event_starts_offset") or "00:00",
        "event_ends_offset": ev_raw.get("event_ends_offset_display") or ev_raw.get("event_ends_offset") or "00:00",
        "raw": ev_raw,
        "match_score_minutes": score,
    }


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

    # Fallback: same-day time overlap (ParkWhiz 11:35 +offset vs SH event 1:35 PM)
    overlap_match, overlap_why = _find_matching_event_by_overlap(
        client, facility_id, parsed, tz_name, debug=debug,
    )
    if overlap_match:
        return overlap_match, overlap_why
    return None, best_why


def _find_matching_event_by_overlap(
    client: SpotHeroClient,
    facility_id: int,
    parsed: ParsedReservation,
    tz_name: str,
    *,
    debug: bool = False,
) -> Tuple[Optional[Dict[str, Any]], str]:
    pw_start, pw_end = _parkwhiz_window(parsed)
    if not pw_start or not pw_end:
        return None, "missing_reservation_window"

    search_from = pw_start.date() - dt.timedelta(days=1)
    search_to = pw_end.date() + dt.timedelta(days=1)
    candidates: List[Tuple[int, Dict[str, Any], dt.datetime, dt.datetime, str]] = []

    for ev_raw in client.iter_upcoming_events(facility_id, search_from, search_to, per_page=25, max_pages=30, debug=debug):
        for win_label, win_start, win_end in _event_time_windows(ev_raw, tz_name):
            # Same calendar day (allow overbooking prevention even if end times differ)
            if pw_start.date() != win_start.date() and pw_end.date() != win_end.date():
                continue
            ov = _overlap_minutes(pw_start, pw_end, win_start, win_end)
            if ov > 0:
                candidates.append((-ov, ev_raw, win_start, win_end, win_label))
                continue
            # Same day, closest start within 3h
            for cand in _candidate_start_times(parsed):
                if cand.date() != win_start.date():
                    continue
                delta = _minutes_apart(cand, win_start)
                if delta <= PARKWHIZ_MATCH_SAME_DAY_MINUTES:
                    candidates.append((delta, ev_raw, win_start, win_end, win_label))

    if not candidates:
        return None, "no_matching_event_in_tolerance"

    candidates.sort(key=lambda x: x[0])
    best = candidates[0]
    ev_raw = best[1]
    log.info(
        "[PARKWHIZ] overlap match event_id=%s window=%s score=%s pw=%s→%s",
        ev_raw.get("event_id"), best[4], best[0],
        pw_start.isoformat(), pw_end.isoformat(),
    )
    return _event_match_payload(ev_raw, best[2], best[3], best[4], abs(best[0])), "matched_overlap"


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
    return _event_match_payload(ev_raw, best[2], best[3], best[4], best[0]), "matched"


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
        # SpotHero already at 0 — likely reduced on a prior poll/deploy. Show intended -1 change.
        return {
            "applied": False,
            "reason": "inventory_already_zero",
            "before": current_total + 1,
            "after": current_total,
        }

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
    log.info(
        "[PARKWHIZ] POST event_id=%s facility=%s inventory %s→%s dry_run=%s",
        ev.event_id, facility_id, current_total, target_qty, dry_run,
    )

    applied = False
    verify_after = None
    if dry_run:
        applied = False
    else:
        # POST succeeded without exception — treat as applied (ParkWhiz must act fast)
        applied = True
        ev_day = ev.event_starts_local.date() if ev.event_starts_local else dt.date.today()
        chk = client.get_event_on_day(facility_id, ev_day, ev.event_id, debug=debug)
        if chk:
            verify_after = _tiers_pairs(chk.tiers)
            verify_total = sum(inv for _, _, inv in verify_after)
            if verify_total != target_qty:
                log.warning(
                    "[PARKWHIZ] verify mismatch event_id=%s expected=%s got=%s (POST was sent)",
                    ev.event_id, target_qty, verify_total,
                )
        else:
            log.warning("[PARKWHIZ] verify refetch missed event_id=%s (POST was sent)", ev.event_id)

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
def _teams_attachment_payload(card: Dict[str, Any]) -> Dict[str, Any]:
    """Standard Teams incoming-webhook envelope (same as SH updater)."""
    return {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": None,
            "content": card,
        }],
    }


def _post_parkwhiz_teams_card(
    card: Dict[str, Any],
    *,
    text_summary: str = "",
    result: Optional[ProcessResult] = None,
) -> bool:
    if not PARKWHIZ_TEAMS_WEBHOOK_URL:
        log.warning("ParkWhiz Teams not sent: PARKWHIZ_TEAMS_WEBHOOK_URL is not configured")
        return False
    try:
        mode = PARKWHIZ_TEAMS_WEBHOOK_MODE
        # Power Automate Adaptive Card field needs a JSON object — never a stringified blob.
        if mode == "powerautomate" and PARKWHIZ_TEAMS_CARD_AS_STRING:
            log.warning(
                "ParkWhiz Teams: PARKWHIZ_TEAMS_CARD_AS_STRING=true breaks Power Automate rendering; sending object instead"
            )

        if mode == "powerautomate":
            # PA "Post adaptive card" → Expression: triggerBody()?['card']
            # Alternate: triggerBody()?['attachments'][0]['content']
            payload = {
                "card": card,
                "adaptiveCard": card,
                "attachments": _teams_attachment_payload(card)["attachments"],
            }
        elif mode in {"incoming", "teams"}:
            payload = _teams_attachment_payload(card)
        elif mode == "powerautomate_root":
            # PA flow reads adaptive card from HTTP body root (no wrapper key)
            payload = card
        elif mode == "text":
            payload = {"type": "message", "text": text_summary or "ParkWhiz reservation processed."}
        else:
            payload = _teams_attachment_payload(card)

        r = requests.post(PARKWHIZ_TEAMS_WEBHOOK_URL, json=payload, timeout=15)
        if r.status_code >= 400:
            log.warning("ParkWhiz Teams post HTTP %s: %s", r.status_code, (r.text or "")[:300])
            return False
        log.info("ParkWhiz Teams posted mode=%s card_as_string=%s", mode, PARKWHIZ_TEAMS_CARD_AS_STRING)
        return True
    except Exception as e:
        log.warning("ParkWhiz Teams post failed: %s", e)
        return False


def _teams_text_summary(result: ProcessResult, tz_name: str) -> str:
    p = result.parsed
    fac = result.facility_match or {}
    ev = result.event_match or {}
    status = "UPDATED" if result.inventory_applied else "REVIEW"
    lines = [
        f"ParkWhiz → SpotHero [{status}]",
        f"Facility: {p.facility_name or '-'}",
        f"SpotHero: {fac.get('name') or '-'} (ID {fac.get('id') or '-'})",
        f"Event: {p.event_title or _sh_event_title(ev.get('raw') or {})}",
        f"Event ID: #{ev.get('event_id') or '-'}",
        f"Inventory: {result.inventory_before} → {result.inventory_after} (applied={result.inventory_applied})",
        f"Confirmation: {p.confirmation_number or '-'} | Purchaser: {p.purchaser or '-'} | Price: {p.gross_price or '-'}",
        f"Window: {_fmt_dt_short(p.reservation_start)} → {_fmt_dt_short(p.reservation_end)} ({tz_name})",
    ]
    if result.error:
        lines.append(f"Note: {result.error[:200]}")
    return "\n".join(lines)


def _fmt_dt(ts: Optional[dt.datetime]) -> str:
    return ts.strftime("%a, %b %d %Y %I:%M %p %Z") if ts else "-"


def _fmt_dt_short(ts: Optional[dt.datetime]) -> str:
    return ts.strftime("%b %d at %I:%M %p") if ts else "-"


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
        return {"label": "Inventory Updated", "color": "Good", "icon": "[OK]"}
    if result.inventory_skipped_reason == "time_parse_failed":
        return {"label": "Review - Time Not Parsed", "color": "Warning", "icon": "[!]"}
    if result.inventory_skipped_reason in {"facility_not_matched", "no_matching_event_in_tolerance"}:
        return {"label": "Review - No SpotHero Match", "color": "Warning", "icon": "[?]"}
    return {"label": "Review Required", "color": "Attention", "icon": "[!]"}


def build_parkwhiz_review_card(
    *,
    result: ProcessResult,
    tz_name: str = DEFAULT_TZ,
) -> Dict[str, Any]:
    p = result.parsed
    fac = result.facility_match or {}
    ev = result.event_match or {}
    ev_raw = ev.get("raw") or {}
    sh_event = _sh_event_title(ev_raw, p.event_title)
    facility_id = str(fac.get("id") or "-")
    event_id = str(ev.get("event_id") or "-")

    if result.inventory_applied:
        headline = "Inventory Updated"
        sub = f"SpotHero inventory {result.inventory_before} → {result.inventory_after}"
        color = "Good"
    elif result.inventory_skipped_reason == "inventory_already_zero":
        headline = "Already Reduced"
        sub = f"SpotHero inventory already at {result.inventory_after} (expected {result.inventory_before} → {result.inventory_after})"
        color = "Good"
    else:
        headline = "Review Required"
        sub = result.error or result.inventory_skipped_reason or "Could not auto-apply"
        color = "Warning"

    facts = [
        {"title": "Status", "value": headline},
        {"title": "ParkWhiz Facility", "value": (p.facility_name or "-")[:100]},
        {"title": "Confirmation", "value": p.confirmation_number or "-"},
        {"title": "Purchaser", "value": p.purchaser or "-"},
        {"title": "Price", "value": p.gross_price or "-"},
        {"title": "Email Event", "value": (p.event_title or "-")[:80]},
        {"title": "ParkWhiz Time", "value": p.raw_time_text or _fmt_dt_short(p.reservation_start)},
        {"title": "SpotHero Facility", "value": (fac.get("name") or "-")[:80]},
        {"title": "SpotHero ID", "value": facility_id},
        {"title": "Control Panel Event", "value": sh_event[:80]},
        {"title": "Event ID", "value": f"#{event_id}"},
        {"title": "SH Window", "value": f"{ev.get('reservation_start', '-')} → {ev.get('reservation_end', '-')}"[:100]},
        {"title": "Inventory", "value": f"{result.inventory_before} → {result.inventory_after}"},
    ]

    operator_url = "https://spothero.com/operator"
    if facility_id.isdigit():
        operator_url = f"https://spothero.com/operator/facilities/{facility_id}/"

    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "msteams": {"width": "Full"},
        "body": [
            {"type": "TextBlock", "text": "ParkWhiz → SpotHero", "weight": "Bolder", "size": "Large"},
            {"type": "TextBlock", "text": sub, "wrap": True, "color": color, "spacing": "Small"},
            {"type": "FactSet", "facts": facts, "spacing": "Medium"},
            {"type": "TextBlock", "text": f"{result.processed_at or ''} · {tz_name}", "isSubtle": True, "size": "Small"},
        ],
        "actions": [
            {"type": "Action.OpenUrl", "title": "Open in SpotHero", "url": operator_url},
        ],
    }


# Reasons that should not be retried automatically
_PERMANENT_SKIP_REASONS = frozenset({
    "facility_not_found_in_email",
    "time_parse_failed",
    "facility_not_in_allowlist",
})

# Inventory already at target — no further action needed
_DONE_SKIP_REASONS = frozenset({
    "inventory_already_zero",
})


def _notified_confirmations(state: Dict[str, Any]) -> set:
    """Confirmation numbers that already received a Teams card."""
    out = {str(x) for x in (state.get("notified_confirmations") or []) if x}
    for rec in (state.get("by_id") or {}).values():
        conf = str(rec.get("confirmation") or "").strip()
        if conf and rec.get("teams_sent"):
            out.add(conf)
    return out


def _skip_message_reason(state: Dict[str, Any], message_id: str) -> Optional[str]:
    """Return why a message is skipped, or None if it should be processed."""
    rec = (state.get("by_id") or {}).get(str(message_id))
    if not rec:
        return None
    if rec.get("applied"):
        return "inventory_already_applied"
    if rec.get("teams_sent"):
        return "teams_already_sent"
    reason = str(rec.get("reason") or "")
    if reason in _PERMANENT_SKIP_REASONS:
        return reason
    if reason in _DONE_SKIP_REASONS:
        return reason
    conf = str(rec.get("confirmation") or "").strip()
    if conf and conf in _notified_confirmations(state):
        return "confirmation_already_notified"
    return None


def _should_skip_message(state: Dict[str, Any], message_id: str) -> bool:
    """Skip only when inventory was applied or failure is permanent."""
    return _skip_message_reason(state, message_id) is not None


def _should_send_teams(state: Dict[str, Any], message_id: str, result: ProcessResult) -> bool:
    """One Teams card per Gmail message and per ParkWhiz confirmation number."""
    if result.inventory_skipped_reason in {"facility_not_in_allowlist"}:
        return False
    rec = (state.get("by_id") or {}).get(str(message_id)) or {}
    if rec.get("teams_sent"):
        return False
    conf = (result.parsed.confirmation_number or "").strip()
    if conf and conf in _notified_confirmations(state):
        log.info(
            "ParkWhiz Teams skipped for msg=%s confirmation=%s (duplicate reservation email)",
            message_id, conf,
        )
        return False
    return True


def _record_message_state(state: Dict[str, Any], result: ProcessResult, *, teams_sent: bool = False) -> None:
    by_id = state.setdefault("by_id", {})
    mid = str(result.message_id)
    prev = by_id.get(mid) or {}
    attempts = int(prev.get("attempts") or 0) + 1
    by_id[mid] = {
        **prev,
        "applied": bool(result.inventory_applied),
        "reason": result.inventory_skipped_reason or ("ok" if result.inventory_applied else prev.get("reason", "")),
        "at": result.processed_at,
        "inv_before": result.inventory_before,
        "inv_after": result.inventory_after,
        "attempts": attempts,
        "teams_sent": bool(prev.get("teams_sent")) or bool(teams_sent),
        "confirmation": result.parsed.confirmation_number or prev.get("confirmation"),
    }
    conf = str(by_id[mid].get("confirmation") or "").strip()
    if teams_sent and conf:
        notified = [str(x) for x in (state.get("notified_confirmations") or []) if x]
        if conf not in notified:
            notified.append(conf)
        state["notified_confirmations"] = notified[-500:]
    done_ids = [
        mid for mid, r in by_id.items()
        if r.get("applied")
        or r.get("teams_sent")
        or r.get("reason") in _PERMANENT_SKIP_REASONS
        or r.get("reason") in _DONE_SKIP_REASONS
    ]
    state["processed_ids"] = done_ids[-500:]


def _load_state(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"processed_ids": [], "by_id": {}, "notified_confirmations": []}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            if not isinstance(data.get("by_id"), dict):
                data["by_id"] = {}
            if not isinstance(data.get("processed_ids"), list):
                data["processed_ids"] = []
            if not isinstance(data.get("notified_confirmations"), list):
                data["notified_confirmations"] = []
            return data
    except Exception:
        pass
    return {"processed_ids": [], "by_id": {}, "notified_confirmations": []}


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
    allowlist: Optional[List[str]] = None,
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

    if allowlist is not None:
        allowed = match_allowlist_name(parsed.facility_name, allowlist)
        if not allowed:
            result.inventory_skipped_reason = "facility_not_in_allowlist"
            result.error = f"Facility not in ParkWhiz allowlist: {parsed.facility_name}"
            log.info(
                "[PARKWHIZ] skip allowlist msg=%s facility=%r (allowlist_size=%d)",
                message_id, parsed.facility_name, len(allowlist),
            )
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
            log.warning("[PARKWHIZ] no event match facility=%s time=%s reason=%s", facility_id, parsed.raw_time_text, why)
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
        result.inventory_applied = bool(inv.get("applied"))
        if not result.inventory_applied and not dry_run:
            result.inventory_skipped_reason = inv.get("reason", "inventory_not_applied")
        # Prefer stored values from a prior successful apply (e.g. after redeploy)
        if result.inventory_skipped_reason == "inventory_already_zero":
            prev = (_load_state(PARKWHIZ_STATE_FILE).get("by_id") or {}).get(str(message_id)) or {}
            prev_before, prev_after = prev.get("inv_before"), prev.get("inv_after")
            if prev_before is not None and prev_after is not None and prev_before > prev_after:
                result.inventory_before = prev_before
                result.inventory_after = prev_after
        log.info(
            "[PARKWHIZ] result msg=%s facility=%s event=%s inv=%s→%s applied=%s reason=%s",
            message_id,
            facility_id,
            ev_match.get("event_id"),
            result.inventory_before,
            result.inventory_after,
            result.inventory_applied,
            result.inventory_skipped_reason or "ok",
        )
    except Exception as e:
        result.inventory_skipped_reason = "processing_error"
        result.error = str(e)[:400]
        log.exception("ParkWhiz message %s failed", message_id)

    return result


def _process_one(
    *,
    gmail: GmailClient,
    message_id: str,
    auth_token: str,
    facilities: List[Dict[str, Any]],
    allowlist: Optional[List[str]],
    tz_name: str,
    dry_run: bool,
    debug: bool,
) -> ProcessResult:
    return process_parkwhiz_message(
        gmail=gmail,
        message_id=message_id,
        auth_token=auth_token,
        facilities=facilities,
        allowlist=allowlist,
        tz_name=tz_name,
        dry_run=dry_run,
        debug=debug,
    )


def _finalize_result(
    *,
    gmail: GmailClient,
    result: ProcessResult,
    tz_name: str,
    state: Dict[str, Any],
) -> None:
    teams_sent = False
    if _should_send_teams(state, result.message_id, result):
        summary = _teams_text_summary(result, tz_name)
        teams_sent = _post_parkwhiz_teams_card(
            build_parkwhiz_review_card(result=result, tz_name=tz_name),
            text_summary=summary,
            result=result,
        )
        if not teams_sent:
            log.warning("ParkWhiz Teams card was not delivered for msg=%s (will retry next poll)", result.message_id)
    else:
        conf = (result.parsed.confirmation_number or "").strip()
        rec = (state.get("by_id") or {}).get(str(result.message_id)) or {}
        if not rec.get("teams_sent") and conf and conf in _notified_confirmations(state):
            teams_sent = True
            log.info(
                "ParkWhiz Teams skipped for msg=%s — duplicate email for confirmation %s",
                result.message_id, conf,
            )
        else:
            log.info("ParkWhiz Teams skipped for msg=%s (already notified)", result.message_id)

    _record_message_state(state, result, teams_sent=teams_sent)

    if result.inventory_applied or result.inventory_skipped_reason in _PERMANENT_SKIP_REASONS:
        try:
            gmail.mark_as_read(result.message_id)
        except Exception as e:
            log.warning("Failed to mark ParkWhiz message %s read: %s", result.message_id, e)


def run_parkwhiz_poll(
    *,
    gmail: GmailClient,
    query: str,
    auth_token: str,
    facilities: List[Dict[str, Any]],
    allowlist: Optional[List[str]] = None,
    tz_name: str = DEFAULT_TZ,
    dry_run: bool = False,
    debug: bool = False,
    state_file: str = PARKWHIZ_STATE_FILE,
    max_messages: int = 10,
) -> List[ProcessResult]:
    state = _load_state(state_file)
    processed_done = set(str(x) for x in (state.get("processed_ids") or []))
    results: List[ProcessResult] = []

    message_ids = gmail.list_message_ids(query, max_results=max_messages)
    pending = [mid for mid in message_ids if not _should_skip_message(state, mid)]
    log.info(
        "ParkWhiz Gmail poll: query=%r candidates=%d pending=%d done=%d workers=%d allowlist=%s",
        query, len(message_ids), len(pending), len(processed_done), PARKWHIZ_MAX_WORKERS,
        len(allowlist) if allowlist is not None else "off",
    )
    for mid in message_ids:
        if mid in pending:
            continue
        skip_reason = _skip_message_reason(state, mid) or "unknown"
        rec = (state.get("by_id") or {}).get(str(mid)) or {}
        log.info(
            "ParkWhiz skip msg=%s reason=%s applied=%s teams_sent=%s prior_reason=%s",
            mid, skip_reason, rec.get("applied"), rec.get("teams_sent"), rec.get("reason"),
        )

    if not pending:
        return results

    if len(pending) == 1:
        pending_results = [_process_one(
            gmail=gmail, message_id=pending[0], auth_token=auth_token,
            facilities=facilities, allowlist=allowlist, tz_name=tz_name, dry_run=dry_run, debug=debug,
        )]
    else:
        pending_results = []
        with ThreadPoolExecutor(max_workers=min(PARKWHIZ_MAX_WORKERS, len(pending))) as pool:
            futs = {
                pool.submit(
                    _process_one,
                    gmail=gmail,
                    message_id=mid,
                    auth_token=auth_token,
                    facilities=facilities,
                    allowlist=allowlist,
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
        _finalize_result(gmail=gmail, result=result, tz_name=tz_name, state=state)
        results.append(result)
        log.info(
            "ParkWhiz processed msg=%s facility=%s inv=%s→%s applied=%s reason=%s",
            result.message_id,
            result.parsed.facility_name,
            result.inventory_before,
            result.inventory_after,
            result.inventory_applied,
            result.inventory_skipped_reason or "ok",
        )

    _save_state(state_file, state)

    return results
