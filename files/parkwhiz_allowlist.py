# files/parkwhiz_allowlist.py
"""Load ParkWhiz facility allowlist from SharePoint Excel (FACILITY NAME column)."""
from __future__ import annotations

import io
import logging
import re
from typing import List, Optional

import pandas as pd

from files.facilities import _norm, _score, best_match_from_list

log = logging.getLogger("app.parkwhiz.allowlist")

_NAME_COLUMNS = frozenset({"FACILITY NAME", "FACILITY", "FACILITY_NAME", "NAME"})
# Strict threshold — avoids false positives like "10 Soden St" matching "62 Queensberry St"
_ALLOWLIST_MIN_SCORE = 0.72
_ADDRESS_PREFIX_RE = re.compile(r"^(\d+(?:\s+\d+)?)")


def _sanitize_facility_text(name: str) -> str:
    """Normalize unicode dashes/spaces from Excel or email without changing meaning."""
    t = (name or "").strip()
    t = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2015\u2212\uFE58\uFE63\uFF0D]", "-", t)
    t = re.sub(r"\s+", " ", t)
    return t


def _address_prefix(name: str) -> str:
    """Leading street number(s), e.g. '10', '15 17', '1025'."""
    n = _norm(name)
    m = _ADDRESS_PREFIX_RE.match(n)
    return m.group(1) if m else ""


def parse_allowlist_xlsx(data: bytes) -> List[str]:
    """Parse facility names from Parkwhiz Events Facilities.xlsx bytes."""
    df = pd.read_excel(io.BytesIO(data), engine="openpyxl")
    if df.empty:
        return []

    col = None
    for c in df.columns:
        if str(c).strip().upper() in _NAME_COLUMNS:
            col = c
            break
    if col is None:
        col = df.columns[0]

    names: List[str] = []
    for raw in df[col].dropna():
        name = str(raw).strip()
        if not name:
            continue
        if name.lower() in {"facility name", "nan"}:
            continue
        names.append(_sanitize_facility_text(name))
    return names


def match_allowlist_name(email_facility: str, allowlist: List[str]) -> Optional[str]:
    """Return matched allowlist entry for a ParkWhiz email facility name, or None."""
    email_facility = _sanitize_facility_text(email_facility)
    if not email_facility or not allowlist:
        return None

    ne = _norm(email_facility)
    for name in allowlist:
        if _norm(name) == ne:
            return name

    email_prefix = _address_prefix(email_facility)
    candidates = allowlist
    if email_prefix:
        same_street = [n for n in allowlist if _address_prefix(n) == email_prefix]
        if same_street:
            candidates = same_street

    facs = [{"id": i, "name": n} for i, n in enumerate(candidates)]
    hit = best_match_from_list(email_facility, facs, min_score=_ALLOWLIST_MIN_SCORE)
    if hit:
        matched = str(hit["name"])
        if email_prefix and _address_prefix(matched) != email_prefix:
            log.warning(
                "ParkWhiz allowlist rejected prefix mismatch email=%r matched=%r",
                email_facility[:80], matched[:80],
            )
            return None
        if float(hit.get("score") or 0) < 1.0:
            log.info(
                "ParkWhiz allowlist fuzzy match email=%r -> sheet=%r score=%s",
                email_facility[:80], matched[:80], hit.get("score"),
            )
        return matched

    return None
