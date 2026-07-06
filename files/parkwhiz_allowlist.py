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
_ALLOWLIST_MIN_SCORE = 0.40


def _sanitize_facility_text(name: str) -> str:
    """Normalize unicode dashes/spaces from Excel or email without changing meaning."""
    t = (name or "").strip()
    t = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2015\u2212\uFE58\uFE63\uFF0D]", "-", t)
    t = re.sub(r"\s+", " ", t)
    return t


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

    facs = [{"id": i, "name": n} for i, n in enumerate(allowlist)]
    hit = best_match_from_list(email_facility, facs)
    if hit:
        return str(hit["name"])

    best_sc, best_name = 0.0, None
    for name in allowlist:
        sc = _score(email_facility, name)
        if sc > best_sc:
            best_sc, best_name = sc, name
    if best_sc >= _ALLOWLIST_MIN_SCORE and best_name:
        log.info(
            "ParkWhiz allowlist fuzzy match email=%r -> sheet=%r score=%.2f",
            email_facility[:80], best_name[:80], best_sc,
        )
        return best_name
    return None
