# files/parkwhiz_allowlist.py
"""Load ParkWhiz facility allowlist from SharePoint Excel (FACILITY NAME column)."""
from __future__ import annotations

import io
import logging
from typing import List, Optional

import pandas as pd

from files.facilities import best_match_from_list

log = logging.getLogger("app.parkwhiz.allowlist")

_NAME_COLUMNS = frozenset({"FACILITY NAME", "FACILITY", "FACILITY_NAME", "NAME"})


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
        names.append(name)
    return names


def match_allowlist_name(email_facility: str, allowlist: List[str]) -> Optional[str]:
    """Return matched allowlist entry for a ParkWhiz email facility name, or None."""
    if not email_facility or not allowlist:
        return None
    facs = [{"id": i, "name": n} for i, n in enumerate(allowlist)]
    hit = best_match_from_list(email_facility, facs)
    return str(hit["name"]) if hit else None
