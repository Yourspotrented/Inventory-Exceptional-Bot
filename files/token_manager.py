from __future__ import annotations
import json, os, time, contextlib
from typing import Dict, Optional, Tuple, Union
from .initialize import download_token  # uses SharePoint + 12h staleness logic

TOKEN_PATH = os.path.join("files", "token.json")

class TokenManager:
    """
    Sync wrapper for batch scripts:
      - Ensure token.json exists & is fresh (>=12h by default; see initialize.download_token()).
      - Return SpotHero auth headers.
      - Also report whether a refresh actually occurred (mtime changed).
    """

    def __init__(self, token_path: Optional[str] = None) -> None:
        self.token_path = token_path or TOKEN_PATH

    def ensure_fresh(
        self,
        *,
        return_refreshed: bool = False,
        force: bool = False,
    ) -> Union[Dict[str, str], Tuple[Dict[str, str], bool]]:
        """
        Ensure token.json is present and fresh.
        - If force=True, we force a refresh by temporarily removing the local file
          so initialize.download_token() re-downloads it.
        - If return_refreshed=True, returns (headers, refreshed_bool). Otherwise just headers.
        """
        before = self._mtime_or_none()

        if force:
            with contextlib.suppress(Exception):
                os.remove(self.token_path)

        # initialize.download_token() already handles 12h staleness checks
        download_token()

        after = self._mtime_or_none()
        refreshed = (before is None) or (after is None) or (after != before)

        headers = self._load_headers()
        return (headers, refreshed) if return_refreshed else headers

    # ---------- helpers ----------
    def _mtime_or_none(self) -> Optional[float]:
        try:
            return os.path.getmtime(self.token_path)
        except Exception:
            return None

    def _load_headers(self) -> Dict[str, str]:
        with open(self.token_path, "r", encoding="utf-8") as f:
            tokens = json.load(f)
        flex = f"{tokens['flex_token_data']['token_type']} {tokens['flex_token_data']['access_token']}"
        reservation = f"{tokens['reservation_token_data']['token_type']} {tokens['reservation_token_data']['access_token']}"
        return {"flex_auth": flex, "reservation_auth": reservation}
