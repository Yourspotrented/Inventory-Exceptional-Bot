# files/gmail_client.py
"""Gmail API client — aligned with other projects (google-auth + gmail.readonly + userId=me)."""
from __future__ import annotations

import base64
import logging
import os
import re
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional

from google.auth.transport.requests import Request as GRequest
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

log = logging.getLogger("app.gmail")

GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
GMAIL_MODIFY_SCOPE = "https://www.googleapis.com/auth/gmail.modify"

try:
    import html2text
    _HAS_HTML2TEXT = True
except ImportError:
    _HAS_HTML2TEXT = False


def _gmail_scopes() -> List[str]:
    mode = (os.getenv("GMAIL_SCOPES") or "readonly").strip().lower()
    if mode in {"modify", "readwrite", "full"}:
        return [GMAIL_MODIFY_SCOPE]
    if mode in {"both", "readonly+modify"}:
        return [GMAIL_READONLY_SCOPE, GMAIL_MODIFY_SCOPE]
    return [GMAIL_READONLY_SCOPE]


class GmailClient:
    """
    OAuth refresh-token Gmail client.

    Uses userId='me' (token owner), same as other projects.
    GMAIL_DELEGATE_USER is only used when explicitly set to an email AND
    domain-wide delegation is configured; otherwise always 'me'.
    """

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        delegate_user: str = "me",
    ) -> None:
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self.refresh_token = refresh_token.strip()
        raw_delegate = (delegate_user or "me").strip()
        # OAuth refresh tokens only work with 'me' unless using service-account delegation.
        if raw_delegate.lower() in {"me", ""}:
            self.user_id = "me"
        elif "@" in raw_delegate:
            log.warning(
                "Gmail OAuth: GMAIL_DELEGATE_USER=%s ignored; using userId='me' (token owner). "
                "Set GMAIL_DELEGATE_USER=me or leave blank.",
                raw_delegate,
            )
            self.user_id = "me"
        else:
            self.user_id = raw_delegate
        self._svc = None

    def _service(self):
        if self._svc is not None:
            return self._svc
        creds = Credentials(
            None,
            refresh_token=self.refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=self.client_id,
            client_secret=self.client_secret,
            scopes=_gmail_scopes(),
        )
        if not creds.valid:
            creds.refresh(GRequest())
        self._svc = build("gmail", "v1", credentials=creds, cache_discovery=False)
        log.info("Gmail service ready userId=%s scopes=%s", self.user_id, _gmail_scopes())
        return self._svc

    def list_message_ids(self, query: str, *, max_results: int = 20) -> List[str]:
        """List message IDs using Gmail search query (requires gmail.readonly scope)."""
        svc = self._service()
        q = (query or "").strip()
        if q and "is:" not in q:
            q = f"{q} is:unread"
        max_results = max(1, min(max_results, 100))
        log.info("Gmail list via q=%r userId=%s", q, self.user_id)

        ids: List[str] = []
        page: Optional[str] = None
        while len(ids) < max_results:
            batch = min(100, max_results - len(ids))
            resp = (
                svc.users()
                .messages()
                .list(userId=self.user_id, q=q, pageToken=page, maxResults=batch)
                .execute()
            )
            ids.extend(str(m["id"]) for m in (resp.get("messages") or []) if m.get("id"))
            page = resp.get("nextPageToken")
            if not page:
                break
        return ids[:max_results]

    def get_message(self, message_id: str) -> Dict[str, Any]:
        svc = self._service()
        return svc.users().messages().get(userId=self.user_id, id=message_id, format="full").execute()

    def mark_as_read(self, message_id: str) -> None:
        if GMAIL_MODIFY_SCOPE not in _gmail_scopes():
            log.debug("Gmail mark_as_read skipped (readonly scope only)")
            return
        svc = self._service()
        svc.users().messages().modify(
            userId=self.user_id,
            id=message_id,
            body={"removeLabelIds": ["UNREAD"]},
        ).execute()


def _decode_b64url(value: str) -> bytes:
    value = value.replace("-", "+").replace("_", "/")
    value += "=" * (-len(value) % 4)
    return base64.b64decode(value)


def _walk_parts_for_text(payload: dict) -> tuple[str, str]:
    if "parts" not in payload:
        mime = payload.get("mimeType", "")
        data = (payload.get("body") or {}).get("data")
        if not data:
            return mime, ""
        return mime, _decode_b64url(data).decode("utf-8", errors="replace")

    stack = list(payload.get("parts") or [])
    text_plain: Optional[str] = None
    text_html: Optional[str] = None

    while stack:
        part = stack.pop()
        mime = part.get("mimeType", "")
        data = (part.get("body") or {}).get("data")
        if data:
            raw = _decode_b64url(data).decode("utf-8", errors="replace")
            if mime.lower().startswith("text/plain") and text_plain is None:
                text_plain = raw
            elif mime.lower().startswith("text/html") and text_html is None:
                text_html = raw
        if "parts" in part:
            stack.extend(part["parts"])

    if text_plain is not None:
        return "text/plain", text_plain
    if text_html is not None:
        return "text/html", text_html
    return "", ""


def _strip_html(html: str) -> str:
    if _HAS_HTML2TEXT:
        try:
            parser = html2text.HTML2Text()
            parser.ignore_images = True
            parser.ignore_links = False
            parser.body_width = 0
            return parser.handle(html)
        except Exception:
            pass
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html or "")
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_message_text(msg: Dict[str, Any]) -> str:
    payload = msg.get("payload") or {}
    mime, body_raw = _walk_parts_for_text(payload)
    if body_raw:
        if mime.lower().startswith("text/html"):
            return _strip_html(body_raw)
        return body_raw
    return str(msg.get("snippet") or "").strip()


def extract_headers(msg: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for h in (msg.get("payload") or {}).get("headers") or []:
        name = (h.get("name") or "").lower()
        if name:
            out[name] = h.get("value") or ""
    return out


def message_received_at(msg: Dict[str, Any]) -> Optional[str]:
    headers = extract_headers(msg)
    if headers.get("date"):
        try:
            return parsedate_to_datetime(headers["date"]).isoformat()
        except Exception:
            return headers["date"]
    internal = msg.get("internalDate")
    if internal:
        try:
            from datetime import datetime, timezone
            return datetime.fromtimestamp(int(internal) / 1000.0, tz=timezone.utc).isoformat()
        except Exception:
            pass
    return None
