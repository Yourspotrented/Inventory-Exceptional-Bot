# files/gmail_client.py
"""Lightweight Gmail API client using OAuth2 refresh tokens."""
from __future__ import annotations

import base64
import logging
import re
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional

import httpx

log = logging.getLogger("app.gmail")

GMAIL_API = "https://gmail.googleapis.com/gmail/v1"
TOKEN_URL = "https://oauth2.googleapis.com/token"


class GmailClient:
    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        delegate_user: str = "me",
        timeout: float = 30.0,
    ) -> None:
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self.refresh_token = refresh_token.strip()
        self.delegate_user = (delegate_user or "me").strip()
        self.timeout = timeout
        self._access_token: Optional[str] = None

    def _user_path(self) -> str:
        user = self.delegate_user if self.delegate_user else "me"
        return f"users/{user}"

    def _refresh_access_token(self) -> str:
        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }
        with httpx.Client(timeout=self.timeout) as client:
            r = client.post(TOKEN_URL, data=data)
        if r.status_code != 200:
            raise RuntimeError(f"Gmail token refresh failed: {r.status_code} {r.text[:300]}")
        token = r.json().get("access_token")
        if not token:
            raise RuntimeError("Gmail token refresh returned no access_token")
        self._access_token = str(token)
        return self._access_token

    def _headers(self) -> Dict[str, str]:
        if not self._access_token:
            self._refresh_access_token()
        return {"Authorization": f"Bearer {self._access_token}"}

    def _request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None, json_body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{GMAIL_API}/{path.lstrip('/')}"
        with httpx.Client(timeout=self.timeout) as client:
            r = client.request(method, url, headers=self._headers(), params=params, json=json_body)
        if r.status_code == 401:
            self._refresh_access_token()
            with httpx.Client(timeout=self.timeout) as client:
                r = client.request(method, url, headers=self._headers(), params=params, json=json_body)
        if r.status_code != 200:
            raise RuntimeError(f"Gmail API {method} {path}: {r.status_code} {r.text[:400]}")
        return r.json() if r.text else {}

    def list_labels(self) -> List[Dict[str, Any]]:
        data = self._request("GET", f"{self._user_path()}/labels")
        return list(data.get("labels") or [])

    def _resolve_label_id(self, label_name: str) -> Optional[str]:
        want = (label_name or "").strip().lstrip("#").lower()
        if not want:
            return None
        for lb in self.list_labels():
            name = str(lb.get("name") or "").strip().lstrip("#").lower()
            if name == want:
                return str(lb.get("id"))
        return None

    @staticmethod
    def _parse_gmail_query(query: str) -> Dict[str, Any]:
        """Parse simple queries like 'label:#parkwhiz-reservation is:unread'."""
        q = (query or "").strip()
        label_name: Optional[str] = None
        unread_only = False
        m = re.search(r"label:([^\s]+)", q, flags=re.I)
        if m:
            label_name = m.group(1).strip().strip("'\"")
        if re.search(r"\bis:unread\b", q, flags=re.I):
            unread_only = True
        return {"label_name": label_name, "unread_only": unread_only, "raw": q}

    def list_message_ids(self, query: str, *, max_results: int = 20) -> List[str]:
        """
        List message IDs for a Gmail search query.

        Prefer labelIds (works with gmail.metadata scope, same as many other bots).
        Fall back to 'q' only when label-based listing is not possible.
        """
        parsed = self._parse_gmail_query(query)
        max_results = max(1, min(max_results, 50))

        if parsed.get("label_name"):
            label_id = self._resolve_label_id(str(parsed["label_name"]))
            if label_id:
                label_ids = [label_id]
                if parsed.get("unread_only"):
                    label_ids.append("UNREAD")
                params: Dict[str, Any] = {
                    "labelIds": label_ids,
                    "maxResults": max_results,
                }
                log.info(
                    "Gmail list via labelIds label=%s unread_only=%s",
                    parsed["label_name"], parsed.get("unread_only"),
                )
                data = self._request("GET", f"{self._user_path()}/messages", params=params)
                msgs = data.get("messages") or []
                return [str(m["id"]) for m in msgs if m.get("id")]
            log.warning("Gmail label not found: %s; falling back to q search", parsed["label_name"])

        params = {"q": query, "maxResults": max_results}
        log.info("Gmail list via q=%r", query)
        data = self._request("GET", f"{self._user_path()}/messages", params=params)
        msgs = data.get("messages") or []
        return [str(m["id"]) for m in msgs if m.get("id")]

    def get_message(self, message_id: str) -> Dict[str, Any]:
        try:
            return self._request(
                "GET",
                f"{self._user_path()}/messages/{message_id}",
                params={"format": "full"},
            )
        except RuntimeError as e:
            # gmail.metadata scope cannot read bodies with format=full
            if "403" not in str(e):
                raise
            log.warning("Gmail format=full denied; falling back to metadata+snippet for %s", message_id)
            return self._request(
                "GET",
                f"{self._user_path()}/messages/{message_id}",
                params={"format": "metadata", "metadataHeaders": ["Subject", "From", "Date"]},
            )

    def mark_as_read(self, message_id: str) -> None:
        self._request(
            "POST",
            f"{self._user_path()}/messages/{message_id}/modify",
            json_body={"removeLabelIds": ["UNREAD"]},
        )


def _decode_part_data(data: str) -> str:
    if not data:
        return ""
    padded = data + "=" * (-len(data) % 4)
    raw = base64.urlsafe_b64decode(padded.encode("ascii"))
    return raw.decode("utf-8", errors="replace")


def _strip_html(html: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html or "")
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


def extract_message_text(msg: Dict[str, Any]) -> str:
    """Return best-effort plain text from a Gmail message payload."""
    snippet = str(msg.get("snippet") or "").strip()
    payload = msg.get("payload") or {}
    parts: List[str] = []

    def walk(part: Dict[str, Any]) -> None:
        mime = (part.get("mimeType") or "").lower()
        body = part.get("body") or {}
        data = body.get("data")
        if data:
            decoded = _decode_part_data(data)
            if "html" in mime:
                parts.append(_strip_html(decoded))
            elif "plain" in mime or not mime:
                parts.append(decoded)
        for child in part.get("parts") or []:
            walk(child)

    walk(payload)
    if parts:
        text = "\n".join(p for p in parts if p.strip())
        if snippet and snippet not in text:
            return f"{text}\n{snippet}"
        return text
    if payload.get("body", {}).get("data"):
        return _decode_part_data(payload["body"]["data"])
    return snippet


def extract_headers(msg: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for h in (msg.get("payload") or {}).get("headers") or []:
        name = (h.get("name") or "").lower()
        if name:
            out[name] = h.get("value") or ""
    return out


def message_received_at(msg: Dict[str, Any]) -> Optional[str]:
    headers = extract_headers(msg)
    for key in ("date",):
        if headers.get(key):
            try:
                return parsedate_to_datetime(headers[key]).isoformat()
            except Exception:
                return headers[key]
    internal = msg.get("internalDate")
    if internal:
        try:
            from datetime import datetime, timezone
            ts = int(internal) / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except Exception:
            pass
    return None
