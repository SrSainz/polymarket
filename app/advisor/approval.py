from __future__ import annotations

import hashlib
import json
import re

from app.advisor.store import AdvisorStore, ApprovalResult
from app.advisor.whatsapp import verify_webhook_signature


class ApprovalService:
    """Process only authenticated, contextual, single-use WhatsApp replies."""

    _REPLY_RE = re.compile(r"^(SI|NO)\s+([A-Z0-9]{6,32})$", re.IGNORECASE)

    def __init__(self, store: AdvisorStore, *, allowed_numbers: tuple[str, ...], app_secret: str) -> None:
        self.store = store
        self.allowed_numbers = {_digits(number) for number in allowed_numbers if _digits(number)}
        self.app_secret = str(app_secret or "")

    def handle_reply(
        self,
        *,
        text: str,
        sender: str,
        provider_message_id: str,
        reply_message_id: str,
        raw_body: bytes,
        signature_header: str,
        now: float | None = None,
    ) -> ApprovalResult:
        if not verify_webhook_signature(raw_body, signature_header, self.app_secret):
            return ApprovalResult("rejected", reason="webhook_signature_invalid")
        parsed = _parse_signed_reply(raw_body)
        if parsed is None:
            return ApprovalResult("rejected", reason="webhook_payload_invalid")
        parsed_text, parsed_sender, parsed_message_id, parsed_reply_message_id = parsed
        if (
            str(text or "").strip() != parsed_text
            or _digits(sender) != parsed_sender
            or str(provider_message_id or "") != parsed_message_id
            or str(reply_message_id or "") != parsed_reply_message_id
        ):
            return ApprovalResult("rejected", reason="caller_fields_do_not_match_signed_payload")
        text = parsed_text
        sender = parsed_sender
        provider_message_id = parsed_message_id
        reply_message_id = parsed_reply_message_id
        normalized_sender = _digits(sender)
        if not normalized_sender or normalized_sender not in self.allowed_numbers:
            return ApprovalResult("rejected", reason="sender_not_allowlisted")
        if not provider_message_id:
            return ApprovalResult("rejected", reason="message_id_missing")
        match = self._REPLY_RE.fullmatch(str(text or "").strip())
        if match is None:
            return ApprovalResult("rejected", reason="reply_format_invalid")
        decision = "yes" if match.group(1).upper() == "SI" else "no"
        code = match.group(2).upper()
        return self._record_reply(
            text=text,
            sender=normalized_sender,
            provider_message_id=provider_message_id,
            reply_message_id=reply_message_id,
            raw_body=raw_body,
            now=now,
        )

    def _record_reply(
        self,
        *,
        text: str,
        sender: str,
        provider_message_id: str,
        reply_message_id: str,
        raw_body: bytes,
        now: float | None,
    ) -> ApprovalResult:
        match = self._REPLY_RE.fullmatch(str(text or "").strip())
        if match is None:
            return ApprovalResult("rejected", reason="reply_format_invalid")
        decision = "yes" if match.group(1).upper() == "SI" else "no"
        code = match.group(2).upper()
        payload_hash = hashlib.sha256(raw_body).hexdigest()
        return self.store.record_and_decide(
            code=code,
            decision=decision,
            sender=_digits(sender),
            provider_message_id=provider_message_id,
            reply_message_id=reply_message_id,
            payload_hash=payload_hash,
            now=now,
        )

    def handle_webhook(
        self,
        *,
        raw_body: bytes,
        signature_header: str,
        now: float | None = None,
    ) -> ApprovalResult:
        """Parse a signed Meta payload; callers cannot inject sender or context."""

        if not verify_webhook_signature(raw_body, signature_header, self.app_secret):
            return ApprovalResult("rejected", reason="webhook_signature_invalid")
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return ApprovalResult("rejected", reason="webhook_payload_invalid")

        parsed_replies = _parse_payload_replies(payload)
        if not parsed_replies:
            return ApprovalResult("rejected", reason="webhook_message_missing")
        results = []
        for text, sender, provider_message_id, reply_message_id in parsed_replies:
            normalized_sender = _digits(sender)
            if not normalized_sender or normalized_sender not in self.allowed_numbers:
                results.append(ApprovalResult("rejected", reason="sender_not_allowlisted"))
                continue
            results.append(
                self._record_reply(
                    text=text,
                    sender=normalized_sender,
                    provider_message_id=provider_message_id,
                    reply_message_id=reply_message_id,
                    raw_body=raw_body,
                    now=now,
                )
            )
        for result in results:
            if result.status not in {"rejected", "duplicate"}:
                return result
        return results[-1]


def _digits(raw: str) -> str:
    return "".join(character for character in str(raw or "") if character.isdigit())


def _first_message(payload: object) -> dict[str, object] | None:
    if not isinstance(payload, dict):
        return None
    for entry in payload.get("entry") or []:
        if not isinstance(entry, dict):
            continue
        for change in entry.get("changes") or []:
            if not isinstance(change, dict):
                continue
            value = change.get("value")
            if not isinstance(value, dict):
                continue
            for message in value.get("messages") or []:
                if isinstance(message, dict) and message.get("type") == "text":
                    return message
    return None


def _parse_signed_reply(raw_body: bytes) -> tuple[str, str, str, str] | None:
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return _parse_payload_reply(payload)


def _parse_payload_reply(payload: object) -> tuple[str, str, str, str] | None:
    replies = _parse_payload_replies(payload)
    return replies[0] if replies else None


def _parse_payload_replies(payload: object) -> list[tuple[str, str, str, str]]:
    replies: list[tuple[str, str, str, str]] = []
    for message in _all_text_messages(payload):
        parsed = _parse_message_reply(message)
        if parsed is not None:
            replies.append(parsed)
    return replies


def _all_text_messages(payload: object) -> list[dict[str, object]]:
    if not isinstance(payload, dict):
        return []
    messages: list[dict[str, object]] = []
    for entry in payload.get("entry") or []:
        if not isinstance(entry, dict):
            continue
        for change in entry.get("changes") or []:
            if not isinstance(change, dict):
                continue
            value = change.get("value")
            if not isinstance(value, dict):
                continue
            for message in value.get("messages") or []:
                if isinstance(message, dict) and message.get("type") == "text":
                    messages.append(message)
    return messages


def _parse_message_reply(message: dict[str, object]) -> tuple[str, str, str, str] | None:
    text_payload = message.get("text")
    context_payload = message.get("context")
    if not isinstance(text_payload, dict) or not isinstance(context_payload, dict):
        return None
    text = str(text_payload.get("body") or "").strip()
    sender = _digits(str(message.get("from") or ""))
    provider_message_id = str(message.get("id") or "")
    reply_message_id = str(context_payload.get("id") or "")
    if not text or not sender or not provider_message_id or not reply_message_id:
        return None
    return text, sender, provider_message_id, reply_message_id
