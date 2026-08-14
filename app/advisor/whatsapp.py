from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import hmac
import os
import re
import time
from typing import Any

import requests

from app.advisor.models import TradeProposal


@dataclass(frozen=True, repr=False)
class WhatsAppConfig:
    send_enabled: bool = False
    policy_confirmed: bool = False
    access_token: str = field(default="", repr=False)
    app_secret: str = field(default="", repr=False)
    webhook_verify_token: str = field(default="", repr=False)
    phone_number_id: str = ""
    graph_api_version: str = "v23.0"
    template_name: str = ""
    template_language: str = "es"
    allowed_numbers: tuple[str, ...] = ()

    @classmethod
    def from_env(cls) -> "WhatsAppConfig":
        return cls(
            send_enabled=_to_bool(os.getenv("WHATSAPP_SEND_ENABLED", "false")),
            policy_confirmed=_to_bool(os.getenv("WHATSAPP_POLICY_CONFIRMED", "false")),
            access_token=os.getenv("WHATSAPP_ACCESS_TOKEN", ""),
            app_secret=os.getenv("WHATSAPP_APP_SECRET", ""),
            webhook_verify_token=os.getenv("WHATSAPP_WEBHOOK_VERIFY_TOKEN", ""),
            phone_number_id=os.getenv("WHATSAPP_PHONE_NUMBER_ID", ""),
            graph_api_version=os.getenv("WHATSAPP_GRAPH_API_VERSION", "v23.0"),
            template_name=os.getenv("WHATSAPP_TEMPLATE_NAME", ""),
            template_language=os.getenv("WHATSAPP_TEMPLATE_LANGUAGE", "es"),
            allowed_numbers=_normalize_numbers(os.getenv("WHATSAPP_ALLOWED_NUMBERS", "")),
        )

    def __repr__(self) -> str:
        return (
            "WhatsAppConfig(send_enabled={!r}, policy_confirmed={!r}, phone_number_id={!r}, "
            "graph_api_version={!r}, template_name={!r}, template_language={!r}, allowed_numbers={!r})"
        ).format(
            self.send_enabled,
            self.policy_confirmed,
            self.phone_number_id,
            self.graph_api_version,
            self.template_name,
            self.template_language,
            self.allowed_numbers,
        )

    @property
    def ready(self) -> bool:
        return all(
            (
                self.send_enabled,
                self.policy_confirmed,
                bool(self.access_token),
                bool(self.phone_number_id),
                bool(self.template_name),
                bool(self.allowed_numbers),
                bool(self.app_secret),
                bool(self.webhook_verify_token),
            )
        )


@dataclass(frozen=True)
class SendResult:
    sent: bool
    status: str
    provider_message_id: str = ""
    reason: str = ""


class WhatsAppGateway:
    """Cloud API adapter; it is intentionally fail-closed and disabled by default."""

    def __init__(self, config: WhatsAppConfig, *, session: requests.Session | None = None) -> None:
        self.config = config
        self.session = session or requests.Session()

    @property
    def ready(self) -> bool:
        return self.config.ready

    def render_proposal(self, proposal: TradeProposal) -> str:
        low, high = proposal.probability_interval
        return (
            "Oportunidad Polymarket (requiere confirmacion)\n"
            f"{proposal.title} | {proposal.outcome}\n"
            f"Probabilidad conservadora: {proposal.probability_used:.1%} "
            f"(rango {low:.1%}-{high:.1%})\n"
            f"Precio maximo: ${proposal.max_price:.3f} | importe maximo: ${proposal.max_notional_usdc:.2f}\n"
            f"Edge neto estimado: {proposal.net_edge_bps:.0f} bps | EV: ${proposal.expected_profit_usdc:.2f}\n"
            f"Modelo: {proposal.model_name} {proposal.model_version}\n"
            f"Responde SI {proposal.confirmation_code} o NO {proposal.confirmation_code}."
        )

    def send_proposal(self, proposal: TradeProposal, recipient: str, *, now: float | None = None) -> SendResult:
        if not self.ready:
            return SendResult(False, "disabled", reason="whatsapp_gate_disabled_or_incomplete")
        if self.config.allowed_numbers and _normalize_number(recipient) not in self.config.allowed_numbers:
            return SendResult(False, "rejected", reason="recipient_not_allowlisted")
        current_time = time.time() if now is None else float(now)
        if current_time >= min(proposal.expires_at, proposal.quote_expires_at):
            return SendResult(False, "expired", reason="proposal_expired")

        body = {
            "messaging_product": "whatsapp",
            "to": _normalize_number(recipient),
            "type": "template",
            "template": {
                "name": self.config.template_name,
                "language": {"code": self.config.template_language},
                "components": [
                    {
                        "type": "body",
                        "parameters": [{"type": "text", "text": self.render_proposal(proposal)}],
                    }
                ],
            },
        }
        try:
            response = self.session.post(
                f"https://graph.facebook.com/{self.config.graph_api_version}/{self.config.phone_number_id}/messages",
                headers={"Authorization": f"Bearer {self.config.access_token}"},
                json=body,
                timeout=15,
            )
            response.raise_for_status()
            try:
                payload: Any = response.json()
            except (TypeError, ValueError):
                return SendResult(False, "reconciliation_required", reason="provider_payload_invalid")
            messages = payload.get("messages") if isinstance(payload, dict) else None
            first_message = messages[0] if isinstance(messages, list) and messages else None
            message_id = (
                str(first_message.get("id") or "").strip()
                if isinstance(first_message, dict)
                else ""
            )
            if not message_id:
                return SendResult(False, "reconciliation_required", reason="provider_message_id_missing")
            return SendResult(True, "sent", provider_message_id=message_id)
        except requests.RequestException as error:
            return SendResult(False, "reconciliation_required", reason=error.__class__.__name__)


def verify_webhook_signature(raw_body: bytes, signature_header: str, app_secret: str) -> bool:
    """Verify Meta's ``X-Hub-Signature-256`` before parsing any user data."""

    if not raw_body or not signature_header or not app_secret:
        return False
    prefix, separator, supplied = signature_header.partition("=")
    if prefix.lower() != "sha256" or not separator or not supplied:
        return False
    expected = hmac.new(app_secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, supplied.lower())


def _to_bool(raw: str) -> bool:
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_number(raw: str) -> str:
    return re.sub(r"\D", "", str(raw or ""))


def _normalize_numbers(raw: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(number for number in (_normalize_number(item) for item in raw.split(",")) if number))
