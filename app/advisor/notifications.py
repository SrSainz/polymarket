from __future__ import annotations

from dataclasses import dataclass
import json
import time

from app.advisor.models import TradeProposal
from app.advisor.proposals import proposal_from_payload
from app.advisor.store import AdvisorStore
from app.advisor.whatsapp import WhatsAppGateway


@dataclass(frozen=True)
class NotificationResult:
    proposal_id: str
    status: str
    reason: str = ""
    provider_message_id: str = ""


class AdvisorNotificationService:
    """Durable outbox dispatcher; uncertain sends are never retried blindly."""

    def __init__(self, store: AdvisorStore, gateway: WhatsAppGateway, *, recipient: str) -> None:
        self.store = store
        self.gateway = gateway
        self.recipient = recipient

    def dispatch_once(self, *, limit: int = 20, now: float | None = None) -> list[NotificationResult]:
        results: list[NotificationResult] = []
        if not self.store.confirmation_key or not self.gateway.ready:
            for row in self.store.pending_outbox(limit=limit, now=now):
                results.append(
                    NotificationResult(
                        str(row["proposal_id"]),
                        "disabled",
                        "whatsapp_gate_disabled_or_incomplete",
                    )
                )
            return results
        for row in self.store.pending_outbox(limit=limit, now=now):
            proposal_id = str(row["proposal_id"])
            if not self.store.claim_outbox(proposal_id, now=now):
                continue
            if not self.store.proposal_integrity_valid(proposal_id):
                self.store.mark_send_integrity_failed(proposal_id)
                results.append(NotificationResult(proposal_id, "failed", "proposal_integrity_invalid"))
                continue
            try:
                proposal = proposal_from_payload(
                    str(row["payload_json"]),
                    code_key=self.store.confirmation_key,
                )
                result = self.gateway.send_proposal(proposal, self.recipient, now=now)
            except (TypeError, ValueError, json.JSONDecodeError) as error:
                self.store.mark_send_failed(proposal_id, error.__class__.__name__)
                results.append(NotificationResult(proposal_id, "failed", error.__class__.__name__))
                continue
            except Exception as error:  # noqa: BLE001
                # Once claimed, an unexpected provider/client failure is
                # ambiguous: the request may have reached Meta.
                self.store.mark_send_reconciliation_required(proposal_id, error.__class__.__name__)
                results.append(NotificationResult(proposal_id, "reconciliation_required", error.__class__.__name__))
                continue

            if result.sent:
                if not self.store.mark_sent(proposal_id, result.provider_message_id):
                    self.store.mark_send_reconciliation_required(proposal_id, "outbox_transition_failed")
                    results.append(NotificationResult(proposal_id, "reconciliation_required", "outbox_transition_failed"))
                else:
                    results.append(NotificationResult(proposal_id, "sent", provider_message_id=result.provider_message_id))
            else:
                if result.status == "reconciliation_required":
                    self.store.mark_send_reconciliation_required(proposal_id, result.reason or result.status)
                else:
                    self.store.mark_send_failed(proposal_id, result.reason or result.status)
                results.append(NotificationResult(proposal_id, result.status, result.reason))
        return results

    def recover_uncertain_sends(self, *, older_than_seconds: float = 120.0, now: float | None = None) -> int:
        current_time = time.time() if now is None else float(now)
        return self.store.recover_uncertain_sends(
            cutoff=current_time - max(float(older_than_seconds), 1.0),
        )
