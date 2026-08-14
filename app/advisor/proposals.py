from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid

from app.advisor.evaluator import AdvisorPolicy
from app.advisor.models import EvaluationResult, Opportunity, TradeProposal


def build_proposal(
    opportunity: Opportunity,
    evaluation: EvaluationResult,
    *,
    policy: AdvisorPolicy,
    confirmation_key: str = "",
    now: float | None = None,
) -> TradeProposal:
    if not evaluation.eligible:
        raise ValueError("Cannot create a proposal from an ineligible opportunity")

    created_at = time.time() if now is None else float(now)
    quote = opportunity.quote
    evidence = opportunity.evidence
    if not confirmation_key:
        raise ValueError("confirmation_key is required for durable proposals")
    proposal_id = uuid.uuid4().hex
    proposal = TradeProposal(
        proposal_id=proposal_id,
        confirmation_code=confirmation_code_for(proposal_id, confirmation_key),
        created_at=created_at,
        expires_at=created_at + policy.proposal_ttl_seconds,
        quote_expires_at=quote.observed_at + policy.maximum_quote_age_seconds,
        market_id=quote.market_id,
        condition_id=quote.condition_id,
        token_id=quote.token_id,
        title=quote.title,
        outcome=quote.outcome,
        side="buy",
        max_price=evaluation.price_limit,
        max_notional_usdc=evaluation.recommended_stake_usdc,
        probability_used=evaluation.probability_used,
        probability_interval=(evidence.lower_probability, evidence.upper_probability),
        net_edge_bps=evaluation.net_edge_bps,
        expected_profit_usdc=evaluation.expected_profit_usdc,
        fee_bps=quote.fee_bps,
        slippage_bps=quote.slippage_bps,
        model_name=evidence.model_name,
        model_version=evidence.model_version,
        model_sample_size=evidence.sample_size,
        model_brier_score=float(evidence.brier_score or 0.0),
        model_independent=evidence.independent,
        resolution_source=quote.resolution_source,
        evidence_refs=evidence.source_refs,
        source_url=quote.source_url,
        model_as_of=evidence.as_of,
    )
    return proposal


def proposal_fingerprint(proposal: TradeProposal) -> str:
    return proposal_fingerprint_payload(proposal_payload(proposal))


def confirmation_code_hash(code: str) -> str:
    return hashlib.sha256(code.strip().upper().encode("utf-8")).hexdigest()


def confirmation_code_for(proposal_id: str, code_key: str) -> str:
    if not code_key:
        return ""
    digest = hmac.new(
        code_key.encode("utf-8"),
        proposal_id.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest[:8].upper()


def proposal_from_payload(payload_json: str, *, code_key: str = "") -> TradeProposal:
    payload = json.loads(payload_json)
    payload["confirmation_code"] = confirmation_code_for(str(payload.get("proposal_id") or ""), code_key)
    # Legacy payloads remain auditable but cannot pass live execution without
    # a persisted evidence timestamp.
    payload.setdefault("model_as_of", 0.0)
    payload["probability_interval"] = tuple(payload.get("probability_interval") or ())
    payload["evidence_refs"] = tuple(payload.get("evidence_refs") or ())
    return TradeProposal(**payload)


def proposal_payload(proposal: TradeProposal) -> dict[str, object]:
    return {
        "proposal_id": proposal.proposal_id,
        "created_at": proposal.created_at,
        "expires_at": proposal.expires_at,
        "quote_expires_at": proposal.quote_expires_at,
        "market_id": proposal.market_id,
        "condition_id": proposal.condition_id,
        "token_id": proposal.token_id,
        "title": proposal.title,
        "outcome": proposal.outcome,
        "side": proposal.side,
        "max_price": proposal.max_price,
        "max_notional_usdc": proposal.max_notional_usdc,
        "probability_used": proposal.probability_used,
        "probability_interval": proposal.probability_interval,
        "net_edge_bps": proposal.net_edge_bps,
        "expected_profit_usdc": proposal.expected_profit_usdc,
        "fee_bps": proposal.fee_bps,
        "slippage_bps": proposal.slippage_bps,
        "model_name": proposal.model_name,
        "model_version": proposal.model_version,
        "model_sample_size": proposal.model_sample_size,
        "model_brier_score": proposal.model_brier_score,
        "model_independent": proposal.model_independent,
        "resolution_source": proposal.resolution_source,
        "evidence_refs": proposal.evidence_refs,
        "source_url": proposal.source_url,
        "model_as_of": proposal.model_as_of,
    }


def proposal_fingerprint_payload(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def proposal_integrity_token(fingerprint: str, integrity_key: str = "") -> str:
    if not integrity_key:
        return fingerprint
    return hmac.new(
        integrity_key.encode("utf-8"),
        fingerprint.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()


def approval_receipt_token(
    *,
    proposal_id: str,
    fingerprint: str,
    confirmation_code_hash_value: str,
    decision: str,
    sender: str,
    inbound_message_id: str,
    outbound_message_id: str,
    payload_hash: str,
    approved_at: float,
    integrity_key: str,
) -> str:
    """Bind a confirmed decision to its signed webhook and outbound message."""
    if not integrity_key:
        return ""
    material = json.dumps(
        {
            "proposal_id": proposal_id,
            "fingerprint": fingerprint,
            "confirmation_code_hash": confirmation_code_hash_value,
            "decision": decision,
            "sender": sender,
            "inbound_message_id": inbound_message_id,
            "outbound_message_id": outbound_message_id,
            "payload_hash": payload_hash,
            "approved_at": f"{float(approved_at):.9f}",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hmac.new(integrity_key.encode("utf-8"), material, hashlib.sha256).hexdigest()
