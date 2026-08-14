from __future__ import annotations

from pathlib import Path
from dataclasses import replace
import hashlib
import hmac
import http.client
import json
import sqlite3
import threading
import time
import pytest

from app.advisor.approval import ApprovalService
from app.advisor.approved_execution import ApprovedExecutionService, SubmissionResult
from app.advisor.broker import PolymarketCLOBSubmitter
from app.advisor.config import AdvisorRuntimeConfig
from app.advisor.evaluator import AdvisorPolicy, OpportunityEvaluator
from app.advisor.execution import ExecutionFlags, authorize_execution
from app.advisor.models import MarketQuote, ModelEvidence, Opportunity
from app.advisor.notifications import AdvisorNotificationService
from app.advisor.probability import JsonProbabilityModel
from app.advisor.runner import ConfirmedExecutionWorker, DailyAdvisorRunner
from app.advisor.service import DailyAdvisor
from app.advisor.sports import SportsMarketDiscovery
from app.advisor.store import AdvisorStore
from app.advisor.whatsapp import WhatsAppConfig, WhatsAppGateway
from app.advisor.whatsapp import verify_webhook_signature
from app.advisor.webhook import build_webhook_handler


def _opportunity(*, now: float = 1_000.0, lower: float = 0.72) -> Opportunity:
    return Opportunity(
        quote=MarketQuote(
            market_id="market-1",
            condition_id="condition-1",
            token_id="token-1",
            title="Team A wins",
            outcome="Yes",
            execution_price=0.55,
            available_size=100.0,
            observed_at=now,
            resolution_source="official-result-source",
            fee_bps=50.0,
            slippage_bps=25.0,
        ),
        evidence=ModelEvidence(
            model_name="sports-baseline",
            model_version="2026-08-13",
            probability=0.75,
            lower_probability=lower,
            upper_probability=0.78,
            calibrated=True,
            sample_size=500,
            brier_score=0.18,
            as_of=now,
            source_refs=("odds-provider:fixture", "lineups:fixture"),
            independent=True,
            market_id="market-1",
            condition_id="condition-1",
            token_id="token-1",
            outcome="Yes",
        ),
        bankroll_usdc=100.0,
    )


def test_store_migrates_legacy_cycles_and_inflight_execution_claims(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy-advisor.db"
    connection = sqlite3.connect(db_path)
    connection.executescript(
        """
        CREATE TABLE advisor_proposals (
            proposal_id TEXT PRIMARY KEY,
            created_at REAL NOT NULL,
            expires_at REAL NOT NULL,
            quote_expires_at REAL NOT NULL,
            status TEXT NOT NULL,
            confirmation_code_hash TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            confirmed_at REAL,
            confirmed_from TEXT,
            claimed_at REAL,
            decision_note TEXT NOT NULL DEFAULT '',
            order_id TEXT NOT NULL DEFAULT '',
            submitted_at REAL
        );
        CREATE TABLE advisor_daily_cycles (
            cycle_day TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            created_at REAL NOT NULL,
            proposal_id TEXT NOT NULL DEFAULT '',
            decision_note TEXT NOT NULL DEFAULT ''
        );
        INSERT INTO advisor_proposals (
            proposal_id, created_at, expires_at, quote_expires_at, status,
            confirmation_code_hash, fingerprint, payload_json, claimed_at,
            order_id, submitted_at
        ) VALUES (
            'legacy-proposal', 1000, 1100, 1050, 'reconciliation_required',
            'hash', 'fingerprint', '{}', 1001, 'order-old', 1002
        );
        INSERT INTO advisor_daily_cycles
            (cycle_day, status, created_at, proposal_id, decision_note)
        VALUES ('1970-01-01', 'no_opportunity', 1000, '', 'legacy close');
        """
    )
    connection.commit()
    connection.close()

    store = AdvisorStore(db_path, integrity_key="integrity", confirmation_key="confirm")
    cycle_columns = {
        str(row[1]): row for row in store.conn.execute("PRAGMA table_info(advisor_daily_cycles)")
    }
    claim = store.conn.execute(
        "SELECT status, order_id, submitted_at FROM advisor_execution_claims WHERE proposal_id = ?",
        ("legacy-proposal",),
    ).fetchone()

    assert cycle_columns["proposal_id"][3] == 0
    assert store.get_daily_cycle("1970-01-01")["proposal_id"] is None
    assert claim is not None
    assert tuple(claim) == ("reconciliation_required", "order-old", 1002.0)
    store.close()


def test_store_migrates_partial_risk_and_claim_tables_idempotently(tmp_path: Path) -> None:
    db_path = tmp_path / "partial-advisor.db"
    connection = sqlite3.connect(db_path)
    connection.executescript(
        """
        CREATE TABLE advisor_proposals (
            proposal_id TEXT PRIMARY KEY,
            created_at REAL NOT NULL,
            expires_at REAL NOT NULL,
            quote_expires_at REAL NOT NULL,
            status TEXT NOT NULL,
            confirmation_code_hash TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE advisor_daily_risk (
            risk_day TEXT PRIMARY KEY,
            realized_loss_usdc REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE advisor_risk_reservations (
            proposal_id TEXT PRIMARY KEY
        );
        CREATE TABLE advisor_execution_claims (
            proposal_id TEXT PRIMARY KEY,
            claimed_at REAL NOT NULL,
            status TEXT NOT NULL
        );
        INSERT INTO advisor_proposals (
            proposal_id, created_at, expires_at, quote_expires_at, status,
            confirmation_code_hash, fingerprint, payload_json
        ) VALUES (
            'legacy-open', 1000, 1100, 1050, 'submitted',
            'hash', 'fingerprint', '{"max_notional_usdc":2.5}'
        );
        """
    )
    connection.commit()
    connection.close()

    store = AdvisorStore(db_path, integrity_key="integrity", confirmation_key="confirm")
    columns = {
        str(row[1]) for row in store.conn.execute("PRAGMA table_info(advisor_execution_claims)")
    }
    assert {"order_id", "submitted_at"}.issubset(columns)
    proposal_columns = {
        str(row[1]) for row in store.conn.execute("PRAGMA table_info(advisor_proposals)")
    }
    assert {"confirmed_at", "confirmed_from", "decision_note"}.issubset(proposal_columns)
    risk_columns = {
        str(row[1]) for row in store.conn.execute("PRAGMA table_info(advisor_daily_risk)")
    }
    assert "reserved_loss_usdc" in risk_columns
    assert store.daily_risk_usdc(now=1_000.0) == 2.5
    reservation = store.conn.execute(
        "SELECT amount_usdc, status FROM advisor_risk_reservations WHERE proposal_id = 'legacy-open'",
    ).fetchone()
    assert reservation is not None
    assert tuple(reservation) == (2.5, "reserved")
    assert store.settle_loss_reservation(
        "legacy-open",
        1.0,
        realized_pnl_usdc=-1.0,
        now=1_001.0,
    )
    assert store.daily_risk_usdc(now=1_001.0) == 1.0
    store.close()

    reopened = AdvisorStore(db_path, integrity_key="integrity", confirmation_key="confirm")
    assert reopened.daily_risk_usdc(now=1_000.0) == 1.0
    assert reopened.conn.execute(
        "SELECT COUNT(*) FROM advisor_legacy_risk_migrations WHERE proposal_id = 'legacy-open'",
    ).fetchone()[0] == 1
    reopened.close()


def test_legacy_risk_migration_blocks_ambiguous_records_and_avoids_double_counting(tmp_path: Path) -> None:
    ambiguous_db = tmp_path / "ambiguous-settlement.db"
    store = AdvisorStore(ambiguous_db, integrity_key="integrity", confirmation_key="confirm")
    store.conn.execute(
        """
        INSERT INTO advisor_proposals (
            proposal_id, created_at, expires_at, quote_expires_at, status,
            confirmation_code_hash, fingerprint, payload_json, realized_pnl_usdc
        ) VALUES ('legacy-settled', 1000, 1100, 1050, 'settled', 'hash', 'fp',
                  '{"max_notional_usdc": 2.0}', -2.0)
        """
    )
    store.conn.commit()
    store.close()

    reopened = AdvisorStore(ambiguous_db, integrity_key="integrity", confirmation_key="confirm")
    assert reopened.conn.execute(
        "SELECT status FROM advisor_proposals WHERE proposal_id = 'legacy-settled'",
    ).fetchone()[0] == "reconciliation_required"
    assert reopened.daily_risk_usdc(now=1_000.0) == float("inf")
    assert reopened.reconcile_execution("legacy-settled", "failed", filled_size=0, now=1_001.0)
    assert reopened.daily_risk_usdc(now=1_001.0) == 0.0
    reopened.close()

    settled_db = tmp_path / "settled-reservation.db"
    store = AdvisorStore(settled_db, integrity_key="integrity", confirmation_key="confirm")
    store.conn.execute(
        """
        INSERT INTO advisor_proposals (
            proposal_id, created_at, expires_at, quote_expires_at, status,
            confirmation_code_hash, fingerprint, payload_json, realized_pnl_usdc
        ) VALUES ('already-settled', 1000, 1100, 1050, 'settled', 'hash', 'fp',
                  '{"max_notional_usdc": 2.0}', -2.0)
        """
    )
    store.conn.execute(
        """
        INSERT INTO advisor_daily_risk
            (risk_day, realized_loss_usdc, reserved_loss_usdc, updated_at)
        VALUES ('1970-01-01', 2.0, 0, 1000)
        """
    )
    store.conn.execute(
        """
        INSERT INTO advisor_risk_reservations
            (proposal_id, risk_day, amount_usdc, status, created_at, released_at)
        VALUES ('already-settled', '1970-01-01', 2.0, 'settled', 1000, 1001)
        """
    )
    store.conn.commit()
    store.close()

    reopened = AdvisorStore(settled_db, integrity_key="integrity", confirmation_key="confirm")
    assert reopened.daily_risk_usdc(now=1_000.0) == 2.0
    assert reopened.conn.execute(
        "SELECT action FROM advisor_legacy_risk_migrations WHERE proposal_id = 'already-settled'",
    ).fetchone()[0] == "settled_existing_reservation"
    reopened.close()


def test_legacy_open_reservation_mismatch_fails_closed(tmp_path: Path) -> None:
    db_path = tmp_path / "mismatched-reservation.db"
    store = AdvisorStore(db_path, integrity_key="integrity", confirmation_key="confirm")
    store.conn.execute(
        """
        INSERT INTO advisor_proposals (
            proposal_id, created_at, expires_at, quote_expires_at, status,
            confirmation_code_hash, fingerprint, payload_json
        ) VALUES ('legacy-open-mismatch', 1000, 1100, 1050, 'submitted', 'hash', 'fp',
                  '{"max_notional_usdc": 2.5}')
        """
    )
    store.conn.execute(
        """
        INSERT INTO advisor_risk_reservations
            (proposal_id, risk_day, amount_usdc, status, created_at)
        VALUES ('legacy-open-mismatch', '1970-01-01', 2.5, 'released', 1000)
        """
    )
    store.conn.commit()
    store.close()

    reopened = AdvisorStore(db_path, integrity_key="integrity", confirmation_key="confirm")
    assert reopened.daily_risk_usdc(now=1_000.0) == float("inf")
    assert reopened.conn.execute(
        "SELECT status FROM advisor_proposals WHERE proposal_id = 'legacy-open-mismatch'",
    ).fetchone()[0] == "reconciliation_required"
    reopened.close()


def test_uncalibrated_or_stale_evidence_abstains() -> None:
    opportunity = _opportunity(now=100.0)
    stale = Opportunity(
        quote=opportunity.quote,
        evidence=ModelEvidence(
            **{**opportunity.evidence.__dict__, "calibrated": False, "as_of": 0.0}
        ),
        bankroll_usdc=100.0,
    )
    result = OpportunityEvaluator().evaluate(stale, now=1_000.0)
    assert not result.eligible
    assert "model_not_calibrated" in result.reasons
    assert "quote_stale" in result.reasons


def test_json_evidence_rejects_string_booleans(tmp_path: Path) -> None:
    quote = _opportunity().quote
    path = tmp_path / "evidence.json"
    path.write_text(
        json.dumps(
            {
                "predictions": [
                    {
                        "market_id": quote.market_id,
                        "condition_id": quote.condition_id,
                        "token_id": quote.token_id,
                        "outcome": quote.outcome,
                        "probability": 0.8,
                        "lower_probability": 0.75,
                        "upper_probability": 0.82,
                        "calibrated": "false",
                        "independent": "false",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    evidence = JsonProbabilityModel(path).estimate(quote, now=1_000.0)
    assert not evidence.calibrated
    assert evidence.source_refs == ("evidence_boolean_invalid",)


def test_daily_advisor_persists_only_positive_net_opportunities(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    advisor = DailyAdvisor(store)
    run = advisor.analyze(_opportunity(), now=1_000.0)
    assert run.proposal is not None
    assert run.evaluation.net_edge_bps > 150
    assert len(store.pending_outbox(now=1_000.0)) == 1
    row = store.get_proposal(run.proposal.proposal_id)
    assert row is not None
    assert run.proposal.confirmation_code not in str(row["payload_json"])
    store.close()


def test_zero_requested_stake_is_rejected_instead_of_being_unlimited() -> None:
    opportunity = _opportunity()
    result = OpportunityEvaluator().evaluate(
        Opportunity(
            quote=opportunity.quote,
            evidence=opportunity.evidence,
            bankroll_usdc=opportunity.bankroll_usdc,
            requested_stake_usdc=0.0,
        ),
        now=1_000.0,
    )
    assert not result.eligible
    assert "requested_stake_invalid" in result.reasons
    assert result.recommended_stake_usdc == 0.0


def test_minimum_order_size_and_suspicious_edge_band_are_fail_closed() -> None:
    opportunity = _opportunity()
    result = OpportunityEvaluator().evaluate(
        Opportunity(
            quote=MarketQuote(**{**opportunity.quote.__dict__, "min_order_size": 10.0}),
            evidence=opportunity.evidence,
            bankroll_usdc=opportunity.bankroll_usdc,
        ),
        now=1_000.0,
    )
    assert not result.eligible
    assert "below_exchange_minimum" in result.reasons

    with pytest.raises(ValueError):
        AdvisorPolicy(minimum_edge_bps=150.0, maximum_edge_bps=100.0)


def test_run_daily_requires_server_side_keys(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db")
    result = DailyAdvisor(store).run_daily([_opportunity()], cycle_day="1970-01-01", now=1_000.0)
    assert result.proposal is None
    assert result.evaluation.reason == "runtime_keys_missing"
    assert store.get_daily_cycle("1970-01-01") is None
    store.close()


def test_non_positive_net_edge_is_always_rejected_even_with_invalid_policy_threshold() -> None:
    opportunity = _opportunity()
    with pytest.raises(ValueError):
        AdvisorPolicy(minimum_edge_bps=0.0)
    policy = AdvisorPolicy()
    losing = Opportunity(
        quote=MarketQuote(**{**opportunity.quote.__dict__, "execution_price": 0.95}),
        evidence=opportunity.evidence,
        bankroll_usdc=100.0,
    )
    result = OpportunityEvaluator(policy).evaluate(losing, now=1_000.0)
    assert not result.eligible
    assert "net_edge_non_positive" in result.reasons


def test_probability_and_edge_thresholds_are_strictly_above() -> None:
    opportunity = _opportunity()
    lower_bound_opportunity = Opportunity(
        quote=opportunity.quote,
        evidence=ModelEvidence(**{**opportunity.evidence.__dict__, "lower_probability": 0.72}),
        bankroll_usdc=opportunity.bankroll_usdc,
    )
    probe = OpportunityEvaluator(AdvisorPolicy(minimum_probability=0.60, minimum_edge_bps=1.0)).evaluate(
        lower_bound_opportunity,
        now=1_000.0,
    )
    policy = AdvisorPolicy(minimum_probability=0.72, minimum_edge_bps=probe.net_edge_bps)
    result = OpportunityEvaluator(policy).evaluate(
        lower_bound_opportunity,
        now=1_000.0,
    )
    assert not result.eligible
    assert "probability_below_threshold" in result.reasons
    assert "net_edge_below_threshold" in result.reasons


def test_whatsapp_is_disabled_without_explicit_policy_gate() -> None:
    proposal_store = AdvisorStore(Path(":memory:"), integrity_key="integrity", confirmation_key="confirm")
    advisor = DailyAdvisor(proposal_store)
    proposal = advisor.analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    gateway = WhatsAppGateway(WhatsAppConfig())
    result = gateway.send_proposal(proposal, "+34600000000")
    assert not result.sent
    assert result.reason == "whatsapp_gate_disabled_or_incomplete"
    proposal_store.close()


def test_whatsapp_reply_requires_allowlist_context_and_single_use(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    advisor = DailyAdvisor(store)
    proposal = advisor.analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    assert store.claim_outbox(proposal.proposal_id, now=1_001.0)
    store.mark_sent(proposal.proposal_id, "provider-message-1")
    service = ApprovalService(store, allowed_numbers=("34600000000",), app_secret="secret")
    import hashlib
    import hmac
    import json
    raw_body = json.dumps({
        "entry": [{"changes": [{"value": {"messages": [{
            "id": "message-1",
            "from": "34600000001",
            "type": "text",
            "text": {"body": f"SI {proposal.confirmation_code}"},
            "context": {"id": "provider-message-1"},
        }]}}]}],
    }).encode("utf-8")
    signature = "sha256=" + hmac.new(b"secret", raw_body, hashlib.sha256).hexdigest()

    bad = service.handle_reply(
        text=f"SI {proposal.confirmation_code}",
        sender="34600000001",
        provider_message_id="message-1",
        reply_message_id="provider-message-1",
        raw_body=raw_body,
        signature_header=signature,
        now=1_050.0,
    )
    assert bad.reason == "sender_not_allowlisted"

    raw_body = json.dumps({
        "entry": [{"changes": [{"value": {"messages": [{
            "id": "message-2",
            "from": "34600000000",
            "type": "text",
            "text": {"body": f"SI {proposal.confirmation_code}"},
            "context": {"id": "provider-message-1"},
        }]}}]}],
    }).encode("utf-8")
    signature = "sha256=" + hmac.new(b"secret", raw_body, hashlib.sha256).hexdigest()
    approved = service.handle_reply(
        text=f"SI {proposal.confirmation_code}",
        sender="+34 600 000 000",
        provider_message_id="message-2",
        reply_message_id="provider-message-1",
        raw_body=raw_body,
        signature_header=signature,
        now=1_050.0,
    )
    assert approved.status == "confirmed"
    duplicate = service.handle_reply(
        text=f"SI {proposal.confirmation_code}",
        sender="34600000000",
        provider_message_id="message-2",
        reply_message_id="provider-message-1",
        raw_body=raw_body,
        signature_header=signature,
        now=1_101.0,
    )
    assert duplicate.status == "duplicate"
    store.close()


def test_no_reply_never_reaches_confirmed_execution_worker(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    assert store.claim_outbox(proposal.proposal_id, now=1_001.0)
    assert store.mark_sent(proposal.proposal_id, "wamid-outbound-no")
    service = ApprovalService(store, allowed_numbers=("34600000000",), app_secret="secret")
    raw_body = json.dumps(
        {
            "entry": [{"changes": [{"value": {"messages": [{
                "id": "wamid-inbound-no",
                "from": "34600000000",
                "type": "text",
                "text": {"body": f"NO {proposal.confirmation_code}"},
                "context": {"id": "wamid-outbound-no"},
            }]}}]}],
        }
    ).encode("utf-8")
    signature = "sha256=" + hmac.new(b"secret", raw_body, hashlib.sha256).hexdigest()
    result = service.handle_webhook(raw_body=raw_body, signature_header=signature, now=1_050.0)
    assert result.status == "rejected"

    calls: list[str] = []
    worker = ConfirmedExecutionWorker(
        ApprovedExecutionService(store, runtime_config=AdvisorRuntimeConfig()),
        quote_loader=lambda _proposal: _opportunity(now=1_051.0).quote,
        balance_loader=lambda: 100.0,
        submit_order=lambda _proposal, _quote, _key: calls.append("called"),  # type: ignore[return-value]
    )
    assert worker.run_once(now=1_051.0) == []
    assert calls == []
    store.close()


def test_execution_requires_every_live_gate_and_revalidates_quote(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    advisor = DailyAdvisor(store)
    proposal = advisor.analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    quote = _opportunity().quote
    closed = authorize_execution(
        proposal,
        quote,
        ExecutionFlags(),
        proposal_status="revalidating",
        available_balance_usdc=100.0,
        daily_loss_usdc=0.0,
        max_daily_loss_usdc=5.0,
        now=1_050.0,
    )
    assert not closed.authorized
    assert closed.reason == "live_execution_gate_closed"

    open_flags = ExecutionFlags(True, "live", False, True, "armed", True)
    worse_quote = MarketQuote(**{**quote.__dict__, "execution_price": proposal.max_price + 0.01})
    rejected = authorize_execution(
        proposal,
        worse_quote,
        open_flags,
        proposal_status="revalidating",
        available_balance_usdc=100.0,
        daily_loss_usdc=0.0,
        max_daily_loss_usdc=5.0,
        integrity_verified=True,
        now=1_050.0,
    )
    assert not rejected.authorized
    assert rejected.reason == "price_worse_than_approved"
    store.close()


def test_execution_rejects_stale_model_evidence_after_confirmation() -> None:
    opportunity = _opportunity(now=1_000.0)
    proposal = DailyAdvisor(
        AdvisorStore(Path(":memory:"), confirmation_key="confirm", integrity_key="integrity")
    ).analyze(opportunity, now=1_000.0).proposal
    assert proposal is not None
    proposal = replace(proposal, expires_at=2_000.0, quote_expires_at=2_000.0)
    flags = ExecutionFlags(True, "live", False, True, "armed", True)
    decision = authorize_execution(
        proposal,
        _opportunity(now=1_501.0).quote,
        flags,
        proposal_status="revalidating",
        available_balance_usdc=100.0,
        daily_loss_usdc=0.0,
        max_daily_loss_usdc=5.0,
        integrity_verified=True,
        max_quote_age_seconds=600.0,
        max_model_age_seconds=500.0,
        now=1_501.0,
    )
    assert not decision.authorized
    assert decision.reason == "model_evidence_stale"


def test_execution_rejects_quote_deadline_crossing_after_claim() -> None:
    opportunity = _opportunity(now=1_000.0)
    proposal = DailyAdvisor(
        AdvisorStore(Path(":memory:"), confirmation_key="confirm", integrity_key="integrity")
    ).analyze(opportunity, now=1_000.0).proposal
    assert proposal is not None
    flags = ExecutionFlags(True, "live", False, True, "armed", True)
    decision = authorize_execution(
        proposal,
        _opportunity(now=1_060.0).quote,
        flags,
        proposal_status="revalidating",
        available_balance_usdc=100.0,
        daily_loss_usdc=0.0,
        max_daily_loss_usdc=5.0,
        integrity_verified=True,
        max_quote_age_seconds=60.0,
        now=1_060.0,
    )
    assert not decision.authorized
    assert decision.reason == "proposal_or_quote_expired"


def test_pending_outbox_and_expiry_respect_quote_deadline(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    assert store.pending_outbox(now=1_059.0)
    assert store.pending_outbox(now=1_060.0) == []
    assert store.expire_due(now=1_060.0) == 1
    assert store.get_proposal(proposal.proposal_id)["status"] == "expired"
    store.close()


def test_webhook_signature_is_required_and_constant_time_compatible() -> None:
    body = b'{"entry":[]}'
    import hashlib
    import hmac

    signature = "sha256=" + hmac.new(b"secret", body, hashlib.sha256).hexdigest()
    assert verify_webhook_signature(body, signature, "secret")
    assert not verify_webhook_signature(body, signature, "wrong")
    assert not verify_webhook_signature(body, "", "secret")


def test_non_finite_inputs_abstain() -> None:
    opportunity = _opportunity()
    quote = MarketQuote(**{**opportunity.quote.__dict__, "execution_price": float("nan")})
    result = OpportunityEvaluator().evaluate(
        Opportunity(quote=quote, evidence=opportunity.evidence, bankroll_usdc=100.0),
        now=1_000.0,
    )
    assert not result.eligible
    assert "non_finite_input" in result.reasons


def test_evidence_must_match_quote_and_not_be_from_future() -> None:
    opportunity = _opportunity(now=1_000.0)
    mismatched = ModelEvidence(
        **{
            **opportunity.evidence.__dict__,
            "market_id": "other-market",
        }
    )
    result = OpportunityEvaluator().evaluate(
        Opportunity(quote=opportunity.quote, evidence=mismatched, bankroll_usdc=100.0),
        now=1_000.0,
    )
    assert not result.eligible
    assert "evidence_market_mismatch" in result.reasons

    future_quote = MarketQuote(**{**opportunity.quote.__dict__, "observed_at": 1_001.0})
    result = OpportunityEvaluator().evaluate(
        Opportunity(quote=future_quote, evidence=opportunity.evidence, bankroll_usdc=100.0),
        now=1_000.0,
    )
    assert not result.eligible
    assert "quote_from_future" in result.reasons

    future_model = ModelEvidence(
        **{**opportunity.evidence.__dict__, "as_of": 1_001.0}
    )
    result = OpportunityEvaluator().evaluate(
        Opportunity(quote=opportunity.quote, evidence=future_model, bankroll_usdc=100.0),
        now=1_000.0,
    )
    assert not result.eligible
    assert "model_evidence_from_future" in result.reasons


def test_signed_webhook_parses_reply_context_without_trusting_caller_fields(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    advisor = DailyAdvisor(store)
    proposal = advisor.analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    assert store.claim_outbox(proposal.proposal_id, now=1_001.0)
    assert store.mark_sent(proposal.proposal_id, "wamid-outbound")
    service = ApprovalService(store, allowed_numbers=("34600000000",), app_secret="secret")
    import hashlib
    import hmac
    import json

    raw_body = json.dumps(
        {
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "messages": [
                                    {
                                        "id": "wamid-inbound",
                                        "from": "34600000000",
                                        "type": "text",
                                        "text": {"body": f"SI {proposal.confirmation_code}"},
                                        "context": {"id": "wamid-outbound"},
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
    ).encode("utf-8")
    signature = "sha256=" + hmac.new(b"secret", raw_body, hashlib.sha256).hexdigest()
    result = service.handle_webhook(raw_body=raw_body, signature_header=signature, now=1_050.0)
    assert result.status == "confirmed"
    store.close()


def test_signed_webhook_processes_all_text_replies_in_one_payload(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    advisor = DailyAdvisor(store)
    first = advisor.analyze(_opportunity(), now=1_000.0).proposal
    second = advisor.analyze(_opportunity(), now=1_000.0).proposal
    assert first is not None and second is not None
    assert store.claim_outbox(first.proposal_id, now=1_001.0)
    assert store.claim_outbox(second.proposal_id, now=1_001.0)
    assert store.mark_sent(first.proposal_id, "wamid-outbound-1")
    assert store.mark_sent(second.proposal_id, "wamid-outbound-2")
    service = ApprovalService(store, allowed_numbers=("34600000000",), app_secret="secret")
    import hashlib
    import hmac
    import json
    raw_body = json.dumps({
        "entry": [{"changes": [{"value": {"messages": [
            {"id": "wamid-inbound-1", "from": "34600000000", "type": "text",
             "text": {"body": f"SI {first.confirmation_code}"}, "context": {"id": "wamid-outbound-1"}},
            {"id": "wamid-inbound-2", "from": "34600000000", "type": "text",
             "text": {"body": f"NO {second.confirmation_code}"}, "context": {"id": "wamid-outbound-2"}},
        ]}}]}],
    }).encode("utf-8")
    signature = "sha256=" + hmac.new(b"secret", raw_body, hashlib.sha256).hexdigest()
    result = service.handle_webhook(raw_body=raw_body, signature_header=signature, now=1_050.0)
    assert result.status == "confirmed"
    assert store.get_proposal(first.proposal_id)["status"] == "confirmed"
    assert store.get_proposal(second.proposal_id)["status"] == "rejected"
    store.close()


def test_approved_execution_is_single_claim_and_rejects_closed_live(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    advisor = DailyAdvisor(store)
    proposal = advisor.analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    assert store.claim_outbox(proposal.proposal_id, now=1_001.0)
    assert store.mark_sent(proposal.proposal_id, "wamid-outbound")
    service = ApprovalService(store, allowed_numbers=("34600000000",), app_secret="secret")
    import hashlib
    import hmac
    import json
    raw_body = json.dumps({
        "entry": [{"changes": [{"value": {"messages": [{
            "id": "wamid-inbound",
            "from": "34600000000",
            "type": "text",
            "text": {"body": f"SI {proposal.confirmation_code}"},
            "context": {"id": "wamid-outbound"},
        }]}}]}],
    }).encode("utf-8")
    signature = "sha256=" + hmac.new(b"secret", raw_body, hashlib.sha256).hexdigest()
    approval = service.handle_reply(
        text=f"SI {proposal.confirmation_code}",
        sender="34600000000",
        provider_message_id="wamid-inbound",
        reply_message_id="wamid-outbound",
        raw_body=raw_body,
        signature_header=signature,
        now=1_050.0,
    )
    assert approval.status == "confirmed"
    executor = ApprovedExecutionService(store, runtime_config=AdvisorRuntimeConfig())
    calls: list[str] = []

    def submit(_proposal: TradeProposal, _quote: MarketQuote, _idempotency_key: str) -> SubmissionResult:
        calls.append("called")
        return SubmissionResult("submitted", "order-1")

    result = executor.execute(
        proposal.proposal_id,
        _opportunity(now=1_051.0).quote,
        submit_order=submit,
        available_balance_usdc=100.0,
        now=1_051.0,
    )
    assert result.status == "rejected"
    assert result.reason == "runtime_live_gate_closed"
    assert calls == []
    second = executor.execute(
        proposal.proposal_id,
        _opportunity(now=1_051.0).quote,
        submit_order=submit,
        available_balance_usdc=100.0,
        now=1_051.0,
    )
    assert second.status == "rejected"
    assert second.reason == "runtime_live_gate_closed"
    store.close()


def test_notification_outbox_is_fail_closed_and_recoverable(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    advisor = DailyAdvisor(store)
    proposal = advisor.analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    dispatcher = AdvisorNotificationService(store, WhatsAppGateway(WhatsAppConfig()), recipient="34600000000")
    result = dispatcher.dispatch_once(now=1_001.0)
    assert result[0].status == "disabled"
    assert store.get_proposal(proposal.proposal_id)["status"] == "pending"
    assert store.claim_outbox(proposal.proposal_id, now=1_001.0)
    assert dispatcher.recover_uncertain_sends(older_than_seconds=2.0, now=1_002.0) == 0
    assert dispatcher.recover_uncertain_sends(older_than_seconds=1.0, now=1_002.0) == 1
    assert store.get_proposal(proposal.proposal_id)["status"] == "notification_uncertain"
    store.close()


def test_malformed_whatsapp_success_is_reconciliation_required() -> None:
    store = AdvisorStore(Path(":memory:"), integrity_key="integrity", confirmation_key="confirm")
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {}

    class Session:
        def post(self, *_args: object, **_kwargs: object) -> Response:
            return Response()

    config = WhatsAppConfig(
        send_enabled=True,
        policy_confirmed=True,
        access_token="access",
        app_secret="secret",
        webhook_verify_token="verify",
        phone_number_id="phone",
        template_name="proposal",
        allowed_numbers=("34600000000",),
    )
    result = WhatsAppGateway(config, session=Session()).send_proposal(
        proposal,
        "34600000000",
        now=1_001.0,
    )
    assert not result.sent
    assert result.status == "reconciliation_required"
    assert result.reason == "provider_message_id_missing"
    store.close()


def test_notification_provider_exception_is_not_left_in_sending_state(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None

    class ExplodingGateway:
        ready = True

        def send_proposal(self, *_args: object, **_kwargs: object):
            raise RuntimeError("provider exploded")

    result = AdvisorNotificationService(
        store,
        ExplodingGateway(),  # type: ignore[arg-type]
        recipient="34600000000",
    ).dispatch_once(now=1_001.0)
    assert result[0].status == "reconciliation_required"
    outbox = store.conn.execute(
        "SELECT status FROM advisor_outbox WHERE proposal_id = ?",
        (proposal.proposal_id,),
    ).fetchone()
    assert outbox[0] == "reconciliation_required"
    store.close()


def test_json_probability_model_requires_exact_identity_and_loads_evidence(tmp_path: Path) -> None:
    quote = _opportunity().quote
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "metadata": {
                    "model_name": "sports-calibrated",
                    "model_version": "v7",
                    "calibrated": True,
                    "sample_size": 800,
                    "brier_score": 0.17,
                    "as_of": 1_000,
                    "source_refs": ["model-registry:v7"],
                    "independent": True,
                },
                "predictions": [
                    {
                        "market_id": quote.market_id,
                        "condition_id": quote.condition_id,
                        "token_id": quote.token_id,
                        "outcome": quote.outcome,
                        "probability": 0.79,
                        "lower_probability": 0.74,
                        "upper_probability": 0.82,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    evidence = JsonProbabilityModel(evidence_path).estimate(quote, now=1_001.0)
    assert evidence.probability == 0.79
    assert evidence.model_name == "sports-calibrated"
    assert evidence.independent

    evidence_path.write_text(
        json.dumps({"predictions": [{"market_id": quote.market_id}]}),
        encoding="utf-8",
    )
    missing = JsonProbabilityModel(evidence_path).estimate(quote, now=1_001.0)
    assert not missing.calibrated
    assert missing.source_refs == ("evidence_missing",)


def test_sports_discovery_paginates_and_rejects_repeated_gamma_pages() -> None:
    def market(number: int) -> dict[str, object]:
        return {
            "id": f"market-{number}",
            "conditionId": f"condition-{number}",
            "question": f"Team {number} wins",
            "sportsMarketType": "NBA",
            "resolutionSource": "official-results",
            "outcomes": '["Yes"]',
            "clobTokenIds": f'["token-{number}"]',
        }

    class PagedGamma:
        def __init__(self, repeated: bool = False) -> None:
            self.repeated = repeated
            self.offsets: list[int] = []

        def list_markets(self, *, offset: int, **_kwargs: object) -> list[dict[str, object]]:
            self.offsets.append(offset)
            if self.repeated:
                return [market(0)]
            return [market(offset)] if offset < 2 else []

    class SimpleCLOB:
        def get_book(self, _token_id: str) -> dict[str, object]:
            return {"asks": [{"price": "0.55", "size": "20"}], "bids": [{"price": "0.54", "size": "20"}]}

        def get_min_order_size(self, _token_id: str) -> float:
            return 1.0

        def get_fee_rate_bps(self, _token_id: str) -> float:
            return 75.0

    paged = PagedGamma()
    quotes = SportsMarketDiscovery(
        paged,
        SimpleCLOB(),
        page_size=1,
        max_pages=5,
        sports_market_types=("NBA",),
    ).discover(now=1_000.0)
    assert len(quotes) == 2
    assert paged.offsets == [0, 1, 2]

    with pytest.raises(RuntimeError, match="page_repeated"):
        SportsMarketDiscovery(
            PagedGamma(repeated=True),
            SimpleCLOB(),
            page_size=1,
            max_pages=3,
            sports_market_types=("NBA",),
        ).discover(now=1_000.0)


def test_server_side_sports_filter_accepts_market_without_repeated_category() -> None:
    class Gamma:
        def list_markets(self, **_kwargs: object) -> list[dict[str, object]]:
            return [{
                "id": "market-filtered",
                "conditionId": "condition-filtered",
                "question": "Team wins",
                "resolutionSource": "official-results",
                "outcomes": '["Yes"]',
                "clobTokenIds": '["token-filtered"]',
            }]

    class CLOB:
        def get_book(self, _token_id: str) -> dict[str, object]:
            return {"asks": [{"price": "0.55", "size": "20"}]}

        def get_min_order_size(self, _token_id: str) -> float:
            return 1.0

        def get_fee_rate_bps(self, _token_id: str) -> float:
            return 75.0

    quotes = SportsMarketDiscovery(
        Gamma(),
        CLOB(),
        sports_market_types=("NBA",),
    ).discover(now=1_000.0)
    assert [quote.market_id for quote in quotes] == ["market-filtered"]


def test_runner_uses_durable_daily_risk_instead_of_constructor_snapshot(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    with store.conn:
        store.conn.execute(
            "INSERT INTO advisor_daily_risk (risk_day, realized_loss_usdc, updated_at) VALUES (?, ?, ?)",
            ("1970-01-01", 5.0, 1_000.0),
        )

    class Discovery:
        def discover(self, *, now: float) -> list[MarketQuote]:
            return [_opportunity(now=now).quote]

    class Model:
        def estimate(self, quote: MarketQuote, *, now: float) -> ModelEvidence:
            return _opportunity(now=now).evidence

    runner = DailyAdvisorRunner(
        DailyAdvisor(store),
        Discovery(),
        Model(),
        bankroll_usdc=100.0,
        daily_loss_usdc=0.0,
    )
    result = runner.run_once(now=1_000.0)
    assert result.run.proposal is None
    assert result.run.evaluation.reason in {"daily_loss_limit_reached", "stake_below_minimum"}
    store.close()


def test_advisor_requires_both_server_side_keys(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", confirmation_key="confirm")
    advisor = DailyAdvisor(store)
    try:
        advisor.analyze(_opportunity(), now=1_000.0)
    except RuntimeError as error:
        assert "integrity" in str(error)
    else:
        raise AssertionError("proposal creation must fail without integrity key")
    store.close()


def test_signed_caller_fields_cannot_override_webhook_body(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    advisor = DailyAdvisor(store)
    proposal = advisor.analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    assert store.claim_outbox(proposal.proposal_id, now=1_001.0)
    assert store.mark_sent(proposal.proposal_id, "provider-message-1")
    service = ApprovalService(store, allowed_numbers=("34600000000",), app_secret="secret")
    import hashlib
    import hmac
    import json
    raw_body = json.dumps({
        "entry": [{"changes": [{"value": {"messages": [{
            "id": "message-1",
            "from": "34600000000",
            "type": "text",
            "text": {"body": f"SI {proposal.confirmation_code}"},
            "context": {"id": "provider-message-1"},
        }]}}]}],
    }).encode("utf-8")
    signature = "sha256=" + hmac.new(b"secret", raw_body, hashlib.sha256).hexdigest()
    result = service.handle_reply(
        text=f"SI {proposal.confirmation_code}",
        sender="34600000000",
        provider_message_id="message-2",
        reply_message_id="provider-message-1",
        raw_body=raw_body,
        signature_header=signature,
        now=1_050.0,
    )
    assert result.reason == "caller_fields_do_not_match_signed_payload"
    store.close()


def test_integrity_covers_execution_cost_terms(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET payload_json = replace(payload_json, 'fee_bps', 'fee_bps_changed') WHERE proposal_id = ?",
            (proposal.proposal_id,),
        )
    assert not store.proposal_integrity_valid(proposal.proposal_id)
    store.close()


def test_incremental_daily_loss_and_requested_nan_are_blocked() -> None:
    opportunity = _opportunity()
    result = OpportunityEvaluator().evaluate(
        Opportunity(
            quote=opportunity.quote,
            evidence=opportunity.evidence,
            bankroll_usdc=100.0,
            daily_loss_usdc=4.5,
        ),
        now=1_000.0,
    )
    assert not result.eligible
    assert "stake_below_minimum" in result.reasons

    nan_result = OpportunityEvaluator().evaluate(
        Opportunity(
            quote=opportunity.quote,
            evidence=opportunity.evidence,
            bankroll_usdc=100.0,
            requested_stake_usdc=float("nan"),
        ),
        now=1_000.0,
    )
    assert not nan_result.eligible
    assert "non_finite_input" in nan_result.reasons


def test_daily_loss_reservation_is_atomic_across_proposals(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    advisor = DailyAdvisor(store)
    first = advisor.analyze(_opportunity(), now=1_000.0).proposal
    second = advisor.analyze(_opportunity(), now=1_000.0).proposal
    assert first is not None and second is not None
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET status = 'revalidating' WHERE proposal_id IN (?, ?)",
            (first.proposal_id, second.proposal_id),
        )
    assert store.reserve_daily_loss(first.proposal_id, 3.0, 5.0, now=1_001.0)
    assert not store.reserve_daily_loss(second.proposal_id, 3.0, 5.0, now=1_001.0)
    assert store.daily_risk_usdc(now=1_001.0) == 3.0
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET status = 'failed' WHERE proposal_id = ?",
            (first.proposal_id,),
        )
    assert store.release_loss_reservation(first.proposal_id, now=1_002.0)
    assert store.daily_risk_usdc(now=1_002.0) == 0.0
    store.close()


def test_run_daily_persists_at_most_one_proposal_per_utc_day(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    advisor = DailyAdvisor(store)
    first = advisor.run_daily([_opportunity(), _opportunity()], cycle_day="1970-01-01", now=1_000.0)
    second = advisor.run_daily([_opportunity()], cycle_day="1970-01-01", now=1_001.0)
    assert first.proposal is not None
    assert second.proposal is None
    assert second.evaluation.reason == "daily_cycle_already_processed"
    assert store.get_daily_cycle("1970-01-01")["proposal_id"] == first.proposal.proposal_id
    assert len(store.pending_outbox(now=1_001.0)) == 1
    store.close()


def test_run_daily_rejects_a_caller_supplied_day_from_another_utc_date(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    result = DailyAdvisor(store).run_daily(
        [_opportunity()],
        cycle_day="1970-01-02",
        now=1_000.0,
    )
    assert result.proposal is None
    assert result.evaluation.reason == "cycle_day_mismatch"
    assert store.get_daily_cycle("1970-01-02") is None
    store.close()


def test_daily_cycle_claim_is_persisted_before_external_work(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")

    assert store.begin_daily_cycle("1970-01-01", now=1_000.0)
    assert not store.begin_daily_cycle("1970-01-01", now=1_001.0)
    cycle = store.get_daily_cycle("1970-01-01")
    assert cycle is not None
    assert cycle["status"] == "running"
    assert store.complete_daily_cycle("1970-01-01", "no_opportunity", now=1_001.0)
    assert store.get_daily_cycle("1970-01-01")["status"] == "no_opportunity"
    store.close()


def test_runner_deduplicates_a_completed_daily_cycle(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    advisor = DailyAdvisor(store)

    class EmptyDiscovery:
        def discover(self, *, now: float) -> list[MarketQuote]:
            return []

    runner = DailyAdvisorRunner(
        advisor,
        EmptyDiscovery(),
        object(),
        bankroll_usdc=100.0,
    )
    first = runner.run_once(now=1_000.0)
    second = runner.run_once(now=1_001.0)

    assert first.run.evaluation.reason == "no_candidates"
    assert second.run.evaluation.reason == "daily_cycle_already_processed"
    assert second.discovered_quotes == 0
    assert second.modelled_opportunities == 0
    store.close()


def test_unknown_submission_status_keeps_reservation_for_reconciliation(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET status = 'revalidating' WHERE proposal_id = ?",
            (proposal.proposal_id,),
        )
    assert store.reserve_daily_loss(proposal.proposal_id, 2.0, 5.0, now=1_001.0)
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET status = 'reconciliation_required' WHERE proposal_id = ?",
            (proposal.proposal_id,),
        )
    assert store.daily_risk_usdc(now=1_001.0) == 2.0
    assert not store.release_loss_reservation(proposal.proposal_id, now=1_002.0)
    assert store.reconcile_execution(
        proposal.proposal_id,
        "failed",
        note="provider_rejected",
        now=1_003.0,
    )
    assert store.daily_risk_usdc(now=1_003.0) == 0.0
    store.close()


def test_unreserved_reconciliation_can_only_close_as_zero_fill_failure(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET status = 'revalidating', claimed_at = ? WHERE proposal_id = ?",
            (1_001.0, proposal.proposal_id),
        )
    assert store.recover_stuck_executions(cutoff=1_002.0) == 1
    assert not store.reconcile_execution(proposal.proposal_id, "submitted", order_id="order-without-reservation")
    assert not store.reconcile_execution(proposal.proposal_id, "failed", filled_size=1.0)
    assert store.reconcile_execution(proposal.proposal_id, "failed", filled_size=0, now=1_003.0)
    assert store.get_proposal(proposal.proposal_id)["status"] == "failed"
    store.close()


def test_ambiguous_notification_requires_explicit_provider_reconciliation(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    assert store.claim_outbox(proposal.proposal_id, now=1_001.0)
    store.mark_send_reconciliation_required(proposal.proposal_id, "provider_timeout")
    assert not store.reconcile_notification(proposal.proposal_id, "sent", now=1_002.0)
    assert store.reconcile_notification(
        proposal.proposal_id,
        "sent",
        provider_message_id="provider-message-reconciled",
        note="provider_lookup_confirmed",
        now=1_002.0,
    )
    row = store.get_proposal(proposal.proposal_id)
    outbox = store.conn.execute(
        "SELECT status, provider_message_id FROM advisor_outbox WHERE proposal_id = ?",
        (proposal.proposal_id,),
    ).fetchone()
    assert row["status"] == "sent"
    assert outbox["status"] == "sent"
    assert outbox["provider_message_id"] == "provider-message-reconciled"
    store.close()


def test_cancelled_without_zero_fill_confirmation_does_not_release_reservation(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET status = 'reconciliation_required' WHERE proposal_id = ?",
            (proposal.proposal_id,),
        )
    assert store.reserve_daily_loss(proposal.proposal_id, 2.0, 5.0, now=1_001.0) is False
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET status = 'revalidating' WHERE proposal_id = ?",
            (proposal.proposal_id,),
        )
    assert store.reserve_daily_loss(proposal.proposal_id, 2.0, 5.0, now=1_001.0)
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET status = 'reconciliation_required' WHERE proposal_id = ?",
            (proposal.proposal_id,),
        )
    assert not store.reconcile_execution(proposal.proposal_id, "cancelled", now=1_002.0)
    assert store.reconcile_execution(proposal.proposal_id, "cancelled", filled_size=0, now=1_003.0)
    assert store.daily_risk_usdc(now=1_003.0) == 0.0
    store.close()


def test_filled_execution_and_settlement_are_auditable(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET status = 'revalidating', claimed_at = ? WHERE proposal_id = ?",
            (1_001.0, proposal.proposal_id),
        )
    with store.conn:
        store.conn.execute(
            "INSERT INTO advisor_execution_claims (proposal_id, claimed_at, status) VALUES (?, ?, 'claimed')",
            (proposal.proposal_id, 1_001.0),
        )
    assert store.reserve_daily_loss(proposal.proposal_id, 2.0, 5.0, now=1_001.0)
    assert store.mark_execution_submitted(
        proposal.proposal_id,
        "order-filled",
        execution_status="filled",
        filled_size=3.5,
        now=1_002.0,
    )
    assert not store.settle_loss_reservation(
        proposal.proposal_id,
        realized_loss_usdc=0.0,
        realized_pnl_usdc=-1.0,
        now=1_002.5,
    )
    row = store.get_proposal(proposal.proposal_id)
    assert row["status"] == "filled"
    assert row["execution_status"] == "filled"
    assert row["filled_size"] == 3.5
    assert store.settle_loss_reservation(
        proposal.proposal_id,
        realized_loss_usdc=0.0,
        realized_pnl_usdc=1.25,
        now=1_003.0,
    )
    row = store.get_proposal(proposal.proposal_id)
    assert row["status"] == "settled"
    assert row["realized_pnl_usdc"] == 1.25
    assert store.conn.execute(
        "SELECT status FROM advisor_execution_claims WHERE proposal_id = ?",
        (proposal.proposal_id,),
    ).fetchone()[0] == "settled"
    assert store.daily_risk_usdc(now=1_003.0) == 0.0
    store.close()


def test_sports_discovery_uses_executable_ask_and_never_models_itself() -> None:
    class FakeGamma:
        def list_sports(self) -> list[dict[str, object]]:
            return [{"sport": "NBA", "tags": "1001"}]

        def list_markets(self, **_kwargs: object) -> list[dict[str, object]]:
            return [{
                "id": "market-1",
                "conditionId": "condition-1",
                "question": "Team A wins",
                "sportsMarketType": "NBA",
                "resolutionSource": "official-results",
                "outcomes": '["Yes", "No"]',
                "clobTokenIds": '["token-yes", "token-no"]',
                "slug": "team-a-wins",
            }]

    class FakeCLOB:
        def get_book(self, token_id: str) -> dict[str, object]:
            return {
                "asks": [{"price": "0.55", "size": "20"}],
                "bids": [{"price": "0.54", "size": "20"}],
                "timestamp": "1000",
                "min_order_size": "1",
            }

        def get_min_order_size(self, _token_id: str) -> float:
            return 1.0

        def get_fee_rate_bps(self, _token_id: str) -> float:
            return 75.0

    quotes = SportsMarketDiscovery(FakeGamma(), FakeCLOB()).discover(now=1_000.0)
    assert len(quotes) == 2
    assert all(quote.execution_price == 0.55 for quote in quotes)
    assert all(quote.available_size == 20.0 for quote in quotes)


def test_clob_cancelled_status_requires_reconciliation() -> None:
    opportunity = _opportunity()
    proposal = DailyAdvisor(
        AdvisorStore(Path(":memory:"), integrity_key="integrity", confirmation_key="confirm")
    ).analyze(opportunity, now=1_000.0).proposal
    assert proposal is not None

    class FakeCLOB:
        def place_market_order(self, *_args: object, **_kwargs: object) -> dict[str, str]:
            return {"orderID": "order-cancelled", "status": "cancelled"}

    result = PolymarketCLOBSubmitter(FakeCLOB())(proposal, opportunity.quote, proposal.proposal_id)
    assert result.status == "cancelled"


def test_reconciliation_transition_updates_claim_before_successful_reconcile(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET status = 'revalidating', claimed_at = ? WHERE proposal_id = ?",
            (1_001.0, proposal.proposal_id),
        )
        store.conn.execute(
            "INSERT INTO advisor_execution_claims (proposal_id, claimed_at, status) VALUES (?, ?, 'claimed')",
            (proposal.proposal_id, 1_001.0),
        )
    assert store.reserve_daily_loss(proposal.proposal_id, 2.0, 5.0, now=1_001.0)
    assert store.mark_reconciliation_required(
        proposal.proposal_id,
        "provider_timeout",
        order_id="order-reconciled",
    )
    assert store.get_proposal(proposal.proposal_id)["order_id"] == "order-reconciled"
    assert not store.mark_reconciliation_required(
        proposal.proposal_id,
        "conflicting_provider_result",
        order_id="different-order",
    )
    assert store.conn.execute(
        "SELECT status FROM advisor_execution_claims WHERE proposal_id = ?",
        (proposal.proposal_id,),
    ).fetchone()[0] == "reconciliation_required"
    assert store.reconcile_execution(
        proposal.proposal_id,
        "submitted",
        now=1_002.0,
    )
    assert store.get_proposal(proposal.proposal_id)["status"] == "submitted"
    store.close()


def test_webhook_thread_uses_request_local_sqlite_connection(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    current_time = time.time()
    proposal = DailyAdvisor(store).analyze(_opportunity(now=current_time), now=current_time).proposal
    assert proposal is not None
    assert store.claim_outbox(proposal.proposal_id, now=current_time + 1.0)
    assert store.mark_sent(proposal.proposal_id, "wamid-outbound")
    approval = ApprovalService(store, allowed_numbers=("34600000000",), app_secret="secret")
    config = WhatsAppConfig(webhook_verify_token="verify")
    server = __import__("http.server", fromlist=["ThreadingHTTPServer"]).ThreadingHTTPServer(
        ("127.0.0.1", 0), build_webhook_handler(approval, config)
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        body = json.dumps({
            "entry": [{"changes": [{"value": {"messages": [{
                "id": "wamid-inbound-thread",
                "from": "34600000000",
                "type": "text",
                "text": {"body": f"SI {proposal.confirmation_code}"},
                "context": {"id": "wamid-outbound"},
            }]}}]}],
        }).encode("utf-8")
        signature = "sha256=" + hmac.new(b"secret", body, hashlib.sha256).hexdigest()
        connection = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
        connection.request(
            "POST",
            "/",
            body=body,
            headers={"Content-Type": "application/json", "X-Hub-Signature-256": signature},
        )
        response = connection.getresponse()
        assert response.status == 200
        assert store.get_proposal(proposal.proposal_id)["status"] == "confirmed"
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        store.close()


def test_runtime_keys_reject_whitespace_only_material() -> None:
    with pytest.raises(ValueError):
        AdvisorRuntimeConfig(integrity_key=" " * 40, confirmation_key="confirm-key-012345678901234567890123456789")


def test_execution_revalidation_enforces_runtime_stake_and_quote_age() -> None:
    opportunity = _opportunity(now=1_000.0)
    proposal = DailyAdvisor(
        AdvisorStore(Path(":memory:"), confirmation_key="confirm", integrity_key="integrity")
    ).analyze(opportunity, now=1_000.0).proposal
    assert proposal is not None
    flags = ExecutionFlags(True, "live", False, True, "armed", True)
    oversized = authorize_execution(
        proposal,
        _opportunity(now=1_001.0).quote,
        flags,
        proposal_status="revalidating",
        available_balance_usdc=100.0,
        daily_loss_usdc=0.0,
        max_daily_loss_usdc=5.0,
        integrity_verified=True,
        max_stake_usdc=1.0,
        now=1_001.0,
    )
    assert not oversized.authorized
    assert oversized.reason == "stake_above_runtime_limit"

    stale = authorize_execution(
        proposal,
        _opportunity(now=1_001.0).quote,
        flags,
        proposal_status="revalidating",
        available_balance_usdc=100.0,
        daily_loss_usdc=0.0,
        max_daily_loss_usdc=5.0,
        integrity_verified=True,
        max_quote_age_seconds=0.5,
        now=1_002.0,
    )
    assert not stale.authorized
    assert stale.reason == "quote_stale"


def test_live_ready_requires_global_gates_and_verified_webhook() -> None:
    config = AdvisorRuntimeConfig(
        enabled=True,
        live_enabled=True,
        broker_configured=True,
        control_state="armed",
        integrity_key="integrity-key-012345678901234567890123456789",
        confirmation_key="confirm-key-012345678901234567890123456789",
        recipient="34600000000",
        whatsapp=WhatsAppConfig(
            send_enabled=True,
            policy_confirmed=True,
            access_token="token",
            app_secret="secret",
            phone_number_id="phone",
            webhook_verify_token="verify",
            template_name="proposal",
            allowed_numbers=("34600000000",),
        ),
    )
    assert not config.live_ready
    assert config.whatsapp.ready


def test_status_tampering_cannot_simulate_whatsapp_approval(tmp_path: Path) -> None:
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key="integrity", confirmation_key="confirm")
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET status = 'confirmed' WHERE proposal_id = ?",
            (proposal.proposal_id,),
        )
    assert not store.claim_confirmed(proposal.proposal_id, now=1_001.0)
    store.close()


def test_positive_execution_persists_order_and_cannot_be_claimed_twice(tmp_path: Path) -> None:
    integrity_key = "integrity-key-012345678901234567890123456789"
    confirmation_key = "confirm-key-012345678901234567890123456789"
    store = AdvisorStore(tmp_path / "advisor.db", integrity_key=integrity_key, confirmation_key=confirmation_key)
    proposal = DailyAdvisor(store).analyze(_opportunity(), now=1_000.0).proposal
    assert proposal is not None
    assert store.claim_outbox(proposal.proposal_id, now=1_001.0)
    assert store.mark_sent(proposal.proposal_id, "wamid-outbound")
    service = ApprovalService(store, allowed_numbers=("34600000000",), app_secret="secret")
    import hashlib
    import hmac
    import json
    raw_body = json.dumps({
        "entry": [{"changes": [{"value": {"messages": [{
            "id": "wamid-inbound",
            "from": "34600000000",
            "type": "text",
            "text": {"body": f"SI {proposal.confirmation_code}"},
            "context": {"id": "wamid-outbound"},
        }]}}]}],
    }).encode("utf-8")
    signature = "sha256=" + hmac.new(b"secret", raw_body, hashlib.sha256).hexdigest()
    assert service.handle_webhook(raw_body=raw_body, signature_header=signature, now=1_050.0).status == "confirmed"
    executor = ApprovedExecutionService(
        store,
        runtime_config=AdvisorRuntimeConfig(
            enabled=True,
            live_enabled=True,
            broker_configured=True,
            control_state="armed",
            global_live_trading=True,
            global_execution_mode="live",
            global_dry_run=False,
            integrity_key=integrity_key,
            confirmation_key=confirmation_key,
            recipient="34600000000",
            whatsapp=WhatsAppConfig(
                send_enabled=True,
                policy_confirmed=True,
                access_token="token",
                app_secret="secret",
                phone_number_id="phone",
                webhook_verify_token="verify",
                template_name="proposal",
                allowed_numbers=("34600000000",),
            ),
        ),
    )
    calls: list[str] = []

    def submit(_proposal: TradeProposal, _quote: MarketQuote, idempotency_key: str) -> SubmissionResult:
        calls.append(idempotency_key)
        return SubmissionResult("submitted", "order-1")

    result = executor.execute(
        proposal.proposal_id,
        _opportunity(now=1_051.0).quote,
        submit_order=submit,
        available_balance_usdc=100.0,
        now=1_051.0,
    )
    assert result.status == "submitted"
    assert result.order_id == "order-1"
    assert calls == [proposal.proposal_id]
    assert store.get_proposal(proposal.proposal_id)["status"] == "submitted"
    assert store.daily_risk_usdc(now=1_051.0) == proposal.max_notional_usdc
    second = executor.execute(
        proposal.proposal_id,
        _opportunity(now=1_051.0).quote,
        submit_order=submit,
        available_balance_usdc=100.0,
        now=1_051.0,
    )
    assert second.status == "not_claimed"
    assert calls == [proposal.proposal_id]
    with store.conn:
        store.conn.execute(
            "UPDATE advisor_proposals SET status = 'confirmed' WHERE proposal_id = ?",
            (proposal.proposal_id,),
        )
    third = executor.execute(
        proposal.proposal_id,
        _opportunity(now=1_051.0).quote,
        submit_order=submit,
        available_balance_usdc=100.0,
        now=1_051.0,
    )
    assert third.status == "not_claimed"
    assert calls == [proposal.proposal_id]
    store.close()
