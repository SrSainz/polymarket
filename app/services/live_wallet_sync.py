from __future__ import annotations

import json
import time
from typing import Any

from app.core.execution_engine import apply_fill_to_database
from app.db import Database
from app.models import CopyInstruction, SignalAction, TradeSide
from app.polymarket.activity_client import ActivityClient


class LiveWalletSyncService:
    def __init__(self, db: Database, activity_client: ActivityClient) -> None:
        self.db = db
        self.activity_client = activity_client

    def sync(
        self,
        *,
        wallet: str,
        mode: str = "live",
        page_limit: int = 500,
        max_pages: int = 20,
    ) -> dict[str, object]:
        safe_wallet = str(wallet or "").strip().lower()
        safe_mode = str(mode or "").strip().lower() or "live"
        if not safe_wallet:
            raise RuntimeError("wallet is required for live wallet sync")

        imported = 0
        duplicates = 0
        skipped = 0
        errors = 0

        raw_activity = self._fetch_activity(wallet=safe_wallet, page_limit=page_limit, max_pages=max_pages)
        trade_rows = sorted(
            [row for row in raw_activity if str(row.get("type") or "").strip().upper() == "TRADE"],
            key=lambda row: (
                int(_safe_float(row.get("timestamp"))),
                str(row.get("transactionHash") or ""),
                str(row.get("asset") or ""),
                str(row.get("side") or ""),
            ),
        )

        for trade in trade_rows:
            sync_key = self._activity_sync_key(trade)
            if self.db.get_bot_state(sync_key):
                duplicates += 1
                continue
            try:
                result = self._import_trade(trade=trade, wallet=safe_wallet, mode=safe_mode)
            except Exception:  # noqa: BLE001
                errors += 1
                continue
            if result == "imported":
                imported += 1
                self.db.set_bot_state(sync_key, "1")
            elif result == "duplicate":
                duplicates += 1
                self.db.set_bot_state(sync_key, "1")
            else:
                skipped += 1

        positions = self._fetch_positions(wallet=safe_wallet, page_limit=page_limit, max_pages=max_pages)
        mismatch_reason = self._positions_mismatch_reason(positions=positions)
        now_ts = int(time.time())
        if mismatch_reason:
            self.db.set_bot_state("position_ledger_mode", "external")
            self.db.set_bot_state("position_ledger_preflight", "blocked")
            self.db.set_bot_state("live_wallet_sync_status", "mismatch")
            self.db.set_bot_state("live_wallet_sync_reason", mismatch_reason)
        else:
            self.db.set_bot_state("position_ledger_preflight", "ready")
            self.db.set_bot_state("position_ledger_mode", safe_mode if self.db.list_copy_positions() else "")
            self.db.set_bot_state("live_wallet_sync_status", "ok")
            self.db.set_bot_state("live_wallet_sync_reason", "")
        self.db.set_bot_state("live_wallet_sync_wallet", safe_wallet)
        self.db.set_bot_state("live_wallet_sync_at", str(now_ts))
        self.db.set_bot_state("live_wallet_sync_imported", str(imported))
        self.db.set_bot_state("live_wallet_sync_duplicates", str(duplicates))
        self.db.set_bot_state("live_wallet_sync_skipped", str(skipped))
        self.db.set_bot_state("live_wallet_sync_errors", str(errors))

        return {
            "wallet": safe_wallet,
            "mode": safe_mode,
            "activity_rows": len(trade_rows),
            "positions_rows": len(positions),
            "imported": imported,
            "duplicates": duplicates,
            "skipped": skipped,
            "errors": errors,
            "ok": mismatch_reason == "",
            "mismatch_reason": mismatch_reason,
        }

    def _fetch_activity(self, *, wallet: str, page_limit: int, max_pages: int) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        safe_limit = max(int(page_limit), 1)
        safe_pages = max(int(max_pages), 1)
        for page_index in range(safe_pages):
            offset = page_index * safe_limit
            page = self.activity_client.get_activity(wallet=wallet, limit=safe_limit, offset=offset)
            if not page:
                break
            items.extend(page)
            if len(page) < safe_limit:
                break
        return items

    def _fetch_positions(self, *, wallet: str, page_limit: int, max_pages: int) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        safe_limit = max(int(page_limit), 1)
        safe_pages = max(int(max_pages), 1)
        for page_index in range(safe_pages):
            offset = page_index * safe_limit
            page = self.activity_client.get_positions(wallet=wallet, limit=safe_limit, offset=offset)
            if not page:
                break
            items.extend(page)
            if len(page) < safe_limit:
                break
        return items

    def _import_trade(self, *, trade: dict[str, Any], wallet: str, mode: str) -> str:
        asset = str(trade.get("asset") or "").strip()
        condition_id = str(trade.get("conditionId") or trade.get("condition_id") or "").strip()
        side = str(trade.get("side") or "").strip().lower()
        size = max(_safe_float(trade.get("size")), 0.0)
        price = max(_safe_float(trade.get("price")), 0.0)
        notional = max(_safe_float(trade.get("usdcSize")), 0.0)
        ts = int(_safe_float(trade.get("timestamp")))
        if notional <= 0 and size > 0 and price > 0:
            notional = size * price
        if not asset or not condition_id or side not in {"buy", "sell"} or size <= 0 or price <= 0 or ts <= 0:
            return "skipped"

        existing = self.db.get_copy_position(asset)
        existing_size = max(float(existing["size"] or 0.0), 0.0) if existing is not None else 0.0
        if side == "buy":
            action = SignalAction.ADD if existing_size > 1e-9 else SignalAction.OPEN
            trade_side = TradeSide.BUY
        else:
            if existing_size <= 1e-9:
                return "skipped"
            action = SignalAction.CLOSE if size >= existing_size - 1e-9 else SignalAction.REDUCE
            trade_side = TradeSide.SELL

        instruction = CopyInstruction(
            action=action,
            side=trade_side,
            asset=asset,
            condition_id=condition_id,
            size=size,
            price=price,
            notional=notional,
            source_wallet=f"wallet-sync:{wallet}",
            source_signal_id=0,
            title=str(trade.get("title") or trade.get("slug") or ""),
            slug=str(trade.get("slug") or ""),
            outcome=str(trade.get("outcome") or ""),
            category="crypto",
            reason=f"wallet_activity_import:{str(trade.get('transactionHash') or '').strip() or 'external'}",
        )
        result = apply_fill_to_database(
            db=self.db,
            instruction=instruction,
            mode=mode,
            filled_size=size,
            fill_price=price,
            fill_notional=notional,
            fee_paid=0.0,
            message=json.dumps(trade, separators=(",", ":"), sort_keys=True),
            status="filled",
            notes=f"wallet_activity_import tx={str(trade.get('transactionHash') or '').strip() or '-'}",
            execution_ts=ts,
        )
        return "imported" if result.status == "filled" else "skipped"

    def _positions_mismatch_reason(self, *, positions: list[dict[str, Any]]) -> str:
        wallet_positions = {
            str(item.get("asset") or "").strip(): max(_safe_float(item.get("size")), 0.0)
            for item in positions
            if str(item.get("asset") or "").strip() and max(_safe_float(item.get("size")), 0.0) > 1e-6
        }
        ledger_positions = {
            str(row["asset"] or "").strip(): max(float(row["size"] or 0.0), 0.0)
            for row in self.db.list_copy_positions()
            if str(row["asset"] or "").strip() and max(float(row["size"] or 0.0), 0.0) > 1e-6
        }
        if set(wallet_positions) != set(ledger_positions):
            return "wallet snapshot mismatch: assets difieren respecto al ledger"
        for asset, wallet_size in wallet_positions.items():
            ledger_size = ledger_positions.get(asset, 0.0)
            if abs(wallet_size - ledger_size) > 1e-4:
                return f"wallet snapshot mismatch: size distinta para {asset}"
        return ""

    def _activity_sync_key(self, trade: dict[str, Any]) -> str:
        transaction_hash = str(trade.get("transactionHash") or "external").strip() or "external"
        asset = str(trade.get("asset") or "").strip()
        side = str(trade.get("side") or "").strip().lower()
        ts = int(_safe_float(trade.get("timestamp")))
        size = max(_safe_float(trade.get("size")), 0.0)
        price = max(_safe_float(trade.get("price")), 0.0)
        return f"live_imported_activity:{transaction_hash}:{asset}:{side}:{ts}:{size:.8f}:{price:.8f}"


def _safe_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
