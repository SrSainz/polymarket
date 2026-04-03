from __future__ import annotations

import json
import time
from typing import Any

from app.core.execution_engine import apply_fill_to_database
from app.db import Database
from app.models import CopyInstruction, SignalAction, TradeSide
from app.polymarket.activity_client import ActivityClient

_LEDGER_DUST_NOTIONAL_USDC = 0.05


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
        closed_imported = 0
        closed_duplicates = 0
        closed_skipped = 0
        closed_errors = 0

        raw_activity = self._fetch_activity(wallet=safe_wallet, page_limit=page_limit, max_pages=max_pages)
        closed_positions = self._fetch_closed_positions(
            wallet=safe_wallet,
            page_limit=page_limit,
            max_pages=max_pages,
        )
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

        closed_rows = sorted(
            closed_positions,
            key=lambda row: (
                int(_safe_float(row.get("timestamp"))),
                str(row.get("slug") or row.get("eventSlug") or ""),
                str(row.get("asset") or ""),
            ),
        )
        for closed_position in closed_rows:
            sync_key = self._closed_position_sync_key(closed_position)
            if self.db.get_bot_state(sync_key):
                closed_duplicates += 1
                continue
            try:
                result = self._import_closed_position(closed_position=closed_position, wallet=safe_wallet, mode=safe_mode)
            except Exception:  # noqa: BLE001
                closed_errors += 1
                continue
            if result == "imported":
                closed_imported += 1
                self.db.set_bot_state(sync_key, "1")
            elif result == "duplicate":
                closed_duplicates += 1
                self.db.set_bot_state(sync_key, "1")
            else:
                closed_skipped += 1

        positions = self._fetch_positions(wallet=safe_wallet, page_limit=page_limit, max_pages=max_pages)
        redeemable_imported, redeemable_skipped = self._import_redeemable_positions(
            positions=positions,
            wallet=safe_wallet,
            mode=safe_mode,
        )
        closed_imported += redeemable_imported
        closed_skipped += redeemable_skipped
        self._prune_ledger_dust_positions()
        mismatch_reason = self._positions_mismatch_reason(positions=positions)
        material_positions = self._material_ledger_positions()
        now_ts = int(time.time())
        if mismatch_reason:
            self.db.set_bot_state("position_ledger_mode", "external")
            self.db.set_bot_state("position_ledger_preflight", "blocked")
            self.db.set_bot_state("live_wallet_sync_status", "mismatch")
            self.db.set_bot_state("live_wallet_sync_reason", mismatch_reason)
        else:
            self.db.set_bot_state("position_ledger_preflight", "ready")
            self.db.set_bot_state("position_ledger_mode", safe_mode if material_positions else "")
            self.db.set_bot_state("live_wallet_sync_status", "ok")
            self.db.set_bot_state("live_wallet_sync_reason", "")
            self.db.delete_bot_state_by_prefix("live_observed_activity:")
        self.db.set_bot_state("live_wallet_sync_wallet", safe_wallet)
        self.db.set_bot_state("live_wallet_sync_at", str(now_ts))
        self.db.set_bot_state("live_wallet_sync_imported", str(imported))
        self.db.set_bot_state("live_wallet_sync_duplicates", str(duplicates))
        self.db.set_bot_state("live_wallet_sync_skipped", str(skipped))
        self.db.set_bot_state("live_wallet_sync_errors", str(errors))
        self.db.set_bot_state("live_wallet_sync_closed_imported", str(closed_imported))
        self.db.set_bot_state("live_wallet_sync_closed_duplicates", str(closed_duplicates))
        self.db.set_bot_state("live_wallet_sync_closed_skipped", str(closed_skipped))
        self.db.set_bot_state("live_wallet_sync_closed_errors", str(closed_errors))

        return {
            "wallet": safe_wallet,
            "mode": safe_mode,
            "activity_rows": len(trade_rows),
            "closed_rows": len(closed_rows),
            "positions_rows": len(positions),
            "imported": imported,
            "duplicates": duplicates,
            "skipped": skipped,
            "errors": errors,
            "closed_imported": closed_imported,
            "closed_duplicates": closed_duplicates,
            "closed_skipped": closed_skipped,
            "closed_errors": closed_errors,
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

    def _fetch_closed_positions(self, *, wallet: str, page_limit: int, max_pages: int) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        safe_limit = max(int(page_limit), 1)
        safe_pages = max(int(max_pages), 1)
        for page_index in range(safe_pages):
            offset = page_index * safe_limit
            page = self.activity_client.get_closed_positions(wallet=wallet, limit=safe_limit, offset=offset)
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

    def _import_closed_position(self, *, closed_position: dict[str, Any], wallet: str, mode: str) -> str:
        asset = str(closed_position.get("asset") or "").strip()
        existing = self.db.get_copy_position(asset)
        if existing is None:
            return "skipped"

        size = max(float(existing["size"] or 0.0), 0.0)
        if size <= 1e-9:
            return "skipped"

        condition_id = str(
            closed_position.get("conditionId")
            or closed_position.get("condition_id")
            or existing["condition_id"]
            or ""
        ).strip()
        ts = int(_safe_float(closed_position.get("timestamp")))
        settlement_price = max(_safe_float(closed_position.get("curPrice")), 0.0)
        if not asset or not condition_id or ts <= 0:
            return "skipped"

        instruction = CopyInstruction(
            action=SignalAction.CLOSE,
            side=TradeSide.SELL,
            asset=asset,
            condition_id=condition_id,
            size=size,
            price=settlement_price,
            notional=size * settlement_price,
            source_wallet=f"wallet-closed:{wallet}",
            source_signal_id=0,
            title=str(closed_position.get("title") or existing["title"] or closed_position.get("slug") or ""),
            slug=str(closed_position.get("slug") or closed_position.get("eventSlug") or existing["slug"] or ""),
            outcome=str(closed_position.get("outcome") or existing["outcome"] or ""),
            category=str(closed_position.get("category") or existing["category"] or "crypto"),
            reason=(
                "wallet_closed_position_import:"
                f"{str(closed_position.get('slug') or closed_position.get('eventSlug') or asset).strip()}"
            ),
        )
        result = apply_fill_to_database(
            db=self.db,
            instruction=instruction,
            mode=mode,
            filled_size=size,
            fill_price=settlement_price,
            fill_notional=size * settlement_price,
            fee_paid=0.0,
            message=json.dumps(closed_position, separators=(",", ":"), sort_keys=True),
            status="filled",
            notes=(
                "wallet_closed_position_import "
                f"pnl={_safe_float(closed_position.get('realizedPnl')):.6f}"
            ),
            execution_ts=ts,
        )
        return "imported" if result.status == "filled" else "skipped"

    def _positions_mismatch_reason(self, *, positions: list[dict[str, Any]]) -> str:
        wallet_positions = {
            str(item.get("asset") or "").strip(): max(_safe_float(item.get("size")), 0.0)
            for item in positions
            if self._is_material_wallet_position(item)
        }
        ledger_positions = {
            str(row["asset"] or "").strip(): max(float(row["size"] or 0.0), 0.0)
            for row in self._material_ledger_positions()
        }
        if set(wallet_positions) != set(ledger_positions):
            return "wallet snapshot mismatch: assets difieren respecto al ledger"
        for asset, wallet_size in wallet_positions.items():
            ledger_size = ledger_positions.get(asset, 0.0)
            if abs(wallet_size - ledger_size) > 1e-4:
                return f"wallet snapshot mismatch: size distinta para {asset}"
        return ""

    def _import_redeemable_positions(
        self,
        *,
        positions: list[dict[str, Any]],
        wallet: str,
        mode: str,
    ) -> tuple[int, int]:
        imported = 0
        skipped = 0
        for item in positions:
            if not bool(item.get("redeemable")):
                continue
            if self._import_redeemable_position(position=item, wallet=wallet, mode=mode) == "imported":
                imported += 1
            else:
                skipped += 1
        return imported, skipped

    def _import_redeemable_position(self, *, position: dict[str, Any], wallet: str, mode: str) -> str:
        asset = str(position.get("asset") or "").strip()
        existing = self.db.get_copy_position(asset)
        if existing is None:
            return "skipped"

        size = max(float(existing["size"] or 0.0), 0.0)
        if size <= 1e-9:
            return "skipped"

        current_value = max(_safe_float(position.get("currentValue")), 0.0)
        cur_price = max(_safe_float(position.get("curPrice")), 0.0)
        settlement_price = cur_price if cur_price > 0 else (current_value / size if current_value > 0 and size > 0 else 0.0)
        ts = int(_safe_float(position.get("timestamp"))) or int(time.time())
        condition_id = str(position.get("conditionId") or existing["condition_id"] or "").strip()
        if not asset or not condition_id or ts <= 0:
            return "skipped"

        instruction = CopyInstruction(
            action=SignalAction.CLOSE,
            side=TradeSide.SELL,
            asset=asset,
            condition_id=condition_id,
            size=size,
            price=settlement_price,
            notional=size * settlement_price,
            source_wallet=f"wallet-redeemable:{wallet}",
            source_signal_id=0,
            title=str(position.get("title") or existing["title"] or position.get("slug") or ""),
            slug=str(position.get("slug") or position.get("eventSlug") or existing["slug"] or ""),
            outcome=str(position.get("outcome") or existing["outcome"] or ""),
            category=str(position.get("category") or existing["category"] or "crypto"),
            reason=(
                "wallet_redeemable_position_import:"
                f"{str(position.get('slug') or position.get('eventSlug') or asset).strip()}"
            ),
        )
        result = apply_fill_to_database(
            db=self.db,
            instruction=instruction,
            mode=mode,
            filled_size=size,
            fill_price=settlement_price,
            fill_notional=size * settlement_price,
            fee_paid=0.0,
            message=json.dumps(position, separators=(",", ":"), sort_keys=True),
            status="filled",
            notes=(
                "wallet_redeemable_position_import "
                f"cash_pnl={_safe_float(position.get('cashPnl')):.6f}"
            ),
            execution_ts=ts,
        )
        return "imported" if result.status == "filled" else "skipped"

    def _material_ledger_positions(self) -> list[Any]:
        return [row for row in self.db.list_copy_positions() if self._is_material_ledger_position(row)]

    def _prune_ledger_dust_positions(self) -> None:
        for row in self.db.list_copy_positions():
            if self._is_material_ledger_position(row):
                continue
            asset = str(row["asset"] or "").strip()
            if asset:
                self.db.delete_copy_position(asset)

    def _is_material_wallet_position(self, item: dict[str, Any]) -> bool:
        asset = str(item.get("asset") or "").strip()
        size = max(_safe_float(item.get("size")), 0.0)
        if not asset or size <= 1e-6:
            return False
        if bool(item.get("redeemable")):
            return False
        notional = self._wallet_position_notional(item)
        return notional > _LEDGER_DUST_NOTIONAL_USDC

    def _wallet_position_notional(self, item: dict[str, Any]) -> float:
        current_value = max(_safe_float(item.get("currentValue")), 0.0)
        if current_value > 0:
            return current_value
        initial_value = max(_safe_float(item.get("initialValue")), 0.0)
        if initial_value > 0:
            return initial_value
        size = max(_safe_float(item.get("size")), 0.0)
        cur_price = max(_safe_float(item.get("curPrice")), 0.0)
        if size > 0 and cur_price > 0:
            return size * cur_price
        return size

    def _is_material_ledger_position(self, row: Any) -> bool:
        asset = str(row["asset"] or "").strip()
        size = max(float(row["size"] or 0.0), 0.0)
        avg_price = max(float(row["avg_price"] or 0.0), 0.0)
        if not asset or size <= 1e-6:
            return False
        return (size * avg_price) > _LEDGER_DUST_NOTIONAL_USDC

    def _activity_sync_key(self, trade: dict[str, Any]) -> str:
        transaction_hash = str(trade.get("transactionHash") or "external").strip() or "external"
        asset = str(trade.get("asset") or "").strip()
        side = str(trade.get("side") or "").strip().lower()
        ts = int(_safe_float(trade.get("timestamp")))
        size = max(_safe_float(trade.get("size")), 0.0)
        price = max(_safe_float(trade.get("price")), 0.0)
        return f"live_imported_activity:{transaction_hash}:{asset}:{side}:{ts}:{size:.8f}:{price:.8f}"

    def _closed_position_sync_key(self, closed_position: dict[str, Any]) -> str:
        asset = str(closed_position.get("asset") or "").strip()
        slug = str(closed_position.get("slug") or closed_position.get("eventSlug") or "").strip()
        ts = int(_safe_float(closed_position.get("timestamp")))
        realized_pnl = _safe_float(closed_position.get("realizedPnl"))
        settlement_price = max(_safe_float(closed_position.get("curPrice")), 0.0)
        return f"live_imported_closed_position:{slug}:{asset}:{ts}:{realized_pnl:.8f}:{settlement_price:.8f}"


def _safe_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
