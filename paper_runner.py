from __future__ import annotations

import argparse
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

from strategy import (
    OfficialApiClient,
    ResearchConfig,
    StrategyState,
    attach_latency_record,
    build_state_from_ws,
    compute_signal,
    discovery,
    mark_state_to_market,
    place_orders_paper,
    update_books_from_event,
)

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None


LOGGER = logging.getLogger("polymarket.paper_runner")


class PaperRunner:
    def __init__(self, config: ResearchConfig) -> None:
        self.config = config
        self.client = OfficialApiClient(config)
        self.discovery_state = discovery(config)
        self.state = StrategyState(
            token_id_yes=self.discovery_state.token_id_yes,
            token_id_no=self.discovery_state.token_id_no,
            cash_usdc=10_000.0,
            equity_usdc=10_000.0,
            peak_equity_usdc=10_000.0,
        )
        self.data_dir = Path(config.storage.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.events_path = self.data_dir / "paper_events.jsonl"
        self.decisions_path = self.data_dir / "paper_decisions.jsonl"
        self.fills_path = self.data_dir / "paper_fills.jsonl"
        self.snapshots_path = self.data_dir / "paper_snapshots.jsonl"
        self._ws_app = None
        self._stop = threading.Event()

    def warmup(self) -> None:
        if not self.config.warmup_rest_snapshots:
            return
        for token_id in (self.discovery_state.token_id_yes, self.discovery_state.token_id_no):
            book = self.client.get_book(token_id)
            normalized = {
                "ts_ms": int(time.time() * 1000),
                "event": "book",
                "token_id": token_id,
                "bids": [[float(level.get("price") or 0.0), float(level.get("size") or 0.0)] for level in book.get("bids", [])],
                "asks": [[float(level.get("price") or 0.0), float(level.get("size") or 0.0)] for level in book.get("asks", [])],
                "extra": {"source": "rest-warmup"},
            }
            update_books_from_event(self.state.books, normalized)
            self._append_jsonl(self.events_path, normalized)

    def run(self, max_seconds: int | None = None) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client no esta instalado; no puedo arrancar el market WS.")
        self.warmup()
        started = time.time()

        def on_open(ws_app) -> None:  # noqa: ANN001
            payload = {
                "type": "market",
                "assets_ids": [self.discovery_state.token_id_yes, self.discovery_state.token_id_no],
            }
            ws_app.send(json.dumps(payload))
            LOGGER.info("paper runner subscribed to %s / %s", self.discovery_state.token_id_yes, self.discovery_state.token_id_no)

        def on_message(_ws_app, message: str) -> None:  # noqa: ANN001
            feed_recv_ts = int(time.time() * 1000)
            try:
                normalized_events = build_state_from_ws(message)
            except Exception as error:  # noqa: BLE001
                LOGGER.warning("no pude normalizar mensaje WS: %s", error)
                return
            for event in normalized_events:
                self._process_event(event, feed_recv_ts)
            if max_seconds is not None and (time.time() - started) >= max_seconds:
                self.stop()

        def on_error(_ws_app, error: object) -> None:
            if not self._stop.is_set():
                LOGGER.warning("market WS error: %s", error)

        def on_close(_ws_app, _status, _msg) -> None:
            LOGGER.info("market WS cerrado")

        self._ws_app = websocket.WebSocketApp(
            self.config.market_ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        self._ws_app.run_forever()

    def stop(self) -> None:
        self._stop.set()
        if self._ws_app is not None:
            try:
                self._ws_app.close()
            except Exception:  # noqa: BLE001
                pass

    def _process_event(self, event: dict[str, Any], feed_recv_ts: int) -> None:
        normalize_ts = int(time.time() * 1000)
        update_books_from_event(self.state.books, event)
        self._append_jsonl(self.events_path, event)
        decision_ts = int(time.time() * 1000)
        decision = compute_signal(self.state, self.discovery_state, self.config, now_ts_ms=decision_ts)
        self._append_jsonl(
            self.decisions_path,
            {
                "feed_recv_ts": feed_recv_ts,
                "normalize_ts": normalize_ts,
                "decision_ts": decision_ts,
                "decision": {
                    "strategy_name": decision.strategy_name,
                    "should_trade": decision.should_trade,
                    "reason": decision.reason,
                    "signal_edge_frac": decision.signal_edge_frac,
                    "signal_edge_usdc": decision.signal_edge_usdc,
                    "metrics": decision.metrics,
                },
            },
        )
        if decision.should_trade and decision.orders:
            order_ts = int(time.time() * 1000)
            fills = place_orders_paper(self.state, decision.orders, self.discovery_state, self.config, now_ts_ms=order_ts)
            fill_ts = int(time.time() * 1000)
            persisted_ts = int(time.time() * 1000)
            attach_latency_record(
                self.state,
                feed_recv_ts=feed_recv_ts,
                normalize_ts=normalize_ts,
                decision_ts=decision_ts,
                order_sent_ts=order_ts,
                fill_ts=fill_ts,
                persisted_ts=persisted_ts,
            )
            for fill in fills:
                self._append_jsonl(self.fills_path, fill.to_log())
        equity = mark_state_to_market(self.state, self.state.books)
        self._append_jsonl(
            self.snapshots_path,
            {
                "ts_ms": int(time.time() * 1000),
                "equity_usdc": equity,
                "cash_usdc": self.state.cash_usdc,
                "realized_pnl_usdc": self.state.realized_pnl_usdc,
                "inventory_usdc": self.state.inventory_usdc(),
                "market_slug": self.discovery_state.slug,
            },
        )

    @staticmethod
    def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Paper runner para BTC Up/Down 5m con WS market y paper fills")
    parser.add_argument("--config", default="", help="Ruta a YAML")
    parser.add_argument("--max-seconds", type=int, default=0, help="Duracion maxima para smoke/manual tests")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    config = ResearchConfig.from_yaml(args.config or None)
    runner = PaperRunner(config)
    try:
        runner.run(max_seconds=args.max_seconds or None)
    except KeyboardInterrupt:
        LOGGER.info("paper runner detenido por usuario")
    finally:
        runner.stop()


if __name__ == "__main__":
    main()
