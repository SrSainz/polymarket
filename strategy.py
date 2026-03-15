from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Literal

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOGGER = logging.getLogger("polymarket.research")

GammaRateKey = Literal["gamma-markets", "gamma-events", "gamma-general"]
DataRateKey = Literal["data-trades", "data-positions", "data-general"]
CLOBRateKey = Literal["clob-general"]
RateKey = GammaRateKey | DataRateKey | CLOBRateKey
EventKind = Literal["book", "delta", "trade", "bbo"]
SubStrategy = Literal["underround_arb", "market_making"]
ExecutionMode = Literal["maker_only", "taker_only", "hybrid"]


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _utc_now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(slots=True)
class SizingConfig:
    max_usdc_per_trade: float = 50.0
    max_shares_per_trade: float = 200.0
    edge_to_size_curve: list[tuple[float, float]] = field(
        default_factory=lambda: [
            (0.0, 0.0),
            (0.005, 0.20),
            (0.010, 0.35),
            (0.015, 0.55),
            (0.020, 0.80),
            (0.030, 1.00),
        ]
    )


@dataclass(slots=True)
class RiskConfig:
    max_inventory_usdc: float = 250.0
    max_daily_loss_usdc: float = 200.0
    kill_switch_drawdown: float = 300.0


@dataclass(slots=True)
class FeeModelConfig:
    use_fee_rate_endpoint: bool = True
    fee_rate_cache_ttl_s: int = 300
    default_taker_fee_bps: float = 20.0
    maker_rebate_bps: float = 0.0


@dataclass(slots=True)
class SlippageModelConfig:
    taker_depth_levels: int = 5
    maker_fill_prob_params: tuple[float, float] = (1.4, 0.6)
    partial_fill_enabled: bool = True
    adverse_selection_bps: float = 2.0
    slippage_multiplier: float = 1.0
    maker_fill_probability_multiplier: float = 1.0
    taker_fill_ratio: float = 1.0


@dataclass(slots=True)
class ExecutionConfig:
    mode: ExecutionMode = "hybrid"
    cancel_replace_interval_ms: int = 750
    maker_join_bbo: bool = True


@dataclass(slots=True)
class RateLimitConfig:
    max_rps_gamma: float = 25.0
    max_rps_data: float = 15.0
    max_rps_clob: float = 20.0


@dataclass(slots=True)
class StorageConfig:
    data_dir: str = "data/research"
    event_log_format: Literal["jsonl", "parquet"] = "jsonl"


@dataclass(slots=True)
class ResearchConfig:
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    data_base_url: str = "https://data-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
    market_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    user_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    rtds_ws_url: str = "wss://ws-live-data.polymarket.com"
    discovery_query: str = "bitcoin up or down"
    discovery_limit: int = 200
    discovery_active_only: bool = True
    paper_mode: bool = True
    sub_strategy: SubStrategy = "underround_arb"
    token_id_yes: str = ""
    token_id_no: str = ""
    market_condition_id: str = ""
    latency_budget_ms: int = 250
    ws_stale_ms: int = 1_500
    fill_rng_seed: int = 7
    warmup_rest_snapshots: bool = True
    sizing: SizingConfig = field(default_factory=SizingConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    fee_model: FeeModelConfig = field(default_factory=FeeModelConfig)
    slippage_model: SlippageModelConfig = field(default_factory=SlippageModelConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    rate_limits: RateLimitConfig = field(default_factory=RateLimitConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)

    @classmethod
    def from_yaml(cls, path: str | Path | None = None) -> ResearchConfig:
        if path is None:
            return cls()
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        return _config_from_mapping(payload)


def _config_from_mapping(payload: dict[str, Any]) -> ResearchConfig:
    config = ResearchConfig()
    for key in (
        "gamma_base_url",
        "data_base_url",
        "clob_base_url",
        "market_ws_url",
        "user_ws_url",
        "rtds_ws_url",
        "discovery_query",
        "discovery_limit",
        "discovery_active_only",
        "paper_mode",
        "sub_strategy",
        "token_id_yes",
        "token_id_no",
        "market_condition_id",
        "latency_budget_ms",
        "ws_stale_ms",
        "fill_rng_seed",
        "warmup_rest_snapshots",
    ):
        if key in payload:
            setattr(config, key, payload[key])
    for section_name, section_type in (
        ("sizing", SizingConfig),
        ("risk", RiskConfig),
        ("fee_model", FeeModelConfig),
        ("slippage_model", SlippageModelConfig),
        ("execution", ExecutionConfig),
        ("rate_limits", RateLimitConfig),
        ("storage", StorageConfig),
    ):
        if section_name in payload and isinstance(payload[section_name], dict):
            setattr(config, section_name, section_type(**payload[section_name]))
    return config


@dataclass(slots=True)
class BookLevel:
    price: float
    size: float


@dataclass(slots=True)
class NormalizedEvent:
    ts_ms: int
    event: EventKind
    token_id: str
    bids: list[list[float]]
    asks: list[list[float]]
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts_ms": self.ts_ms,
            "event": self.event,
            "token_id": self.token_id,
            "bids": self.bids,
            "asks": self.asks,
            "extra": self.extra,
        }


@dataclass(slots=True)
class OrderBookState:
    token_id: str
    bids: list[BookLevel] = field(default_factory=list)
    asks: list[BookLevel] = field(default_factory=list)
    updated_ts_ms: int = 0
    last_trade_price: float | None = None
    best_bid: float | None = None
    best_ask: float | None = None

    def midpoint(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    def spread(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    def total_depth(self, side: Literal["bids", "asks"], levels: int) -> float:
        rows = self.bids if side == "bids" else self.asks
        return sum(level.size for level in rows[:levels])


@dataclass(slots=True)
class MarketDiscovery:
    market_id: str
    slug: str
    title: str
    token_id_yes: str
    token_id_no: str
    yes_outcome: str
    no_outcome: str
    fees_enabled: bool
    fee_rate_bps_yes: float | None = None
    fee_rate_bps_no: float | None = None
    condition_id: str = ""
    end_date_iso: str = ""
    market_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PaperOrder:
    ts_ms: int
    token_id: str
    side: Literal["BUY", "SELL"]
    price: float
    size: float
    order_type: Literal["maker", "taker"]
    strategy_name: str
    note: str = ""


@dataclass(slots=True)
class FillReport:
    ts_ms: int
    token_id: str
    side: Literal["BUY", "SELL"]
    requested_size: float
    filled_size: float
    avg_price: float
    notional: float
    fee_usdc: float
    slippage_usdc: float
    slippage_bps: float
    order_type: Literal["maker", "taker"]
    fill_probability: float
    status: Literal["filled", "partial", "missed"]
    note: str = ""

    def to_log(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SignalDecision:
    strategy_name: str
    should_trade: bool
    reason: str
    orders: list[PaperOrder] = field(default_factory=list)
    signal_edge_frac: float = 0.0
    signal_edge_usdc: float = 0.0
    latency_blocked: bool = False
    metrics: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class OpenMakerOrder:
    order: PaperOrder
    posted_ts_ms: int
    queue_ahead_size: float
    last_replace_ts_ms: int


@dataclass(slots=True)
class StrategyState:
    token_id_yes: str
    token_id_no: str
    books: dict[str, OrderBookState] = field(default_factory=dict)
    open_maker_orders: list[OpenMakerOrder] = field(default_factory=list)
    inventory_shares: dict[str, float] = field(default_factory=dict)
    inventory_cost: dict[str, float] = field(default_factory=dict)
    cash_usdc: float = 10_000.0
    realized_pnl_usdc: float = 0.0
    equity_usdc: float = 10_000.0
    peak_equity_usdc: float = 10_000.0
    trade_count: int = 0
    filled_orders: int = 0
    sent_orders: int = 0
    canceled_orders: int = 0
    fill_count: int = 0
    cumulative_slippage_usdc: float = 0.0
    cumulative_fees_usdc: float = 0.0
    latency_records: list[dict[str, int]] = field(default_factory=list)
    decision_log: list[dict[str, Any]] = field(default_factory=list)
    trade_flow: deque[tuple[int, float, float]] = field(default_factory=lambda: deque(maxlen=256))
    last_window_id: int | None = None
    last_mark_price: dict[str, float] = field(default_factory=dict)

    def inventory_usdc(self) -> float:
        total = 0.0
        for token_id, shares in self.inventory_shares.items():
            total += abs(shares) * self.last_mark_price.get(token_id, 0.0)
        return total

    def drawdown_usdc(self) -> float:
        return max(self.peak_equity_usdc - self.equity_usdc, 0.0)


class SlidingWindowRateLimiter:
    def __init__(self, max_requests: float, window_seconds: float = 1.0) -> None:
        self.max_requests = max(float(max_requests), 1.0)
        self.window_seconds = max(float(window_seconds), 0.1)
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                while self._timestamps and now - self._timestamps[0] > self.window_seconds:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_requests:
                    self._timestamps.append(now)
                    return
                sleep_for = self.window_seconds - (now - self._timestamps[0])
            if sleep_for > 0:
                time.sleep(min(sleep_for, 0.2))


class OfficialApiClient:
    def __init__(self, config: ResearchConfig) -> None:
        self.config = config
        self.session = requests.Session()
        retry = Retry(
            total=4,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.rate_limiters: dict[RateKey, SlidingWindowRateLimiter] = {
            "gamma-markets": SlidingWindowRateLimiter(min(config.rate_limits.max_rps_gamma, 30.0)),
            "gamma-events": SlidingWindowRateLimiter(min(config.rate_limits.max_rps_gamma, 50.0)),
            "gamma-general": SlidingWindowRateLimiter(config.rate_limits.max_rps_gamma),
            "data-trades": SlidingWindowRateLimiter(min(config.rate_limits.max_rps_data, 20.0)),
            "data-positions": SlidingWindowRateLimiter(min(config.rate_limits.max_rps_data, 15.0)),
            "data-general": SlidingWindowRateLimiter(config.rate_limits.max_rps_data),
            "clob-general": SlidingWindowRateLimiter(config.rate_limits.max_rps_clob),
        }
        self._fee_rate_cache: dict[str, tuple[float, float]] = {}

    def get_json(self, base_url: str, path: str, params: dict[str, Any] | None, rate_key: RateKey) -> Any:
        self.rate_limiters[rate_key].acquire()
        url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
        response = self.session.get(url, params=params, timeout=15)
        response.raise_for_status()
        return response.json()

    def discover_active_btc_5m_market(self) -> MarketDiscovery:
        for candidate_slug in self._candidate_btc_5m_slugs():
            try:
                market = self.get_market(candidate_slug)
            except requests.RequestException:
                continue
            if isinstance(market, dict) and _looks_like_btc_5m_market(market) and bool(market.get("active")) and not bool(market.get("closed")):
                return _market_to_discovery(market, self)

        payload = self.get_json(
            self.config.gamma_base_url,
            "/markets",
            {
                "active": str(self.config.discovery_active_only).lower(),
                "closed": "false",
                "limit": self.config.discovery_limit,
            },
            "gamma-markets",
        )
        markets = payload if isinstance(payload, list) else []
        filtered = [market for market in markets if _looks_like_btc_5m_market(market)]
        if not filtered:
            raise RuntimeError("No he encontrado ningun mercado activo de BTC 5m en Gamma.")
        filtered.sort(key=lambda item: _market_sort_key(item), reverse=True)
        return _market_to_discovery(filtered[0], self)

    @staticmethod
    def _candidate_btc_5m_slugs() -> list[str]:
        base_epoch = int(time.time() // 300 * 300)
        offsets = (0, -300, 300, -600, 600, -900, 900)
        return [f"btc-updown-5m-{base_epoch + offset}" for offset in offsets]

    def get_market(self, market_id_or_slug: str) -> dict[str, Any]:
        market_id_or_slug = str(market_id_or_slug).strip()
        if not market_id_or_slug:
            raise ValueError("market id/slug requerido")
        if market_id_or_slug.startswith("btc-") or "-updown-" in market_id_or_slug:
            payload = self.get_json(self.config.gamma_base_url, f"/markets/slug/{market_id_or_slug}", None, "gamma-markets")
        else:
            payload = self.get_json(self.config.gamma_base_url, f"/markets/{market_id_or_slug}", None, "gamma-markets")
        if isinstance(payload, list):
            return payload[0] if payload else {}
        return payload if isinstance(payload, dict) else {}

    def get_book(self, token_id: str) -> dict[str, Any]:
        return self.get_json(self.config.clob_base_url, "/book", {"token_id": token_id}, "clob-general")

    def get_midpoint(self, token_id: str) -> float | None:
        payload = self.get_json(self.config.clob_base_url, "/midpoint", {"token_id": token_id}, "clob-general")
        return _coerce_float(payload.get("mid"), default=0.0) or None

    def get_price(self, token_id: str, side: Literal["BUY", "SELL"]) -> float | None:
        payload = self.get_json(
            self.config.clob_base_url,
            "/price",
            {"token_id": token_id, "side": side},
            "clob-general",
        )
        raw = payload.get("price") if isinstance(payload, dict) else None
        return _coerce_float(raw, default=0.0) or None

    def get_prices_history(self, market: str, interval: str, fidelity: int) -> list[dict[str, Any]]:
        payload = self.get_json(
            self.config.clob_base_url,
            "/prices-history",
            {"market": market, "interval": interval, "fidelity": fidelity},
            "clob-general",
        )
        if isinstance(payload, dict):
            history = payload.get("history") or payload.get("items") or []
            return history if isinstance(history, list) else []
        return payload if isinstance(payload, list) else []

    def get_fee_rate_bps(self, token_id: str, *, fallback_bps: float | None = None) -> float:
        fallback = self.config.fee_model.default_taker_fee_bps if fallback_bps is None else fallback_bps
        if not self.config.fee_model.use_fee_rate_endpoint:
            return float(fallback)
        cached = self._fee_rate_cache.get(token_id)
        now = time.time()
        if cached and now - cached[1] <= self.config.fee_model.fee_rate_cache_ttl_s:
            return cached[0]
        try:
            payload = self.get_json(
                self.config.clob_base_url,
                "/fee-rate",
                {"token_id": token_id},
                "clob-general",
            )
        except requests.RequestException:
            return float(fallback)
        bps = (
            _coerce_float(payload.get("takerRateBps"), default=0.0)
            or _coerce_float(payload.get("feeRateBps"), default=0.0)
            or float(fallback)
        )
        self._fee_rate_cache[token_id] = (bps, now)
        return bps

    def get_public_trades(self, *, user: str | None = None, market: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if user:
            params["user"] = user
        if market:
            params["market"] = market
        payload = self.get_json(self.config.data_base_url, "/trades", params, "data-trades")
        return payload if isinstance(payload, list) else payload.get("items", []) if isinstance(payload, dict) else []

    def get_positions(self, user: str) -> list[dict[str, Any]]:
        payload = self.get_json(self.config.data_base_url, "/positions", {"user": user}, "data-positions")
        return payload if isinstance(payload, list) else payload.get("items", []) if isinstance(payload, dict) else []


def _looks_like_btc_5m_market(market: dict[str, Any]) -> bool:
    title = str(market.get("question") or market.get("title") or "").lower()
    slug = str(market.get("slug") or "").lower()
    return "bitcoin up or down" in title or ("btc-updown-5m" in slug)


def _market_sort_key(market: dict[str, Any]) -> float:
    for key in ("endDate", "end_date_iso", "endDateIso", "acceptingOrdersTimestamp"):
        raw = market.get(key)
        if raw:
            try:
                return float(str(raw).replace("Z", "").replace("T", "").replace(":", "").replace("-", "")[:14])
            except ValueError:
                continue
    return 0.0


def _market_to_discovery(market: dict[str, Any], client: OfficialApiClient) -> MarketDiscovery:
    token_map = _extract_token_map(market)
    yes = token_map.get("yes")
    no = token_map.get("no")
    if not yes or not no:
        raise RuntimeError("El mercado activo no incluye token_id para YES/NO.")
    fees_enabled = bool(market.get("feesEnabled", False))
    discovery = MarketDiscovery(
        market_id=str(market.get("id") or market.get("conditionId") or market.get("slug") or ""),
        slug=str(market.get("slug") or ""),
        title=str(market.get("question") or market.get("title") or ""),
        token_id_yes=yes["token_id"],
        token_id_no=no["token_id"],
        yes_outcome=yes["label"],
        no_outcome=no["label"],
        fees_enabled=fees_enabled,
        condition_id=str(market.get("conditionId") or ""),
        end_date_iso=str(market.get("endDate") or market.get("end_date_iso") or ""),
        market_payload=dict(market),
    )
    if fees_enabled:
        discovery.fee_rate_bps_yes = client.get_fee_rate_bps(discovery.token_id_yes)
        discovery.fee_rate_bps_no = client.get_fee_rate_bps(discovery.token_id_no)
    return discovery


def _extract_token_map(market: dict[str, Any]) -> dict[str, dict[str, str]]:
    tokens: list[dict[str, Any]] = []
    if isinstance(market.get("tokens"), list):
        tokens = [item for item in market["tokens"] if isinstance(item, dict)]
    if not tokens:
        clob_ids = market.get("clobTokenIds")
        outcomes = market.get("outcomes")
        try:
            ids = json.loads(clob_ids) if isinstance(clob_ids, str) else list(clob_ids or [])
            labels = json.loads(outcomes) if isinstance(outcomes, str) else list(outcomes or [])
        except (TypeError, json.JSONDecodeError):
            ids, labels = [], []
        for label, token_id in zip(labels, ids):
            tokens.append({"outcome": label, "token_id": token_id})
    result: dict[str, dict[str, str]] = {}
    for token in tokens:
        label = str(token.get("outcome") or token.get("name") or "").strip()
        token_id = str(token.get("token_id") or token.get("tokenId") or token.get("id") or "").strip()
        if not label or not token_id:
            continue
        lowered = label.lower()
        if lowered in {"yes", "up", "subira", "subirá"}:
            result["yes"] = {"label": label, "token_id": token_id}
        elif lowered in {"no", "down", "bajara", "bajará"}:
            result["no"] = {"label": label, "token_id": token_id}
    return result


def discovery(config: ResearchConfig | None = None) -> MarketDiscovery:
    research_config = config or ResearchConfig()
    client = OfficialApiClient(research_config)
    if research_config.token_id_yes and research_config.token_id_no:
        market = client.discover_active_btc_5m_market()
        market.token_id_yes = research_config.token_id_yes
        market.token_id_no = research_config.token_id_no
        if research_config.market_condition_id:
            market.condition_id = research_config.market_condition_id
        return market
    return client.discover_active_btc_5m_market()


def build_state_from_ws(raw_payload: str | dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(raw_payload, str):
        payload: Any = json.loads(raw_payload)
    else:
        payload = raw_payload
    items = payload if isinstance(payload, list) else [payload]
    normalized: list[dict[str, Any]] = []
    recv_ts = _utc_now_ms()
    for item in items:
        if not isinstance(item, dict):
            continue
        token_id = str(item.get("asset_id") or item.get("asset") or item.get("token_id") or item.get("market") or "").strip()
        if not token_id:
            continue
        event_type = str(item.get("event_type") or item.get("type") or "").strip().lower()
        ts_ms = _coerce_int(item.get("timestamp") or item.get("ts_ms") or recv_ts, default=recv_ts)
        if event_type in {"book", "snapshot"} or ("bids" in item or "asks" in item or "buys" in item or "sells" in item):
            normalized.append(
                NormalizedEvent(
                    ts_ms=ts_ms,
                    event="book",
                    token_id=token_id,
                    bids=_levels_to_pairs(item.get("bids") or item.get("buys") or []),
                    asks=_levels_to_pairs(item.get("asks") or item.get("sells") or []),
                    extra={"source": event_type or "book"},
                ).to_dict()
            )
            continue
        if event_type in {"price_change", "delta"}:
            changes = item.get("changes") or item.get("price_changes") or []
            bids: list[list[float]] = []
            asks: list[list[float]] = []
            for change in changes:
                if not isinstance(change, dict):
                    continue
                side = str(change.get("side") or "").upper()
                pair = [_coerce_float(change.get("price")), _coerce_float(change.get("size") or change.get("new_size") or change.get("remaining_size"))]
                if side == "BUY":
                    bids.append(pair)
                elif side == "SELL":
                    asks.append(pair)
            normalized.append(
                NormalizedEvent(
                    ts_ms=ts_ms,
                    event="delta",
                    token_id=token_id,
                    bids=bids,
                    asks=asks,
                    extra={"source": "price_change"},
                ).to_dict()
            )
            continue
        if event_type in {"last_trade_price", "trade"} or "last_trade_price" in item:
            normalized.append(
                NormalizedEvent(
                    ts_ms=ts_ms,
                    event="trade",
                    token_id=token_id,
                    bids=[],
                    asks=[],
                    extra={
                        "price": _coerce_float(item.get("price") or item.get("last_trade_price")),
                        "size": _coerce_float(item.get("size") or item.get("last_size")),
                    },
                ).to_dict()
            )
            continue
        if event_type in {"best_bid_ask", "bbo"}:
            normalized.append(
                NormalizedEvent(
                    ts_ms=ts_ms,
                    event="bbo",
                    token_id=token_id,
                    bids=[[_coerce_float(item.get("best_bid") or item.get("bid")), _coerce_float(item.get("best_bid_size") or item.get("bid_size") or 0)]],
                    asks=[[_coerce_float(item.get("best_ask") or item.get("ask")), _coerce_float(item.get("best_ask_size") or item.get("ask_size") or 0)]],
                    extra={"source": "bbo"},
                ).to_dict()
            )
    return normalized


def _levels_to_pairs(levels: Iterable[Any]) -> list[list[float]]:
    result: list[list[float]] = []
    for level in levels:
        if isinstance(level, dict):
            result.append([_coerce_float(level.get("price")), _coerce_float(level.get("size"))])
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            result.append([_coerce_float(level[0]), _coerce_float(level[1])])
    return result


def update_books_from_event(books: dict[str, OrderBookState], event: dict[str, Any]) -> None:
    token_id = str(event.get("token_id") or "").strip()
    if not token_id:
        return
    book = books.setdefault(token_id, OrderBookState(token_id=token_id))
    kind = str(event.get("event") or "")
    ts_ms = _coerce_int(event.get("ts_ms") or _utc_now_ms())
    if kind == "book":
        book.bids = _pairs_to_levels(event.get("bids") or [], reverse=True)
        book.asks = _pairs_to_levels(event.get("asks") or [], reverse=False)
    elif kind == "delta":
        _apply_side_deltas(book.bids, event.get("bids") or [], reverse=True)
        _apply_side_deltas(book.asks, event.get("asks") or [], reverse=False)
    elif kind == "bbo":
        if event.get("bids"):
            book.bids = _pairs_to_levels(event["bids"], reverse=True)
        if event.get("asks"):
            book.asks = _pairs_to_levels(event["asks"], reverse=False)
    elif kind == "trade":
        book.last_trade_price = _coerce_float((event.get("extra") or {}).get("price"), 0.0) or book.last_trade_price
    book.best_bid = book.bids[0].price if book.bids else None
    book.best_ask = book.asks[0].price if book.asks else None
    book.updated_ts_ms = ts_ms


def _pairs_to_levels(levels: Iterable[Iterable[float]], *, reverse: bool) -> list[BookLevel]:
    normalized = [BookLevel(price=_coerce_float(price), size=max(_coerce_float(size), 0.0)) for price, size in levels]
    normalized = [level for level in normalized if level.price > 0 and level.size >= 0]
    normalized.sort(key=lambda row: row.price, reverse=reverse)
    return normalized


def _apply_side_deltas(levels: list[BookLevel], changes: Iterable[Iterable[float]], *, reverse: bool) -> None:
    mapping = {round(level.price, 6): level.size for level in levels}
    for price, size in changes:
        price_key = round(_coerce_float(price), 6)
        new_size = _coerce_float(size)
        if new_size <= 0:
            mapping.pop(price_key, None)
        else:
            mapping[price_key] = new_size
    levels[:] = [BookLevel(price=price, size=size) for price, size in mapping.items()]
    levels.sort(key=lambda row: row.price, reverse=reverse)


def compute_signal(
    state: StrategyState,
    discovery_state: MarketDiscovery,
    config: ResearchConfig,
    *,
    now_ts_ms: int | None = None,
) -> SignalDecision:
    now_ms = now_ts_ms or _utc_now_ms()
    if _should_kill_switch(state, config):
        return SignalDecision(config.sub_strategy, False, "kill switch de riesgo", latency_blocked=False)
    if config.sub_strategy == "market_making":
        return _compute_market_making_signal(state, discovery_state, config, now_ms)
    return _compute_underround_signal(state, discovery_state, config, now_ms)


def _compute_underround_signal(state: StrategyState, discovery_state: MarketDiscovery, config: ResearchConfig, now_ms: int) -> SignalDecision:
    yes_book = state.books.get(discovery_state.token_id_yes)
    no_book = state.books.get(discovery_state.token_id_no)
    if yes_book is None or no_book is None or yes_book.best_ask is None or no_book.best_ask is None:
        return SignalDecision("underround_arb", False, "libro incompleto")
    book_age_ms = max(now_ms - yes_book.updated_ts_ms, now_ms - no_book.updated_ts_ms)
    if book_age_ms > config.latency_budget_ms:
        return SignalDecision("underround_arb", False, f"book_age_ms {book_age_ms} > {config.latency_budget_ms}", latency_blocked=True)
    pair_sum = yes_book.best_ask + no_book.best_ask
    gross_edge_frac = max(1.0 - pair_sum, 0.0)
    if gross_edge_frac <= 0:
        return SignalDecision("underround_arb", False, f"pair sum {pair_sum:.4f} >= 1")
    fee_yes = (discovery_state.fee_rate_bps_yes or config.fee_model.default_taker_fee_bps) / 10_000
    fee_no = (discovery_state.fee_rate_bps_no or config.fee_model.default_taker_fee_bps) / 10_000
    slip_yes = estimate_taker_slippage_bps(yes_book, config.sizing.max_usdc_per_trade / 2, "BUY", config.slippage_model)
    slip_no = estimate_taker_slippage_bps(no_book, config.sizing.max_usdc_per_trade / 2, "BUY", config.slippage_model)
    slippage_frac = (slip_yes + slip_no) / 20_000
    adverse_frac = config.slippage_model.adverse_selection_bps / 10_000
    fee_frac = fee_yes + fee_no
    net_edge_frac = gross_edge_frac - fee_frac - slippage_frac - adverse_frac
    if net_edge_frac <= 0.001:
        return SignalDecision(
            "underround_arb",
            False,
            f"edge neta insuficiente {net_edge_frac:.4f}",
            metrics={"pair_sum": pair_sum, "gross_edge_frac": gross_edge_frac, "net_edge_frac": net_edge_frac},
        )
    trade_budget = compute_size(
        net_edge_frac,
        available_liquidity_usdc=min(
            top_of_book_notional(yes_book, "asks", config.slippage_model.taker_depth_levels),
            top_of_book_notional(no_book, "asks", config.slippage_model.taker_depth_levels),
        )
        * 2,
        config=config,
        inventory_remaining=max(config.risk.max_inventory_usdc - state.inventory_usdc(), 0.0),
    )
    if trade_budget <= 0:
        return SignalDecision("underround_arb", False, "sin tamaño por riesgo o liquidez")
    yes_notional = trade_budget / 2
    no_notional = trade_budget / 2
    yes_size = min(config.sizing.max_shares_per_trade, yes_notional / yes_book.best_ask)
    no_size = min(config.sizing.max_shares_per_trade, no_notional / no_book.best_ask)
    order_type: Literal["maker", "taker"] = "taker" if config.execution.mode in {"taker_only", "hybrid"} else "maker"
    orders = [
        PaperOrder(now_ms, discovery_state.token_id_yes, "BUY", yes_book.best_ask, yes_size, order_type, "underround_arb", "buy-yes"),
        PaperOrder(now_ms, discovery_state.token_id_no, "BUY", no_book.best_ask, no_size, order_type, "underround_arb", "buy-no"),
    ]
    signal_edge_usdc = trade_budget * net_edge_frac
    return SignalDecision(
        "underround_arb",
        True,
        "underround detectado",
        orders=orders,
        signal_edge_frac=net_edge_frac,
        signal_edge_usdc=signal_edge_usdc,
        metrics={
            "pair_sum": pair_sum,
            "gross_edge_frac": gross_edge_frac,
            "net_edge_frac": net_edge_frac,
            "book_age_ms": float(book_age_ms),
            "fee_frac": fee_frac,
            "slippage_frac": slippage_frac,
        },
    )


def _compute_market_making_signal(state: StrategyState, discovery_state: MarketDiscovery, config: ResearchConfig, now_ms: int) -> SignalDecision:
    yes_book = state.books.get(discovery_state.token_id_yes)
    no_book = state.books.get(discovery_state.token_id_no)
    if yes_book is None or no_book is None or yes_book.best_bid is None or yes_book.best_ask is None or no_book.best_bid is None or no_book.best_ask is None:
        return SignalDecision("market_making", False, "libro incompleto")
    book_age_ms = max(now_ms - yes_book.updated_ts_ms, now_ms - no_book.updated_ts_ms)
    if book_age_ms > config.latency_budget_ms:
        return SignalDecision("market_making", False, f"book_age_ms {book_age_ms} > {config.latency_budget_ms}", latency_blocked=True)
    spread_yes = yes_book.spread() or 0.0
    spread_no = no_book.spread() or 0.0
    if min(spread_yes, spread_no) < 0.01:
        return SignalDecision("market_making", False, "spread insuficiente")
    if state.inventory_usdc() >= config.risk.max_inventory_usdc:
        return SignalDecision("market_making", False, "inventario al limite")
    fair_yes = _midpoint_probability(yes_book, no_book, discovery_state.token_id_yes)
    fair_no = 1.0 - fair_yes
    edge_yes = max((yes_book.best_ask - yes_book.best_bid) / 2 - config.slippage_model.adverse_selection_bps / 10_000, 0.0)
    edge_no = max((no_book.best_ask - no_book.best_bid) / 2 - config.slippage_model.adverse_selection_bps / 10_000, 0.0)
    signal_edge_frac = min(edge_yes, edge_no)
    trade_budget = compute_size(
        signal_edge_frac,
        available_liquidity_usdc=min(top_of_book_notional(yes_book, "bids", 2), top_of_book_notional(no_book, "bids", 2)) * 2,
        config=config,
        inventory_remaining=max(config.risk.max_inventory_usdc - state.inventory_usdc(), 0.0),
    )
    if trade_budget <= 0:
        return SignalDecision("market_making", False, "sin tamaño para market making")
    yes_quote = yes_book.best_bid if config.execution.maker_join_bbo else max(round(fair_yes - 0.01, 3), 0.001)
    no_quote = no_book.best_bid if config.execution.maker_join_bbo else max(round(fair_no - 0.01, 3), 0.001)
    share_size = min(config.sizing.max_shares_per_trade, max(trade_budget / 2 / max(yes_quote, 0.01), 1.0))
    orders = [
        PaperOrder(now_ms, discovery_state.token_id_yes, "BUY", yes_quote, share_size, "maker", "market_making", "join-yes-bid"),
        PaperOrder(now_ms, discovery_state.token_id_no, "BUY", no_quote, share_size, "maker", "market_making", "join-no-bid"),
    ]
    return SignalDecision(
        "market_making",
        True,
        "spread suficiente para market making",
        orders=orders,
        signal_edge_frac=signal_edge_frac,
        signal_edge_usdc=trade_budget * signal_edge_frac,
        metrics={"spread_yes": spread_yes, "spread_no": spread_no, "fair_yes": fair_yes, "fair_no": fair_no},
    )


def _midpoint_probability(yes_book: OrderBookState, no_book: OrderBookState, token_id: str) -> float:
    yes_mid = yes_book.midpoint() or 0.5
    no_mid = no_book.midpoint() or 0.5
    denom = yes_mid + no_mid
    if denom <= 0:
        return 0.5
    return yes_mid / denom if token_id == yes_book.token_id else no_mid / denom


def compute_size(
    edge_frac: float,
    available_liquidity_usdc: float,
    *,
    config: ResearchConfig,
    inventory_remaining: float,
) -> float:
    if edge_frac <= 0 or available_liquidity_usdc <= 0 or inventory_remaining <= 0:
        return 0.0
    curve = sorted(config.sizing.edge_to_size_curve, key=lambda item: item[0])
    fraction = 0.0
    for idx, (edge, weight) in enumerate(curve):
        if edge_frac <= edge:
            if idx == 0:
                fraction = weight
            else:
                prev_edge, prev_weight = curve[idx - 1]
                span = max(edge - prev_edge, 1e-9)
                fraction = prev_weight + ((edge_frac - prev_edge) / span) * (weight - prev_weight)
            break
    else:
        fraction = curve[-1][1]
    budget = config.sizing.max_usdc_per_trade * max(min(fraction, 1.0), 0.0)
    return min(budget, available_liquidity_usdc, inventory_remaining)


def estimate_taker_slippage_bps(
    book: OrderBookState,
    target_notional_usdc: float,
    side: Literal["BUY", "SELL"],
    model: SlippageModelConfig,
) -> float:
    rows = book.asks if side == "BUY" else book.bids
    if not rows or target_notional_usdc <= 0:
        return 0.0
    levels = rows[: max(model.taker_depth_levels, 1)]
    best = levels[0].price
    remaining = target_notional_usdc
    spent = 0.0
    filled = 0.0
    for level in levels:
        tradable_shares = min(level.size, remaining / max(level.price, 1e-9))
        level_notional = tradable_shares * level.price
        spent += level_notional
        filled += tradable_shares
        remaining -= level_notional
        if remaining <= 1e-9:
            break
    if filled <= 0 or spent <= 0:
        return 0.0
    vwap = spent / filled
    if best <= 0:
        return 0.0
    return max(((vwap / best) - 1.0) * 10_000, 0.0) * max(model.slippage_multiplier, 0.0)


def top_of_book_notional(book: OrderBookState, side: Literal["bids", "asks"], levels: int) -> float:
    rows = book.bids if side == "bids" else book.asks
    return sum(level.price * level.size for level in rows[: max(levels, 1)])


def fee_cost_usdc(notional_usdc: float, fee_rate_bps: float) -> float:
    return notional_usdc * fee_rate_bps / 10_000.0


def maker_fill_probability(
    order: PaperOrder,
    book: OrderBookState,
    recent_trade_intensity: float,
    model: SlippageModelConfig,
) -> float:
    alpha, beta = model.maker_fill_prob_params
    if order.side == "BUY":
        reference = book.best_bid or order.price
        ticks_away = max((reference - order.price) / 0.01, 0.0)
    else:
        reference = book.best_ask or order.price
        ticks_away = max((order.price - reference) / 0.01, 0.0)
    queue_penalty = min(order.size / max(top_of_book_notional(book, "bids" if order.side == "BUY" else "asks", 1), 1.0), 4.0)
    x = alpha + (recent_trade_intensity / 50.0) - beta * ticks_away - 0.4 * queue_penalty
    probability = max(min(1.0 / (1.0 + math.exp(-x)), 0.995), 0.01)
    probability *= max(model.maker_fill_probability_multiplier, 0.0)
    return max(min(probability, 0.995), 0.0)


def place_orders_paper(
    state: StrategyState,
    orders: list[PaperOrder],
    discovery_state: MarketDiscovery,
    config: ResearchConfig,
    *,
    now_ts_ms: int | None = None,
    rng: random.Random | None = None,
) -> list[FillReport]:
    rng = rng or random.Random(config.fill_rng_seed)
    current_ts = now_ts_ms or _utc_now_ms()
    fills: list[FillReport] = []
    fee_map = {
        discovery_state.token_id_yes: discovery_state.fee_rate_bps_yes or config.fee_model.default_taker_fee_bps,
        discovery_state.token_id_no: discovery_state.fee_rate_bps_no or config.fee_model.default_taker_fee_bps,
    }
    for order in orders:
        state.sent_orders += 1
        book = state.books.get(order.token_id)
        if book is None:
            fills.append(FillReport(current_ts, order.token_id, order.side, order.size, 0.0, order.price, 0.0, 0.0, 0.0, 0.0, order.order_type, 0.0, "missed", "no-book"))
            continue
        if order.order_type == "taker":
            fill = _simulate_taker_fill(order, book, fee_map.get(order.token_id, 0.0), config, current_ts)
        else:
            fill = _simulate_maker_fill(order, book, fee_map.get(order.token_id, 0.0), config, current_ts, state, rng)
        fills.append(fill)
        if fill.filled_size > 0:
            _apply_fill_to_inventory(state, fill)
            state.fill_count += 1
            state.filled_orders += 1
        elif fill.status == "missed":
            state.canceled_orders += 1
    return fills


def _simulate_taker_fill(order: PaperOrder, book: OrderBookState, fee_rate_bps: float, config: ResearchConfig, current_ts: int) -> FillReport:
    rows = book.asks if order.side == "BUY" else book.bids
    target_shares = order.size
    fill_ratio = max(min(config.slippage_model.taker_fill_ratio, 1.0), 0.0)
    if fill_ratio <= 0:
        return FillReport(current_ts, order.token_id, order.side, order.size, 0.0, order.price, 0.0, 0.0, 0.0, 0.0, "taker", 0.0, "missed", "stress-no-fill")
    target_shares *= fill_ratio
    remaining = target_shares
    filled = 0.0
    spent = 0.0
    best_price = rows[0].price if rows else order.price
    for level in rows[: max(config.slippage_model.taker_depth_levels, 1)]:
        level_fill = min(level.size, remaining)
        spent += level_fill * level.price
        filled += level_fill
        remaining -= level_fill
        if remaining <= 1e-9:
            break
    if filled <= 0:
        return FillReport(current_ts, order.token_id, order.side, order.size, 0.0, order.price, 0.0, 0.0, 0.0, 0.0, "taker", 1.0, "missed", "sin liquidez")
    avg_price = spent / filled
    notional = spent
    fee_usdc = fee_cost_usdc(notional, fee_rate_bps)
    raw_slippage_usdc = max((avg_price - best_price) * filled if order.side == "BUY" else (best_price - avg_price) * filled, 0.0)
    raw_slippage_bps = max(((avg_price / max(best_price, 1e-9)) - 1.0) * 10_000, 0.0)
    slippage_usdc = raw_slippage_usdc * max(config.slippage_model.slippage_multiplier, 0.0)
    slippage_bps = raw_slippage_bps * max(config.slippage_model.slippage_multiplier, 0.0)
    return FillReport(
        current_ts,
        order.token_id,
        order.side,
        order.size,
        filled,
        avg_price,
        notional,
        fee_usdc,
        slippage_usdc,
        slippage_bps,
        "taker",
        1.0,
        "partial" if filled < order.size else "filled",
        "sweep-book",
    )


def _simulate_maker_fill(
    order: PaperOrder,
    book: OrderBookState,
    fee_rate_bps: float,
    config: ResearchConfig,
    current_ts: int,
    state: StrategyState,
    rng: random.Random,
) -> FillReport:
    recent_trade_intensity = sum(size for _, _, size in state.trade_flow)
    probability = maker_fill_probability(order, book, recent_trade_intensity, config.slippage_model)
    draw = rng.random()
    if draw > probability:
        return FillReport(current_ts, order.token_id, order.side, order.size, 0.0, order.price, 0.0, 0.0, 0.0, 0.0, "maker", probability, "missed", "maker-no-fill")
    filled_size = order.size
    if config.slippage_model.partial_fill_enabled:
        fill_fraction = min(max(probability * 1.2, 0.15), 1.0)
        filled_size = order.size * fill_fraction
    notional = filled_size * order.price
    fee_usdc = max(fee_cost_usdc(notional, fee_rate_bps) - fee_cost_usdc(notional, config.fee_model.maker_rebate_bps), 0.0)
    return FillReport(
        current_ts,
        order.token_id,
        order.side,
        order.size,
        filled_size,
        order.price,
        notional,
        fee_usdc,
        0.0,
        0.0,
        "maker",
        probability,
        "partial" if filled_size < order.size else "filled",
        "maker-fill",
    )


def _apply_fill_to_inventory(state: StrategyState, fill: FillReport) -> None:
    token_id = fill.token_id
    current_shares = state.inventory_shares.get(token_id, 0.0)
    current_cost = state.inventory_cost.get(token_id, 0.0)
    signed_size = fill.filled_size if fill.side == "BUY" else -fill.filled_size
    signed_cost = fill.notional if fill.side == "BUY" else -fill.notional
    new_shares = current_shares + signed_size
    new_cost = current_cost + signed_cost
    cash_delta = -(fill.notional + fill.fee_usdc) if fill.side == "BUY" else (fill.notional - fill.fee_usdc)
    if current_shares > 0 and fill.side == "SELL":
        avg_cost = current_cost / max(current_shares, 1e-9)
        realized = (fill.avg_price - avg_cost) * fill.filled_size - fill.fee_usdc
        state.realized_pnl_usdc += realized
    elif fill.side == "BUY":
        state.realized_pnl_usdc -= fill.fee_usdc
    state.cash_usdc += cash_delta
    state.inventory_shares[token_id] = new_shares
    state.inventory_cost[token_id] = new_cost
    state.last_mark_price[token_id] = fill.avg_price
    state.cumulative_slippage_usdc += fill.slippage_usdc
    state.cumulative_fees_usdc += fill.fee_usdc
    state.trade_count += 1


def cancel_replace_logic(
    state: StrategyState,
    config: ResearchConfig,
    *,
    now_ts_ms: int | None = None,
) -> list[OpenMakerOrder]:
    current_ts = now_ts_ms or _utc_now_ms()
    survivors: list[OpenMakerOrder] = []
    for open_order in state.open_maker_orders:
        age_ms = current_ts - open_order.posted_ts_ms
        if age_ms >= config.execution.cancel_replace_interval_ms:
            state.canceled_orders += 1
            continue
        survivors.append(open_order)
    state.open_maker_orders = survivors
    return survivors


def place_orders_live(
    orders: list[PaperOrder],
    *,
    api_key_env: str = "POLY_API_KEY",
    api_secret_env: str = "POLY_API_SECRET",
    api_passphrase_env: str = "POLY_API_PASSPHRASE",
) -> None:
    if not (os.getenv(api_key_env) and os.getenv(api_secret_env) and os.getenv(api_passphrase_env)):
        raise RuntimeError("Live trading desactivado: faltan credenciales CLOB en variables de entorno.")
    raise NotImplementedError("place_orders_live se deja intencionalmente server-side y requiere wiring auth adicional.")


def _should_kill_switch(state: StrategyState, config: ResearchConfig) -> bool:
    if state.realized_pnl_usdc <= -abs(config.risk.max_daily_loss_usdc):
        return True
    if state.drawdown_usdc() >= abs(config.risk.kill_switch_drawdown):
        return True
    return False


def mark_state_to_market(state: StrategyState, books: dict[str, OrderBookState]) -> float:
    mark_inventory = 0.0
    for token_id, shares in state.inventory_shares.items():
        book = books.get(token_id)
        mark = book.midpoint() if book else None
        if mark is None and book is not None:
            mark = book.best_bid or book.best_ask
        mark = mark or state.last_mark_price.get(token_id, 0.0)
        state.last_mark_price[token_id] = mark
        mark_inventory += shares * mark
    state.equity_usdc = state.cash_usdc + mark_inventory
    state.peak_equity_usdc = max(state.peak_equity_usdc, state.equity_usdc)
    return state.equity_usdc


def attach_latency_record(
    state: StrategyState,
    *,
    feed_recv_ts: int,
    normalize_ts: int,
    decision_ts: int,
    order_sent_ts: int,
    fill_ts: int,
    persisted_ts: int,
) -> None:
    state.latency_records.append(
        {
            "feed_recv_ts": feed_recv_ts,
            "normalize_ts": normalize_ts,
            "decision_ts": decision_ts,
            "order_sent_ts": order_sent_ts,
            "fill_ts": fill_ts,
            "persisted_ts": persisted_ts,
        }
    )


def strategy_cli() -> None:
    parser = argparse.ArgumentParser(description="Polymarket BTC 5m research strategy utilities")
    parser.add_argument("--config", default="", help="Ruta a YAML de config")
    parser.add_argument("--discover", action="store_true", help="Descubre el mercado BTC 5m activo")
    args = parser.parse_args()
    config = ResearchConfig.from_yaml(args.config or None)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    if args.discover:
        market = discovery(config)
        print(json.dumps(asdict(market), indent=2))
        return
    parser.print_help()


if __name__ == "__main__":
    strategy_cli()
