from __future__ import annotations

import json
import math
import statistics
import time
from dataclasses import asdict, dataclass
from typing import Any

from app.core.state_store import BookState, LiquidationPrint, StateStore
from app.polymarket.spot_feed import SpotSnapshot


@dataclass(frozen=True, slots=True)
class FeatureFrame:
    window_id: str
    market_slug: str
    market_title: str
    cadence: str
    generated_at_ns: int
    up_asset: str
    down_asset: str
    up_label: str
    down_label: str
    best_bid_up: float
    best_ask_up: float
    best_bid_down: float
    best_ask_down: float
    spread_ticks_up: float
    spread_ticks_down: float
    spread_bps_up: float
    spread_bps_down: float
    top_imbalance_up: float
    top_imbalance_down: float
    level_imbalance_3_up: float
    level_imbalance_3_down: float
    microprice_up: float
    microprice_down: float
    pair_sum_bps: float
    locked_edge_bps: float
    paired_ofi_z: float
    internal_bullish_pressure_5s: float
    internal_bearish_pressure_5s: float
    internal_bullish_pressure_30s: float
    internal_bearish_pressure_30s: float
    external_spot_pressure_5s: float
    external_spot_pressure_30s: float
    cvd_5s: float
    cvd_30s: float
    paired_cvd: float
    trade_aggressor_imbalance: float
    sweep_cost_25_bps: float
    sweep_cost_50_bps: float
    sweep_cost_100_bps: float
    book_slope_up: float
    book_slope_down: float
    book_convexity_up: float
    book_convexity_down: float
    spot_anchor_delta_bps: float
    basis_drift_bps: float
    spot_vol_5s: float
    liq_buy_notional_30s: float
    liq_sell_notional_30s: float
    liq_burst_zscore: float
    near_liq_cluster_distance_bps: float
    seconds_into_window: int
    seconds_left: int
    window_third: str
    inventory_skew: float
    market_event_lag_ms: float
    spot_age_ms: int
    readiness_score: float = 0.0
    regime: str = ""
    taker_fee_bps_estimate: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class FeatureEngine:
    def build_for_market(
        self,
        *,
        market: dict[str, Any],
        state_store: StateStore,
        official_price_to_beat: float,
        spot_snapshot: SpotSnapshot | None,
        seconds_into_window: int,
        current_up_exposure: float = 0.0,
        current_down_exposure: float = 0.0,
        cadence: str = "tick",
        taker_fee_bps_estimate: float = 0.0,
    ) -> FeatureFrame | None:
        outcomes = _parse_json_list(market.get("outcomes"))
        token_ids = _parse_json_list(market.get("clobTokenIds"))
        if len(outcomes) != 2 or len(token_ids) != 2:
            return None

        outcome_map = {str(label): str(token_id) for label, token_id in zip(outcomes, token_ids)}
        up_label, down_label = _resolve_binary_labels(outcomes)
        up_asset = outcome_map.get(up_label, str(token_ids[0]))
        down_asset = outcome_map.get(down_label, str(token_ids[1]))
        up_book = state_store.get_book(up_asset)
        down_book = state_store.get_book(down_asset)
        if up_book is None or down_book is None:
            return None

        now_ns = time.time_ns()
        best_bid_up, best_ask_up = _best_bid(up_book), _best_ask(up_book)
        best_bid_down, best_ask_down = _best_bid(down_book), _best_ask(down_book)
        if best_bid_up <= 0 or best_ask_up <= 0 or best_bid_down <= 0 or best_ask_down <= 0:
            return None

        spread_bps_up = _spread_bps(best_bid_up, best_ask_up)
        spread_bps_down = _spread_bps(best_bid_down, best_ask_down)
        spread_ticks_up = _spread_ticks(best_bid_up, best_ask_up, up_book.tick_size)
        spread_ticks_down = _spread_ticks(best_bid_down, best_ask_down, down_book.tick_size)
        top_imbalance_up = _top_imbalance(up_book)
        top_imbalance_down = _top_imbalance(down_book)
        level_imbalance_3_up = _level_imbalance(up_book, depth=3)
        level_imbalance_3_down = _level_imbalance(down_book, depth=3)
        paired_ofi_z = float((top_imbalance_up + level_imbalance_3_up) - (top_imbalance_down + level_imbalance_3_down))
        microprice_up = _microprice(up_book, fallback=best_ask_up)
        microprice_down = _microprice(down_book, fallback=best_ask_down)
        pair_sum_bps = (best_ask_up + best_ask_down - 1.0) * 10_000
        locked_edge_bps = max((1.0 - (best_ask_up + best_ask_down)) * 10_000, 0.0)

        internal_5s = _internal_pressure(
            state_store=state_store,
            up_asset=up_asset,
            up_label=up_label,
            down_asset=down_asset,
            down_label=down_label,
            lookback_seconds=5.0,
        )
        internal_30s = _internal_pressure(
            state_store=state_store,
            up_asset=up_asset,
            up_label=up_label,
            down_asset=down_asset,
            down_label=down_label,
            lookback_seconds=30.0,
        )
        external_5s = _external_pressure(state_store=state_store, lookback_seconds=5.0)
        external_30s = _external_pressure(state_store=state_store, lookback_seconds=30.0)
        cvd_5s = internal_5s["bullish"] - internal_5s["bearish"]
        cvd_30s = internal_30s["bullish"] - internal_30s["bearish"]
        paired_cvd = cvd_30s
        total_internal_30s = internal_30s["bullish"] + internal_30s["bearish"]
        trade_aggressor_imbalance = (
            (internal_30s["bullish"] - internal_30s["bearish"]) / total_internal_30s if total_internal_30s > 0 else 0.0
        )

        sweep_cost_25 = _pair_sweep_cost_bps(up_book, down_book, notional_per_leg=25.0)
        sweep_cost_50 = _pair_sweep_cost_bps(up_book, down_book, notional_per_leg=50.0)
        sweep_cost_100 = _pair_sweep_cost_bps(up_book, down_book, notional_per_leg=100.0)
        book_slope_up, book_convexity_up = _book_shape(up_book)
        book_slope_down, book_convexity_down = _book_shape(down_book)

        lead_price = float(spot_snapshot.lead_price or 0.0) if spot_snapshot is not None else 0.0
        anchor = float(official_price_to_beat or 0.0)
        spot_anchor_delta_bps = ((lead_price - anchor) / anchor * 10_000) if lead_price > 0 and anchor > 0 else 0.0
        basis_drift_bps = ((float(spot_snapshot.basis) / anchor) * 10_000) if spot_snapshot is not None and anchor > 0 else 0.0
        spot_vol_5s = _spot_volatility(state_store=state_store, lookback_seconds=5.0)

        liquidations_30s = state_store.recent_liquidations(lookback_seconds=30.0, symbol="BTCUSDT")
        liq_buy_notional = sum(row.notional for row in liquidations_30s if row.side == "buy")
        liq_sell_notional = sum(row.notional for row in liquidations_30s if row.side == "sell")
        liq_burst_zscore = _liquidation_burst_zscore(state_store=state_store, symbol="BTCUSDT")
        near_liq_cluster_distance_bps = _nearest_liquidation_cluster_bps(
            current_price=lead_price,
            liquidations=state_store.recent_liquidations(lookback_seconds=900.0, symbol="BTCUSDT"),
        )

        seconds_into_window = max(int(seconds_into_window), 0)
        seconds_left = max(300 - seconds_into_window, 0)
        inventory_skew = _inventory_skew(current_up_exposure=current_up_exposure, current_down_exposure=current_down_exposure)

        return FeatureFrame(
            window_id=str(market.get("slug") or ""),
            market_slug=str(market.get("slug") or ""),
            market_title=str(market.get("question") or market.get("slug") or ""),
            cadence=cadence,
            generated_at_ns=now_ns,
            up_asset=up_asset,
            down_asset=down_asset,
            up_label=up_label,
            down_label=down_label,
            best_bid_up=best_bid_up,
            best_ask_up=best_ask_up,
            best_bid_down=best_bid_down,
            best_ask_down=best_ask_down,
            spread_ticks_up=spread_ticks_up,
            spread_ticks_down=spread_ticks_down,
            spread_bps_up=spread_bps_up,
            spread_bps_down=spread_bps_down,
            top_imbalance_up=top_imbalance_up,
            top_imbalance_down=top_imbalance_down,
            level_imbalance_3_up=level_imbalance_3_up,
            level_imbalance_3_down=level_imbalance_3_down,
            microprice_up=microprice_up,
            microprice_down=microprice_down,
            pair_sum_bps=pair_sum_bps,
            locked_edge_bps=locked_edge_bps,
            paired_ofi_z=paired_ofi_z,
            internal_bullish_pressure_5s=internal_5s["bullish"],
            internal_bearish_pressure_5s=internal_5s["bearish"],
            internal_bullish_pressure_30s=internal_30s["bullish"],
            internal_bearish_pressure_30s=internal_30s["bearish"],
            external_spot_pressure_5s=external_5s["pressure"],
            external_spot_pressure_30s=external_30s["pressure"],
            cvd_5s=cvd_5s,
            cvd_30s=cvd_30s,
            paired_cvd=paired_cvd,
            trade_aggressor_imbalance=trade_aggressor_imbalance,
            sweep_cost_25_bps=sweep_cost_25,
            sweep_cost_50_bps=sweep_cost_50,
            sweep_cost_100_bps=sweep_cost_100,
            book_slope_up=book_slope_up,
            book_slope_down=book_slope_down,
            book_convexity_up=book_convexity_up,
            book_convexity_down=book_convexity_down,
            spot_anchor_delta_bps=spot_anchor_delta_bps,
            basis_drift_bps=basis_drift_bps,
            spot_vol_5s=spot_vol_5s,
            liq_buy_notional_30s=liq_buy_notional,
            liq_sell_notional_30s=liq_sell_notional,
            liq_burst_zscore=liq_burst_zscore,
            near_liq_cluster_distance_bps=near_liq_cluster_distance_bps,
            seconds_into_window=seconds_into_window,
            seconds_left=seconds_left,
            window_third=_window_third(seconds_into_window),
            inventory_skew=inventory_skew,
            market_event_lag_ms=state_store.latest_event_lag_ms(),
            spot_age_ms=int(spot_snapshot.age_ms if spot_snapshot is not None else 0),
            taker_fee_bps_estimate=max(float(taker_fee_bps_estimate), 0.0),
        )


def _parse_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]


def _resolve_binary_labels(outcomes: list[str]) -> tuple[str, str]:
    normalized = {str(item).strip().lower(): str(item) for item in outcomes}
    up_label = normalized.get("up") or outcomes[0]
    down_label = normalized.get("down") or next((item for item in outcomes if item != up_label), outcomes[-1])
    return str(up_label), str(down_label)


def _best_bid(book: BookState) -> float:
    return float(book.bids[0].price) if book.bids else 0.0


def _best_ask(book: BookState) -> float:
    return float(book.asks[0].price) if book.asks else 0.0


def _spread_bps(best_bid: float, best_ask: float) -> float:
    if best_bid <= 0 or best_ask <= 0:
        return 0.0
    midpoint = (best_bid + best_ask) / 2
    if midpoint <= 0:
        return 0.0
    return ((best_ask - best_bid) / midpoint) * 10_000


def _spread_ticks(best_bid: float, best_ask: float, tick_size: float) -> float:
    if tick_size <= 0:
        return 0.0
    return max((best_ask - best_bid) / tick_size, 0.0)


def _top_imbalance(book: BookState) -> float:
    if not book.bids or not book.asks:
        return 0.0
    bid_size = float(book.bids[0].size)
    ask_size = float(book.asks[0].size)
    total = bid_size + ask_size
    if total <= 0:
        return 0.0
    return (bid_size - ask_size) / total


def _level_imbalance(book: BookState, *, depth: int) -> float:
    bids = sum(level.size for level in book.bids[:depth])
    asks = sum(level.size for level in book.asks[:depth])
    total = bids + asks
    if total <= 0:
        return 0.0
    return (bids - asks) / total


def _microprice(book: BookState, *, fallback: float) -> float:
    if not book.bids or not book.asks:
        return fallback
    best_bid = float(book.bids[0].price)
    best_ask = float(book.asks[0].price)
    bid_size = float(book.bids[0].size)
    ask_size = float(book.asks[0].size)
    total = bid_size + ask_size
    if total <= 0:
        return fallback
    return ((best_ask * bid_size) + (best_bid * ask_size)) / total


def _internal_pressure(
    *,
    state_store: StateStore,
    up_asset: str,
    up_label: str,
    down_asset: str,
    down_label: str,
    lookback_seconds: float,
) -> dict[str, float]:
    bullish = 0.0
    bearish = 0.0
    for trade in state_store.recent_trades(up_asset, lookback_seconds=lookback_seconds):
        bullish += _directional_notional(label=up_label, side=trade.side, notional=trade.notional)
        bearish += _directional_notional(label=_opposite_label(up_label, down_label), side=trade.side, notional=trade.notional)
    for trade in state_store.recent_trades(down_asset, lookback_seconds=lookback_seconds):
        bullish += _directional_notional(label=down_label, side=trade.side, notional=trade.notional)
        bearish += _directional_notional(label=_opposite_label(down_label, up_label), side=trade.side, notional=trade.notional)
    return {"bullish": bullish, "bearish": bearish}


def _directional_notional(*, label: str, side: str, notional: float) -> float:
    side_lower = str(side or "").strip().lower()
    label_lower = str(label or "").strip().lower()
    if label_lower == "up":
        if side_lower == "buy":
            return notional
        if side_lower == "sell":
            return 0.0
    if label_lower == "down":
        if side_lower == "sell":
            return notional
        if side_lower == "buy":
            return 0.0
    return 0.0


def _opposite_label(primary: str, fallback: str) -> str:
    primary_lower = str(primary or "").strip().lower()
    if primary_lower == "up":
        return "down"
    if primary_lower == "down":
        return "up"
    return fallback


def _external_pressure(*, state_store: StateStore, lookback_seconds: float) -> dict[str, float]:
    points = state_store.recent_spot_points("btcusdt", lookback_seconds=lookback_seconds)
    if len(points) < 2:
        return {"pressure": 0.0}
    signed_notional = 0.0
    for point in points:
        if point.side == "buy":
            signed_notional += point.notional
        elif point.side == "sell":
            signed_notional -= point.notional
    if signed_notional != 0:
        return {"pressure": signed_notional}
    first = points[0].price
    last = points[-1].price
    return {"pressure": last - first}


def _pair_sweep_cost_bps(up_book: BookState, down_book: BookState, *, notional_per_leg: float) -> float:
    up_cost = _sweep_cost_bps(up_book, notional=notional_per_leg)
    down_cost = _sweep_cost_bps(down_book, notional=notional_per_leg)
    return max(up_cost, down_cost)


def _sweep_cost_bps(book: BookState, *, notional: float) -> float:
    if not book.asks:
        return 0.0
    top_price = float(book.asks[0].price)
    if top_price <= 0 or notional <= 0:
        return 0.0
    shares_remaining = notional / top_price
    total_cost = 0.0
    filled = 0.0
    for level in book.asks:
        if shares_remaining <= 1e-9:
            break
        take = min(float(level.size), shares_remaining)
        total_cost += take * float(level.price)
        filled += take
        shares_remaining -= take
    if filled <= 0:
        return 0.0
    avg_price = total_cost / filled
    return max(((avg_price - top_price) / top_price) * 10_000, 0.0)


def _book_shape(book: BookState) -> tuple[float, float]:
    if len(book.asks) < 2 or len(book.bids) < 2:
        return 0.0, 0.0
    ask_span = float(book.asks[min(2, len(book.asks) - 1)].price - book.asks[0].price)
    bid_span = float(book.bids[0].price - book.bids[min(2, len(book.bids) - 1)].price)
    ask_depth = sum(level.size for level in book.asks[:3])
    bid_depth = sum(level.size for level in book.bids[:3])
    slope = ((bid_depth - ask_depth) / max(bid_depth + ask_depth, 1e-9)) * 100
    convexity = (ask_span - bid_span) * 100
    return slope, convexity


def _spot_volatility(*, state_store: StateStore, lookback_seconds: float) -> float:
    points = state_store.recent_spot_points("btcusdt", lookback_seconds=lookback_seconds)
    if len(points) < 3:
        return 0.0
    returns: list[float] = []
    for previous, current in zip(points[:-1], points[1:]):
        if previous.price <= 0 or current.price <= 0:
            continue
        returns.append(math.log(current.price / previous.price))
    if len(returns) < 2:
        return 0.0
    return float(statistics.pstdev(returns) * 10_000)


def _liquidation_burst_zscore(*, state_store: StateStore, symbol: str) -> float:
    windows = (30.0, 60.0, 120.0, 300.0)
    notionals = [
        sum(row.notional for row in state_store.recent_liquidations(lookback_seconds=window, symbol=symbol))
        for window in windows
    ]
    current = notionals[0]
    trailing = notionals[1:]
    if len(trailing) < 2 or max(trailing) <= 0:
        return 0.0
    mean_value = statistics.fmean(trailing)
    std_value = statistics.pstdev(trailing)
    if std_value <= 1e-9:
        return 0.0
    return (current - mean_value) / std_value


def _nearest_liquidation_cluster_bps(*, current_price: float, liquidations: list[LiquidationPrint]) -> float:
    if current_price <= 0 or not liquidations:
        return 0.0
    bins: dict[float, float] = {}
    for row in liquidations:
        if row.price <= 0 or row.notional <= 0:
            continue
        bucket = round(row.price / 25.0) * 25.0
        bins[bucket] = bins.get(bucket, 0.0) + row.notional
    if not bins:
        return 0.0
    strongest_price = max(bins.items(), key=lambda item: item[1])[0]
    return abs((strongest_price - current_price) / current_price) * 10_000


def _inventory_skew(*, current_up_exposure: float, current_down_exposure: float) -> float:
    total = max(current_up_exposure + current_down_exposure, 0.0)
    if total <= 0:
        return 0.0
    return (current_up_exposure - current_down_exposure) / total


def _window_third(seconds_into_window: int) -> str:
    if seconds_into_window < 100:
        return "opening"
    if seconds_into_window < 200:
        return "mid"
    return "late"
