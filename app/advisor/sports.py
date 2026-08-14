from __future__ import annotations

import json
import math
import time
from typing import Any, Protocol

from app.advisor.models import MarketQuote, ModelEvidence, Opportunity
from app.polymarket.clob_client import CLOBClient
from app.polymarket.gamma_client import GammaClient


class ProbabilityModel(Protocol):
    """Independent, calibrated sports model supplied by the operator."""

    def estimate(self, quote: MarketQuote, *, now: float) -> ModelEvidence:
        """Return evidence whose identity matches ``quote``."""


class SportsMarketDiscovery:
    """Build executable quotes from public Gamma metadata and CLOB books.

    This class deliberately stops before probability estimation. A market price
    is not treated as an independent probability model.
    """

    def __init__(
        self,
        gamma: GammaClient,
        clob: CLOBClient,
        *,
        page_size: int = 100,
        max_pages: int = 20,
        max_markets: int = 2_000,
        sports_tag_ids: tuple[str, ...] = (),
        sports_market_types: tuple[str, ...] = (),
    ) -> None:
        self.gamma = gamma
        self.clob = clob
        self.page_size = max(1, min(int(page_size), 500))
        self.max_pages = max(1, min(int(max_pages), 100))
        self.max_markets = max(self.page_size, min(int(max_markets), 10_000))
        self.sports_tag_ids = tuple(dict.fromkeys(str(value).strip() for value in sports_tag_ids if str(value).strip()))
        self.sports_market_types = tuple(
            dict.fromkeys(str(value).strip() for value in sports_market_types if str(value).strip())
        )

    def discover(self, *, now: float | None = None) -> list[MarketQuote]:
        current_time = time.time() if now is None else float(now)
        if not math.isfinite(current_time):
            return []
        quotes: list[MarketQuote] = []
        seen_market_keys: set[str] = set()
        seen_page_keys: set[tuple[str, tuple[str, ...]]] = set()
        filters = self._sports_filters()
        for filter_name, tag_id, market_types in filters:
            offset = 0
            for page_number in range(self.max_pages):
                page = self.gamma.list_markets(
                    active=True,
                    closed=False,
                    limit=self.page_size,
                    offset=offset,
                    tag_id=tag_id,
                    sports_market_types=market_types,
                )
                if not isinstance(page, list):
                    raise RuntimeError("gamma_markets_page_invalid")
                if not page:
                    break
                page_key = (filter_name, tuple(_market_key(market) for market in page))
                if page_key in seen_page_keys:
                    raise RuntimeError("gamma_markets_page_repeated")
                seen_page_keys.add(page_key)
                for market in page:
                    market_key = _market_key(market)
                    if not market_key:
                        raise RuntimeError("gamma_market_identity_missing")
                    if market_key in seen_market_keys:
                        continue
                    seen_market_keys.add(market_key)
                    if len(seen_market_keys) > self.max_markets:
                        raise RuntimeError("gamma_market_limit_reached")
                    if not _is_sports_market(market, server_filtered=bool(tag_id or market_types)):
                        continue
                    quotes.extend(self._quotes_for_market(market, now=current_time))
                if len(page) < self.page_size:
                    break
                offset += self.page_size
            else:
                raise RuntimeError("gamma_page_limit_reached")
        return quotes

    def quote_for_identity(
        self,
        *,
        market_id: str,
        condition_id: str,
        token_id: str,
        outcome: str,
        now: float | None = None,
    ) -> MarketQuote:
        """Reload one executable quote and require an exact market identity."""

        matches = [
            quote
            for quote in self.discover(now=now)
            if (
                quote.market_id == str(market_id)
                and quote.condition_id == str(condition_id)
                and quote.token_id == str(token_id)
                and quote.outcome == str(outcome)
            )
        ]
        if len(matches) != 1:
            raise RuntimeError("quote_identity_not_unique")
        return matches[0]

    def _sports_filters(self) -> list[tuple[str, str | None, tuple[str, ...]]]:
        if self.sports_tag_ids:
            return [(f"tag:{tag_id}", tag_id, self.sports_market_types) for tag_id in self.sports_tag_ids]
        if self.sports_market_types:
            return [("market-types", None, self.sports_market_types)]
        list_sports = getattr(self.gamma, "list_sports", None)
        if not callable(list_sports):
            raise RuntimeError("gamma_sports_filter_unavailable")
        tags: set[str] = set()
        for sport in list_sports():
            if not isinstance(sport, dict):
                continue
            raw_tags = str(sport.get("tags") or "")
            tags.update(value.strip() for value in raw_tags.split(",") if value.strip())
        if not tags:
            raise RuntimeError("gamma_sports_tags_unavailable")
        return [(f"tag:{tag_id}", tag_id, ()) for tag_id in sorted(tags)]

    def _quotes_for_market(self, market: dict[str, Any], *, now: float) -> list[MarketQuote]:
        market_id = str(market.get("id") or market.get("marketId") or "").strip()
        condition_id = str(market.get("conditionId") or market.get("condition_id") or "").strip()
        title = str(market.get("question") or market.get("title") or "").strip()
        resolution_source = str(
            market.get("resolutionSource")
            or market.get("resolution_source")
            or _first_event_value(market, "resolutionSource")
            or ""
        ).strip()
        if (
            not market_id
            or not condition_id
            or not title
            or not resolution_source
            or market.get("active") is False
            or market.get("closed") is True
        ):
            return []
        outcomes = _string_list(market.get("outcomes"))
        token_ids = _string_list(market.get("clobTokenIds") or market.get("clob_token_ids"))
        if len(outcomes) != len(token_ids) or not token_ids:
            return []
        slug = str(market.get("slug") or "").strip()
        source_url = f"https://polymarket.com/event/{slug}" if slug else ""
        quotes: list[MarketQuote] = []
        for outcome, token_id in zip(outcomes, token_ids):
            try:
                book = self.clob.get_book(token_id)
            except Exception:  # noqa: BLE001
                continue
            ask = _best_ask(book)
            if ask is None:
                continue
            ask_price, ask_size = ask
            observed_at = _book_timestamp(book, fallback=now)
            min_order_size = _positive_float(book.get("min_order_size"))
            if min_order_size <= 0:
                try:
                    min_order_size = _positive_float(self.clob.get_min_order_size(token_id))
                except Exception:  # noqa: BLE001
                    min_order_size = 0.0
            raw_fee_bps = self.clob.get_fee_rate_bps(token_id)
            if raw_fee_bps is None:
                # Unknown fees must not be treated as zero edge drag.
                continue
            fee_bps = _positive_float(raw_fee_bps)
            quotes.append(
                MarketQuote(
                    market_id=market_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    title=title,
                    outcome=outcome,
                    execution_price=ask_price,
                    available_size=ask_size,
                    observed_at=observed_at,
                    market_status="open",
                    resolution_source=resolution_source,
                    fee_bps=fee_bps,
                    source_url=source_url,
                    min_order_size=min_order_size,
                )
            )
        return quotes


def opportunities_from_quotes(
    quotes: list[MarketQuote],
    model: ProbabilityModel,
    *,
    bankroll_usdc: float,
    daily_loss_usdc: float = 0.0,
    now: float,
) -> list[Opportunity]:
    """Attach independently generated evidence without weakening identity checks."""
    opportunities: list[Opportunity] = []
    for quote in quotes:
        evidence = model.estimate(quote, now=now)
        opportunities.append(
            Opportunity(
                quote=quote,
                evidence=evidence,
                bankroll_usdc=bankroll_usdc,
                daily_loss_usdc=daily_loss_usdc,
            )
        )
    return opportunities


def _is_sports_market(market: dict[str, Any], *, server_filtered: bool = False) -> bool:
    sports_type = str(market.get("sportsMarketType") or market.get("sports_market_type") or "").strip()
    if sports_type:
        return True
    if server_filtered:
        return True
    values: list[str] = []
    for key in ("category", "subcategory", "sportsMarketType", "sports_market_type"):
        values.extend(_string_list(market.get(key)))
    for event in market.get("events") or []:
        if isinstance(event, dict):
            values.extend(_string_list(event.get("category")))
            values.extend(_string_list(event.get("subcategory")))
    return any("sport" in value.lower() for value in values)


def _market_key(market: object) -> str:
    if not isinstance(market, dict):
        raise RuntimeError("gamma_market_invalid")
    return str(
        market.get("id")
        or market.get("marketId")
        or market.get("conditionId")
        or market.get("slug")
        or ""
    ).strip()


def _first_event_value(market: dict[str, Any], key: str) -> str:
    for event in market.get("events") or []:
        if isinstance(event, dict) and event.get(key):
            return str(event[key])
    return ""


def _string_list(raw: object) -> list[str]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            raw = [item.strip() for item in raw.split(",")]
    if not isinstance(raw, (list, tuple)):
        return []
    return [str(item).strip() for item in raw if str(item).strip()]


def _best_ask(book: object) -> tuple[float, float] | None:
    if not isinstance(book, dict):
        return None
    asks = book.get("asks") or []
    levels: list[tuple[float, float]] = []
    for level in asks:
        if not isinstance(level, dict):
            continue
        price = _positive_float(level.get("price"))
        size = _positive_float(level.get("size"))
        if 0 < price < 1 and size > 0:
            levels.append((price, size))
    return min(levels) if levels else None


def _book_timestamp(book: dict[str, Any], *, fallback: float) -> float:
    raw = book.get("timestamp") or book.get("updated_at") or fallback
    timestamp = _positive_float(raw)
    if timestamp > 100_000_000_000:
        timestamp /= 1_000.0
    return timestamp or fallback


def _positive_float(value: object) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if parsed > 0 else 0.0
