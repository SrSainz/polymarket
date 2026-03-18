from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.lab_artifacts import dump_json, wallet_hypotheses_path
from app.db import Database
from app.polymarket.activity_client import ActivityClient
from app.polymarket.gamma_client import GammaClient


class WalletPatternMiner:
    def __init__(
        self,
        db: Database,
        activity_client: ActivityClient,
        gamma_client: GammaClient,
        research_root: Path,
        *,
        watched_wallets: list[str] | None = None,
    ) -> None:
        self.db = db
        self.activity_client = activity_client
        self.gamma_client = gamma_client
        self.research_root = research_root
        self.watched_wallets = list(watched_wallets or [])

    def run(self, *, wallet_limit: int = 5) -> dict[str, Any]:
        wallets = self._wallets(wallet_limit=wallet_limit)
        category_counter: Counter[str] = Counter()
        price_band_counter: Counter[str] = Counter()
        side_counter: Counter[str] = Counter()
        slug_counter: Counter[str] = Counter()
        pnl_by_band: Counter[str] = Counter()
        total_rows = 0

        for wallet in wallets:
            closed_positions = self.activity_client.get_closed_positions(wallet, limit=200)
            trades = self.activity_client.get_trades(wallet=wallet, limit=200)
            for row in closed_positions:
                category = self._category(row)
                price_band = _price_band(_safe_float(row.get("avgPrice") or row.get("price") or row.get("entryPrice")))
                slug = str(row.get("slug") or row.get("marketSlug") or "").strip()
                pnl = _safe_float(row.get("pnl") or row.get("realizedPnl") or row.get("profit"))
                if category:
                    category_counter[category] += 1
                if price_band:
                    price_band_counter[price_band] += 1
                    pnl_by_band[price_band] += pnl
                if slug:
                    slug_counter[slug] += 1
                total_rows += 1
            for row in trades:
                side = str(row.get("side") or row.get("type") or row.get("action") or "").strip().upper()
                if side:
                    side_counter[side] += 1

        patterns = _build_patterns(
            wallets=wallets,
            total_rows=total_rows,
            category_counter=category_counter,
            price_band_counter=price_band_counter,
            side_counter=side_counter,
            slug_counter=slug_counter,
            pnl_by_band=pnl_by_band,
        )
        hypotheses = _build_hypotheses(
            category_counter=category_counter,
            price_band_counter=price_band_counter,
            side_counter=side_counter,
            slug_counter=slug_counter,
            pnl_by_band=pnl_by_band,
        )
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "wallets": wallets,
            "patterns": patterns,
            "hypotheses": hypotheses,
        }
        dump_json(wallet_hypotheses_path(self.research_root), payload)
        return payload

    def _wallets(self, *, wallet_limit: int) -> list[str]:
        rows = self.db.conn.execute(
            """
            SELECT wallet
            FROM selected_wallets
            ORDER BY rank ASC
            LIMIT ?
            """,
            (wallet_limit,),
        ).fetchall()
        wallets = [str(row["wallet"]).strip().lower() for row in rows if str(row["wallet"]).strip()]
        if wallets:
            return wallets
        deduped: list[str] = []
        for wallet in self.watched_wallets:
            normalized = str(wallet).strip().lower()
            if normalized and normalized not in deduped:
                deduped.append(normalized)
        return deduped[:wallet_limit]

    def _category(self, row: dict[str, Any]) -> str:
        category = str(row.get("category") or row.get("marketCategory") or "").strip().lower()
        if category:
            return category
        slug = str(row.get("slug") or row.get("marketSlug") or "").strip()
        if not slug:
            return ""
        return self.gamma_client.get_category(slug)


def _build_patterns(
    *,
    wallets: list[str],
    total_rows: int,
    category_counter: Counter[str],
    price_band_counter: Counter[str],
    side_counter: Counter[str],
    slug_counter: Counter[str],
    pnl_by_band: Counter[str],
) -> list[dict[str, Any]]:
    patterns: list[dict[str, Any]] = []
    if category_counter:
        category, count = category_counter.most_common(1)[0]
        patterns.append(
            {
                "label": "Categoria dominante",
                "value": f"{category} {round(count / max(total_rows, 1) * 100, 1)}%",
                "detail": "Los wallets top concentran actividad en esta familia de mercados.",
            }
        )
    if price_band_counter:
        band, count = price_band_counter.most_common(1)[0]
        patterns.append(
            {
                "label": "Banda de entrada",
                "value": f"{band} {round(count / max(total_rows, 1) * 100, 1)}%",
                "detail": f"PnL agregado de la banda: {pnl_by_band.get(band, 0.0):.2f} USDC.",
            }
        )
    if side_counter:
        side, count = side_counter.most_common(1)[0]
        patterns.append(
            {
                "label": "Sesgo de ejecucion",
                "value": f"{side} {count}",
                "detail": "Direccion mas repetida en el flujo reciente de trades.",
            }
        )
    if slug_counter:
        slug, count = slug_counter.most_common(1)[0]
        patterns.append(
            {
                "label": "Slug repetido",
                "value": f"{slug} x{count}",
                "detail": "Mercado recurrente entre wallets top, util para convertirlo en hipotesis testeable.",
            }
        )
    if wallets:
        patterns.append(
            {
                "label": "Wallets analizados",
                "value": str(len(wallets)),
                "detail": ", ".join(wallets[:3]) + (" ..." if len(wallets) > 3 else ""),
            }
        )
    return patterns


def _build_hypotheses(
    *,
    category_counter: Counter[str],
    price_band_counter: Counter[str],
    side_counter: Counter[str],
    slug_counter: Counter[str],
    pnl_by_band: Counter[str],
) -> list[dict[str, Any]]:
    hypotheses: list[dict[str, Any]] = []
    dominant_category = category_counter.most_common(1)[0][0] if category_counter else ""
    dominant_band = price_band_counter.most_common(1)[0][0] if price_band_counter else ""
    dominant_side = side_counter.most_common(1)[0][0] if side_counter else ""
    repeated_slug = slug_counter.most_common(1)[0][0] if slug_counter else ""

    if dominant_category == "crypto":
        hypotheses.append(
            {
                "id": "crypto-focus",
                "title": "Priorizar variantes crypto-first",
                "priority": 1,
                "detail": "Los mejores wallets siguen cargados en crypto; conviene testear filtros y sizing especificos para BTC 5m.",
            }
        )
    if dominant_band and pnl_by_band.get(dominant_band, 0.0) > 0:
        hypotheses.append(
            {
                "id": f"price-band-{dominant_band}",
                "title": f"Convertir la banda {dominant_band} en filtro de entrada",
                "priority": 1,
                "detail": "La banda de precio dominante tambien aporta PnL neto; eso justifica crear una variante con ese sesgo como gate.",
            }
        )
    if dominant_side == "BUY":
        hypotheses.append(
            {
                "id": "buy-pressure",
                "title": "Modelar presion compradora de wallets top",
                "priority": 2,
                "detail": "Hay predominio de aperturas BUY; se puede probar un filtro de confirmacion de flujo antes de abrir bracket.",
            }
        )
    if repeated_slug:
        hypotheses.append(
            {
                "id": "consensus-market",
                "title": "Backtestear mercados de consenso entre wallets",
                "priority": 2,
                "detail": f"El slug {repeated_slug} aparece varias veces; sirve para derivar una variante o un ranking de mercados observables.",
            }
        )
    return hypotheses[:4]


def _price_band(price: float) -> str:
    if price <= 0:
        return ""
    if price < 0.20:
        return "cheap"
    if price < 0.65:
        return "mid"
    return "rich"


def _safe_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
