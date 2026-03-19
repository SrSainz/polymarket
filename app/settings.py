from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.core.strategy_registry import StrategyRegistry, apply_variant_overrides, load_strategy_registry


class BotConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    watched_wallets: list[str] = Field(default_factory=list)
    auto_select_wallets: bool = True
    top_wallets_to_copy: int = 3
    leaderboard_category: Literal["OVERALL", "CRYPTO", "POLITICS", "SPORTS"] = "OVERALL"
    leaderboard_time_period: Literal["DAY", "WEEK", "MONTH", "ALL"] = "MONTH"
    leaderboard_candidate_limit: int = 25
    prioritize_dynamic_wallets: bool = False
    dynamic_wallet_slots: int = 1
    dynamic_leaderboard_category: Literal["OVERALL", "CRYPTO", "POLITICS", "SPORTS"] = "CRYPTO"
    dynamic_leaderboard_time_period: Literal["DAY", "WEEK", "MONTH", "ALL"] = "DAY"
    min_dynamic_recent_trades: int = 5
    min_dynamic_trade_share: float = 0.20
    max_dynamic_share_for_base_wallet: float = 0.65
    wallet_selection_refresh_minutes: int = 30
    min_wallet_win_rate: float = 0.55
    min_closed_positions_for_scoring: int = 10
    min_recent_trades: int = 8
    min_copyable_positions_per_wallet: int = 0
    recent_trade_lookback_hours: int = 24
    recent_trades_limit_per_wallet: int = 200
    closed_positions_limit: int = 200
    seed_new_wallets_without_backfill: bool = True
    require_recent_trade_for_position: bool = False
    position_recent_trade_lookback_hours: int = 72
    position_recent_trades_limit: int = 300
    polling_interval_seconds: float = 0.25
    market_feed_enabled: bool = True
    market_feed_stale_seconds: float = 2.5
    spot_feed_enabled: bool = True
    spot_feed_stale_seconds: float = 1.5
    execution_mode: Literal["paper", "live"] = "paper"
    dry_run: bool = True
    strategy_mode: Literal["copy_wallets", "btc5m_orderbook"] = "btc5m_orderbook"
    strategy_entry_mode: Literal["buy_above", "buy_opposite", "vidarx_micro", "arb_micro"] = "arb_micro"
    strategy_variant: str = "default"
    strategy_notes: str = ""
    incubation_stage: Literal["disabled", "idea", "backtest_pass", "paper", "live_small", "scaled", "paused"] = "disabled"
    incubation_auto_promote: bool = False
    incubation_min_days: int = 14
    incubation_min_resolutions: int = 20
    incubation_max_drawdown: float = 50.0
    incubation_min_backtest_pnl: float = 0.0
    incubation_min_backtest_fill_rate: float = 0.20
    incubation_min_backtest_hit_rate: float = 0.35
    incubation_min_backtest_edge_bps: float = 0.0
    strategy_trigger_price: float = 0.98
    strategy_trade_allocation_pct: float = 0.10
    strategy_fixed_trade_amount: float = 0.0
    strategy_max_opposite_price: float = 0.25
    strategy_max_target_spread: float = 0.03
    strategy_max_open_positions: int = 1
    strategy_min_seconds_into_window: int = 10
    strategy_max_seconds_into_window: int = 240

    bankroll: float = 10000.0
    sizing_mode: Literal["fixed_amount_per_trade", "proportional_to_source"] = "proportional_to_source"
    fixed_amount_per_trade: float = 25.0
    proportional_scale: float = 0.15
    noise_threshold_shares: float = 1.0
    min_trade_amount: float = 5.0
    min_price: float = 0.0
    max_price: float = 1.0
    skip_expired_source_positions: bool = True
    expired_market_grace_hours: int = 6
    short_horizon_only: bool = True
    max_market_horizon_days: int = 7
    forced_include_market_keywords: list[str] = Field(
        default_factory=lambda: [
            "btc 5 minute up or down",
            "bitcoin 5 minute up or down",
            "btc-updown-5m",
        ]
    )
    dynamic_keywords: list[str] = Field(
        default_factory=lambda: [
            "5m",
            "btc 5 minute up or down",
            "bitcoin 5 minute up or down",
            "btc-updown-5m",
        ]
    )
    dynamic_max_allocation_pct: float = 0.20
    dynamic_skip_manual_confirmation: bool = True
    btc5m_reserve_enabled: bool = False
    btc5m_reserved_notional: float = 200.0
    btc5m_reserved_allocation_pct: float = 0.0
    btc5m_reserve_protected_pct: float = 0.35
    btc5m_ignore_global_exposure_limit: bool = True
    btc5m_relaxed_risk: bool = True
    btc5m_strict_realism_mode: bool = True
    btc5m_reference_max_age_ms: int = 1200
    btc5m_allow_rtds_anchor_fallback: bool = True
    live_only_btc5m: bool = False
    live_btc5m_ticket_allocation_pct: float = 0.10
    live_btc5m_max_open_positions: int = 3
    btc5m_reserve_keywords: list[str] = Field(
        default_factory=lambda: [
            "btc 5 minute up or down",
            "bitcoin 5 minute up or down",
            "btc-updown-5m",
        ]
    )

    autonomous_decisions_enabled: bool = False
    autonomous_take_profit_pct: float = 0.15
    autonomous_stop_loss_pct: float = 0.10
    autonomous_depreciation_window_minutes: int = 30
    autonomous_depreciation_threshold_pct: float = 0.04
    autonomous_reduce_fraction: float = 0.50
    autonomous_cooldown_minutes: int = 30

    manual_confirmation_enabled: bool = False
    confirmation_start_hour: int = 8
    confirmation_end_hour: int = 20
    confirmation_timeout_minutes: int = 30
    confirmation_timezone: str = "Europe/Madrid"
    live_control_default_state: Literal["paused", "armed"] = "paused"
    telegram_execution_notifications_enabled: bool = True
    telegram_daily_summary_enabled: bool = True
    telegram_daily_summary_hour: int = 20
    telegram_daily_summary_timezone: str = "Europe/Madrid"
    telegram_status_summary_enabled: bool = False
    telegram_status_summary_interval_minutes: int = 30
    telegram_status_summary_recent_limit: int = 5
    runtime_diagnostics_enabled: bool = True
    runtime_diagnostics_interval_minutes: int = 60
    runtime_diagnostics_lookback_minutes: int = 180
    runtime_diagnostics_execution_limit: int = 200
    runtime_diagnostics_decision_limit: int = 1200
    runtime_guard_enabled: bool = True
    runtime_guard_lookback_minutes: int = 180
    runtime_guard_loss_streak: int = 3
    runtime_guard_max_recent_pnl: float = -35.0
    runtime_guard_cooldown_minutes: int = 45

    max_position_per_market: float = 75.0
    max_total_exposure: float = 250.0
    max_daily_loss: float = 40.0
    max_daily_loss_pct: float = 0.10
    profit_keep_ratio: float = 0.50
    slippage_limit: float = 0.03

    allowed_tags: list[str] = Field(default_factory=list)
    blocked_tags: list[str] = Field(default_factory=list)

    @field_validator("watched_wallets", mode="before")
    @classmethod
    def validate_wallets(cls, value: list[str]) -> list[str]:
        if not value:
            return []
        normalized = []
        for wallet in value:
            wallet = wallet.strip().lower()
            if wallet and wallet not in normalized:
                normalized.append(wallet)
        return normalized

    @field_validator(
        "allowed_tags",
        "blocked_tags",
        "dynamic_keywords",
        "forced_include_market_keywords",
        "btc5m_reserve_keywords",
        mode="before",
    )
    @classmethod
    def normalize_tags(cls, value: list[str]) -> list[str]:
        if not value:
            return []
        return [item.strip().lower() for item in value if item and item.strip()]

    @field_validator("strategy_variant", mode="before")
    @classmethod
    def normalize_strategy_variant(cls, value: str) -> str:
        return str(value or "").strip() or "default"

    @field_validator("strategy_notes", mode="before")
    @classmethod
    def normalize_strategy_notes(cls, value: str) -> str:
        return str(value or "").strip()

    @model_validator(mode="after")
    def validate_copy_sources(self) -> "BotConfig":
        if not self.auto_select_wallets and not self.watched_wallets:
            raise ValueError("Either enable auto_select_wallets or provide watched_wallets")
        if self.dynamic_wallet_slots < 0:
            raise ValueError("dynamic_wallet_slots must be >= 0")
        if self.dynamic_wallet_slots > self.top_wallets_to_copy:
            raise ValueError("dynamic_wallet_slots cannot exceed top_wallets_to_copy")
        if self.min_dynamic_recent_trades < 0:
            raise ValueError("min_dynamic_recent_trades must be >= 0")
        if self.min_dynamic_trade_share < 0 or self.min_dynamic_trade_share > 1:
            raise ValueError("min_dynamic_trade_share must be between 0 and 1")
        if self.max_dynamic_share_for_base_wallet < 0 or self.max_dynamic_share_for_base_wallet > 1:
            raise ValueError("max_dynamic_share_for_base_wallet must be between 0 and 1")
        if self.min_copyable_positions_per_wallet < 0:
            raise ValueError("min_copyable_positions_per_wallet must be >= 0")
        if self.min_price < 0 or self.min_price > 1:
            raise ValueError("min_price must be between 0 and 1")
        if self.max_price < 0 or self.max_price > 1:
            raise ValueError("max_price must be between 0 and 1")
        if self.min_price > self.max_price:
            raise ValueError("min_price cannot be greater than max_price")
        if self.strategy_trigger_price <= 0 or self.strategy_trigger_price > 1:
            raise ValueError("strategy_trigger_price must be in (0, 1]")
        if self.incubation_min_days < 0:
            raise ValueError("incubation_min_days must be >= 0")
        if self.incubation_min_resolutions < 1:
            raise ValueError("incubation_min_resolutions must be >= 1")
        if self.incubation_max_drawdown < 0:
            raise ValueError("incubation_max_drawdown must be >= 0")
        if self.incubation_min_backtest_fill_rate < 0 or self.incubation_min_backtest_fill_rate > 1:
            raise ValueError("incubation_min_backtest_fill_rate must be between 0 and 1")
        if self.incubation_min_backtest_hit_rate < 0 or self.incubation_min_backtest_hit_rate > 1:
            raise ValueError("incubation_min_backtest_hit_rate must be between 0 and 1")
        if self.strategy_trade_allocation_pct <= 0 or self.strategy_trade_allocation_pct > 1:
            raise ValueError("strategy_trade_allocation_pct must be in (0, 1]")
        if self.strategy_fixed_trade_amount < 0:
            raise ValueError("strategy_fixed_trade_amount must be >= 0")
        if self.strategy_max_opposite_price <= 0 or self.strategy_max_opposite_price > 1:
            raise ValueError("strategy_max_opposite_price must be in (0, 1]")
        if self.strategy_max_target_spread < 0 or self.strategy_max_target_spread > 1:
            raise ValueError("strategy_max_target_spread must be between 0 and 1")
        if self.strategy_max_open_positions < 1:
            raise ValueError("strategy_max_open_positions must be >= 1")
        if self.strategy_min_seconds_into_window < 0:
            raise ValueError("strategy_min_seconds_into_window must be >= 0")
        if self.strategy_max_seconds_into_window < 1:
            raise ValueError("strategy_max_seconds_into_window must be >= 1")
        if self.strategy_min_seconds_into_window >= self.strategy_max_seconds_into_window:
            raise ValueError("strategy_min_seconds_into_window must be lower than strategy_max_seconds_into_window")
        if self.expired_market_grace_hours < 0:
            raise ValueError("expired_market_grace_hours must be >= 0")
        if self.max_market_horizon_days < 1:
            raise ValueError("max_market_horizon_days must be >= 1")
        if self.position_recent_trade_lookback_hours < 1:
            raise ValueError("position_recent_trade_lookback_hours must be >= 1")
        if self.position_recent_trades_limit < 1:
            raise ValueError("position_recent_trades_limit must be >= 1")
        if self.polling_interval_seconds < 0.2:
            raise ValueError("polling_interval_seconds must be >= 0.2")
        if self.market_feed_stale_seconds <= 0:
            raise ValueError("market_feed_stale_seconds must be > 0")
        if self.spot_feed_stale_seconds <= 0:
            raise ValueError("spot_feed_stale_seconds must be > 0")
        if self.dynamic_max_allocation_pct < 0 or self.dynamic_max_allocation_pct > 1:
            raise ValueError("dynamic_max_allocation_pct must be between 0 and 1")
        if self.runtime_diagnostics_interval_minutes < 1:
            raise ValueError("runtime_diagnostics_interval_minutes must be >= 1")
        if self.runtime_diagnostics_lookback_minutes < 1:
            raise ValueError("runtime_diagnostics_lookback_minutes must be >= 1")
        if self.runtime_diagnostics_execution_limit < 1:
            raise ValueError("runtime_diagnostics_execution_limit must be >= 1")
        if self.runtime_diagnostics_decision_limit < 1:
            raise ValueError("runtime_diagnostics_decision_limit must be >= 1")
        if self.runtime_guard_lookback_minutes < 1:
            raise ValueError("runtime_guard_lookback_minutes must be >= 1")
        if self.runtime_guard_loss_streak < 1:
            raise ValueError("runtime_guard_loss_streak must be >= 1")
        if self.runtime_guard_cooldown_minutes < 1:
            raise ValueError("runtime_guard_cooldown_minutes must be >= 1")
        if self.btc5m_reserved_notional < 0:
            raise ValueError("btc5m_reserved_notional must be >= 0")
        if self.btc5m_reserved_allocation_pct < 0 or self.btc5m_reserved_allocation_pct > 1:
            raise ValueError("btc5m_reserved_allocation_pct must be between 0 and 1")
        if self.btc5m_reserve_protected_pct < 0 or self.btc5m_reserve_protected_pct > 1:
            raise ValueError("btc5m_reserve_protected_pct must be between 0 and 1")
        if self.btc5m_reference_max_age_ms < 100:
            raise ValueError("btc5m_reference_max_age_ms must be >= 100")
        if self.live_btc5m_ticket_allocation_pct <= 0 or self.live_btc5m_ticket_allocation_pct > 1:
            raise ValueError("live_btc5m_ticket_allocation_pct must be in (0, 1]")
        if self.live_btc5m_max_open_positions < 1:
            raise ValueError("live_btc5m_max_open_positions must be >= 1")
        if self.autonomous_take_profit_pct < 0:
            raise ValueError("autonomous_take_profit_pct must be >= 0")
        if self.autonomous_stop_loss_pct < 0:
            raise ValueError("autonomous_stop_loss_pct must be >= 0")
        if self.autonomous_depreciation_threshold_pct < 0:
            raise ValueError("autonomous_depreciation_threshold_pct must be >= 0")
        if self.autonomous_depreciation_window_minutes < 1:
            raise ValueError("autonomous_depreciation_window_minutes must be >= 1")
        if not (0 < self.autonomous_reduce_fraction <= 1):
            raise ValueError("autonomous_reduce_fraction must be in (0, 1]")
        if self.autonomous_cooldown_minutes < 0:
            raise ValueError("autonomous_cooldown_minutes must be >= 0")
        if not (0 <= self.confirmation_start_hour <= 23):
            raise ValueError("confirmation_start_hour must be between 0 and 23")
        if not (1 <= self.confirmation_end_hour <= 24):
            raise ValueError("confirmation_end_hour must be between 1 and 24")
        if self.confirmation_start_hour >= self.confirmation_end_hour:
            raise ValueError("confirmation_start_hour must be lower than confirmation_end_hour")
        if self.confirmation_timeout_minutes < 1:
            raise ValueError("confirmation_timeout_minutes must be >= 1")
        if not (0 <= self.telegram_daily_summary_hour <= 23):
            raise ValueError("telegram_daily_summary_hour must be between 0 and 23")
        if self.telegram_status_summary_interval_minutes < 5:
            raise ValueError("telegram_status_summary_interval_minutes must be >= 5")
        if self.telegram_status_summary_recent_limit < 1:
            raise ValueError("telegram_status_summary_recent_limit must be >= 1")
        if self.max_daily_loss_pct <= 0 or self.max_daily_loss_pct > 1:
            raise ValueError("max_daily_loss_pct must be in (0, 1]")
        if self.profit_keep_ratio < 0 or self.profit_keep_ratio > 1:
            raise ValueError("profit_keep_ratio must be between 0 and 1")
        return self


class EnvSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    live_trading: bool = False
    log_level: str = "INFO"

    bot_wallet_address: str = ""

    data_api_host: str = "https://data-api.polymarket.com"
    gamma_api_host: str = "https://gamma-api.polymarket.com"
    clob_host: str = "https://clob.polymarket.com"
    clob_ws_host: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    spot_ws_host: str = "wss://ws-live-data.polymarket.com"

    polymarket_private_key: str = ""
    polymarket_chain_id: int = 137
    polymarket_funder: str = ""
    polymarket_signature_type: int = 1

    polymarket_api_key: str = ""
    polymarket_api_secret: str = ""
    polymarket_api_passphrase: str = ""

    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8765

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    @classmethod
    def from_env(cls) -> "EnvSettings":
        return cls(
            live_trading=_to_bool(os.getenv("LIVE_TRADING", "false")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            bot_wallet_address=os.getenv("BOT_WALLET_ADDRESS", "").strip().lower(),
            data_api_host=os.getenv("POLYMARKET_DATA_API_HOST", "https://data-api.polymarket.com"),
            gamma_api_host=os.getenv("POLYMARKET_GAMMA_API_HOST", "https://gamma-api.polymarket.com"),
            clob_host=os.getenv("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com"),
            clob_ws_host=os.getenv("POLYMARKET_CLOB_WS_HOST", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
            spot_ws_host=os.getenv("POLYMARKET_SPOT_WS_HOST", "wss://ws-live-data.polymarket.com"),
            polymarket_private_key=os.getenv("POLYMARKET_PRIVATE_KEY", ""),
            polymarket_chain_id=int(os.getenv("POLYMARKET_CHAIN_ID", "137")),
            polymarket_funder=os.getenv("POLYMARKET_FUNDER", ""),
            polymarket_signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "1")),
            polymarket_api_key=os.getenv("POLYMARKET_API_KEY", ""),
            polymarket_api_secret=os.getenv("POLYMARKET_API_SECRET", ""),
            polymarket_api_passphrase=os.getenv("POLYMARKET_API_PASSPHRASE", ""),
            dashboard_host=os.getenv("DASHBOARD_HOST", "127.0.0.1"),
            dashboard_port=int(os.getenv("DASHBOARD_PORT", "8765")),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        )


@dataclass(frozen=True)
class AppPaths:
    root: Path
    db_path: Path
    logs_dir: Path
    reports_dir: Path

    @property
    def research_dir(self) -> Path:
        return self.root / "data" / "research"

    @property
    def experiments_dir(self) -> Path:
        return self.research_dir / "experiments"

    @property
    def hypotheses_dir(self) -> Path:
        return self.research_dir / "hypotheses"

    @property
    def datasets_dir(self) -> Path:
        return self.research_dir / "datasets"

    @property
    def strategy_registry_path(self) -> Path:
        return self.root / "config" / "strategy_registry.yaml"

    def ensure(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.research_dir.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class AppSettings:
    config: BotConfig
    env: EnvSettings
    paths: AppPaths
    strategy_registry: StrategyRegistry | None = None


def load_settings(root_dir: Path | None = None) -> AppSettings:
    root = root_dir or Path(__file__).resolve().parents[1]
    env_path = root / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    config_path = root / "config" / "settings.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path}")

    with config_path.open("r", encoding="utf-8") as file_handle:
        raw_config = yaml.safe_load(file_handle) or {}

    paths = AppPaths(
        root=root,
        db_path=root / "data" / "bot.db",
        logs_dir=root / "data" / "logs",
        reports_dir=root / "data" / "reports",
    )
    paths.ensure()
    strategy_registry = load_strategy_registry(paths.strategy_registry_path)
    normalized_config = apply_variant_overrides(raw_config, strategy_registry)
    config = BotConfig.model_validate(normalized_config)
    env = EnvSettings.from_env()

    return AppSettings(config=config, env=env, paths=paths, strategy_registry=strategy_registry)


def _to_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}
