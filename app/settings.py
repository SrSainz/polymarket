from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, field_validator


class BotConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    watched_wallets: list[str] = Field(default_factory=list)
    polling_interval_seconds: int = 45
    execution_mode: Literal["paper", "live"] = "paper"
    dry_run: bool = True

    bankroll: float = 500.0
    sizing_mode: Literal["fixed_amount_per_trade", "proportional_to_source"] = "proportional_to_source"
    fixed_amount_per_trade: float = 25.0
    proportional_scale: float = 0.15
    noise_threshold_shares: float = 1.0
    min_trade_amount: float = 5.0

    max_position_per_market: float = 75.0
    max_total_exposure: float = 250.0
    max_daily_loss: float = 40.0
    slippage_limit: float = 0.03

    allowed_tags: list[str] = Field(default_factory=list)
    blocked_tags: list[str] = Field(default_factory=list)

    @field_validator("watched_wallets", mode="before")
    @classmethod
    def validate_wallets(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("watched_wallets cannot be empty")
        normalized = []
        for wallet in value:
            wallet = wallet.strip().lower()
            if wallet and wallet not in normalized:
                normalized.append(wallet)
        if not normalized:
            raise ValueError("watched_wallets cannot be empty")
        return normalized

    @field_validator("allowed_tags", "blocked_tags", mode="before")
    @classmethod
    def normalize_tags(cls, value: list[str]) -> list[str]:
        if not value:
            return []
        return [item.strip().lower() for item in value if item and item.strip()]


class EnvSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    live_trading: bool = False
    log_level: str = "INFO"

    bot_wallet_address: str = ""

    data_api_host: str = "https://data-api.polymarket.com"
    gamma_api_host: str = "https://gamma-api.polymarket.com"
    clob_host: str = "https://clob.polymarket.com"

    polymarket_private_key: str = ""
    polymarket_chain_id: int = 137
    polymarket_funder: str = ""
    polymarket_signature_type: int = 1

    polymarket_api_key: str = ""
    polymarket_api_secret: str = ""
    polymarket_api_passphrase: str = ""

    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8765

    @classmethod
    def from_env(cls) -> "EnvSettings":
        return cls(
            live_trading=_to_bool(os.getenv("LIVE_TRADING", "false")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            bot_wallet_address=os.getenv("BOT_WALLET_ADDRESS", "").strip().lower(),
            data_api_host=os.getenv("POLYMARKET_DATA_API_HOST", "https://data-api.polymarket.com"),
            gamma_api_host=os.getenv("POLYMARKET_GAMMA_API_HOST", "https://gamma-api.polymarket.com"),
            clob_host=os.getenv("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com"),
            polymarket_private_key=os.getenv("POLYMARKET_PRIVATE_KEY", ""),
            polymarket_chain_id=int(os.getenv("POLYMARKET_CHAIN_ID", "137")),
            polymarket_funder=os.getenv("POLYMARKET_FUNDER", ""),
            polymarket_signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "1")),
            polymarket_api_key=os.getenv("POLYMARKET_API_KEY", ""),
            polymarket_api_secret=os.getenv("POLYMARKET_API_SECRET", ""),
            polymarket_api_passphrase=os.getenv("POLYMARKET_API_PASSPHRASE", ""),
            dashboard_host=os.getenv("DASHBOARD_HOST", "127.0.0.1"),
            dashboard_port=int(os.getenv("DASHBOARD_PORT", "8765")),
        )


@dataclass(frozen=True)
class AppPaths:
    root: Path
    db_path: Path
    logs_dir: Path
    reports_dir: Path

    def ensure(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class AppSettings:
    config: BotConfig
    env: EnvSettings
    paths: AppPaths


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

    config = BotConfig.model_validate(raw_config)
    env = EnvSettings.from_env()

    paths = AppPaths(
        root=root,
        db_path=root / "data" / "bot.db",
        logs_dir=root / "data" / "logs",
        reports_dir=root / "data" / "reports",
    )
    paths.ensure()

    return AppSettings(config=config, env=env, paths=paths)


def _to_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}
