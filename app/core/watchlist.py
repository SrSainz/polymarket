from __future__ import annotations

from app.settings import BotConfig


class Watchlist:
    def __init__(self, config: BotConfig) -> None:
        self._wallets = config.watched_wallets

    @property
    def wallets(self) -> list[str]:
        return self._wallets
