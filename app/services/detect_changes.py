from __future__ import annotations

from app.core.normalizer import detect_position_changes
from app.models import NormalizedSignal, SourcePosition


class DetectChangesService:
    def __init__(self, noise_threshold: float) -> None:
        self.noise_threshold = noise_threshold

    def run(
        self,
        *,
        wallet: str,
        previous: dict[str, SourcePosition],
        current: dict[str, SourcePosition],
    ) -> list[NormalizedSignal]:
        return detect_position_changes(
            wallet=wallet,
            previous=previous,
            current=current,
            noise_threshold=self.noise_threshold,
        )
