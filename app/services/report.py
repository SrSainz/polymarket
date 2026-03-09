from __future__ import annotations

from datetime import datetime
from pathlib import Path

from app.db import Database


class ReportService:
    def __init__(self, db: Database, reports_dir: Path) -> None:
        self.db = db
        self.reports_dir = reports_dir

    def generate(self) -> tuple[str, Path]:
        now = datetime.utcnow()
        positions = self.db.list_copy_positions()
        executions = self.db.get_recent_executions(limit=25)

        total_exposure = self.db.get_total_exposure()
        cumulative_pnl = self.db.get_cumulative_pnl()

        lines: list[str] = []
        lines.append(f"# Polymarket Copy Bot Report ({now.isoformat()} UTC)")
        lines.append("")
        lines.append("## Summary")
        lines.append(f"- Open copied positions: {len(positions)}")
        lines.append(f"- Current exposure (USDC est): {total_exposure:.2f}")
        lines.append(f"- Cumulative PnL (realized): {cumulative_pnl:.2f}")
        lines.append("")
        lines.append("## Copied Positions")

        if not positions:
            lines.append("- None")
        else:
            for row in positions:
                lines.append(
                    "- "
                    f"{row['title']} | asset={row['asset']} | size={float(row['size']):.4f} | "
                    f"avg_price={float(row['avg_price']):.4f} | category={row['category']}"
                )

        lines.append("")
        lines.append("## Recent Executions")
        if not executions:
            lines.append("- None")
        else:
            for row in executions:
                ts = datetime.utcfromtimestamp(int(row["ts"]))
                lines.append(
                    "- "
                    f"{ts.isoformat()}Z | mode={row['mode']} | status={row['status']} | "
                    f"action={row['action']} | side={row['side']} | size={float(row['size']):.4f} | "
                    f"price={float(row['price']):.4f} | pnl_delta={float(row['pnl_delta']):.4f}"
                )

        content = "\n".join(lines) + "\n"
        output_path = self.reports_dir / f"report_{now.strftime('%Y%m%d_%H%M%S')}.md"
        output_path.write_text(content, encoding="utf-8")
        return content, output_path
