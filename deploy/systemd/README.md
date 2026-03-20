# Systemd Deployment

These units pin the public dashboard to the `live` runtime and keep `paper` from taking over the public port.

## Files

- `polymarket-live.service`
- `polymarket-dashboard-live.service`

## Install on the NAS

```bash
cd ~/apps/polymarket
mkdir -p ~/.config/systemd/user

cp deploy/systemd/polymarket-live.service ~/.config/systemd/user/
cp deploy/systemd/polymarket-dashboard-live.service ~/.config/systemd/user/

systemctl --user daemon-reload
```

## Disable paper from the public port

```bash
systemctl --user stop polymarket-paper.service || true
systemctl --user disable polymarket-paper.service || true
systemctl --user mask polymarket-paper.service || true

systemctl --user stop polymarket-dashboard.service || true
systemctl --user disable polymarket-dashboard.service || true
```

## Start live

```bash
cd ~/apps/polymarket
source .venv/bin/activate

python run.py clear-ledger --runtime-mode live

systemctl --user enable polymarket-live.service
systemctl --user enable polymarket-dashboard-live.service
systemctl --user restart polymarket-live.service
systemctl --user restart polymarket-dashboard-live.service
```

## Verify

```bash
systemctl --user status polymarket-live.service --no-pager
systemctl --user status polymarket-dashboard-live.service --no-pager

curl -s http://127.0.0.1:8765/api/summary | python -m json.tool | egrep "live_control_state|strategy_runtime_mode|strategy_operability_state|strategy_last_note|strategy_feed_connected|strategy_feed_tracked_assets"

sqlite3 data/bot_live.db "
SELECT key, value
FROM bot_state
WHERE key IN (
  'live_control_state',
  'strategy_runtime_mode',
  'strategy_operability_state',
  'strategy_last_note',
  'strategy_feed_connected',
  'strategy_feed_tracked_assets'
)
ORDER BY key;
"
```

Expected:

- `live_control_state = armed`
- `strategy_runtime_mode = live`
- public port `8765` serves `dashboard --runtime-mode live`

## Optional: keep paper privately

If you still want `paper`, run it on another port:

```bash
nohup env DASHBOARD_PORT=8766 .venv/bin/python run.py dashboard --runtime-mode paper > data/logs/dashboard-paper.out 2>&1 &
```
