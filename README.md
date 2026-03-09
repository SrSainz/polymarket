# Polymarket Copy Bot (Paper-First, CLI)

Proyecto local en Python 3.11 para replicar operaciones de wallets de Polymarket en modo **paper trading** por defecto, con módulo **live trading desacoplado** y apagado.

Este diseño sigue la logica del video de referencia (`2eACyYW9OXg`):
1. Configurar wallets objetivo.
2. Leer actividad/posiciones reales de Polymarket.
3. Detectar cambios de posicion (open/add/reduce/close).
4. Aplicar sizing proporcional o fijo.
5. Pasar por filtros de riesgo.
6. Ejecutar en paper (y opcionalmente en live con credenciales reales).

## Estructura

```text
polymarket_copy_bot/
  README.md
  .env.example
  requirements.txt
  pyproject.toml
  run.py
  config/
    settings.yaml
  app/
    __init__.py
    settings.py
    logger.py
    models.py
    db.py
    polymarket/
      __init__.py
      gamma_client.py
      clob_client.py
      activity_client.py
      auth.py
    core/
      __init__.py
      watchlist.py
      tracker.py
      normalizer.py
      risk.py
      sizing.py
      copier.py
      paper_broker.py
      live_broker.py
      reconciler.py
    services/
      __init__.py
      sync_wallets.py
      detect_changes.py
      execute_copy.py
      report.py
  data/
    bot.db
    logs/
    reports/
  tests/
    test_sizing.py
    test_risk.py
    test_normalizer.py
```

## Requisitos

- Python 3.11
- Windows/macOS/Linux

## Instalacion

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

## Configuracion

Edita `config/settings.yaml`:

- `watched_wallets`: wallets origen a copiar
- `polling_interval_seconds`: frecuencia de polling
- `execution_mode`: `paper` o `live`
- `bankroll`: capital de referencia del bot
- `sizing_mode`: `fixed_amount_per_trade` o `proportional_to_source`
- `fixed_amount_per_trade`: notional fijo por trade
- `proportional_scale`: multiplicador de copia proporcional
- `max_position_per_market`
- `max_total_exposure`
- `max_daily_loss`
- `slippage_limit`
- `allowed_tags` / `blocked_tags`

Nota sobre tags: Polymarket no entrega siempre taxonomia uniforme por posicion; el bot usa `category` del market (Gamma API) como etiqueta principal.

## Comandos CLI

```bash
python run.py sync
python run.py paper
python run.py live
python run.py report
python run.py once
python run.py dashboard
```

### Orden recomendada para arrancar

1. `python run.py once`
2. `python run.py report`
3. `python run.py dashboard`
4. `python run.py paper` (loop continuo)

Ejemplo real de `once`:

```text
sync => snapshots=3 new_signals=3
execute => pending=3 filled=3 blocked=0 skipped=0 failed=0
```

## Modo Paper (default)

- Simula ejecuciones con precio estimado (`clob midpoint` cuando existe, si no `reference_price`).
- Guarda operaciones en SQLite (`executions`).
- Actualiza posiciones replicadas (`copy_positions`).
- Calcula PnL realizado para ventas/reducciones.

## Dashboard Web

- Arranque: `python run.py dashboard`
- URL por defecto: `http://127.0.0.1:8765`
- Modo local: lee SQLite del bot (señales/ejecuciones reales del bot)
- Modo public API (si no existe backend local): lee `data-api.polymarket.com` directo en browser
- Wallet en modo public: `/?wallet=0x...`
- Forzar API remota del bot (ej. Vercel -> NAS): `/?api=https://tu-api-bot`
- Endpoints:
  - `/api/summary`
  - `/api/positions`
  - `/api/executions`
  - `/api/signals`

## Vercel

- Incluye `vercel.json` para servir `web/index.html` y assets.
- En Vercel funcionará en modo **public API** automaticamente (sin tu SQLite local).
- Si quieres mostrar datos privados/persistentes de tu bot en Vercel, necesitas una base remota (por ejemplo Postgres) y API backend.

## Modo Live (desacoplado, apagado)

Por defecto esta apagado con `LIVE_TRADING=false`.

Para habilitar:
- Cambia `LIVE_TRADING=true` en `.env`
- Rellena credenciales reales de Polymarket
- Instala `py-clob-client` (no incluida por defecto para mantener dependencias minimas)

Variables live:
- `POLYMARKET_PRIVATE_KEY`
- `POLYMARKET_CHAIN_ID`
- `POLYMARKET_FUNDER`
- `POLYMARKET_SIGNATURE_TYPE`
- `POLYMARKET_API_KEY`
- `POLYMARKET_API_SECRET`
- `POLYMARKET_API_PASSPHRASE`

Si faltan credenciales reales o `py-clob-client`, el modo live fallara de forma explicita sin tocar paper.

## Persistencia

SQLite `data/bot.db`:
- `source_positions_current`
- `source_positions_history`
- `signals`
- `copy_positions`
- `executions`
- `daily_pnl`

## Logs y reportes

- Logs: `data/logs/bot.log`
- Reportes: `data/reports/report_YYYYMMDD_HHMMSS.md`

## Tests

```bash
pytest
```

Cobertura minima incluida:
- normalizador de cambios
- sizing
- control de riesgo

## Endpoints usados (reales/documentados)

- `https://data-api.polymarket.com/positions?user=...`
- `https://data-api.polymarket.com/activity?user=...`
- `https://data-api.polymarket.com/trades?user=...`
- `https://gamma-api.polymarket.com/markets?slug=...`
- `https://clob.polymarket.com/midpoint?token_id=...`
- `https://clob.polymarket.com/book?token_id=...`

## Disclaimer

Esto no es asesoria financiera. Usa paper trading primero y valida riesgo/latencia antes de cualquier uso en live.

