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
      wallet_selector.py
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
    test_wallet_selector.py
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
- `auto_select_wallets`: seleccion automatica de wallets top
- `top_wallets_to_copy`: numero de wallets seleccionadas
- `leaderboard_category` + `leaderboard_time_period`: universo de ranking
- `prioritize_dynamic_wallets`: prioriza wallets con actividad en mercados dinamicos (BTC 5m, etc.)
- `dynamic_wallet_slots`: cupo de wallets dinamicas dentro del total seleccionado (ej. `1` de `top_wallets_to_copy=3`)
- `dynamic_leaderboard_category` / `dynamic_leaderboard_time_period`: fallback especifico para buscar wallets dinamicas
- `min_dynamic_recent_trades`: minimo de trades dinamicos recientes exigidos por wallet
- `min_dynamic_trade_share`: porcentaje minimo de trades dinamicos dentro de su actividad reciente
- `min_wallet_win_rate`: winrate minimo requerido
- `min_recent_trades`: actividad minima reciente
- `seed_new_wallets_without_backfill`: al entrar una wallet nueva, toma baseline y evita copiar posiciones antiguas
- `polling_interval_seconds`: frecuencia de polling
- `execution_mode`: `paper` o `live`
- `bankroll`: capital de referencia del bot (default 1000)
  - capital operativo diario = `max(bankroll + pnl_realizado_hasta_ayer, 0)`
- `sizing_mode`: `fixed_amount_per_trade` o `proportional_to_source`
- `fixed_amount_per_trade`: notional fijo por trade
- `proportional_scale`: multiplicador de copia proporcional
- `min_price` / `max_price`: filtro de entrada por precio (evita mercados extremos de 0.00x o 0.99x)
- `skip_expired_source_positions`: ignora posiciones de origen en mercados ya concluidos
- `expired_market_grace_hours`: margen (horas) tras `endDate` antes de considerar un mercado expirado
- `short_horizon_only`: copia solo mercados con vencimiento cercano
- `max_market_horizon_days`: horizonte maximo (ej. `7` para daily/weekly)
- `forced_include_market_keywords`: excepciones de inclusion (ej. `BTC 5 Minute Up or Down`)
- `dynamic_keywords`: palabras clave para detectar mercados dinamicos (ej. BTC, 5m, 15m)
- `dynamic_max_allocation_pct`: porcentaje maximo de capital en mercados dinamicos (ej. `0.20`)
- `dynamic_skip_manual_confirmation`: si `true`, no pide confirmacion manual en mercados dinamicos
- `btc5m_reserve_enabled`: activa reserva exclusiva para BTC 5m
- `btc5m_reserved_notional`: capital reservado para BTC 5m (ej. `200` USDC)
- `btc5m_reserve_protected_pct`: que parte de esa reserva se protege realmente frente a otras operaciones (0-1)
- `btc5m_ignore_global_exposure_limit`: permite que BTC 5m use su cupo aunque el resto de cartera haya agotado su limite global
- `btc5m_reserve_keywords`: patrones para identificar ese mercado reservado
- `autonomous_decisions_enabled`: activa decisiones propias de salida
- `autonomous_take_profit_pct`: cierre por beneficio objetivo
- `autonomous_stop_loss_pct`: cierre por perdida maxima
- `autonomous_depreciation_window_minutes`: ventana para medir caida de precio
- `autonomous_depreciation_threshold_pct`: umbral de depreciacion para reducir
- `autonomous_reduce_fraction`: porcentaje de reduccion en depreciacion
- `autonomous_cooldown_minutes`: evita sobreoperar el mismo activo
- `manual_confirmation_enabled`: pedir confirmacion manual antes de ejecutar en ventana horaria
  - en este build, la confirmacion manual solo se aplica en `live`; en `paper` ejecuta automatico
- `confirmation_start_hour` / `confirmation_end_hour`: franja de confirmacion (hora local)
- `confirmation_timeout_minutes`: si no respondes a tiempo, ejecuta automatico
- `confirmation_timezone`: zona horaria para la ventana (ej. `Europe/Madrid`)
- `telegram_daily_summary_enabled`: envio diario de resumen por Telegram
- `telegram_daily_summary_hour`: hora local para enviar resumen diario
- `telegram_daily_summary_timezone`: zona horaria del resumen diario
- `max_position_per_market`
- `max_total_exposure`
- `max_daily_loss`
- `max_daily_loss_pct` (cap porcentual del bankroll para perdida diaria, por ejemplo `0.10`)
  - limite diario efectivo = `min(max_daily_loss, capital_operativo * max_daily_loss_pct) + ganancias_realizadas_del_dia`
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
sync => wallets=3 snapshots=9 new_signals=3 dropped_wallets=0 rebalance_signals=0
execute => pending=3 filled=3 blocked=0 skipped=0 failed=0 auto_candidates=0 auto_filled=0 auto_failed=0
          approvals_requested=0 approvals_user_filled=0 approvals_timeout_filled=0 approvals_failed=0
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
- Incluye:
  - wallets top seleccionadas (score, winrate, actividad)
  - bloqueos de riesgo agregados (ventana 24h)
  - PnL total = realized + unrealized (mark-to-market por `clob midpoint`)
  - boton `Limpiar y reiniciar` para resetear estado local y volver a arrancar tracking desde cero
- Endpoints:
  - `/api/summary`
  - `/api/positions`
  - `/api/executions`
  - `/api/signals`
  - `/api/selected-wallets`
  - `/api/risk-blocks`
  - `POST /api/reset` (requiere JSON `{"confirm":"reset"}`)

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

Variables de confirmacion manual por Telegram:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Flujo de confirmacion manual:
1. En `live`, si hay señal en la franja configurada (por defecto 08:00-20:00), el bot envia mensaje Telegram.
2. Puedes pulsar `Comprar`, `Vender` o `Saltar`.
3. Si no respondes en `confirmation_timeout_minutes` (por defecto 30), ejecuta automatico como hasta ahora.

Resumen diario Telegram:
1. Si `telegram_daily_summary_enabled=true` y hay credenciales Telegram, el bot envia 1 mensaje diario.
2. Contiene PnL neto del dia, ganancias/perdidas brutas, numero de operaciones y exposicion.

Si faltan credenciales reales o `py-clob-client`, el modo live fallara de forma explicita sin tocar paper.

## Persistencia

SQLite `data/bot.db`:
- `source_positions_current`
- `source_positions_history`
- `signals`
- `copy_positions`
- `executions`
- `daily_pnl`
- `selected_wallets`

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
- `https://data-api.polymarket.com/trades?limit=...` (actividad global)
- `https://data-api.polymarket.com/closed-positions?user=...`
- `https://data-api.polymarket.com/v1/leaderboard?category=...&timePeriod=...`
- `https://gamma-api.polymarket.com/markets?slug=...`
- `https://clob.polymarket.com/midpoint?token_id=...`
- `https://clob.polymarket.com/book?token_id=...`

## Disclaimer

Esto no es asesoria financiera. Usa paper trading primero y valida riesgo/latencia antes de cualquier uso en live.

