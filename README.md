# BTC 5m Microstructure Lab

Laboratorio en Python 3.11 para estudiar y simular estrategias de microestructura en los mercados `Bitcoin Up or Down` de Polymarket.

La linea actual del proyecto ya no es copiar wallets. El objetivo es:

1. leer el mercado BTC 5m actual
2. inspeccionar el orderbook de `Up` y `Down`
3. detectar ineficiencias como `underround` (`Up + Down < 1`)
4. barrer varios niveles del libro en paper
5. liquidar al cierre y medir PnL por ventana

## Modos de estrategia

- `arb_micro`: primer motor orientado a arbitraje de dos patas
- `vidarx_micro`: laboratorio heuristico legado para comparar microestructura
- `buy_above` / `buy_opposite`: modos antiguos conservados solo como referencia tecnica

## Estructura

```text
polymarket_copy_bot/
  run.py
  config/settings.yaml
  app/
    db.py
    settings.py
    models.py
    core/
      paper_broker.py
      live_broker.py
      risk.py
    polymarket/
      gamma_client.py
      clob_client.py
      auth.py
    services/
      btc5m_strategy.py
      dashboard_server.py
      report.py
  web/
    index.html
    assets/
      app.js
      styles.css
  tests/
```

## Instalacion

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

## Configuracion base

El proyecto arranca por defecto en paper con `arb_micro`.

Archivo: [config/settings.yaml](C:/Users/sergi/Desktop/polymarket/polymarket_copy_bot/config/settings.yaml)

Campos principales:

- `polling_interval_seconds`: frecuencia del loop
- `execution_mode`: `paper` o `live`
- `strategy_entry_mode`: `arb_micro`, `vidarx_micro`, `buy_above`, `buy_opposite`
- `bankroll`: capital teorico de partida
- `strategy_trade_allocation_pct`: presupuesto por ciclo
- `min_trade_amount`: minimo por pata
- `max_daily_loss_pct`: freno de perdida diaria

## Comandos

```bash
python run.py once
python run.py paper
python run.py live
python run.py dashboard
python run.py report
```

## Flujo recomendado

1. `python run.py dashboard`
2. `python run.py once`
3. `python run.py paper`

## Dashboard

- arranque: `python run.py dashboard`
- url por defecto: `http://127.0.0.1:8765`

El panel muestra:

- capital disponible
- dinero metido
- PnL total y en vivo
- mercado BTC 5m actual
- reparto entre patas
- resoluciones cerradas
- setups con mejor rendimiento

## Nota importante sobre velocidad

El laboratorio actual combina `WebSocket + cache de libro` para el mercado activo y `REST` como fallback. Sirve para medir ideas y comparar setups con bastante menos retraso que antes, pero todavia no reproduce la latencia de un bot competitivo de microestructura.

Por defecto el loop del laboratorio ya corre cada `1s`, y el dashboard muestra si la lectura va por `websocket` o ha caido a `rest fallback`.

Para acercarnos a una operativa tipo `vidarx`, el siguiente salto tecnico es:

1. bajar el loop de decision por debajo de `1s`
2. deteccion de underround en milisegundos
3. sweep de varios niveles del libro sin esperar al siguiente polling

## Persistencia

SQLite en `data/bot.db`:

- `copy_positions`
- `executions`
- `daily_pnl`
- `strategy_windows`
- `bot_state`

## Tests

```bash
python -m pytest -q
```
