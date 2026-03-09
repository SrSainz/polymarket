const DEFAULT_WALLET = "0xa81f087970a7ce196eacb3271e96e89294d91bb8";
const DATA_API = "https://data-api.polymarket.com";
const API_BASE_STORAGE_KEY = "polymarket_bot_api_base";

let runtimeMode = "local";
let watchedWallet = DEFAULT_WALLET;
let apiBase = "";

const fmt = (value, digits = 4) => {
  const asNumber = Number(value);
  if (Number.isNaN(asNumber)) return "-";
  return asNumber.toFixed(digits);
};

function tsToIso(ts) {
  if (!ts) return "-";
  const date = new Date(ts * 1000);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toISOString().replace(".000Z", "Z");
}

function statusPill(status) {
  const safe = String(status || "").toLowerCase();
  return `<span class="pill ${safe}">${safe || "-"}</span>`;
}

function escapeHtml(raw) {
  return String(raw || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function shortWallet(wallet) {
  const value = String(wallet || "");
  if (value.length <= 14) return value;
  return `${value.slice(0, 8)}...${value.slice(-6)}`;
}

async function getJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

async function safeGetJson(url, fallback) {
  try {
    return await getJson(url);
  } catch (_error) {
    return fallback;
  }
}

function buildApiUrl(path) {
  if (!apiBase) return path;
  return `${apiBase}${path}`;
}

function withCacheBust(url) {
  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}_t=${Date.now()}`;
}

function loadSavedApiBase() {
  try {
    return (window.localStorage.getItem(API_BASE_STORAGE_KEY) || "").trim();
  } catch (_error) {
    return "";
  }
}

function saveApiBase(value) {
  try {
    if (!value) {
      window.localStorage.removeItem(API_BASE_STORAGE_KEY);
      return;
    }
    window.localStorage.setItem(API_BASE_STORAGE_KEY, value);
  } catch (_error) {
    // Ignore storage failures.
  }
}

function paintSummary(summary) {
  document.getElementById("openPositions").textContent = String(summary.open_positions ?? 0);
  document.getElementById("exposure").textContent = fmt(summary.exposure, 2);

  const pnlTotal = Number(summary.pnl_total ?? summary.cumulative_pnl ?? 0);
  const realized = Number(summary.realized_pnl ?? summary.cumulative_pnl ?? 0);
  const unrealized = Number(summary.unrealized_pnl ?? 0);
  document.getElementById("pnl").textContent = fmt(pnlTotal, 2);
  document.getElementById("pnlBreakdown").textContent = `realized ${fmt(realized, 2)} / unrealized ${fmt(unrealized, 2)}`;

  document.getElementById("pendingSignals").textContent = String(summary.pending_signals ?? "-");

  const modeText =
    runtimeMode === "local"
      ? "local db mode"
      : runtimeMode === "public-fallback"
      ? `public api fallback (${watchedWallet})`
      : `public api mode (${watchedWallet})`;
  document.getElementById("lastUpdated").textContent = `Ultima actualizacion: ${new Date().toISOString().replace(".000Z", "Z")} | ${modeText}`;
}

function paintSelectedWallets(items) {
  const body = document.getElementById("selectedWalletsList");
  document.getElementById("selectedWalletsCount").textContent = String(items.length);
  if (!items.length) {
    body.innerHTML = `<li class="mini-item"><strong>Sin wallets seleccionadas</strong><span>Revisa filtros y API</span></li>`;
    document.getElementById("selectedWalletsMeta").textContent = "sin datos de seleccion";
    return;
  }

  body.innerHTML = items
    .map(
      (item) => `
      <li class="mini-item">
        <strong>#${Number(item.rank || 0)} ${escapeHtml(shortWallet(item.wallet))}</strong>
        <span>score ${fmt(item.score, 3)} | win ${fmt((Number(item.win_rate) || 0) * 100, 1)}% | 24h ${Number(item.recent_trades || 0)}</span>
      </li>
    `
    )
    .join("");

  document.getElementById("selectedWalletsMeta").textContent = "Top por score (winrate + actividad + pnl)";
}

function paintRiskBlocks(payload) {
  const items = payload.items || [];
  const hours = Number(payload.hours || 24);
  const blockedTotal = Number(payload.blocked_total || 0);
  const body = document.getElementById("riskBlocksList");
  document.getElementById("riskBlocksCount").textContent = String(blockedTotal);

  if (!items.length) {
    body.innerHTML = `<li class="mini-item"><strong>Sin bloqueos recientes</strong><span>La estrategia no detecto frenos de riesgo</span></li>`;
    document.getElementById("riskBlocksMeta").textContent = `ventana ${hours}h`;
    return;
  }

  body.innerHTML = items
    .map(
      (item) => `
      <li class="mini-item">
        <strong>${escapeHtml(item.reason)}</strong>
        <span>${Number(item.count || 0)} bloqueos</span>
      </li>
    `
    )
    .join("");

  document.getElementById("riskBlocksMeta").textContent = `ventana ${hours}h`;
}

function paintPositions(items) {
  const body = document.getElementById("positionsBody");
  document.getElementById("positionsCount").textContent = String(items.length);

  if (!items.length) {
    body.innerHTML = `<tr><td colspan="5">No hay posiciones copiadas.</td></tr>`;
    return;
  }

  body.innerHTML = items
    .map(
      (item) => `
      <tr>
        <td data-label="Mercado">${escapeHtml(item.title || item.slug || item.asset)}</td>
        <td data-label="Outcome">${escapeHtml(item.outcome || "-")}</td>
        <td data-label="Size">${fmt(item.size)}</td>
        <td data-label="Avg Price">${fmt(item.avg_price)}</td>
        <td data-label="Realized PnL">${fmt(item.realized_pnl)}</td>
      </tr>
    `
    )
    .join("");
}

function paintExecutions(items) {
  const body = document.getElementById("executionsBody");
  if (!items.length) {
    body.innerHTML = `<tr><td colspan="7">No hay ejecuciones.</td></tr>`;
    return;
  }

  body.innerHTML = items
    .map(
      (item) => `
      <tr>
        <td data-label="Hora UTC">${tsToIso(item.ts)}</td>
        <td data-label="Modo">${escapeHtml(item.mode)}</td>
        <td data-label="Accion">${escapeHtml(item.action)}</td>
        <td data-label="Lado">${escapeHtml(item.side)}</td>
        <td data-label="Size">${fmt(item.size)}</td>
        <td data-label="Price">${fmt(item.price)}</td>
        <td data-label="PnL Delta">${fmt(item.pnl_delta)}</td>
      </tr>
    `
    )
    .join("");
}

function paintSignals(items) {
  const body = document.getElementById("signalsBody");
  if (!items.length) {
    body.innerHTML = `<tr><td colspan="7">No hay senales todavia.</td></tr>`;
    return;
  }

  body.innerHTML = items
    .map(
      (item) => `
      <tr>
        <td data-label="Hora UTC">${tsToIso(item.detected_at)}</td>
        <td data-label="Accion">${escapeHtml(item.action)}</td>
        <td data-label="Prev">${fmt(item.prev_size)}</td>
        <td data-label="New">${fmt(item.new_size)}</td>
        <td data-label="Delta">${fmt(item.delta_size)}</td>
        <td data-label="Status">${statusPill(item.status)}</td>
        <td data-label="Nota">${escapeHtml(item.note || "")}</td>
      </tr>
    `
    )
    .join("");
}

async function refreshAll() {
  try {
    if (runtimeMode === "local") {
      const [summary, positions, executions, signals, selectedWallets, riskBlocks] = await Promise.all([
        getJson(withCacheBust(buildApiUrl("/api/summary"))),
        getJson(withCacheBust(buildApiUrl("/api/positions"))),
        getJson(withCacheBust(buildApiUrl("/api/executions?limit=50"))),
        getJson(withCacheBust(buildApiUrl("/api/signals?limit=100"))),
        safeGetJson(withCacheBust(buildApiUrl("/api/selected-wallets?limit=3")), { items: [] }),
        safeGetJson(withCacheBust(buildApiUrl("/api/risk-blocks?hours=24&limit=3")), {
          items: [],
          hours: 24,
          blocked_total: 0,
        }),
      ]);

      paintSummary(summary);
      paintPositions(positions.items || []);
      paintExecutions(executions.items || []);
      paintSignals(signals.items || []);
      paintSelectedWallets(selectedWallets.items || []);
      paintRiskBlocks(riskBlocks || {});
      return;
    }

    const [positionsRaw, activityRaw] = await Promise.all([
      getJson(withCacheBust(`${DATA_API}/positions?user=${encodeURIComponent(watchedWallet)}&limit=200`)),
      getJson(withCacheBust(`${DATA_API}/activity?user=${encodeURIComponent(watchedWallet)}&limit=100`)),
    ]);

    const positions = (positionsRaw || []).map((item) => ({
      title: item.title || item.slug || item.asset,
      slug: item.slug || "",
      asset: item.asset || "",
      outcome: item.outcome || "",
      size: Number(item.size || 0),
      avg_price: Number(item.avgPrice || item.curPrice || 0),
      realized_pnl: Number(item.realizedPnl || item.cashPnl || 0),
    }));

    const executions = (activityRaw || []).map((item, index) => ({
      id: index + 1,
      ts: Number(item.timestamp || 0),
      mode: "source",
      status: "observed",
      action: (item.side || "").toLowerCase(),
      side: (item.side || "").toLowerCase(),
      size: Number(item.size || 0),
      price: Number(item.price || 0),
      pnl_delta: 0,
    }));

    const realized = positions.reduce((acc, item) => acc + Number(item.realized_pnl || 0), 0);
    const summary = {
      open_positions: positions.length,
      exposure: positions.reduce((acc, item) => acc + item.size * item.avg_price, 0),
      cumulative_pnl: realized,
      realized_pnl: realized,
      unrealized_pnl: 0,
      pnl_total: realized,
      pending_signals: "-",
    };

    paintSummary(summary);
    paintPositions(positions);
    paintExecutions(executions);
    paintSignals([]);
    paintSelectedWallets([]);
    paintRiskBlocks({ items: [], hours: 24, blocked_total: 0 });
  } catch (error) {
    document.getElementById("lastUpdated").textContent = `Error de actualizacion: ${error.message}`;
  }
}

let timer = null;

function configureAutoRefresh() {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }

  const seconds = Number(document.getElementById("refreshSeconds").value || 10);
  const safeSeconds = Math.min(Math.max(seconds, 3), 300);
  timer = setInterval(() => {
    refreshAll();
  }, safeSeconds * 1000);
}

document.getElementById("refreshBtn").addEventListener("click", async () => {
  const button = document.getElementById("refreshBtn");
  const originalLabel = button.textContent;
  button.disabled = true;
  button.textContent = "Refrescando...";
  try {
    await refreshAll();
    configureAutoRefresh();
  } finally {
    button.disabled = false;
    button.textContent = originalLabel || "Refrescar";
  }
});

document.getElementById("refreshSeconds").addEventListener("change", () => {
  configureAutoRefresh();
});

async function bootstrap() {
  const params = new URLSearchParams(window.location.search);
  watchedWallet = (params.get("wallet") || DEFAULT_WALLET).toLowerCase();
  const apiParam = (params.get("api") || "").trim().replace(/\/+$/, "");
  const savedApiBase = loadSavedApiBase();
  apiBase = apiParam || savedApiBase;
  saveApiBase(apiBase);

  try {
    await getJson(withCacheBust(buildApiUrl("/api/health")));
    runtimeMode = "local";
  } catch (error) {
    runtimeMode = apiBase ? "public-fallback" : "public";
    if (apiBase) {
      document.getElementById("lastUpdated").textContent =
        `No conecta con API local (${apiBase}): ${error.message}. Mostrando fallback publico.`;
    }
  }

  document.querySelector(".kicker").textContent =
    runtimeMode === "local"
      ? "Copy Trading Monitor (Local DB)"
      : runtimeMode === "public-fallback"
      ? "Copy Trading Monitor (Public API Fallback)"
      : "Copy Trading Monitor (Public API)";

  await refreshAll();
  configureAutoRefresh();
}

bootstrap();
