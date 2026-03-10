const DEFAULT_WALLET = "0xa81f087970a7ce196eacb3271e96e89294d91bb8";
const DATA_API = "https://data-api.polymarket.com";
const API_BASE_STORAGE_KEY = "polymarket_bot_api_base";
const DEFAULT_REMOTE_API_BY_HOST = {
  "polymarket-fawn.vercel.app": "https://scores-trade-kept-developed.trycloudflare.com",
};
const DONUT_GAIN_COLOR = "#3a9f62";
const DONUT_LOSS_COLOR = "#d0675f";

let runtimeMode = "local";
let watchedWallet = DEFAULT_WALLET;
let apiBase = "";

const fmt = (value, digits = 4) => {
  const asNumber = Number(value);
  if (Number.isNaN(asNumber)) return "-";
  return asNumber.toFixed(digits);
};

function fmtUsd(value, digits = 2) {
  const asNumber = Number(value);
  if (Number.isNaN(asNumber)) return "-";
  const sign = asNumber > 0 ? "+" : "";
  return `${sign}$${asNumber.toFixed(digits)}`;
}

function fmtUsdPlain(value, digits = 2) {
  const asNumber = Number(value);
  if (Number.isNaN(asNumber)) return "-";
  return `$${asNumber.toFixed(digits)}`;
}

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

function modeLabel() {
  if (runtimeMode === "local") return "Local DB";
  if (runtimeMode === "public-fallback") return "Fallback publico";
  return "Public API";
}

function setCardTone(elementId, value) {
  const node = document.getElementById(elementId)?.closest(".card");
  if (!node) return;
  node.classList.remove("is-positive", "is-negative");
  const num = Number(value);
  if (Number.isNaN(num)) return;
  if (num > 0) node.classList.add("is-positive");
  if (num < 0) node.classList.add("is-negative");
}

function normalizeCategory(raw) {
  const value = String(raw || "").trim().toLowerCase();
  if (!value) return "otros";
  return value;
}

async function getJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

async function postJson(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    cache: "no-store",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
  let body = {};
  try {
    body = await response.json();
  } catch (_error) {
    body = {};
  }
  if (!response.ok) {
    const message = body.error || `HTTP ${response.status}`;
    throw new Error(message);
  }
  return body;
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
  document.getElementById("exposureMark").textContent = `mark-to-market ${fmtUsdPlain(Number(summary.exposure_mark ?? summary.exposure ?? 0), 2)}`;

  const pnlTotal = Number(summary.pnl_total ?? summary.cumulative_pnl ?? 0);
  const realized = Number(summary.realized_pnl ?? summary.cumulative_pnl ?? 0);
  const unrealized = Number(summary.unrealized_pnl ?? 0);
  document.getElementById("pnl").textContent = fmtUsd(pnlTotal, 2);
  document.getElementById("pnlBreakdown").textContent = `realized ${fmtUsd(realized, 2)} / unrealized ${fmtUsd(unrealized, 2)}`;
  setCardTone("pnl", pnlTotal);

  document.getElementById("pendingSignals").textContent = String(summary.pending_signals ?? "-");
  document.getElementById("executedSignals").textContent = String(summary.executed_signals ?? 0);
  document.getElementById("failedSignals").textContent = String(summary.failed_signals ?? 0);
  document.getElementById("modeSummary").textContent = modeLabel();

  const modeText =
    runtimeMode === "local"
      ? "local db mode"
      : runtimeMode === "public-fallback"
      ? `public api fallback (${watchedWallet})`
      : `public api mode (${watchedWallet})`;
  const nowText = new Date().toISOString().replace(".000Z", "Z");
  document.getElementById("lastUpdated").textContent = `Ultima actualizacion: ${nowText} | ${modeText}`;
  document.getElementById("headerTimestamp").textContent = nowText;
  document.getElementById("runtimeBadge").textContent = modeLabel();
  document.getElementById("systemNotice").textContent =
    `Pendientes ${summary.pending_signals ?? 0}, ejecutadas ${summary.executed_signals ?? 0}, fallidas ${summary.failed_signals ?? 0}. Resultado diario ${fmtUsd(Number(summary.daily_realized_pnl || 0), 2)}.`;
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
        <span>pnl ${fmtUsd(Number(item.pnl || 0), 2)}</span>
      </li>
    `
    )
    .join("");

  document.getElementById("selectedWalletsMeta").textContent = "Wallets en seguimiento ahora mismo";
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

function paintExposureDonut(summary) {
  const chart = document.getElementById("exposureDonut");
  const legend = document.getElementById("exposureLegend");
  const meta = document.getElementById("exposureDonutMeta");

  const netGain = Math.max(Number(summary.daily_realized_pnl || 0), 0);
  const dailyLoss = Math.max(Number(summary.daily_loss_gross || 0), 0);

  const rows = [
    { label: "ganancia neta", value: netGain, color: DONUT_GAIN_COLOR },
    { label: "perdida diaria", value: dailyLoss, color: DONUT_LOSS_COLOR },
  ].filter((row) => row.value > 0);
  const total = rows.reduce((acc, row) => acc + row.value, 0);

  if (total <= 0) {
    chart.style.background = "conic-gradient(#d6d6d6 0deg 360deg)";
    legend.innerHTML = `<li><span><span class="dot" style="background:#d6d6d6"></span>sin datos</span><span>0%</span></li>`;
    meta.textContent = "sin resultados diarios";
    return;
  }

  let currentDeg = 0;
  const gradientParts = [];
  const legendItems = [];
  rows.forEach((row) => {
    const pct = (row.value / total) * 100;
    const deg = (pct / 100) * 360;
    gradientParts.push(`${row.color} ${currentDeg}deg ${currentDeg + deg}deg`);
    currentDeg += deg;

    legendItems.push(
      `<li><span><span class="dot" style="background:${row.color}"></span>${escapeHtml(row.label)}</span><span>${fmt(pct, 1)}%</span></li>`
    );
  });

  chart.style.background = `conic-gradient(${gradientParts.join(", ")})`;
  legend.innerHTML = legendItems.join("");
  meta.textContent = `hoy neto ${fmtUsd(Number(summary.daily_realized_pnl || 0), 2)} | perdidas ${fmtUsd(-dailyLoss, 2)}`;
}

function paintOperationPnl(items) {
  const body = document.getElementById("opsPnlList");
  const count = document.getElementById("opsPnlCount");
  const meta = document.getElementById("opsPnlMeta");
  const latest = (items || []).slice(0, 6);
  count.textContent = String(latest.length);

  if (!latest.length) {
    body.innerHTML = `<li class="mini-item"><strong>Sin operaciones</strong><span>todavia no hay ejecuciones</span></li>`;
    meta.textContent = "ultimas operaciones";
    return;
  }

  body.innerHTML = latest
    .map((item) => {
      const delta = Number(item.pnl_delta || 0);
      const notional = Math.abs(Number(item.notional || 0));
      const klass = delta > 0 ? "pnl-pos" : delta < 0 ? "pnl-neg" : "pnl-flat";
      return `
        <li class="mini-item">
          <strong>${escapeHtml(tsToIso(item.ts))} | ${escapeHtml(item.action || "-")} ${escapeHtml(item.side || "-")}</strong>
          <span>${escapeHtml(shortWallet(item.source_wallet || item.mode || "-"))}</span>
          <span>metido ${fmtUsd(notional, 2)}</span>
          <span class="${klass}">resultado ${fmtUsd(delta, 4)}</span>
        </li>
      `;
    })
    .join("");

  const sum = latest.reduce((acc, item) => acc + Number(item.pnl_delta || 0), 0);
  const invested = latest.reduce((acc, item) => acc + Math.abs(Number(item.notional || 0)), 0);
  meta.textContent = `ultimas ${latest.length}: metido ${fmtUsd(invested, 2)} | resultado ${fmtUsd(sum, 4)}`;
}

function paintPositions(items) {
  const body = document.getElementById("positionsBody");
  document.getElementById("positionsCount").textContent = String(items.length);

  if (!items.length) {
    body.innerHTML = `<tr><td colspan="7">No hay posiciones copiadas.</td></tr>`;
    return;
  }

  body.innerHTML = items
    .map((item) => {
      const notional = Math.abs(Number(item.size || 0) * Number(item.avg_price || 0));
      const unrealized = Number(item.unrealized_pnl || 0);
      const unrealizedClass = unrealized > 0 ? "pnl-pos" : unrealized < 0 ? "pnl-neg" : "pnl-flat";
      return `
      <tr>
        <td data-label="Mercado">${escapeHtml(item.title || item.slug || item.asset)}</td>
        <td data-label="Outcome">${escapeHtml(item.outcome || "-")}</td>
        <td data-label="Monto">${fmtUsdPlain(notional, 2)}</td>
        <td data-label="Avg">${fmt(item.avg_price)}</td>
        <td data-label="Mark">${fmt(item.mark_price)}</td>
        <td data-label="PnL vivo"><span class="${unrealizedClass}">${fmtUsd(unrealized, 2)}</span></td>
        <td data-label="Realized">${fmtUsd(Number(item.realized_pnl || 0), 2)}</td>
      </tr>
    `;
    })
    .join("");
}

function paintExecutions(items) {
  const body = document.getElementById("executionsBody");
  if (!items.length) {
    body.innerHTML = `<tr><td colspan="6">No hay ejecuciones.</td></tr>`;
    return;
  }

  body.innerHTML = items
    .map((item) => {
      const delta = Number(item.pnl_delta || 0);
      const pnlClass = delta > 0 ? "pnl-pos" : delta < 0 ? "pnl-neg" : "pnl-flat";
      return `
      <tr>
        <td data-label="Hora UTC">${tsToIso(item.ts)}</td>
        <td data-label="Accion">${escapeHtml(item.action)}</td>
        <td data-label="Modo">${escapeHtml(item.mode || "-")} / ${escapeHtml(item.side || "-")}</td>
        <td data-label="Wallet fuente">${escapeHtml(shortWallet(item.source_wallet || "-"))}</td>
        <td data-label="Monto USDC">${fmtUsd(Math.abs(Number(item.notional || 0)), 2)}</td>
        <td data-label="Resultado USD"><span class="${pnlClass}">${fmtUsd(delta, 4)}</span></td>
      </tr>
    `
    })
    .join("");
}

function paintSignals(items) {
  const body = document.getElementById("signalsBody");
  if (!items.length) {
    body.innerHTML = `<tr><td colspan="6">No hay senales todavia.</td></tr>`;
    return;
  }

  body.innerHTML = items
    .map(
      (item) => `
      <tr>
        <td data-label="Hora UTC">${tsToIso(item.detected_at)}</td>
        <td data-label="Mercado">${escapeHtml(item.title || item.slug || item.asset)}</td>
        <td data-label="Accion">${escapeHtml(item.action)} | ${fmt(item.delta_size)}</td>
        <td data-label="Delta">${fmtUsdPlain(Math.abs(Number(item.delta_size || 0) * Number(item.reference_price || 0)), 2)}</td>
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
        safeGetJson(withCacheBust(buildApiUrl("/api/selected-wallets?limit=6")), { items: [] }),
        safeGetJson(withCacheBust(buildApiUrl("/api/risk-blocks?hours=24&limit=5")), {
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
      paintExposureDonut(summary || {});
      paintOperationPnl(executions.items || []);
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
      mark_price: Number(item.curPrice || item.avgPrice || 0),
      unrealized_pnl: 0,
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
      notional: Number(item.size || 0) * Number(item.price || 0),
      source_wallet: watchedWallet,
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
      daily_realized_pnl: 0,
      daily_profit_gross: 0,
      daily_loss_gross: 0,
      pending_signals: "-",
    };

    paintSummary(summary);
    paintPositions(positions);
    paintExecutions(executions);
    paintSignals([]);
    paintSelectedWallets([]);
    paintRiskBlocks({ items: [], hours: 24, blocked_total: 0 });
    paintExposureDonut(summary);
    paintOperationPnl(executions);
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

document.getElementById("resetBtn").addEventListener("click", async () => {
  const button = document.getElementById("resetBtn");
  if (runtimeMode !== "local") {
    document.getElementById("lastUpdated").textContent =
      "Reset no disponible en modo Public API. Usa la URL del backend local.";
    return;
  }

  const accepted = window.confirm(
    "Esto limpiara posiciones, senales, ejecuciones y seleccion actual. Se reiniciara desde cero. Continuar?"
  );
  if (!accepted) return;

  const originalLabel = button.textContent;
  button.disabled = true;
  button.textContent = "Limpiando...";
  try {
    const result = await postJson(withCacheBust(buildApiUrl("/api/reset")), { confirm: "reset" });
    const deleted = result.deleted || {};
    const positions = Number(deleted.copy_positions || 0);
    const executions = Number(deleted.executions || 0);
    const signals = Number(deleted.signals || 0);
    document.getElementById("lastUpdated").textContent =
      `Reset completo: posiciones ${positions}, ejecuciones ${executions}, senales ${signals}.`;
    await refreshAll();
  } catch (error) {
    document.getElementById("lastUpdated").textContent = `Error al limpiar: ${error.message}`;
  } finally {
    button.disabled = false;
    button.textContent = originalLabel || "Limpiar y reiniciar";
  }
});

async function bootstrap() {
  const params = new URLSearchParams(window.location.search);
  watchedWallet = (params.get("wallet") || DEFAULT_WALLET).toLowerCase();
  const apiParam = (params.get("api") || "").trim().replace(/\/+$/, "");
  const savedApiBase = loadSavedApiBase();
  const hostDefaultApi = DEFAULT_REMOTE_API_BY_HOST[window.location.hostname] || "";
  apiBase = apiParam || savedApiBase || hostDefaultApi;
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
  document.getElementById("runtimeBadge").textContent = modeLabel();
  document.getElementById("modeSummary").textContent = modeLabel();

  const resetBtn = document.getElementById("resetBtn");
  if (runtimeMode !== "local") {
    resetBtn.disabled = true;
    resetBtn.title = "Solo disponible cuando el dashboard esta conectado al backend local";
  } else {
    resetBtn.disabled = false;
    resetBtn.title = "";
  }

  await refreshAll();
  configureAutoRefresh();
}

bootstrap();
