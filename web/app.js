const DEFAULT_WALLET = "0xa81f087970a7ce196eacb3271e96e89294d91bb8";
const DATA_API = "https://data-api.polymarket.com";

let runtimeMode = "local";
let watchedWallet = DEFAULT_WALLET;

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

async function getJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

function paintSummary(summary) {
  document.getElementById("openPositions").textContent = String(summary.open_positions ?? 0);
  document.getElementById("exposure").textContent = fmt(summary.exposure, 2);
  document.getElementById("pnl").textContent = fmt(summary.cumulative_pnl, 2);
  document.getElementById("pendingSignals").textContent = String(summary.pending_signals ?? "-");

  const modeText = runtimeMode === "local" ? "local db mode" : `public api mode (${watchedWallet})`;
  document.getElementById("lastUpdated").textContent = `Última actualización: ${new Date().toISOString().replace(".000Z", "Z")} | ${modeText}`;
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
        <td>${escapeHtml(item.title || item.slug || item.asset)}</td>
        <td>${escapeHtml(item.outcome || "-")}</td>
        <td>${fmt(item.size)}</td>
        <td>${fmt(item.avg_price)}</td>
        <td>${fmt(item.realized_pnl)}</td>
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
        <td>${tsToIso(item.ts)}</td>
        <td>${escapeHtml(item.mode)}</td>
        <td>${escapeHtml(item.action)}</td>
        <td>${escapeHtml(item.side)}</td>
        <td>${fmt(item.size)}</td>
        <td>${fmt(item.price)}</td>
        <td>${fmt(item.pnl_delta)}</td>
      </tr>
    `
    )
    .join("");
}

function paintSignals(items) {
  const body = document.getElementById("signalsBody");
  if (!items.length) {
    body.innerHTML = `<tr><td colspan="7">No hay señales todavía.</td></tr>`;
    return;
  }

  body.innerHTML = items
    .map(
      (item) => `
      <tr>
        <td>${tsToIso(item.detected_at)}</td>
        <td>${escapeHtml(item.action)}</td>
        <td>${fmt(item.prev_size)}</td>
        <td>${fmt(item.new_size)}</td>
        <td>${fmt(item.delta_size)}</td>
        <td>${statusPill(item.status)}</td>
        <td>${escapeHtml(item.note || "")}</td>
      </tr>
    `
    )
    .join("");
}

async function refreshAll() {
  try {
    if (runtimeMode === "local") {
      const [summary, positions, executions, signals] = await Promise.all([
        getJson("/api/summary"),
        getJson("/api/positions"),
        getJson("/api/executions?limit=50"),
        getJson("/api/signals?limit=100"),
      ]);

      paintSummary(summary);
      paintPositions(positions.items || []);
      paintExecutions(executions.items || []);
      paintSignals(signals.items || []);
      return;
    }

    const [positionsRaw, activityRaw] = await Promise.all([
      getJson(`${DATA_API}/positions?user=${encodeURIComponent(watchedWallet)}&limit=200`),
      getJson(`${DATA_API}/activity?user=${encodeURIComponent(watchedWallet)}&limit=100`),
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

    const summary = {
      open_positions: positions.length,
      exposure: positions.reduce((acc, item) => acc + item.size * item.avg_price, 0),
      cumulative_pnl: positions.reduce((acc, item) => acc + Number(item.realized_pnl || 0), 0),
      pending_signals: "-",
    };

    paintSummary(summary);
    paintPositions(positions);
    paintExecutions(executions);
    paintSignals([]);
  } catch (error) {
    document.getElementById("lastUpdated").textContent = `Error de actualización: ${error.message}`;
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

document.getElementById("refreshBtn").addEventListener("click", () => {
  refreshAll();
  configureAutoRefresh();
});

document.getElementById("refreshSeconds").addEventListener("change", () => {
  configureAutoRefresh();
});

async function bootstrap() {
  const params = new URLSearchParams(window.location.search);
  watchedWallet = (params.get("wallet") || DEFAULT_WALLET).toLowerCase();

  try {
    await getJson("/api/health");
    runtimeMode = "local";
  } catch (_error) {
    runtimeMode = "public";
  }

  document.querySelector(".kicker").textContent =
    runtimeMode === "local" ? "Copy Trading Monitor (Local DB)" : "Copy Trading Monitor (Public API)";

  await refreshAll();
  configureAutoRefresh();
}

bootstrap();
