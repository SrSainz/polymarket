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
let lastSummary = null;
let lastPositions = [];
let lastExecutions = [];

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

function fmtBtcPrice(value) {
  const asNumber = Number(value);
  if (Number.isNaN(asNumber) || asNumber <= 0) return "-";
  return asNumber.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function fmtPct(value, digits = 0) {
  const asNumber = Number(value);
  if (Number.isNaN(asNumber)) return "-";
  return `${asNumber.toFixed(digits)}%`;
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

function tradingModeLabel(summary) {
  const isLive = Boolean(summary.live_mode_active);
  return isLive ? "LIVE" : "PAPER";
}

function strategyLabel(summary) {
  const mode = String(summary.strategy_mode || "").trim();
  const entry = String(summary.strategy_entry_mode || "").trim();
  if (!mode) return "-";
  if (entry === "arb_micro") return "Arbitraje BTC5m";
  if (entry === "vidarx_micro") return "Simulador Vidarx";
  if (mode !== "btc5m_orderbook") return mode;
  return `BTC5m / ${entry || "-"}`;
}

function isVidarxLab(summary = lastSummary) {
  return Boolean(summary && ["vidarx_micro", "arb_micro"].includes(summary.strategy_entry_mode));
}

function currentBreakdown(summary) {
  return Array.isArray(summary?.strategy_current_market_breakdown) ? summary.strategy_current_market_breakdown : [];
}

function friendlyOutcomeName(raw) {
  const value = String(raw || "").trim().toLowerCase();
  if (value === "up") return "Sube";
  if (value === "down") return "Baja";
  return String(raw || "-");
}

function ratioLabel(summary) {
  const breakdown = currentBreakdown(summary);
  if (breakdown.length >= 2) {
    const [first, second] = breakdown;
    return `${friendlyOutcomeName(first.outcome)} ${fmtPct(first.share_pct, 0)} / ${friendlyOutcomeName(second.outcome)} ${fmtPct(second.share_pct, 0)}`;
  }
  if (summary?.strategy_primary_outcome) {
    const primaryPct = Number(summary.strategy_primary_ratio || 0) * 100;
    const hedgePct = Math.max(100 - primaryPct, 0);
    if (summary.strategy_hedge_outcome) {
      return `${friendlyOutcomeName(summary.strategy_primary_outcome)} ${fmtPct(primaryPct, 0)} / ${friendlyOutcomeName(summary.strategy_hedge_outcome)} ${fmtPct(hedgePct, 0)}`;
    }
    return `${friendlyOutcomeName(summary.strategy_primary_outcome)} ${fmtPct(primaryPct, 0)}`;
  }
  return summary?.strategy_market_bias || "-";
}

function desiredRatioLabel(summary) {
  const upRatio = Number(summary?.strategy_desired_up_ratio ?? 0.5);
  const downRatio = Number(summary?.strategy_desired_down_ratio ?? Math.max(1 - upRatio, 0));
  if (Number.isNaN(upRatio) || Number.isNaN(downRatio)) return "-";
  return `${friendlyOutcomeName("up")} ${fmtPct(upRatio * 100, 0)} / ${friendlyOutcomeName("down")} ${fmtPct(downRatio * 100, 0)}`;
}

function actualRatioLabel(summary) {
  const breakdown = currentBreakdown(summary);
  if (breakdown.length >= 2) {
    const [first, second] = breakdown;
    return `${friendlyOutcomeName(first.outcome)} ${fmtPct(first.share_pct, 0)} / ${friendlyOutcomeName(second.outcome)} ${fmtPct(second.share_pct, 0)}`;
  }
  const upRatio = Number(summary?.strategy_current_up_ratio ?? summary?.strategy_primary_ratio ?? 0.5);
  if (Number.isNaN(upRatio)) return "-";
  return `${friendlyOutcomeName("up")} ${fmtPct(upRatio * 100, 0)} / ${friendlyOutcomeName("down")} ${fmtPct(Math.max((1 - upRatio) * 100, 0), 0)}`;
}

function bracketPhaseLabel(summary) {
  const raw = String(summary?.strategy_bracket_phase || "").trim().toLowerCase();
  if (raw === "abrir") return "Abriendo bracket";
  if (raw === "redistribuir") return "Recalibrando reparto";
  if (raw === "acompanar") return "Acompañando sesgo";
  if (raw === "observando") return "Observando";
  return raw || "Observando";
}

function timingLabel(summary) {
  const regime = String(summary?.strategy_timing_regime || "").trim();
  if (!regime) return "-";
  if (regime === "early-mid") return "inicio-mitad";
  if (regime === "mid-late") return "mitad-final";
  return regime;
}

function feedModeInfo(summary) {
  const source = String(summary?.strategy_data_source || "rest-fallback");
  const ageMs = Number(summary?.strategy_feed_age_ms || 0);
  const trackedAssets = Number(summary?.strategy_feed_tracked_assets || 0);
  const connected = Boolean(summary?.strategy_feed_connected);

  if (source === "websocket") {
    return {
      label: "WebSocket",
      meta: `${ageMs} ms | ${trackedAssets} assets`,
      className: "feed-pill ws",
      summaryLabel: `ws ${ageMs}ms / ${trackedAssets} assets`,
      connected,
    };
  }
  if (source === "websocket-warming") {
    return {
      label: "WS calentando",
      meta: `${trackedAssets} assets`,
      className: "feed-pill warming",
      summaryLabel: `ws calentando / ${trackedAssets} assets`,
      connected,
    };
  }
  return {
    label: "REST fallback",
    meta: connected ? "cache parcial" : "sin ws",
    className: "feed-pill rest",
    summaryLabel: "rest fallback",
    connected,
  };
}

function currentEdgeInfo(summary) {
  if (String(summary?.strategy_entry_mode || "") !== "arb_micro") {
    return { pairSum: null, edgePct: null, fairValue: null, label: "sin edge" };
  }
  const storedPairSum = Number(summary?.strategy_pair_sum || 0);
  const triggerValue = Number(summary?.strategy_trigger_price_seen || 0);
  const pairSum = storedPairSum || triggerValue;
  const fairValue = Number(summary?.strategy_fair_value || 0);
  const storedEdge = Number(summary?.strategy_edge_pct || 0);
  if ((!pairSum || Number.isNaN(pairSum)) && (!storedEdge || Number.isNaN(storedEdge))) {
    return { pairSum: null, edgePct: null, fairValue: null, label: "sin edge" };
  }
  const priceMode = String(summary?.strategy_price_mode || "");
  return {
    pairSum: pairSum || null,
    edgePct: storedEdge ? storedEdge * 100 : Math.max((1 - pairSum) * 100, 0),
    fairValue: fairValue || null,
    label: priceMode === "cheap-side" ? "lado barato" : "arbitraje cerrado",
  };
}

function currentSpotInfo(summary) {
  const current = Number(summary?.strategy_spot_price || 0);
  const anchor = Number(summary?.strategy_spot_anchor || 0);
  const deltaBps = Number(summary?.strategy_spot_delta_bps || 0);
  const fairUp = Number(summary?.strategy_spot_fair_up || 0);
  const fairDown = Number(summary?.strategy_spot_fair_down || 0);
  const ageMs = Number(summary?.strategy_spot_age_ms || 0);
  const source = String(summary?.strategy_spot_source || "").trim();
  const binance = Number(summary?.strategy_spot_binance || 0);
  const chainlink = Number(summary?.strategy_spot_chainlink || 0);
  const deltaUsd = current > 0 && anchor > 0 ? current - anchor : 0;
  return {
    available: current > 0 && anchor > 0,
    hasCurrent: current > 0,
    hasAnchor: anchor > 0,
    current,
    anchor,
    deltaBps,
    deltaUsd,
    fairUp,
    fairDown,
    ageMs,
    source: source || "-",
    binance,
    chainlink,
  };
}

function latestStrategyExecution() {
  return (Array.isArray(lastExecutions) ? lastExecutions : []).find((item) =>
    String(item?.source_wallet || "").toLowerCase().includes("strategy")
  );
}

function currentWindowDirection(summary) {
  const breakdown = currentBreakdown(summary);
  if (!breakdown.length) return "Sin posicion abierta";
  return breakdown.map((item) => `${friendlyOutcomeName(item.outcome)} ${fmtPct(item.share_pct, 0)}`).join(" / ");
}

function simplifiedStrategyReason(summary) {
  const note = String(summary?.strategy_last_note || "").trim();
  const noteLower = note.toLowerCase();
  const pairSum = Number(summary?.strategy_pair_sum || 0);
  const openExposure = Number(summary?.strategy_current_market_total_exposure || 0);

  if (!note) return "Esperando a que aparezca un arbitraje con margen real.";
  if (noteLower.includes("drawdown stop")) {
    return "El simulador se ha parado porque supero la perdida maxima permitida.";
  }
  if (noteLower.includes("open market limit reached")) {
    return openExposure > 0
      ? `Ya hay un bracket abierto con ${fmtUsdPlain(openExposure, 2)} metidos y esperamos a que cierre.`
      : "Ya hay un bracket abierto y no abrimos otro hasta que cierre.";
  }
  if (noteLower.includes("no locked edge")) {
    const strongestEdge = Math.max(Number(summary?.strategy_edge_pct || 0) * 100, 0);
    return pairSum > 0
      ? strongestEdge >= 8
        ? `La ventaja direccional existe, pero el precio conjunto sigue caro. La suma va por ${fmt(pairSum, 3)} y el bot espera mejor entrada.`
        : `No hay margen bloqueado suficiente ahora mismo. La suma de las dos patas va por ${fmt(pairSum, 3)}.`
      : "No hay margen bloqueado suficiente ahora mismo.";
  }
  if (noteLower.includes("incomplete book")) {
    return "Una de las dos patas tiene poca liquidez visible y el bot prefiere esperar.";
  }
  if (noteLower.includes("market cap exhausted")) {
    return "Ya hay bastante dinero metido en este bracket y no compensa seguir cargando.";
  }
  if (noteLower.includes("max fills")) {
    return "Ya se alcanzo el maximo de compras permitido para este bracket.";
  }
  if (noteLower.includes("cooldown")) {
    return "Acabamos de comprar en este bracket y esperamos unos segundos antes de repetir.";
  }
  if (noteLower.includes("no active btc5m market")) {
    return "Todavia no hay un bracket BTC 5m listo para operar.";
  }
  return note;
}

function friendlyWindowState(summary) {
  const openExposure = Number(summary?.strategy_current_market_total_exposure || 0);
  const currentLivePnl = Number(summary?.strategy_current_market_live_pnl || 0);
  const lastExecution = latestStrategyExecution();
  const lastAction = String(lastExecution?.action || "").toLowerCase();
  const lastExecutionAgeSeconds = lastExecution ? Math.max((Date.now() / 1000) - Number(lastExecution.ts || 0), 0) : Infinity;
  const note = String(summary?.strategy_last_note || "").toLowerCase();

  if (note.includes("drawdown stop")) {
    return { label: "Parado por perdida maxima", detail: simplifiedStrategyReason(summary) };
  }
  if (note.includes("open market limit reached")) {
    return { label: "Bloqueado por bracket abierto", detail: simplifiedStrategyReason(summary) };
  }
  if (openExposure > 0) {
    if (lastAction === "close" && lastExecutionAgeSeconds <= 90) {
      return {
        label: "Cerrando",
        detail: `La ventana anterior se esta liquidando. Quedan ${fmtUsdPlain(openExposure, 2)} abiertos y va ${fmtUsd(currentLivePnl, 2)} en vivo.`,
      };
    }
    return {
      label: "Comprando",
      detail: `Dentro de esta ventana el bot va ${currentWindowDirection(summary)}. Lleva ${fmtUsdPlain(openExposure, 2)} metidos y ${fmtUsd(currentLivePnl, 2)} en vivo.`,
    };
  }
  if (lastAction === "close" && lastExecutionAgeSeconds <= 90) {
    return {
      label: "Cerrando",
      detail: `Acaba de cerrar la ventana anterior con ${fmtUsd(Number(lastExecution?.pnl_delta || 0), 2)} de resultado.`,
    };
  }
  return { label: "Esperando arbitraje", detail: simplifiedStrategyReason(summary) };
}

function shortSlug(slug) {
  const value = String(slug || "");
  if (!value) return "-";
  return value.replace("btc-updown-5m-", "BTC5m ");
}

function setLiveBadge(summary) {
  const badge = document.getElementById("tradingBadge");
  if (!badge) return;
  const isLive = Boolean(summary.live_mode_active);
  badge.textContent = tradingModeLabel(summary);
  badge.classList.remove("live-badge", "paper-badge");
  badge.classList.add(isLive ? "live-badge" : "paper-badge");
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

function normalizeMarketText(...parts) {
  return parts
    .map((part) => String(part || "").toLowerCase())
    .join(" ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function isBtc5mMarket(item) {
  const haystack = normalizeMarketText(item?.title, item?.slug, item?.category, item?.event_slug);
  if (!haystack) return false;

  const hasBtc = haystack.includes("btc") || haystack.includes("bitcoin");
  const hasFiveMinuteWindow =
    haystack.includes("5m") ||
    haystack.includes("5 min") ||
    haystack.includes("5 mins") ||
    haystack.includes("5 minute") ||
    haystack.includes("5 minutes") ||
    haystack.includes("next 5 minute") ||
    haystack.includes("next 5 minutes");
  const hasDirection =
    haystack.includes("up or down") || haystack.includes("updown") || haystack.includes("up down");

  return hasBtc && hasFiveMinuteWindow && hasDirection;
}

function summarizeBucket(bucketItems) {
  const items = Array.isArray(bucketItems) ? bucketItems : [];
  return {
    count: items.length,
    exposure: items.reduce((acc, item) => acc + Math.abs(Number(item.size || 0) * Number(item.avg_price || 0)), 0),
    unrealized: items.reduce((acc, item) => acc + Number(item.unrealized_pnl || 0), 0),
  };
}

function splitPositionBuckets(summary, items) {
  const safeItems = Array.isArray(items) ? items : [];
  const totalSummary = summarizeBucket(safeItems);
  if (!isVidarxLab(summary)) {
    return {
      currentItems: safeItems,
      archivedItems: [],
      currentSummary: totalSummary,
      archivedSummary: summarizeBucket([]),
      totalCount: safeItems.length,
      totalExposure: totalSummary.exposure,
    };
  }

  const activeVidarxSlug = String(summary?.strategy_market_slug || "");
  const currentItems = safeItems.filter(
    (item) => isBtc5mMarket(item) && (!activeVidarxSlug || String(item.slug || "") === activeVidarxSlug)
  );
  const archivedItems = safeItems.filter(
    (item) => !isBtc5mMarket(item) || (activeVidarxSlug && String(item.slug || "") !== activeVidarxSlug)
  );

  return {
    currentItems,
    archivedItems,
    currentSummary: summarizeBucket(currentItems),
    archivedSummary: summarizeBucket(archivedItems),
    totalCount: safeItems.length,
    totalExposure: totalSummary.exposure,
  };
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

function paintSummary(summary, items = lastPositions) {
  lastSummary = summary;
  const buckets = splitPositionBuckets(summary, items);
  const windowState = friendlyWindowState(summary);
  const currentWindowExposure = Number(summary.strategy_current_market_total_exposure ?? buckets.currentSummary.exposure ?? 0);
  const totalExposure = Number(summary.exposure ?? buckets.totalExposure ?? 0);
  const totalExposureMark = Number(summary.exposure_mark ?? totalExposure);
  document.getElementById("openPositionsLabel").textContent = isVidarxLab(summary)
    ? "Patas abiertas en esta ventana"
    : "Patas abiertas ahora";
  document.getElementById("openPositions").textContent = String(buckets.currentSummary.count);
  document.getElementById("openPositionsMeta").textContent = isVidarxLab(summary)
    ? `totales simulador ${summary.open_positions ?? buckets.totalCount ?? 0}`
    : "operaciones vivas ahora mismo";
  document.getElementById("exposureLabel").textContent = isVidarxLab(summary)
    ? "Dinero metido en esta ventana"
    : "Dinero metido total";
  document.getElementById("exposure").textContent = fmtUsdPlain(
    isVidarxLab(summary) ? currentWindowExposure : totalExposure,
    2
  );
  document.getElementById("exposureMark").textContent = isVidarxLab(summary)
    ? `total simulador ${fmtUsdPlain(totalExposure, 2)} | valor vivo ${fmtUsdPlain(totalExposureMark, 2)}`
    : `valor ahora ${fmtUsdPlain(totalExposureMark, 2)}`;

  const pnlTotal = Number(summary.pnl_total ?? summary.cumulative_pnl ?? 0);
  const realized = Number(summary.realized_pnl ?? summary.cumulative_pnl ?? 0);
  const unrealized = Number(summary.unrealized_pnl ?? 0);
  document.getElementById("pnl").textContent = fmtUsd(pnlTotal, 2);
  document.getElementById("pnlBreakdown").textContent = `cerrado ${fmtUsd(realized, 2)} / en vivo ${fmtUsd(unrealized, 2)}`;
  setCardTone("pnl", pnlTotal);

  document.getElementById("pendingSignals").textContent = isVidarxLab(summary)
    ? String(summary.strategy_plan_legs ?? 0)
    : String(summary.pending_signals ?? "-");
  const liveCashBalance = Number(summary.live_cash_balance ?? 0);
  const liveAvailableToTrade = Number(summary.live_available_to_trade ?? liveCashBalance);
  const liveEquityEstimate = Number(summary.live_equity_estimate ?? summary.live_total_capital ?? liveCashBalance);
  const liveBalanceUpdatedAt = Number(summary.live_balance_updated_at ?? 0);
  const liveSnapshotText = liveBalanceUpdatedAt > 0 ? tsToIso(liveBalanceUpdatedAt) : "sin snapshot";
  document.getElementById("liveCashBalance").textContent = fmtUsdPlain(liveEquityEstimate, 2);
  document.getElementById("liveCashMeta").textContent =
    `disponible ${fmtUsdPlain(liveAvailableToTrade, 2)} | caja ${fmtUsdPlain(liveCashBalance, 2)} | snapshot ${liveSnapshotText}`;
  document.getElementById("heroCashBalance").textContent = fmtUsdPlain(liveAvailableToTrade, 2);
  document.getElementById("heroCashMeta").textContent =
    `capital total ${fmtUsdPlain(liveEquityEstimate, 2)} | caja ${fmtUsdPlain(liveCashBalance, 2)}`;
  const liveExecutionsTodayNode = document.getElementById("liveExecutionsToday");
  if (liveExecutionsTodayNode) {
    liveExecutionsTodayNode.textContent = String(summary.live_executions_today ?? 0);
  }
  const livePnlToday = Number(summary.live_realized_pnl_today ?? 0);
  const livePnlNode = document.getElementById("livePnlToday");
  if (livePnlNode) {
    livePnlNode.textContent = fmtUsd(livePnlToday, 2);
    livePnlNode.classList.remove("pnl-pos", "pnl-neg", "pnl-flat");
    livePnlNode.classList.add(livePnlToday > 0 ? "pnl-pos" : livePnlToday < 0 ? "pnl-neg" : "pnl-flat");
  }
  document.getElementById("strategyModeCard").textContent = isVidarxLab(summary)
    ? windowState.label
    : strategyLabel(summary);
  const strategyTitle = String(summary.strategy_market_title || summary.strategy_market_slug || "sin setup");
  const strategyOutcome = String(summary.strategy_target_outcome || "");
  const strategyPrice = Number(summary.strategy_target_price || 0);
  const triggerOutcome = String(summary.strategy_trigger_outcome || "");
  const triggerPrice = Number(summary.strategy_trigger_price_seen || 0);
  const strategyNote = String(summary.strategy_last_note || "");
  const strategyWindowSeconds = Number(summary.strategy_window_seconds || 0);
  const strategyPlanLegs = Number(summary.strategy_plan_legs || 0);
  const strategyBias = ratioLabel(summary);
  const desiredRatio = desiredRatioLabel(summary);
  const actualRatio = actualRatioLabel(summary);
  const bracketPhase = bracketPhaseLabel(summary);
  const strategyCycleBudget = Number(summary.strategy_cycle_budget || 0);
  const strategyDataSource = String(summary.strategy_data_source || "rest-fallback");
  const strategyFeedConnected = Boolean(summary.strategy_feed_connected);
  const strategyFeedAgeMs = Number(summary.strategy_feed_age_ms || 0);
  const strategyFeedTrackedAssets = Number(summary.strategy_feed_tracked_assets || 0);
  const feedInfo = feedModeInfo(summary);
  const edgeInfo = currentEdgeInfo(summary);
  const spotInfo = currentSpotInfo(summary);
  const strategySpeedLabel = feedInfo.summaryLabel;
  const currentMarketLivePnl = Number(summary.strategy_current_market_live_pnl || buckets.currentSummary.unrealized || 0);
  const currentMarketExposure = Number(summary.strategy_current_market_total_exposure || summary.strategy_current_market_exposure || buckets.currentSummary.exposure || 0);
  const replenishmentCount = Number(summary.strategy_replenishment_count || 0);
  const timing = timingLabel(summary);
  const strategyNoteText = strategyNote || "sin trigger";
  document.getElementById("strategyCardMeta").textContent = isVidarxLab(summary)
    ? windowState.detail
    : strategyOutcome
    ? `${strategyOutcome} @ ${fmt(strategyPrice, 3)} | ${strategyTitle}`
    : strategyNote || strategyTitle;
  document.getElementById("strategyHeroTitle").textContent = strategyTitle;
  document.getElementById("heroTargetOutcome").textContent = isVidarxLab(summary)
    ? `${currentWindowDirection(summary)} | objetivo ${desiredRatio}`
    : strategyOutcome
    ? `${strategyOutcome} @ ${fmt(strategyPrice, 3)}`
    : "-";
  document.getElementById("heroTriggerSeen").textContent = isVidarxLab(summary)
    ? strategyWindowSeconds > 0
      ? `${timing} | ${strategyWindowSeconds}s | ${strategyPlanLegs} compras | ${bracketPhase}`
      : "-"
    : triggerOutcome
    ? `${triggerOutcome} @ ${fmt(triggerPrice, 3)}`
    : "-";
  const heroFeedSource = document.getElementById("heroFeedSource");
  heroFeedSource.textContent = feedInfo.label;
  heroFeedSource.className = feedInfo.className;
  document.getElementById("heroFeedMeta").textContent =
    edgeInfo.pairSum !== null
      ? `${edgeInfo.label} | pair sum ${fmt(edgeInfo.pairSum, 3)} | edge ${fmt(edgeInfo.edgePct, 2)}%${edgeInfo.fairValue ? ` | fair ${fmt(edgeInfo.fairValue, 3)}` : ""}${spotInfo.hasCurrent ? ` | BTC ${fmtBtcPrice(spotInfo.current)}` : ""}${spotInfo.hasAnchor ? ` vs beat ${fmtBtcPrice(spotInfo.anchor)}` : ""}${spotInfo.available ? ` (${fmt(spotInfo.deltaBps, 1)}bps)` : ""} | ${feedInfo.meta}`
      : `${spotInfo.hasCurrent ? `BTC ${fmtBtcPrice(spotInfo.current)}${spotInfo.hasAnchor ? ` vs beat ${fmtBtcPrice(spotInfo.anchor)}` : ""}${spotInfo.available ? ` (${fmt(spotInfo.deltaBps, 1)}bps)` : ""} | ` : ""}${feedInfo.meta} | ${windowState.label.toLowerCase()}`;
  document.getElementById("strategyBadge").textContent = strategyLabel(summary);
  setLiveBadge(summary);

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
  const lastLiveExecution = Number(summary.last_live_execution_ts || 0);
  const lastLiveText = lastLiveExecution > 0 ? tsToIso(lastLiveExecution) : "sin operaciones live";
  document.getElementById("systemNotice").textContent = isVidarxLab(summary)
    ? currentMarketExposure > 0
      ? `${windowState.label}. ${windowState.detail} Objetivo ${desiredRatio}, actual ${actualRatio}, fase ${bracketPhase.toLowerCase()}. En total, el simulador lleva ${fmtUsd(pnlTotal, 2)} con capital ${fmtUsdPlain(liveEquityEstimate, 2)} y caja ${fmtUsdPlain(liveAvailableToTrade, 2)}. Ventanas cerradas hoy ${summary.strategy_resolution_count_today ?? 0}, resultado ${fmtUsd(Number(summary.strategy_resolution_pnl_today || 0), 2)}.`
      : `${windowState.label}. ${windowState.detail} Objetivo ${desiredRatio}, fase ${bracketPhase.toLowerCase()}. El simulador total lleva ${fmtUsd(pnlTotal, 2)} con capital ${fmtUsdPlain(liveEquityEstimate, 2)} y caja ${fmtUsdPlain(liveAvailableToTrade, 2)}. Ventanas cerradas hoy ${summary.strategy_resolution_count_today ?? 0}, resultado ${fmtUsd(Number(summary.strategy_resolution_pnl_today || 0), 2)}.`
    : `Modo ${tradingModeLabel(summary)}. Disponible ${fmtUsdPlain(liveAvailableToTrade, 2)}, saldo wallet ${fmtUsdPlain(liveCashBalance, 2)}, equity bot ${fmtUsdPlain(liveEquityEstimate, 2)}. Estrategia ${strategyLabel(summary)}: ${strategyNoteText}. Live hoy ${summary.live_executions_today ?? 0} ops, PnL ${fmtUsd(livePnlToday, 2)}, ultima live ${lastLiveText}.`;

  paintLabOverview(summary);
}

function paintLabOverview(summary) {
  const windowSeconds = Math.max(Number(summary.strategy_window_seconds || 0), 0);
  const windowPct = Math.min((windowSeconds / 300) * 100, 100);
  const deployed = Math.max(Number(summary.strategy_current_market_total_exposure || summary.strategy_current_market_exposure || 0), 0);
  const cycleBudget = Math.max(Number(summary.strategy_cycle_budget || 0), 0);
  const exposurePct = cycleBudget > 0 ? Math.min((deployed / cycleBudget) * 100, 100) : 0;
  const feedInfo = feedModeInfo(summary);
  const edgeInfo = currentEdgeInfo(summary);
  const spotInfo = currentSpotInfo(summary);

  document.getElementById("labModeValue").textContent = isVidarxLab(summary)
    ? `${timingLabel(summary)} / ${String(summary.strategy_price_mode || "sin banda").replaceAll("-", " ")}`
    : strategyLabel(summary);
  document.getElementById("labWindowValue").textContent =
    String(summary.strategy_market_title || summary.strategy_market_slug || "-");
  document.getElementById("labFeedValue").textContent = feedInfo.label;
  document.getElementById("labSpotCurrent").textContent = spotInfo.hasCurrent ? fmtBtcPrice(spotInfo.current) : "-";
  document.getElementById("labSpotAnchor").textContent = spotInfo.hasAnchor ? fmtBtcPrice(spotInfo.anchor) : "-";
  document.getElementById("labSpotDelta").textContent =
    spotInfo.available ? `${fmtUsd(spotInfo.deltaUsd, 2)} | ${fmt(spotInfo.deltaBps, 1)}bps` : spotInfo.hasCurrent ? "esperando ancla" : "-";
  document.getElementById("labSpotFair").textContent =
    spotInfo.available ? `Sube ${fmtPct(spotInfo.fairUp * 100, 1)} / Baja ${fmtPct(spotInfo.fairDown * 100, 1)}` : spotInfo.hasCurrent ? "esperando ancla" : "-";
  document.getElementById("labEdgeValue").textContent =
    edgeInfo.edgePct !== null
      ? `${edgeInfo.label} | ${fmt(edgeInfo.edgePct, 2)}%${edgeInfo.fairValue ? ` | fair ${fmt(edgeInfo.fairValue, 3)}` : ""}`
      : "-";
  document.getElementById("labWindowFill").style.width = `${windowPct}%`;
  document.getElementById("labExposureFill").style.width = `${exposurePct}%`;
  document.getElementById("labMeta").textContent = isVidarxLab(summary)
    ? `ventana ${windowSeconds}s | objetivo ${desiredRatioLabel(summary)} | actual ${actualRatioLabel(summary)} | ${bracketPhaseLabel(summary).toLowerCase()} | compras ${summary.strategy_plan_legs || 0} | ${edgeInfo.label} ${edgeInfo.pairSum !== null ? fmt(edgeInfo.pairSum, 3) : "-"} | ${spotInfo.hasCurrent ? `${spotInfo.source} ${spotInfo.ageMs}ms | BTC ${fmtBtcPrice(spotInfo.current)}${spotInfo.hasAnchor ? ` | beat ${fmtBtcPrice(spotInfo.anchor)}` : ""}` : feedInfo.summaryLabel} | dinero metido ${fmtUsdPlain(deployed, 2)}`
    : `modo ${strategyLabel(summary)} | trigger ${summary.strategy_trigger_outcome || "-"} @ ${fmt(Number(summary.strategy_trigger_price_seen || 0), 3)}`;
}

function paintSelectedWallets(items) {
  const body = document.getElementById("selectedWalletsList");
  if (isVidarxLab()) {
    const breakdown = currentBreakdown(lastSummary);
    const state = friendlyWindowState(lastSummary);
    const currentExposure = Number(lastSummary?.strategy_current_market_total_exposure || 0);
    const currentLivePnl = Number(lastSummary?.strategy_current_market_live_pnl || 0);
    const planRows = [
      ["Estado", state.label],
      ["Objetivo", desiredRatioLabel(lastSummary)],
      ["Actual", actualRatioLabel(lastSummary)],
      ["Fase", bracketPhaseLabel(lastSummary)],
      ["Reparto actual", currentWindowDirection(lastSummary)],
      ["Dinero metido", fmtUsdPlain(currentExposure, 2)],
      ["PnL de esta ventana", fmtUsd(currentLivePnl, 2)],
      ...breakdown.map((item) => [
        friendlyOutcomeName(item.outcome),
        `${fmtPct(item.share_pct, 0)} del dinero | ${fmtUsdPlain(Number(item.exposure || 0), 2)} | vivo ${fmtUsd(Number(item.unrealized_pnl || 0), 2)}`,
      ]),
    ];
    document.getElementById("selectedWalletsCount").textContent = String(planRows.length);
    body.innerHTML = planRows
      .map(
        ([label, value]) => `
      <li class="mini-item">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(value)}</span>
      </li>
    `
      )
      .join("");
    document.getElementById("selectedWalletsMeta").textContent = state.detail;
    return;
  }

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
  if (isVidarxLab()) {
    const body = document.getElementById("riskBlocksList");
    const items = Array.isArray(lastSummary?.strategy_recent_resolutions) ? lastSummary.strategy_recent_resolutions : [];
    document.getElementById("riskBlocksCount").textContent = String(items.length);
    if (!items.length) {
      body.innerHTML = `<li class="mini-item"><strong>Sin cierres todavia</strong><span>cuando se resuelva una ventana aparecera aqui</span></li>`;
      document.getElementById("riskBlocksMeta").textContent = "sin ventanas cerradas aun";
      return;
    }
    body.innerHTML = items
      .map(
        (item) => `
      <li class="mini-item">
        <strong>${escapeHtml(shortSlug(item.slug))}</strong>
        <span>${escapeHtml(item.winning_outcome || "sin ganador")} | ${fmtUsdPlain(Number(item.notional || 0), 2)}</span>
        <span class="${Number(item.pnl || 0) > 0 ? "pnl-pos" : Number(item.pnl || 0) < 0 ? "pnl-neg" : "pnl-flat"}">${fmtUsd(Number(item.pnl || 0), 2)}</span>
      </li>
    `
      )
      .join("");
    document.getElementById("riskBlocksMeta").textContent = `hoy ${lastSummary?.strategy_resolution_count_today || 0} ventanas | ${fmtUsd(Number(lastSummary?.strategy_resolution_pnl_today || 0), 2)}`;
    return;
  }

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
    meta.textContent = "ultimos movimientos";
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
          <span>${escapeHtml(shortSlug(item.notes?.startsWith("vidarx_resolution:") ? item.notes.split(":")[1] : item.slug || item.source_wallet || item.mode || "-"))}</span>
          <span>metido ${fmtUsdPlain(notional, 2)}</span>
          <span class="${klass}">resultado ${fmtUsd(delta, 4)}</span>
        </li>
      `;
    })
    .join("");

  const sum = latest.reduce((acc, item) => acc + Number(item.pnl_delta || 0), 0);
  const invested = latest.reduce((acc, item) => acc + Math.abs(Number(item.notional || 0)), 0);
  meta.textContent = `ultimos ${latest.length}: metido ${fmtUsdPlain(invested, 2)} | resultado ${fmtUsd(sum, 4)}`;
}

function paintStrategySetups(summary) {
  const body = document.getElementById("strategySetupsList");
  const count = document.getElementById("strategySetupsCount");
  const meta = document.getElementById("strategySetupsMeta");
  const items = Array.isArray(summary?.strategy_setup_performance) ? summary.strategy_setup_performance : [];
  count.textContent = String(items.length);

  if (!items.length) {
    body.innerHTML = `<li class="mini-item"><strong>Sin datos cerrados</strong><span>Necesitamos mas ventanas resueltas para comparar setups</span></li>`;
    meta.textContent = "sin historial suficiente";
    return;
  }

  body.innerHTML = items
    .map((item) => {
      const pnlClass =
        Number(item.pnl_total || 0) > 0 ? "pnl-pos" : Number(item.pnl_total || 0) < 0 ? "pnl-neg" : "pnl-flat";
      const ratioPct = Number(item.primary_ratio_avg || 0) * 100;
      return `
        <li class="mini-item">
          <strong>${escapeHtml(String(item.price_mode || "-"))} / ${escapeHtml(timingLabel({ strategy_timing_regime: item.timing_regime }))}</strong>
          <span>${Number(item.windows || 0)} ventanas | acierto ${fmtPct(Number(item.win_rate_pct || 0), 0)} | sesgo ${fmtPct(ratioPct, 0)}</span>
          <span class="${pnlClass}">total ${fmtUsd(Number(item.pnl_total || 0), 2)} | media ${fmtUsd(Number(item.pnl_avg || 0), 2)}</span>
        </li>
      `;
    })
    .join("");

  const best = items[0];
  meta.textContent = `mejor ahora: ${String(best.price_mode || "-")} / ${timingLabel({ strategy_timing_regime: best.timing_regime })} | ${fmtUsd(Number(best.pnl_total || 0), 2)}`;
}

function renderPositionRows(items, emptyLabel) {
  if (!items.length) {
    return `<tr><td colspan="7">${escapeHtml(emptyLabel)}</td></tr>`;
  }

  return items
    .map((item) => {
      const notional = Math.abs(Number(item.size || 0) * Number(item.avg_price || 0));
      const unrealized = Number(item.unrealized_pnl || 0);
      const unrealizedClass = unrealized > 0 ? "pnl-pos" : unrealized < 0 ? "pnl-neg" : "pnl-flat";
      return `
      <tr>
        <td data-label="Mercado">${escapeHtml(item.title || item.slug || item.asset)}</td>
        <td data-label="Outcome">${escapeHtml(friendlyOutcomeName(item.outcome || "-"))}</td>
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

function paintPositions(items) {
  lastPositions = Array.isArray(items) ? items : [];
  const buckets = splitPositionBuckets(lastSummary, lastPositions);
  const btcItems = buckets.currentItems;
  const generalItems = buckets.archivedItems;

  document.getElementById("positionsBtcCount").textContent = String(btcItems.length);
  document.getElementById("positionsGeneralCount").textContent = String(generalItems.length);
  document.getElementById("positionsBtcBody").innerHTML = renderPositionRows(
    btcItems,
    isVidarxLab() ? "No hay posiciones abiertas en la ventana activa." : "No hay posiciones BTC 5m abiertas."
  );
  document.getElementById("positionsGeneralBody").innerHTML = renderPositionRows(
    generalItems,
    "No hay posiciones general abiertas."
  );

  const btcSummary = buckets.currentSummary;
  const generalSummary = buckets.archivedSummary;

  const currentExposure = Number(lastSummary?.strategy_current_market_total_exposure || btcSummary.exposure || 0);
  const currentLivePnl = Number(lastSummary?.strategy_current_market_live_pnl || btcSummary.unrealized || 0);
  document.getElementById("btcBucketCount").textContent = `${btcItems.length} ops.`;
  document.getElementById("btcBucketExposure").textContent = fmtUsdPlain(currentExposure, 2);
  document.getElementById("btcBucketPnl").textContent = fmtUsd(currentLivePnl, 2);

  if (isVidarxLab()) {
    const resolvedNotional = (Array.isArray(lastSummary?.strategy_recent_resolutions) ? lastSummary.strategy_recent_resolutions : []).reduce(
      (acc, item) => acc + Number(item.notional || 0),
      0
    );
    document.getElementById("generalBucketCount").textContent = `${Number(lastSummary?.strategy_resolution_count_today || 0)} ventanas`;
    document.getElementById("generalBucketExposure").textContent = fmtUsdPlain(resolvedNotional, 2);
    document.getElementById("generalBucketPnl").textContent = fmtUsd(
      Number(lastSummary?.strategy_resolution_pnl_today || 0),
      2
    );
  } else {
    document.getElementById("generalBucketCount").textContent = `${generalItems.length} pos.`;
    document.getElementById("generalBucketExposure").textContent = fmtUsdPlain(generalSummary.exposure, 2);
    document.getElementById("generalBucketPnl").textContent = fmtUsd(generalSummary.unrealized, 2);
  }
}

function paintExecutions(items) {
  lastExecutions = Array.isArray(items) ? items : [];
  const body = document.getElementById("executionsBody");
  if (!items.length) {
    body.innerHTML = `<tr><td colspan="8">No hay ejecuciones.</td></tr>`;
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
        <td data-label="Status">${statusPill(item.status)}</td>
        <td data-label="Wallet fuente">${escapeHtml(shortWallet(item.source_wallet || "-"))}</td>
        <td data-label="Monto USDC">${fmtUsd(Math.abs(Number(item.notional || 0)), 2)}</td>
        <td data-label="Resultado USD"><span class="${pnlClass}">${fmtUsd(delta, 4)}</span></td>
        <td data-label="Notas">${escapeHtml(item.notes || "-")}</td>
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
      const [summary, positions, executions] = await Promise.all([
        getJson(withCacheBust(buildApiUrl("/api/summary"))),
        getJson(withCacheBust(buildApiUrl("/api/positions"))),
        getJson(withCacheBust(buildApiUrl("/api/executions?limit=50"))),
      ]);

      paintExecutions(executions.items || []);
      paintSummary(summary, positions.items || []);
      paintPositions(positions.items || []);
      paintSignals([]);
      paintSelectedWallets([]);
      paintRiskBlocks({});
      paintExposureDonut(summary || {});
      paintOperationPnl(executions.items || []);
      paintStrategySetups(summary || {});
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

    paintExecutions(executions);
    paintSummary(summary, positions);
    paintPositions(positions);
    paintSignals([]);
    paintSelectedWallets([]);
    paintRiskBlocks({ items: [], hours: 24, blocked_total: 0 });
    paintExposureDonut(summary);
    paintOperationPnl(executions);
    paintStrategySetups(summary);
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

document.getElementById("apiBaseBtn").addEventListener("click", async () => {
  const input = document.getElementById("apiBaseInput");
  const value = String(input.value || "").trim().replace(/\/+$/, "");
  apiBase = value;
  saveApiBase(apiBase);
  try {
    await getJson(withCacheBust(buildApiUrl("/api/health")));
    runtimeMode = "local";
    document.querySelector(".kicker").textContent = "Proyecto principal (Local DB)";
    document.getElementById("runtimeBadge").textContent = modeLabel();
    document.getElementById("lastUpdated").textContent = `Backend API guardado: ${apiBase || "local"}`;
    document.getElementById("resetBtn").disabled = false;
    document.getElementById("resetBtn").title = "";
  } catch (error) {
    runtimeMode = apiBase ? "public-fallback" : "public";
    document.querySelector(".kicker").textContent =
      runtimeMode === "public-fallback" ? "Proyecto principal (Public API Fallback)" : "Proyecto principal (Public API)";
    document.getElementById("runtimeBadge").textContent = modeLabel();
    document.getElementById("resetBtn").disabled = true;
    document.getElementById("resetBtn").title = "Solo disponible cuando el dashboard esta conectado al backend local";
    document.getElementById("lastUpdated").textContent = apiBase
      ? `No conecta con ${apiBase}: ${error.message}. Se mantiene fallback publico.`
      : "Backend API borrado. Usando solo fallback publico.";
  }
  await refreshAll();
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
  document.getElementById("apiBaseInput").value = apiBase;
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
      ? "Proyecto principal (Local DB)"
      : runtimeMode === "public-fallback"
      ? "Proyecto principal (Public API Fallback)"
      : "Proyecto principal (Public API)";
  document.getElementById("runtimeBadge").textContent = modeLabel();
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
