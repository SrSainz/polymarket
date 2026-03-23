const DEFAULT_WALLET = "0xa81f087970a7ce196eacb3271e96e89294d91bb8";
const DATA_API = "https://data-api.polymarket.com";
const API_BASE_STORAGE_KEY = "polymarket_bot_api_base";
const DEFAULT_REMOTE_API_BY_HOST = {};
const DEPRECATED_REMOTE_APIS = new Set([
  "https://scores-trade-kept-developed.trycloudflare.com",
]);
const DONUT_GAIN_COLOR = "#3a9f62";
const DONUT_LOSS_COLOR = "#d0675f";
const UI_BUILD = "2026-03-23-shadow-lifecycle1";

let runtimeMode = "local";
let watchedWallet = DEFAULT_WALLET;
let apiBase = "";
let lastSummary = null;
let lastPositions = [];
let lastExecutions = [];

function isPublicRuntime() {
  return runtimeMode === "public" || runtimeMode === "public-fallback";
}

function isBackendDisconnectedRuntime() {
  return runtimeMode === "backend-unreachable";
}

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

function fmtUsdMaybe(value, available = true, digits = 2) {
  return available ? fmtUsd(value, digits) : "sin cierres";
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

function fmtBps(value, digits = 1) {
  const asNumber = Number(value);
  if (Number.isNaN(asNumber)) return "-";
  return `${asNumber.toFixed(digits)}bps`;
}

function fmtSeconds(value, digits = 1) {
  const asNumber = Number(value);
  if (Number.isNaN(asNumber) || asNumber <= 0) return "-";
  return `${asNumber.toFixed(digits)}s`;
}

function tsToIso(ts) {
  if (!ts) return "-";
  const date = new Date(ts * 1000);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toISOString().replace(".000Z", "Z");
}

function isoText(raw) {
  if (!raw) return "-";
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return String(raw);
  return date.toISOString().replace(".000Z", "Z");
}

function fmtAgeCompact(seconds) {
  const safe = Number(seconds);
  if (Number.isNaN(safe) || safe < 0) return "-";
  if (safe < 1) return "<1s";
  if (safe < 60) return `${Math.round(safe)}s`;
  if (safe < 3600) return `${Math.round(safe / 60)}m`;
  return `${Math.round(safe / 3600)}h`;
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

function normalizeApiBase(raw) {
  const value = String(raw || "").trim().replace(/\/+$/, "");
  if (!value) return "";
  if (DEPRECATED_REMOTE_APIS.has(value)) return "";
  return value;
}

function modeLabel() {
  if (runtimeMode === "local") return "Local DB";
  if (runtimeMode === "backend-unreachable") return "NAS desconectado";
  if (runtimeMode === "public-fallback") return "Fallback publico";
  return "Public API";
}

function tradingModeLabel(summary) {
  if (runtimeMode === "backend-unreachable") return "NAS OFF";
  if (isPublicRuntime()) return "PUBLICO";
  const sessionMode = String(summary.strategy_runtime_mode || "").trim().toLowerCase();
  if (sessionMode === "shadow") return "SHADOW";
  const isLiveSession = Boolean(summary.live_control_is_live_session);
  const canExecute = Boolean(summary.live_control_can_execute);
  if (isLiveSession) return canExecute ? "LIVE ARMADO" : "LIVE PAUSADO";
  return "PAPER";
}

function strategyLabel(summary) {
  if (runtimeMode === "backend-unreachable") return "Backend NAS";
  if (isPublicRuntime()) return "Perfil publico";
  const mode = String(summary.strategy_mode || "").trim();
  const entry = String(summary.strategy_entry_mode || "").trim();
  const variant = strategyVariant(summary);
  if (!mode) return "-";
  let label = mode;
  if (entry === "arb_micro") label = "Arbitraje BTC5m";
  else if (entry === "vidarx_micro") label = "Simulador Vidarx";
  else if (mode === "btc5m_orderbook") label = `BTC5m / ${entry || "-"}`;
  return variant ? `${label} · ${variant}` : label;
}

function strategyVariant(summary = lastSummary) {
  const value = String(summary?.strategy_variant || "").trim();
  if (!value || value === "default") return "";
  return value;
}

function incubationMeta(summary = lastSummary) {
  if (!summary || isPublicRuntime()) return "";
  const stage = String(summary?.strategy_incubation_stage || "").trim();
  const stageLabel = String(summary?.strategy_incubation_stage_label || "").trim();
  const recoLabel = String(summary?.strategy_incubation_recommendation_label || "").trim();
  const variant = strategyVariant(summary) || "default";
  if (!stageLabel || stage === "disabled") {
    return variant === "default" ? "" : `variante ${variant}`;
  }
  const progress = Number(summary?.strategy_incubation_progress_pct || 0);
  const resolutions = Number(summary?.strategy_incubation_resolutions || 0);
  return `variante ${variant} | ${stageLabel.toLowerCase()} | ${recoLabel.toLowerCase()} | ${resolutions} cierres | ${fmtPct(progress, 0)}`;
}

function incubationTransitionMeta(summary = lastSummary) {
  if (!summary || isPublicRuntime()) return "";
  const label = String(summary?.strategy_incubation_transition_label || "").trim();
  const nextStage = String(summary?.strategy_incubation_next_stage || "").trim();
  if (!label || !nextStage) return "";
  return `${label.toLowerCase()} -> ${nextStage}`;
}

function variantBacktestMeta(summary = lastSummary) {
  if (!summary || isPublicRuntime()) return "";
  const status = String(summary?.strategy_variant_backtest_status || "").trim();
  const pnl = Number(summary?.strategy_variant_backtest_pnl || 0);
  const edge = Number(summary?.strategy_variant_backtest_real_edge_bps || 0);
  const fill = Number(summary?.strategy_variant_backtest_fill_rate || 0);
  const windows = Number(summary?.strategy_variant_backtest_windows || 0);
  if (!status && !windows) return "";
  return `backtest ${status || "n/a"} | ${windows} ventanas | pnl ${fmtUsd(pnl, 2)} | edge ${fmt(edge, 1)}bps | fill ${fmtPct(fill * 100, 0)}`;
}

function datasetMeta(summary = lastSummary) {
  if (!summary || isPublicRuntime()) return "";
  const windows = Number(summary?.strategy_dataset_windows || 0);
  const events = Number(summary?.strategy_dataset_events || 0);
  if (!windows && !events) return "";
  return `dataset nativo ${windows} ventanas | ${events} eventos`;
}

function liveControlInfo(summary = lastSummary) {
  const isLiveSession = Boolean(summary?.live_control_is_live_session);
  const canExecute = Boolean(summary?.live_control_can_execute);
  const label = String(summary?.live_control_label || (isLiveSession ? "Live pausado" : "Solo paper"));
  const reason = String(summary?.live_control_reason || "").trim();
  const updatedAt = Number(summary?.live_control_updated_at || 0);
  const statusSummaryEnabled = Boolean(summary?.telegram_status_summary_enabled);
  const statusSummaryIntervalMinutes = Number(summary?.telegram_status_summary_interval_minutes || 0);
  const statusSummaryLastSentAt = Number(summary?.telegram_status_summary_last_sent_at || 0);
  return {
    isLiveSession,
    canExecute,
    label,
    reason,
    updatedAt,
    statusSummaryEnabled,
    statusSummaryIntervalMinutes,
    statusSummaryLastSentAt,
  };
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

function compareRatioLabel(ratio) {
  const upRatio = Number(ratio ?? 0.5);
  if (Number.isNaN(upRatio)) return "-";
  return `${friendlyOutcomeName("up")} ${fmtPct(upRatio * 100, 0)} / ${friendlyOutcomeName("down")} ${fmtPct(Math.max((1 - upRatio) * 100, 0), 0)}`;
}

function compareBreakdownLabel(snapshot) {
  const items = Array.isArray(snapshot?.breakdown) ? snapshot.breakdown : [];
  if (!items.length) return "sin patas abiertas";
  return items
    .map((item) => `${friendlyOutcomeName(item.outcome)} ${fmtPct(Number(item.share_pct || 0), 0)}`)
    .join(" / ");
}

function comparePriceLabel(snapshot) {
  const beat = Number(snapshot?.official_price_to_beat || 0);
  const spot = Number(snapshot?.spot_price || 0);
  const fairUp = Number(snapshot?.fair_up || 0);
  const fairDown = Number(snapshot?.fair_down || 0);
  const referenceQuality = String(snapshot?.reference_quality || "").trim();
  const spotText = spot > 0 ? fmtBtcPrice(spot) : "-";
  const beatText = beat > 0 ? fmtBtcPrice(beat) : "-";
  const fairText =
    fairUp > 0 || fairDown > 0
      ? `Sube ${fmtPct(fairUp * 100, 1)} / Baja ${fmtPct(fairDown * 100, 1)}`
      : "-";
  return `${spotText} | beat ${beatText} | ${fairText}${referenceQuality ? ` | ${referenceQuality}` : ""}`;
}

function compareBudgetLabel(snapshot) {
  const remaining = Number(snapshot?.remaining_cycle_budget || 0);
  const effectiveMin = Number(snapshot?.effective_min_notional || 0);
  return `${fmtUsdPlain(remaining, 2)} | min ${fmtUsdPlain(effectiveMin, 2)}`;
}

function comparePriceHeadline(snapshot) {
  const spot = Number(snapshot?.spot_price || 0);
  const fairUp = Number(snapshot?.fair_up || 0);
  const fairDown = Number(snapshot?.fair_down || 0);
  const spotText = spot > 0 ? fmtBtcPrice(spot) : "-";
  const fairText =
    fairUp > 0 || fairDown > 0
      ? `Sube ${fmtPct(fairUp * 100, 1)} / Baja ${fmtPct(fairDown * 100, 1)}`
      : "fair -";
  return `${spotText} | ${fairText}`;
}

function comparePriceMeta(snapshot) {
  const beat = Number(snapshot?.official_price_to_beat || 0);
  const quality = String(snapshot?.reference_quality || "").trim();
  const operability = String(snapshot?.operability_state || "").trim();
  const beatText = beat > 0 ? `beat ${fmtBtcPrice(beat)}` : "sin beat oficial";
  return `${beatText}${quality ? ` | ${quality}` : ""}${operability ? ` | ${operability}` : ""}`;
}

function renderCompareList(snapshot) {
  const rows = [
    ["Ventana", snapshot?.slug || snapshot?.title || "-"],
    ["Estado", snapshot?.operability_state || "-"],
    ["Modo precio", snapshot?.price_mode || "-"],
    ["Budget", fmtUsdPlain(Number(snapshot?.cycle_budget || 0), 2)],
    ["Restante / min", compareBudgetLabel(snapshot)],
    ["Precio / fair", comparePriceLabel(snapshot)],
    ["Objetivo", compareRatioLabel(snapshot?.desired_up_ratio)],
    ["Actual", compareRatioLabel(snapshot?.current_up_ratio)],
    ["Patas", `${Number(snapshot?.open_legs || 0)} | ${compareBreakdownLabel(snapshot)}`],
    ["Exposición", fmtUsdPlain(Number(snapshot?.exposure || 0), 2)],
    ["Nota", snapshot?.last_note || "-"],
  ];
  return rows
    .map(
      ([label, value]) => `
      <li class="mini-item">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(String(value || "-"))}</span>
      </li>
    `
    )
    .join("");
}

function compareRecentTicketsLabel(snapshot) {
  const items = Array.isArray(snapshot?.recent_executions) ? snapshot.recent_executions : [];
  if (!items.length) return "sin aperturas recientes";
  return items
    .slice(0, 4)
    .map((item) => fmtUsdPlain(Number(item?.notional || 0), 2))
    .join(" / ");
}

function renderCompareStats(snapshot) {
  const rows = [
    [
      "Aperturas",
      `${Number(snapshot?.open_execution_count || 0)} | total ${fmtUsdPlain(Number(snapshot?.open_total_notional || 0), 2)}`,
    ],
    ["Ticket medio", fmtUsdPlain(Number(snapshot?.open_avg_notional || 0), 2)],
    [
      "Rango ticket",
      `${fmtUsdPlain(Number(snapshot?.open_min_notional || 0), 2)} -> ${fmtUsdPlain(Number(snapshot?.open_max_notional || 0), 2)}`,
    ],
    ["Ultimos tickets", compareRecentTicketsLabel(snapshot)],
    ["Ultima apertura", Number(snapshot?.last_execution_ts || 0) > 0 ? tsToIso(Number(snapshot.last_execution_ts)) : "-"],
  ];
  return rows
    .map(
      ([label, value]) => `
      <li class="mini-item">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(String(value || "-"))}</span>
      </li>
    `
    )
    .join("");
}

function renderCompareCurrentSnapshotItems(paper, shadow, sampleSummary = {}) {
  const rows = [
    [
      "Precio / fair",
      `paper ${comparePriceLabel(paper)} | shadow ${comparePriceLabel(shadow)}`,
    ],
    [
      "Budget / min",
      `paper ${compareBudgetLabel(paper)} | shadow ${compareBudgetLabel(shadow)}`,
    ],
    [
      "Reparto actual",
      `paper ${compareRatioLabel(paper?.current_up_ratio)} | shadow ${compareRatioLabel(shadow?.current_up_ratio)}`,
    ],
    [
      "Actividad actual",
      `aperturas ${Number(paper?.open_execution_count || 0)} vs ${Number(shadow?.open_execution_count || 0)} | exposicion ${fmtUsdPlain(Number(paper?.exposure || 0), 2)} vs ${fmtUsdPlain(Number(shadow?.exposure || 0), 2)}`,
    ],
    [
      "Despliegue reciente",
      `paper ${fmtUsdPlain(Number(sampleSummary?.paper_latest_notional || 0), 2)} | shadow ${fmtUsdPlain(Number(sampleSummary?.shadow_latest_notional || 0), 2)}`,
    ],
  ];
  return rows
    .map(
      ([label, value]) => `
      <li class="mini-item compare-delta-item">
        <div class="compare-delta-top">
          <strong>${escapeHtml(label)}</strong>
        </div>
        <div class="compare-delta-bottom">
          <span>${escapeHtml(String(value || "-"))}</span>
        </div>
      </li>
    `
    )
    .join("");
}

function compareHistory(summary) {
  const history = summary?.strategy_runtime_window_compare?.history;
  if (!history || typeof history !== "object") {
    return {
      available: false,
      series: { paper: [], shadow: [] },
      points: [],
      summary: {},
      window_limit: 0,
    };
  }
  return history;
}

function compareGapLeaderLabel(value, positiveLabel, negativeLabel, unit = "") {
  const safe = Number(value || 0);
  if (Number.isNaN(safe) || safe === 0) return `sin brecha${unit ? ` ${unit}` : ""}`;
  if (safe > 0) return `paper ${positiveLabel}${unit ? ` ${unit}` : ""}`;
  return `shadow ${negativeLabel}${unit ? ` ${unit}` : ""}`;
}

function compareOperabilityLabel(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "sin bloqueo dominante";
  if (raw === "waiting_book") return "shadow se queda en waiting_book";
  if (raw === "waiting_edge") return "shadow espera edge";
  if (raw === "waiting_budget") return "shadow espera presupuesto";
  if (raw === "degraded_reference") return "shadow bloqueado por referencia";
  if (raw === "waiting_time") return "shadow espera ventana";
  return `shadow ${raw}`;
}

function compareParticipationLabel(historySummary) {
  return `paper ${fmtPct(Number(historySummary?.paper_participation_pct || 0), 1)} / shadow ${fmtPct(Number(historySummary?.shadow_participation_pct || 0), 1)}`;
}

function compareBooleanLabel(value) {
  return value ? "si" : "no";
}

function compareFocusHeadline(paper, shadow, sampleSummary = {}) {
  const paperFills = Number(paper?.open_execution_count || 0);
  const shadowFills = Number(shadow?.open_execution_count || 0);
  const paperLegs = Number(paper?.open_legs || 0);
  const shadowLegs = Number(shadow?.open_legs || 0);
  const shadowState = String(shadow?.operability_state || sampleSummary?.shadow_dominant_operability_state || "").trim();

  if (paperFills > 0 && shadowFills === 0) return "Paper ya esta dentro y shadow sigue fuera";
  if (paperLegs >= 2 && shadowLegs === 1) return "Shadow se queda cojo con una sola pata";
  if (paperLegs >= 2 && shadowLegs >= 2) return "Ambos tienen paquete de dos patas";
  if (shadowState === "budget_limited") return "Shadow se esta quedando sin presupuesto util";
  if (shadowState === "waiting_book") return "Shadow no ve libro suficiente para entrar";
  if (shadowState === "waiting_edge") return "Shadow espera edge valido antes de entrar";
  if (shadowState === "degraded_reference") return "Shadow se bloquea por referencia degradada";
  if (paperFills === 0 && shadowFills === 0) return "Ambos siguen esperando una entrada valida";
  return "Comparativa activa paper vs shadow";
}

function compareFocusMeta(paper, shadow, sampleSummary = {}) {
  const shadowState = String(shadow?.operability_state || sampleSummary?.shadow_dominant_operability_state || "").trim();
  const shadowNote = String(shadow?.last_note || "").trim();
  const parts = [
    `paper ${Number(paper?.open_execution_count || 0)} fills / ${Number(paper?.open_legs || 0)} patas`,
    `shadow ${Number(shadow?.open_execution_count || 0)} fills / ${Number(shadow?.open_legs || 0)} patas`,
  ];
  if (shadowState) parts.push(`shadow ${shadowState}`);
  if (shadowNote) parts.push(shadowNote);
  return parts.join(" | ");
}

function shortCompareWindow(point) {
  const slug = String(point?.slug || "").trim();
  const title = String(point?.title || "").trim();
  if (slug) return shortSlug(slug);
  if (title) return title;
  return "-";
}

function comparePresenceLabel(point) {
  const paperStatus = String(point?.paper_status || "");
  const shadowStatus = String(point?.shadow_status || "");
  if (paperStatus === "missing") return "shadow entro y paper no";
  if (shadowStatus === "missing") return "paper entro y shadow no";
  return "ambos participaron";
}

function renderCompareTrendChart(points, valueKey, tone) {
  const items = Array.isArray(points) ? points : [];
  if (!items.length) {
    return `<div class="compare-chart-empty">Todavia no hay historial suficiente para dibujar esta curva.</div>`;
  }

  const values = items.map((item) => Number(item?.[valueKey] || 0));
  const width = 360;
  const height = 180;
  const padX = 14;
  const padTop = 16;
  const padBottom = 24;
  const minValue = Math.min(...values, 0);
  const maxValue = Math.max(...values, 0);
  const range = maxValue - minValue || 1;
  const plotWidth = width - padX * 2;
  const plotHeight = height - padTop - padBottom;
  const step = items.length > 1 ? plotWidth / (items.length - 1) : 0;
  const coords = values.map((value, index) => {
    const x = items.length > 1 ? padX + step * index : padX + plotWidth / 2;
    const y = padTop + ((maxValue - value) / range) * plotHeight;
    return { x, y, value };
  });
  const linePoints = coords.map((point) => `${point.x.toFixed(2)},${point.y.toFixed(2)}`).join(" ");
  const lastPoint = coords[coords.length - 1];
  const areaPoints = [
    `${coords[0].x.toFixed(2)},${(height - padBottom).toFixed(2)}`,
    ...coords.map((point) => `${point.x.toFixed(2)},${point.y.toFixed(2)}`),
    `${lastPoint.x.toFixed(2)},${(height - padBottom).toFixed(2)}`,
  ].join(" ");
  const zeroVisible = minValue <= 0 && maxValue >= 0;
  const zeroY = zeroVisible ? padTop + ((maxValue - 0) / range) * plotHeight : 0;
  const topLabel = fmtUsdPlain(maxValue, Math.abs(maxValue) >= 100 ? 0 : 2);
  const bottomLabel = fmtUsdPlain(minValue, Math.abs(minValue) >= 100 ? 0 : 2);
  const lastLabelY = Math.max(lastPoint.y - 8, padTop + 10);

  return `
    <svg class="compare-chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Evolucion temporal">
      <line class="compare-grid-line" x1="${padX}" y1="${padTop}" x2="${width - padX}" y2="${padTop}"></line>
      <line class="compare-grid-line" x1="${padX}" y1="${padTop + plotHeight / 2}" x2="${width - padX}" y2="${padTop + plotHeight / 2}"></line>
      <line class="compare-grid-line" x1="${padX}" y1="${height - padBottom}" x2="${width - padX}" y2="${height - padBottom}"></line>
      ${zeroVisible ? `<line class="compare-zero-line" x1="${padX}" y1="${zeroY}" x2="${width - padX}" y2="${zeroY}"></line>` : ""}
      <polygon class="compare-line-fill-${tone}" points="${areaPoints}"></polygon>
      <polyline class="compare-line-stroke compare-line-${tone}" points="${linePoints}"></polyline>
      <circle class="compare-last-dot-${tone}" cx="${lastPoint.x}" cy="${lastPoint.y}" r="4.5"></circle>
      <text class="compare-axis-label" x="${padX}" y="${padTop - 4}">${escapeHtml(topLabel)}</text>
      <text class="compare-axis-label" x="${padX}" y="${height - 6}">${escapeHtml(bottomLabel)}</text>
      <text class="compare-axis-label" x="${lastPoint.x}" y="${lastLabelY}" text-anchor="end">${escapeHtml(fmtUsdPlain(lastPoint.value, 2))}</text>
    </svg>
  `;
}

function renderCompareDeltaItems(history) {
  const points = Array.isArray(history?.points) ? history.points.slice(-6).reverse() : [];
  if (!points.length) {
    return `
      <li class="mini-item">
        <strong>Sin historial comparable</strong>
        <span>Necesitamos varias ventanas con datos de paper y shadow para ver la brecha en el tiempo.</span>
      </li>
    `;
  }
  return points
    .map(
      (point) => `
      <li class="mini-item compare-delta-item">
        <div class="compare-delta-top">
          <strong>${escapeHtml(shortCompareWindow(point))}</strong>
          <span>${escapeHtml(tsToIso(Number(point?.ts || 0)))}</span>
        </div>
        <div class="compare-delta-bottom">
          <span>${escapeHtml(comparePresenceLabel(point))}</span>
          <span>${escapeHtml(`PnL ${fmtUsd(Number(point?.paper_realized_pnl || 0), 2)} paper / ${fmtUsd(Number(point?.shadow_realized_pnl || 0), 2)} shadow / gap ${fmtUsd(Number(point?.pnl_gap || 0), 2)}`)}</span>
          <span>${escapeHtml(`Despliegue ${fmtUsdPlain(Number(point?.paper_deployed_notional || 0), 2)} vs ${fmtUsdPlain(Number(point?.shadow_deployed_notional || 0), 2)} | fills ${Number(point?.paper_filled_orders || 0)} vs ${Number(point?.shadow_filled_orders || 0)}`)}</span>
          <span>${escapeHtml(`Dos patas ${compareBooleanLabel(Boolean(point?.paper_two_sided))} vs ${compareBooleanLabel(Boolean(point?.shadow_two_sided))} | settlement ${compareBooleanLabel(Boolean(point?.paper_settlement_visible))} vs ${compareBooleanLabel(Boolean(point?.shadow_settlement_visible))}`)}</span>
          <span>${escapeHtml(`Cadencia ${fmtSeconds(Number(point?.paper_open_cadence_seconds || 0), 1)} vs ${fmtSeconds(Number(point?.shadow_open_cadence_seconds || 0), 1)} | span ${fmtSeconds(Number(point?.paper_open_span_seconds || 0), 0)} vs ${fmtSeconds(Number(point?.shadow_open_span_seconds || 0), 0)}`)}</span>
        </div>
      </li>
    `
    )
    .join("");
}

function paintRuntimeCompare(summary) {
  const section = document.getElementById("runtimeCompareSection");
  const badge = document.getElementById("runtimeCompareBadge");
  const meta = document.getElementById("runtimeCompareMeta");
  const dbMeta = document.getElementById("runtimeCompareDbMeta");
  const paperCurrentPrice = document.getElementById("comparePaperCurrentPrice");
  const paperCurrentMeta = document.getElementById("comparePaperCurrentMeta");
  const shadowCurrentPrice = document.getElementById("compareShadowCurrentPrice");
  const shadowCurrentMeta = document.getElementById("compareShadowCurrentMeta");
  const budgetNow = document.getElementById("compareBudgetNow");
  const budgetNowMeta = document.getElementById("compareBudgetNowMeta");
  const ratioNow = document.getElementById("compareRatioNow");
  const ratioNowMeta = document.getElementById("compareRatioNowMeta");
  const focusHeadline = document.getElementById("compareFocusHeadline");
  const focusMeta = document.getElementById("compareFocusMeta");
  const twoSided = document.getElementById("compareTwoSided");
  const twoSidedMeta = document.getElementById("compareTwoSidedMeta");
  const oneSided = document.getElementById("compareOneSided");
  const oneSidedMeta = document.getElementById("compareOneSidedMeta");
  const settlement = document.getElementById("compareSettlement");
  const settlementMeta = document.getElementById("compareSettlementMeta");
  const cadence = document.getElementById("compareCadence");
  const cadenceMeta = document.getElementById("compareCadenceMeta");
  const paperPnl = document.getElementById("comparePaperPnl");
  const paperMeta = document.getElementById("comparePaperMeta");
  const shadowPnl = document.getElementById("compareShadowPnl");
  const shadowMeta = document.getElementById("compareShadowMeta");
  const gapPnl = document.getElementById("compareGapPnl");
  const gapPnlMeta = document.getElementById("compareGapPnlMeta");
  const gapActivity = document.getElementById("compareGapActivity");
  const gapActivityMeta = document.getElementById("compareGapActivityMeta");
  const paperChartMeta = document.getElementById("comparePaperChartMeta");
  const shadowChartMeta = document.getElementById("compareShadowChartMeta");
  const paperChart = document.getElementById("comparePaperChart");
  const shadowChart = document.getElementById("compareShadowChart");
  const deltaList = document.getElementById("compareDeltaList");
  const windowCount = document.getElementById("compareWindowCount");
  const paperMode = document.getElementById("comparePaperMode");
  const shadowMode = document.getElementById("compareShadowMode");
  const paperList = document.getElementById("comparePaperList");
  const shadowList = document.getElementById("compareShadowList");
  const paperStats = document.getElementById("comparePaperStats");
  const shadowStats = document.getElementById("compareShadowStats");
  const compareDetails = document.getElementById("compareCurrentWindowDetails");
  if (
    !section ||
    !badge ||
    !meta ||
    !dbMeta ||
    !paperCurrentPrice ||
    !paperCurrentMeta ||
    !shadowCurrentPrice ||
    !shadowCurrentMeta ||
    !budgetNow ||
    !budgetNowMeta ||
    !ratioNow ||
    !ratioNowMeta ||
    !focusHeadline ||
    !focusMeta ||
    !twoSided ||
    !twoSidedMeta ||
    !oneSided ||
    !oneSidedMeta ||
    !settlement ||
    !settlementMeta ||
    !cadence ||
    !cadenceMeta ||
    !paperPnl ||
    !paperMeta ||
    !shadowPnl ||
    !shadowMeta ||
    !gapPnl ||
    !gapPnlMeta ||
    !gapActivity ||
    !gapActivityMeta ||
    !paperChartMeta ||
    !shadowChartMeta ||
    !paperChart ||
    !shadowChart ||
    !deltaList ||
    !windowCount ||
    !paperMode ||
    !shadowMode ||
    !paperList ||
    !shadowList ||
    !paperStats ||
    !shadowStats
  ) {
    return;
  }

  const compare = summary?.strategy_runtime_window_compare;
  if (isBackendDisconnectedRuntime() || !compare?.available) {
    section.hidden = true;
    return;
  }

  section.hidden = false;
  const paper = compare.paper || {};
  const shadow = compare.shadow || {};
  const status = String(compare.status || "");
  badge.textContent =
    status === "shared" ? "misma ventana" : status === "paper-missing" ? "paper sin ventana" : "ventanas distintas";
  dbMeta.textContent = compare?.db_path
    ? `db comparativa ${String(compare.db_path)} | generado ${Number(compare.generated_at || 0) > 0 ? tsToIso(Number(compare.generated_at)) : "ahora"} | api ${String(summary?.dashboard_build || "-")}`
    : "db comparativa no disponible";

  const history = compareHistory(summary);
  const historySummary = history.summary || {};
  const historyPoints = Array.isArray(history?.points) ? history.points : [];
  const paperSeries = Array.isArray(history?.series?.paper) ? history.series.paper : [];
  const shadowSeries = Array.isArray(history?.series?.shadow) ? history.series.shadow : [];
  const sampleSeries = {
    paper: Array.isArray(history?.sample_series?.paper) ? history.sample_series.paper : [],
    shadow: Array.isArray(history?.sample_series?.shadow) ? history.sample_series.shadow : [],
  };
  const sampleSummary = history?.sample_summary || {};
  const hasClosedHistory = Boolean(history.available && (historyPoints.length || paperSeries.length || shadowSeries.length));
  const useSampleFallback = !hasClosedHistory && (sampleSeries.paper.length || sampleSeries.shadow.length);
  const trendPaperSeries = useSampleFallback ? sampleSeries.paper : paperSeries;
  const trendShadowSeries = useSampleFallback ? sampleSeries.shadow : shadowSeries;
  const trendValueKey = useSampleFallback ? "open_total_notional" : "cumulative_realized_pnl";
  const comparableCount = Number(historySummary.shared_window_count || 0);
  const totalPointCount = Number(historySummary.point_count || historyPoints.length || 0);

  meta.textContent =
    status === "shared"
      ? hasClosedHistory
        ? `Comparando ${String(compare.shared_slug || shadow.slug || "-")} con el mismo setup en papel y shadow.`
        : `Comparando ${String(compare.shared_slug || shadow.slug || "-")} con la misma ventana activa. Aun no hay cierres comparables, asi que mostramos la foto actual y el despliegue reciente.`
      : "La ventana activa de shadow no coincide con la que tiene paper abierta ahora mismo.";

  paperCurrentPrice.textContent = comparePriceHeadline(paper);
  paperCurrentMeta.textContent = comparePriceMeta(paper);
  shadowCurrentPrice.textContent = comparePriceHeadline(shadow);
  shadowCurrentMeta.textContent = comparePriceMeta(shadow);
  budgetNow.textContent = `paper ${fmtUsdPlain(Number(paper?.remaining_cycle_budget || 0), 2)} / shadow ${fmtUsdPlain(Number(shadow?.remaining_cycle_budget || 0), 2)}`;
  budgetNowMeta.textContent = `min ${fmtUsdPlain(Number(paper?.effective_min_notional || 0), 2)} / ${fmtUsdPlain(Number(shadow?.effective_min_notional || 0), 2)} | exp ${fmtUsdPlain(Number(paper?.exposure || 0), 2)} / ${fmtUsdPlain(Number(shadow?.exposure || 0), 2)}`;
  ratioNow.textContent = `paper ${compareRatioLabel(paper?.current_up_ratio)}`;
  ratioNowMeta.textContent = `shadow ${compareRatioLabel(shadow?.current_up_ratio)} | objetivo ${compareRatioLabel(shadow?.desired_up_ratio)}`;
  focusHeadline.textContent = compareFocusHeadline(paper, shadow, sampleSummary);
  focusMeta.textContent = compareFocusMeta(paper, shadow, sampleSummary);

  const lifecycleHistoryAvailable =
    Number(historySummary?.paper_active_window_count || 0) > 0 ||
    Number(historySummary?.shadow_active_window_count || 0) > 0;
  if (lifecycleHistoryAvailable) {
    twoSided.textContent = `paper ${fmtPct(Number(historySummary?.paper_two_sided_window_pct || 0), 0)} / shadow ${fmtPct(Number(historySummary?.shadow_two_sided_window_pct || 0), 0)}`;
    twoSidedMeta.textContent = `${Number(historySummary?.paper_two_sided_window_count || 0)}/${Number(historySummary?.paper_active_window_count || 0)} paper y ${Number(historySummary?.shadow_two_sided_window_count || 0)}/${Number(historySummary?.shadow_active_window_count || 0)} shadow con las dos patas visibles`;
    oneSided.textContent = `paper ${Number(historySummary?.paper_one_sided_window_count || 0)} / shadow ${Number(historySummary?.shadow_one_sided_window_count || 0)}`;
    oneSidedMeta.textContent = "ventanas activas que se quedaron con una sola pata visible";
    settlement.textContent = `paper ${fmtPct(Number(historySummary?.paper_settlement_window_pct || 0), 0)} / shadow ${fmtPct(Number(historySummary?.shadow_settlement_window_pct || 0), 0)}`;
    settlementMeta.textContent = `${Number(historySummary?.paper_settlement_window_count || 0)} paper y ${Number(historySummary?.shadow_settlement_window_count || 0)} shadow con cierre visible por strategy_resolution`;
    cadence.textContent = `paper ${fmtSeconds(Number(historySummary?.paper_avg_open_cadence_seconds || 0), 1)} / shadow ${fmtSeconds(Number(historySummary?.shadow_avg_open_cadence_seconds || 0), 1)}`;
    cadenceMeta.textContent = `span medio ${fmtSeconds(Number(historySummary?.paper_avg_open_span_seconds || 0), 0)} / ${fmtSeconds(Number(historySummary?.shadow_avg_open_span_seconds || 0), 0)} entre primer y ultimo fill de apertura`;
  } else {
    twoSided.textContent = `paper ${compareBooleanLabel(Number(paper?.open_legs || 0) >= 2)} / shadow ${compareBooleanLabel(Number(shadow?.open_legs || 0) >= 2)}`;
    twoSidedMeta.textContent = `ventana actual | ${Number(paper?.open_legs || 0)} patas paper / ${Number(shadow?.open_legs || 0)} patas shadow`;
    oneSided.textContent = `paper ${compareBooleanLabel(Number(paper?.open_legs || 0) === 1)} / shadow ${compareBooleanLabel(Number(shadow?.open_legs || 0) === 1)}`;
    oneSidedMeta.textContent = "sin suficiente historial cerrado; mostramos solo la foto actual";
    settlement.textContent = "sin cierres";
    settlementMeta.textContent = "necesitamos al menos una ventana cerrada para medir settlement visible";
    cadence.textContent = "sin historial";
    cadenceMeta.textContent = "la cadencia se calcula a partir de fills reales dentro de cada ventana";
  }

  if (hasClosedHistory) {
    paperPnl.textContent = fmtUsd(Number(historySummary.paper_comparable_realized_pnl || 0), 2);
    shadowPnl.textContent = fmtUsd(Number(historySummary.shadow_comparable_realized_pnl || 0), 2);
    gapPnl.textContent = fmtUsd(Number(historySummary.comparable_pnl_gap || 0), 2);
    gapActivity.textContent = compareParticipationLabel(historySummary);
    paperMeta.textContent = `${comparableCount} comparables | total runtime ${fmtUsdPlain(Number(historySummary.paper_total_realized_pnl || 0), 2)} en ${Number(historySummary.paper_window_count || 0)} ventanas`;
    shadowMeta.textContent = `${comparableCount} comparables | total runtime ${fmtUsdPlain(Number(historySummary.shadow_total_realized_pnl || 0), 2)} en ${Number(historySummary.shadow_window_count || 0)} ventanas`;
    gapPnlMeta.textContent = `${comparableCount} comparables | gap total ${fmtUsdPlain(Number(historySummary.total_pnl_gap || 0), 2)} fuera de comparables`;
    gapActivityMeta.textContent =
      `${comparableCount}/${totalPointCount} compartidas | ${compareOperabilityLabel(sampleSummary?.shadow_dominant_operability_state)}${Number(sampleSummary?.shadow_dominant_operability_count || 0) > 0 ? ` (${fmtPct(Number(sampleSummary?.shadow_dominant_operability_pct || 0), 0)})` : ""}`;
    paperChartMeta.textContent = `${paperSeries.length} ventanas recientes | total runtime ${fmtUsd(Number(historySummary.paper_total_deployed_notional || 0), 2)} desplegados`;
    shadowChartMeta.textContent = `${shadowSeries.length} ventanas recientes | total runtime ${fmtUsd(Number(historySummary.shadow_total_deployed_notional || 0), 2)} desplegados`;
    deltaList.innerHTML = renderCompareDeltaItems(history);
    windowCount.textContent = `${comparableCount} comparables`;
  } else {
    const paperRealized = Number(paper?.total_realized_pnl || 0);
    const shadowRealized = Number(shadow?.total_realized_pnl || 0);
    const paperHasClosed = Number(paper?.closed_window_count || 0) > 0;
    const shadowHasClosed = Number(shadow?.closed_window_count || 0) > 0;
    paperPnl.textContent = "sin cierres";
    shadowPnl.textContent = "sin cierres";
    gapPnl.textContent = "sin cierres";
    gapActivity.textContent = useSampleFallback
      ? `paper ${Number(sampleSummary?.paper_sample_count || 0)} / shadow ${Number(sampleSummary?.shadow_sample_count || 0)} muestras`
      : "sin historial";
    paperMeta.textContent = `${Number(paper?.closed_window_count || 0)} cierres | total runtime ${fmtUsdPlain(paperRealized, 2)} / ${fmtUsdPlain(Number(paper?.historical_deployed_notional || 0), 2)} historicos`;
    shadowMeta.textContent = `${Number(shadow?.closed_window_count || 0)} cierres | total runtime ${fmtUsdPlain(shadowRealized, 2)} / ${fmtUsdPlain(Number(shadow?.historical_deployed_notional || 0), 2)} historicos`;
    gapPnlMeta.textContent = useSampleFallback
      ? `${Math.max(sampleSeries.paper.length, sampleSeries.shadow.length)} muestras recientes | aun no hay cierres compartidos`
      : "misma ventana, aun sin cierres comparables";
    gapActivityMeta.textContent = useSampleFallback
      ? `${Math.max(sampleSeries.paper.length, sampleSeries.shadow.length)} muestras | ${compareOperabilityLabel(sampleSummary?.shadow_dominant_operability_state)}${Number(sampleSummary?.shadow_dominant_operability_count || 0) > 0 ? ` (${fmtPct(Number(sampleSummary?.shadow_dominant_operability_pct || 0), 0)})` : ""}`
      : `aperturas actuales ${Number(paper?.open_execution_count || 0)} paper / ${Number(shadow?.open_execution_count || 0)} shadow`;
    paperChartMeta.textContent = useSampleFallback
      ? `${sampleSeries.paper.length} muestras recientes | despliegue actual ${fmtUsdPlain(Number(sampleSummary?.paper_latest_notional || 0), 2)}`
      : "sin historial ni muestras recientes";
    shadowChartMeta.textContent = useSampleFallback
      ? `${sampleSeries.shadow.length} muestras recientes | despliegue actual ${fmtUsdPlain(Number(sampleSummary?.shadow_latest_notional || 0), 2)}`
      : "sin historial ni muestras recientes";
    deltaList.innerHTML = renderCompareCurrentSnapshotItems(paper, shadow, sampleSummary);
    windowCount.textContent = useSampleFallback
      ? `${Math.max(sampleSeries.paper.length, sampleSeries.shadow.length)} muestras`
      : "foto actual";
  }

  paperChart.innerHTML = renderCompareTrendChart(trendPaperSeries, trendValueKey, "paper");
  shadowChart.innerHTML = renderCompareTrendChart(trendShadowSeries, trendValueKey, "shadow");

  paperMode.textContent = String(paper.runtime_mode || "paper").toUpperCase();
  shadowMode.textContent = String(shadow.runtime_mode || "shadow").toUpperCase();
  paperList.innerHTML = renderCompareList(paper);
  shadowList.innerHTML = renderCompareList(shadow);
  paperStats.innerHTML = renderCompareStats(paper);
  shadowStats.innerHTML = renderCompareStats(shadow);
  if (compareDetails) {
    compareDetails.open = !hasClosedHistory;
  }
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
  if (source === "websocket-idle") {
    return {
      label: "WS inactivo",
      meta: connected ? `${trackedAssets} assets sin suscripcion activa` : "sin suscripcion activa",
      className: "feed-pill warming",
      summaryLabel: `ws inactivo / ${trackedAssets} assets`,
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
  const label =
    priceMode === "cheap-side"
      ? "lado barato"
      : priceMode === "repair-bracket"
      ? "repair"
      : priceMode === "biased-bracket"
      ? "bracket sesgado"
      : priceMode === "underround"
      ? "underround"
      : "arbitraje";
  return {
    pairSum: pairSum || null,
    edgePct: storedEdge ? storedEdge * 100 : Math.max((1 - pairSum) * 100, 0),
    fairValue: fairValue || null,
    label,
  };
}

function currentSpotInfo(summary) {
  const current = Number(summary?.strategy_spot_price || 0);
  const anchor = Number(summary?.strategy_spot_anchor || 0);
  const localAnchor = Number(summary?.strategy_spot_local_anchor || 0);
  const officialBeat = Number(summary?.strategy_official_price_to_beat || 0);
  const referenceQuality = String(summary?.strategy_reference_quality || "").trim();
  const referenceComparable = Boolean(summary?.strategy_reference_comparable);
  const referenceNote = String(summary?.strategy_reference_note || "").trim();
  const chainlink = Number(summary?.strategy_spot_chainlink || 0);
  const anchorSource = String(summary?.strategy_anchor_source || "").trim();
  const deltaBps = Number(summary?.strategy_spot_delta_bps || 0);
  const fairUp = Number(summary?.strategy_spot_fair_up || 0);
  const fairDown = Number(summary?.strategy_spot_fair_down || 0);
  const ageMs = Number(summary?.strategy_spot_age_ms || 0);
  const source = String(summary?.strategy_spot_source || "").trim();
  const priceMode = String(summary?.strategy_spot_price_mode || "").trim();
  const binance = Number(summary?.strategy_spot_binance || 0);
  const beatKind = officialBeat > 0 ? "official" : anchor > 0 && referenceQuality === "rtds-derived" ? "rtds-derived" : anchor > 0 ? "fallback" : "missing";
  const beatReference = officialBeat > 0 ? officialBeat : anchor;
  const polymarketCurrent = chainlink > 0 ? chainlink : 0;
  const fallbackCurrent = current > 0 && chainlink <= 0 ? current : 0;
  const deltaUsd = current > 0 && beatReference > 0 ? current - beatReference : 0;
  const deltaMarketBps = current > 0 && beatReference > 0 ? ((current / beatReference) - 1) * 10000 : 0;
  const anchorDriftUsd = localAnchor > 0 && officialBeat > 0 ? localAnchor - officialBeat : 0;
  const anchorDriftBps = localAnchor > 0 && officialBeat > 0 ? ((localAnchor / officialBeat) - 1) * 10000 : 0;
  const anchorDriftRefUsd = localAnchor > 0 && beatReference > 0 ? localAnchor - beatReference : 0;
  const anchorDriftRefBps = localAnchor > 0 && beatReference > 0 ? ((localAnchor / beatReference) - 1) * 10000 : 0;
  const chainlinkDeltaUsd = chainlink > 0 && beatReference > 0 ? chainlink - beatReference : 0;
  const chainlinkDeltaBps = chainlink > 0 && beatReference > 0 ? ((chainlink / beatReference) - 1) * 10000 : 0;
  const binanceDeltaUsd = binance > 0 && beatReference > 0 ? binance - beatReference : 0;
  const binanceDeltaBps = binance > 0 && beatReference > 0 ? ((binance / beatReference) - 1) * 10000 : 0;
  return {
    available: referenceComparable && current > 0 && beatReference > 0,
    hasCurrent: current > 0,
    hasAnchor: anchor > 0,
    hasLocalAnchor: localAnchor > 0,
    hasOfficialBeat: officialBeat > 0,
    hasChainlink: chainlink > 0,
    hasBinance: binance > 0,
    beatKind,
    referenceQuality: referenceQuality || "missing",
    referenceComparable,
    referenceNote: referenceNote || "sin referencia comparable",
    current,
    anchor,
    localAnchor,
    officialBeat,
    polymarketCurrent,
    fallbackCurrent,
    anchorSource: anchorSource || "-",
    deltaBps,
    deltaUsd,
    deltaMarketBps,
    anchorDriftUsd,
    anchorDriftBps,
    anchorDriftRefUsd,
    anchorDriftRefBps,
    priceMode: priceMode || "missing",
    chainlinkDeltaUsd,
    chainlinkDeltaBps,
    binanceDeltaUsd,
    binanceDeltaBps,
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

function latestObservedExecution() {
  return Array.isArray(lastExecutions) && lastExecutions.length ? lastExecutions[0] : null;
}

function snapshotTiming(summary) {
  const nowMs = Date.now();
  const backendDate = summary?.timestamp_utc ? new Date(summary.timestamp_utc) : null;
  const backendMs = backendDate && !Number.isNaN(backendDate.getTime()) ? backendDate.getTime() : 0;
  const strategyMs = Number(summary?.strategy_last_updated_at || 0) > 0 ? Number(summary.strategy_last_updated_at) * 1000 : 0;
  const liveBalanceMs = Number(summary?.live_balance_updated_at || 0) > 0 ? Number(summary.live_balance_updated_at) * 1000 : 0;
  return {
    backendText: backendMs > 0 ? isoText(summary.timestamp_utc) : new Date().toISOString().replace(".000Z", "Z"),
    backendAgeSeconds: backendMs > 0 ? Math.max((nowMs - backendMs) / 1000, 0) : 0,
    strategyAgeSeconds: strategyMs > 0 ? Math.max((nowMs - strategyMs) / 1000, 0) : 0,
    liveBalanceAgeSeconds: liveBalanceMs > 0 ? Math.max((nowMs - liveBalanceMs) / 1000, 0) : 0,
  };
}

function windowTiming(summary) {
  const elapsed = Math.max(Number(summary?.strategy_window_seconds || 0), 0);
  const remaining = Math.max(300 - elapsed, 0);
  return {
    elapsed,
    remaining,
    pct: Math.min((elapsed / 300) * 100, 100),
  };
}

function publicLatestActionLabel(item = latestObservedExecution()) {
  const latest = item;
  if (!latest) return "Sin movimientos recientes";
  const action = String(latest.action || "-").toLowerCase();
  const actionLabel = action === "buy" ? "Compra" : action === "sell" ? "Venta" : action || "-";
  const outcome = friendlyOutcomeName(latest.outcome || "-");
  const price = Number(latest.price || 0);
  const market = String(latest.title || latest.notes || "").trim();
  const marketLabel = market ? ` en ${market}` : "";
  return `${actionLabel} ${outcome}${price > 0 ? ` @ ${fmt(price, 3)}` : ""}${marketLabel}`;
}

function applyPublicModeLabels() {
  document.getElementById("heroCashLabel").textContent = "Exposición pública";
  document.getElementById("heroTargetLabel").textContent = "Último movimiento";
  document.getElementById("heroTriggerLabel").textContent = "Actividad visible";
  document.getElementById("heroFeedLabel").textContent = "Fuente";
  document.getElementById("pnlLabel").textContent = "P&L realizado visible";
  document.getElementById("liveCashLabel").textContent = "Datos del simulador";
  document.getElementById("pendingSignalsLabel").textContent = "Señales del bot";
  document.getElementById("strategyModeLabel").textContent = "Estado de la web";
  document.getElementById("positionsCurrentTitle").textContent = "Posiciones públicas";
  document.getElementById("positionsCurrentCopy").textContent = "Lo que la Data API publica muestra ahora mismo para esta wallet.";
  document.getElementById("positionsArchiveTitle").textContent = "Sincronización del NAS";
  document.getElementById("positionsArchiveCopy").textContent = "Conecta el backend del NAS para separar ventana actual, archivo y simulador.";
  document.getElementById("executionsTitle").textContent = "Actividad pública reciente";
  document.getElementById("executionsCopy").textContent = "Movimientos visibles de la wallet observada, no del simulador del NAS.";
}

function applyBackendDisconnectedLabels() {
  document.getElementById("heroCashLabel").textContent = "Backend del NAS";
  document.getElementById("heroTargetLabel").textContent = "Estado";
  document.getElementById("heroTriggerLabel").textContent = "Siguiente paso";
  document.getElementById("heroFeedLabel").textContent = "Conexión";
  document.getElementById("pnlLabel").textContent = "Ganancia / pérdida";
  document.getElementById("liveCashLabel").textContent = "Capital del simulador";
  document.getElementById("pendingSignalsLabel").textContent = "Compras previstas en esta ventana";
  document.getElementById("strategyModeLabel").textContent = "Estado ahora";
  document.getElementById("positionsCurrentTitle").textContent = "Patas abiertas";
  document.getElementById("positionsCurrentCopy").textContent = "Conecta el backend del NAS para ver la ventana real y sus patas.";
  document.getElementById("positionsArchiveTitle").textContent = "Archivo / resto";
  document.getElementById("positionsArchiveCopy").textContent = "Sin backend no podemos separar ventana actual, archivo y simulador.";
  document.getElementById("executionsTitle").textContent = "Ejecuciones recientes";
  document.getElementById("executionsCopy").textContent = "Esta vista necesita el backend del NAS para mostrar compras, cierres y PnL reales.";
}

function applyLocalModeLabels() {
  document.getElementById("heroCashLabel").textContent = "Capital operativo";
  document.getElementById("heroTargetLabel").textContent = "Reparto actual";
  document.getElementById("heroTriggerLabel").textContent = "Tiempo de ventana";
  document.getElementById("heroFeedLabel").textContent = "Salud del feed";
  document.getElementById("pnlLabel").textContent = "Ganancia / pérdida total";
  document.getElementById("liveCashLabel").textContent = "Capital total del simulador";
  document.getElementById("pendingSignalsLabel").textContent = "Siguientes compras";
  document.getElementById("strategyModeLabel").textContent = "Estado ahora";
  document.getElementById("positionsCurrentTitle").textContent = "Patas abiertas";
  document.getElementById("positionsCurrentCopy").textContent = "Compras que siguen abiertas dentro del mercado actual.";
  document.getElementById("positionsArchiveTitle").textContent = "Otras patas / archivo";
  document.getElementById("positionsArchiveCopy").textContent = "Operaciones fuera del mercado principal o ya desplazadas.";
  document.getElementById("executionsTitle").textContent = "Ejecuciones recientes";
  document.getElementById("executionsCopy").textContent = "Todas las compras y cierres que ha ido haciendo el simulador.";
}

function currentWindowDirection(summary) {
  const breakdown = currentBreakdown(summary);
  if (!breakdown.length) return "Sin posicion abierta";
  return breakdown
    .map((item) => `${friendlyOutcomeName(item.outcome)} ${fmtPct(item.share_pct, 0)} por acciones`)
    .join(" / ");
}

function simplifiedStrategyReason(summary) {
  const note = String(summary?.strategy_last_note || "").trim();
  const noteLower = note.toLowerCase();
  const pairSum = Number(summary?.strategy_pair_sum || 0);
  const totalExposure = Number(summary?.exposure || summary?.strategy_current_market_total_exposure || 0);

  if (!note) return "Esperando a que aparezca un arbitraje con margen real.";
  if (noteLower.includes("drawdown stop")) {
    return "El simulador se ha parado porque supero la perdida maxima permitida.";
  }
  if (noteLower.includes("open market limit reached") || noteLower.includes("concurrent market limit reached")) {
    return totalExposure > 0
      ? `Sigue viva la ventana anterior con ${fmtUsdPlain(totalExposure, 2)} expuestos y el bot no mezcla dos brackets a la vez.`
      : "Hay otra ventana todavía abierta y el bot espera a resolverla antes de abrir la siguiente.";
  }
  if (noteLower.includes("no locked edge")) {
    const strongestEdge = Math.max(Number(summary?.strategy_edge_pct || 0) * 100, 0);
    return pairSum > 0
      ? strongestEdge >= 8
        ? `Se ve ventaja en una pata, pero todavia no compensa abrir el bracket con la mezcla actual. La suma va por ${fmt(pairSum, 3)}.`
        : `No hay margen suficiente ahora mismo para abrir o sesgar el bracket. La suma de las dos patas va por ${fmt(pairSum, 3)}.`
      : "No hay margen suficiente ahora mismo para abrir o sesgar el bracket.";
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
    return "El motor acaba de ejecutar y se toma una pausa muy corta para no sobrerreaccionar.";
  }
  if (noteLower.includes("no active btc5m market")) {
    return "Todavia no hay un bracket BTC 5m listo para operar.";
  }
  return note;
}

function operabilityInfo(summary) {
  const state = String(summary?.strategy_operability_state || "").trim();
  const label = String(summary?.strategy_operability_label || "").trim();
  const reason = String(summary?.strategy_operability_reason || "").trim();
  const blocking = Boolean(summary?.strategy_operability_blocking);
  if (label || reason || state) {
    return {
      state: state || "observing",
      label: label || "Observando",
      reason: reason || simplifiedStrategyReason(summary),
      blocking,
    };
  }
  return {
    state: "observing",
    label: "Observando",
    reason: simplifiedStrategyReason(summary),
    blocking: false,
  };
}

function friendlyWindowState(summary) {
  const openExposure = Number(summary?.strategy_current_market_total_exposure || 0);
  const currentLivePnl = Number(summary?.strategy_current_market_live_pnl || 0);
  const strategyPlanLegs = Number(summary?.strategy_plan_legs || 0);
  const priceMode = String(summary?.strategy_price_mode || "").toLowerCase();
  const bracketPhase = String(summary?.strategy_bracket_phase || "").toLowerCase();
  const lastExecution = latestStrategyExecution();
  const lastAction = String(lastExecution?.action || "").toLowerCase();
  const lastExecutionAgeSeconds = lastExecution ? Math.max((Date.now() / 1000) - Number(lastExecution.ts || 0), 0) : Infinity;
  const note = String(summary?.strategy_last_note || "").toLowerCase();
  const operability = operabilityInfo(summary);

  if (note.includes("drawdown stop")) {
    return { label: "Parado por perdida maxima", detail: simplifiedStrategyReason(summary) };
  }
  if (note.includes("open market limit reached") || note.includes("concurrent market limit reached")) {
    return { label: "Bloqueado por bracket abierto", detail: simplifiedStrategyReason(summary) };
  }
  if (openExposure > 0) {
    if (priceMode === "repair-bracket" || (bracketPhase === "redistribuir" && strategyPlanLegs > 0 && note.includes("repair "))) {
      return {
        label: "Reparando",
        detail: `El bot esta cubriendo la pata infraponderada. Quedan ${fmtUsdPlain(openExposure, 2)} abiertos y va ${fmtUsd(currentLivePnl, 2)} en vivo.`,
      };
    }
    if (bracketPhase === "redistribuir" && strategyPlanLegs <= 0) {
      return {
        label: "Rebalanceando",
        detail: `Hay ${fmtUsdPlain(openExposure, 2)} abiertos en esta ventana, pero el bot no ve precio suficiente para recomponer el reparto sin empeorarlo.`,
      };
    }
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
  return {
    label: operability.label || "Esperando arbitraje",
    detail: operability.reason || simplifiedStrategyReason(summary),
  };
}

function backendWarningText() {
  if (runtimeMode === "local") return "";
  if (runtimeMode === "backend-unreachable") {
    return apiBase
      ? `Aviso: la web no puede conectar con el backend del NAS en ${apiBase}. Revisa la URL publica del dashboard o el tunel.`
      : "Aviso: no hay backend del NAS configurado. Introduce una URL valida arriba para ver el bot real.";
  }
  if (runtimeMode === "public-fallback") {
    return apiBase
      ? `Aviso: esta web no esta conectando bien con el backend del NAS (${apiBase}) y esta mostrando un fallback publico.`
      : "Aviso: esta web esta mostrando un fallback publico porque no tiene backend del NAS configurado.";
  }
  return "Aviso: esta web esta en modo publico y no esta leyendo la base real del NAS.";
}

function shortSlug(slug) {
  const value = String(slug || "");
  if (!value) return "-";
  return value.replace("btc-updown-5m-", "BTC5m ");
}

function setLiveBadge(summary) {
  const badge = document.getElementById("tradingBadge");
  if (!badge) return;
  if (isPublicRuntime()) {
    badge.textContent = "PUBLICO";
    badge.classList.remove("live-badge", "paper-badge");
    return;
  }
  const info = liveControlInfo(summary);
  badge.textContent = tradingModeLabel(summary);
  badge.classList.remove("live-badge", "paper-badge");
  badge.classList.add(info.canExecute ? "live-badge" : "paper-badge");
}

function applyLiveControlUi(summary = lastSummary) {
  const controlBadge = document.getElementById("liveControlBadge");
  const summaryBadge = document.getElementById("liveSummaryBadge");
  const meta = document.getElementById("liveControlMeta");
  const armBtn = document.getElementById("armLiveBtn");
  const pauseBtn = document.getElementById("pauseLiveBtn");
  const summaryNowBtn = document.getElementById("summaryNowBtn");
  if (!controlBadge || !summaryBadge || !meta || !armBtn || !pauseBtn || !summaryNowBtn) return;

  if (isPublicRuntime()) {
    controlBadge.textContent = "PUBLICO";
    summaryBadge.textContent = "Resumen Telegram: n/a";
    meta.textContent = "Conecta el backend real del NAS para armar, pausar y pedir resúmenes.";
    controlBadge.classList.remove("live-badge", "paper-badge");
    controlBadge.classList.add("paper-badge");
    armBtn.disabled = true;
    pauseBtn.disabled = true;
    summaryNowBtn.disabled = true;
    return;
  }

  const info = liveControlInfo(summary);
  const updatedText = info.updatedAt > 0 ? tsToIso(info.updatedAt) : "sin cambios";
  const lastSentText = info.statusSummaryLastSentAt > 0 ? tsToIso(info.statusSummaryLastSentAt) : "sin enviar";
  controlBadge.textContent = info.label.toUpperCase();
  controlBadge.classList.remove("live-badge", "paper-badge");
  controlBadge.classList.add(info.canExecute ? "live-badge" : "paper-badge");
  summaryBadge.textContent = info.statusSummaryEnabled
    ? `Resumen Telegram: ${info.statusSummaryIntervalMinutes || 30}m`
    : "Resumen Telegram: off";
  meta.textContent =
    `${info.reason || (info.isLiveSession ? "sin motivo" : "el bot no esta en live")} | ` +
    `cambio ${updatedText} | ultimo resumen ${lastSentText}`;

  const localAvailable = runtimeMode === "local";
  armBtn.disabled = !localAvailable || !info.isLiveSession || info.canExecute;
  pauseBtn.disabled = !localAvailable || !info.isLiveSession || !info.canExecute;
  summaryNowBtn.disabled = !localAvailable;
  armBtn.title =
    !localAvailable ? "Solo disponible con backend local" : !info.isLiveSession ? "Activa run.py live con LIVE_TRADING=true" : "";
  pauseBtn.title =
    !localAvailable ? "Solo disponible con backend local" : !info.isLiveSession ? "Activa run.py live con LIVE_TRADING=true" : "";
  summaryNowBtn.title = !localAvailable ? "Solo disponible con backend local" : "";
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

function disconnectedSummary() {
  return {
    timestamp_utc: new Date().toISOString(),
    strategy_mode: "btc5m_orderbook",
    strategy_entry_mode: "arb_micro",
    strategy_market_title: "backend NAS desconectado",
    strategy_last_note: "Sin conexion con el backend del NAS",
    strategy_last_updated_at: 0,
    strategy_data_source: "rest-fallback",
    strategy_feed_connected: 0,
    strategy_feed_age_ms: 0,
    strategy_feed_tracked_assets: 0,
    strategy_price_mode: "",
    strategy_pair_sum: 0,
    strategy_edge_pct: 0,
    strategy_fair_value: 0,
    strategy_spot_price: 0,
    strategy_spot_anchor: 0,
    strategy_spot_local_anchor: 0,
    strategy_official_price_to_beat: 0,
    strategy_anchor_source: "",
    strategy_reference_quality: "",
    strategy_reference_comparable: false,
    strategy_reference_note: "",
    strategy_spot_price_mode: "",
    strategy_operability_state: "",
    strategy_operability_label: "",
    strategy_operability_reason: "",
    strategy_operability_blocking: false,
    strategy_window_seconds: 0,
    strategy_plan_legs: 0,
    strategy_current_market_total_exposure: 0,
    strategy_current_market_live_pnl: 0,
    strategy_resolution_count_today: 0,
    strategy_resolution_pnl_today: 0,
    live_cash_balance: 0,
    live_available_to_trade: 0,
    live_equity_estimate: 0,
    live_total_capital: 0,
    live_control_state: "paper",
    live_control_label: "Solo paper",
    live_control_reason: "backend NAS desconectado",
    live_control_updated_at: 0,
    live_control_can_execute: false,
    live_control_is_live_session: false,
    telegram_status_summary_enabled: false,
    telegram_status_summary_interval_minutes: 30,
    telegram_status_summary_last_sent_at: 0,
    exposure: 0,
    exposure_mark: 0,
    open_positions: 0,
    cumulative_pnl: 0,
    realized_pnl: 0,
    unrealized_pnl: 0,
    pnl_total: 0,
    pending_signals: 0,
    live_mode_active: false,
  };
}

function loadSavedApiBase() {
  try {
    return normalizeApiBase(window.localStorage.getItem(API_BASE_STORAGE_KEY) || "");
  } catch (_error) {
    return "";
  }
}

function saveApiBase(value) {
  try {
    const normalized = normalizeApiBase(value);
    if (!normalized) {
      window.localStorage.removeItem(API_BASE_STORAGE_KEY);
      return;
    }
    window.localStorage.setItem(API_BASE_STORAGE_KEY, normalized);
  } catch (_error) {
    // Ignore storage failures.
  }
}

function paintSummary(summary, items = lastPositions) {
  lastSummary = summary;
  const headerBuildMeta = document.getElementById("headerBuildMeta");
  if (isBackendDisconnectedRuntime()) {
    applyBackendDisconnectedLabels();
  } else if (isPublicRuntime()) {
    applyPublicModeLabels();
  } else {
    applyLocalModeLabels();
  }
  paintRuntimeCompare(summary);
  const buckets = splitPositionBuckets(summary, items);
  const windowState = friendlyWindowState(summary);
  const timingInfo = windowTiming(summary);
  const snapshotInfo = snapshotTiming(summary);
  const currentWindowExposure = Number(summary.strategy_current_market_total_exposure ?? buckets.currentSummary.exposure ?? 0);
  const totalExposure = Number(summary.exposure ?? buckets.totalExposure ?? 0);
  const totalExposureMark = Number(summary.exposure_mark ?? totalExposure);
  document.getElementById("openPositionsLabel").textContent = isPublicRuntime()
    ? "Posiciones publicas abiertas"
    : isVidarxLab(summary)
    ? "Patas abiertas en esta ventana"
    : "Patas abiertas ahora";
  document.getElementById("openPositions").textContent = String(isPublicRuntime() ? buckets.totalCount : buckets.currentSummary.count);
  document.getElementById("openPositionsMeta").textContent = isPublicRuntime()
    ? "fuente Data API publica"
    : isVidarxLab(summary)
    ? `totales simulador ${summary.open_positions ?? buckets.totalCount ?? 0}`
    : "operaciones vivas ahora mismo";
  document.getElementById("exposureLabel").textContent = isPublicRuntime()
    ? "Dinero visible en posiciones"
    : isVidarxLab(summary)
    ? "Dinero metido en esta ventana"
    : "Dinero metido total";
  document.getElementById("exposure").textContent = fmtUsdPlain(
    isPublicRuntime() ? totalExposure : isVidarxLab(summary) ? currentWindowExposure : totalExposure,
    2
  );
  document.getElementById("exposureMark").textContent = isPublicRuntime()
    ? `P&L realizado visible ${fmtUsd(Number(summary.realized_pnl ?? summary.cumulative_pnl ?? 0), 2)}`
    : isVidarxLab(summary)
    ? `total simulador ${fmtUsdPlain(totalExposure, 2)} | valor vivo ${fmtUsdPlain(totalExposureMark, 2)}`
    : `valor ahora ${fmtUsdPlain(totalExposureMark, 2)}`;

  const pnlTotal = Number(summary.pnl_total ?? summary.cumulative_pnl ?? 0);
  const realized = Number(summary.realized_pnl ?? summary.cumulative_pnl ?? 0);
  const unrealized = Number(summary.unrealized_pnl ?? 0);
  document.getElementById("pnl").textContent = fmtUsd(pnlTotal, 2);
  document.getElementById("pnlBreakdown").textContent = isPublicRuntime()
    ? "sin mark-to-market fiable en fallback publico"
    : `cerrado ${fmtUsd(realized, 2)} / en vivo ${fmtUsd(unrealized, 2)} | fuente daily_pnl + MTM copy_positions`;
  setCardTone("pnl", pnlTotal);

  document.getElementById("pendingSignals").textContent = isPublicRuntime()
    ? "-"
    : isVidarxLab(summary)
    ? String(summary.strategy_plan_legs ?? 0)
    : String(summary.pending_signals ?? "-");
  const liveCashBalance = Number(summary.live_cash_balance ?? 0);
  const liveAvailableToTrade = Number(summary.live_available_to_trade ?? liveCashBalance);
  const liveEquityEstimate = Number(summary.live_equity_estimate ?? summary.live_total_capital ?? liveCashBalance);
  const liveBalanceUpdatedAt = Number(summary.live_balance_updated_at ?? 0);
  const liveSnapshotText = liveBalanceUpdatedAt > 0 ? tsToIso(liveBalanceUpdatedAt) : "sin snapshot";
  document.getElementById("liveCashBalance").textContent = isPublicRuntime() ? "-" : fmtUsdPlain(liveEquityEstimate, 2);
  document.getElementById("liveCashMeta").textContent = isPublicRuntime()
    ? "requiere backend del NAS para caja, capital y estado reales"
    : `disponible ${fmtUsdPlain(liveAvailableToTrade, 2)} | caja ${fmtUsdPlain(liveCashBalance, 2)} | saldo ${fmtAgeCompact(snapshotInfo.liveBalanceAgeSeconds)}`;
  document.getElementById("heroCashBalance").textContent = isPublicRuntime()
    ? fmtUsdPlain(totalExposure, 2)
    : fmtUsdPlain(liveAvailableToTrade, 2);
  document.getElementById("heroCashMeta").textContent = isPublicRuntime()
    ? `${summary.open_positions ?? buckets.totalCount ?? 0} posiciones visibles | wallet ${shortWallet(watchedWallet)}`
    : `capital total ${fmtUsdPlain(liveEquityEstimate, 2)} | caja ${fmtUsdPlain(liveCashBalance, 2)} | saldo ${fmtAgeCompact(snapshotInfo.liveBalanceAgeSeconds)}`;
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
  setCardTone("livePnlToday", livePnlToday);
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
  const operability = operabilityInfo(summary);
  const strategySpeedLabel = feedInfo.summaryLabel;
  const currentMarketLivePnl = Number(summary.strategy_current_market_live_pnl || buckets.currentSummary.unrealized || 0);
  const currentMarketExposure = Number(summary.strategy_current_market_total_exposure ?? buckets.currentSummary.exposure ?? 0);
  const replenishmentCount = Number(summary.strategy_replenishment_count || 0);
  const timing = timingLabel(summary);
  const strategyNoteText = strategyNote || "sin trigger";
  const latestObserved = latestObservedExecution();
  const transitionText = incubationTransitionMeta(summary);
  const backtestText = variantBacktestMeta(summary);
  const datasetText = datasetMeta(summary);
  document.getElementById("strategyCardMeta").textContent = isPublicRuntime()
    ? "Solo datos publicos. Conecta el backend del NAS para ver el bot real."
    : isVidarxLab(summary)
    ? `${operability.label} | ${windowState.detail} | snapshot ${fmtAgeCompact(snapshotInfo.strategyAgeSeconds)}`
    : strategyOutcome
    ? `${strategyOutcome} @ ${fmt(strategyPrice, 3)} | ${strategyTitle}`
    : strategyNote || strategyTitle;
  document.getElementById("strategyHeroTitle").textContent = isPublicRuntime()
    ? `Perfil publico de ${shortWallet(watchedWallet)}`
    : strategyTitle;
  document.getElementById("heroTargetOutcome").textContent = isPublicRuntime()
    ? publicLatestActionLabel()
    : isVidarxLab(summary)
    ? currentMarketExposure > 0
      ? actualRatio
      : `Objetivo ${desiredRatio}`
    : strategyOutcome
    ? `${strategyOutcome} @ ${fmt(strategyPrice, 3)}`
    : "-";
  document.getElementById("heroTriggerSeen").textContent = isPublicRuntime()
    ? `${summary.open_positions ?? buckets.totalCount ?? 0} posiciones | ${Array.isArray(lastExecutions) ? lastExecutions.length : 0} movimientos`
    : isVidarxLab(summary)
    ? strategyWindowSeconds > 0
      ? `${timingInfo.elapsed}s transcurridos | restan ${timingInfo.remaining}s | ${bracketPhase}`
      : "-"
    : triggerOutcome
    ? `${triggerOutcome} @ ${fmt(triggerPrice, 3)}`
    : "-";
  const heroFeedSource = document.getElementById("heroFeedSource");
  heroFeedSource.textContent = isPublicRuntime()
    ? "Data API publica"
    : spotInfo.referenceComparable
    ? feedInfo.label
    : `${feedInfo.label} | degradado`;
  heroFeedSource.className = isPublicRuntime()
    ? "feed-pill rest"
    : spotInfo.referenceComparable
    ? feedInfo.className
    : "feed-pill rest";
  document.getElementById("heroFeedMeta").textContent = isPublicRuntime()
    ? `${runtimeMode === "public-fallback" ? "fallback publico" : "modo publico"} | backend NAS no conectado | wallet ${shortWallet(watchedWallet)}`
    : edgeInfo.pairSum !== null
      ? `${feedInfo.meta} | ${operability.label.toLowerCase()} | edge ${fmt(edgeInfo.edgePct, 2)}%${spotInfo.available ? ` | ${fmt(spotInfo.deltaBps, 1)}bps` : ""}`
      : `${feedInfo.meta} | ${operability.label.toLowerCase()}${spotInfo.hasCurrent ? ` | BTC ${fmtBtcPrice(spotInfo.current)}` : ""}`;
  document.getElementById("strategyBadge").textContent = strategyLabel(summary);
  setLiveBadge(summary);
  applyLiveControlUi(summary);

  const modeText =
    runtimeMode === "local"
      ? "local db mode"
      : runtimeMode === "public-fallback"
      ? `public api fallback (${watchedWallet})`
      : `public api mode (${watchedWallet})`;
  document.getElementById("lastUpdated").textContent = `Ultima actualizacion ${snapshotInfo.backendText} | snapshot ${fmtAgeCompact(snapshotInfo.backendAgeSeconds)} | ${modeText}`;
  document.getElementById("headerTimestamp").textContent = `snapshot backend ${snapshotInfo.backendText} | motor ${fmtAgeCompact(snapshotInfo.strategyAgeSeconds)}`;
  if (headerBuildMeta) {
    headerBuildMeta.textContent = `ui ${UI_BUILD} | api ${String(summary?.dashboard_build || "-")}`;
  }
  const lastUpdatedHero = document.getElementById("lastUpdatedHero");
  if (lastUpdatedHero) {
    lastUpdatedHero.textContent = `backend ${snapshotInfo.backendText} | motor ${fmtAgeCompact(snapshotInfo.strategyAgeSeconds)}`;
  }
  document.getElementById("runtimeBadge").textContent = modeLabel();
  const lastLiveExecution = Number(summary.last_live_execution_ts || 0);
  const lastLiveText = lastLiveExecution > 0 ? tsToIso(lastLiveExecution) : "sin operaciones live";
  const backendWarning = backendWarningText();
  const strategySummary = isPublicRuntime()
    ? `Mostrando datos publicos de ${shortWallet(watchedWallet)}. Para ver el bot real necesitas el backend del NAS.`
    : isVidarxLab(summary)
    ? currentMarketExposure > 0
      ? `${windowState.label}. Objetivo ${desiredRatio}, actual ${actualRatio}, restan ${timingInfo.remaining}s. Exposicion ${fmtUsdPlain(currentMarketExposure, 2)} y capital ${fmtUsdPlain(liveEquityEstimate, 2)}.`
      : `${windowState.label}. ${operability.label}. Capital ${fmtUsdPlain(liveEquityEstimate, 2)} y caja ${fmtUsdPlain(liveAvailableToTrade, 2)}.`
    : `Modo ${tradingModeLabel(summary)}. Disponible ${fmtUsdPlain(liveAvailableToTrade, 2)}. ${strategyNoteText}.`;
  const incubationText = incubationMeta(summary);
  const researchText = [transitionText, backtestText, datasetText].filter(Boolean).join(". ");
  document.getElementById("systemNotice").textContent = backendWarning
    ? `${backendWarning} ${strategySummary}${incubationText ? ` ${incubationText}.` : ""}${researchText ? ` ${researchText}.` : ""}`
    : `${strategySummary}${incubationText ? ` ${incubationText}.` : ""}${researchText ? ` ${researchText}.` : ""}`;

  paintLabOverview(summary);
  paintMicrostructurePanels(summary);
}

function paintLabOverview(summary) {
  if (isPublicRuntime()) {
    const totalExposure = Number(summary.exposure ?? 0);
    document.getElementById("labModeValue").textContent = "Perfil publico";
    document.getElementById("labBudgetRemaining").textContent = "-";
    document.getElementById("labEffectiveMin").textContent = "-";
    document.getElementById("labBlockGate").textContent = "requiere backend";
    document.getElementById("labWindowValue").textContent = String(latestObservedExecution()?.title || lastPositions[0]?.title || "-");
    document.getElementById("labFeedValue").textContent = "Data API publica";
    document.getElementById("labOperabilityValue").textContent = "Sin backend";
    document.getElementById("labOperabilityReason").textContent = "Necesitas conectar el backend real del NAS para saber si el bot puede operar.";
    document.getElementById("labSpotCurrent").textContent = "-";
    document.getElementById("labChainlinkPrice").textContent = "-";
    document.getElementById("labSpotAnchor").textContent = "-";
    document.getElementById("labSpotDelta").textContent = "requiere backend del NAS";
    document.getElementById("labChainlinkDelta").textContent = "requiere backend del NAS";
    document.getElementById("labAnchorDrift").textContent = "-";
    document.getElementById("labSpotFair").textContent = "no disponible en fallback";
    document.getElementById("labEdgeValue").textContent = "no disponible";
    document.getElementById("labWindowFill").style.width = "0%";
    document.getElementById("labExposureFill").style.width = `${Math.min((totalExposure / 1000) * 100, 100)}%`;
    document.getElementById("labMeta").textContent =
      `Solo datos publicos de ${shortWallet(watchedWallet)}.`;
    return;
  }
  const timingInfo = windowTiming(summary);
  const snapshotInfo = snapshotTiming(summary);
  const windowSeconds = timingInfo.elapsed;
  const windowPct = timingInfo.pct;
  const deployed = Math.max(Number(summary.strategy_current_market_total_exposure ?? 0), 0);
  const cycleBudget = Math.max(Number(summary.strategy_cycle_budget || 0), 0);
  const remainingBudget = Math.max(Number(summary.strategy_cycle_budget_remaining ?? cycleBudget - deployed), 0);
  const effectiveMinNotional = Math.max(Number(summary.strategy_effective_min_notional || 0), 0);
  const budgetShortfall = Math.max(Number(summary.strategy_cycle_budget_shortfall || 0), 0);
  const exposurePct = cycleBudget > 0 ? Math.min((deployed / cycleBudget) * 100, 100) : 0;
  const feedInfo = feedModeInfo(summary);
  const edgeInfo = currentEdgeInfo(summary);
  const spotInfo = currentSpotInfo(summary);
  const operability = operabilityInfo(summary);
  const visibleComparablePrice = spotInfo.hasCurrent ? spotInfo.current : spotInfo.polymarketCurrent;
  const visibleDeltaUsd = visibleComparablePrice > 0 && spotInfo.hasAnchor ? spotInfo.deltaUsd : 0;
  const visibleDeltaBps = visibleComparablePrice > 0 && spotInfo.hasAnchor ? spotInfo.deltaMarketBps : 0;
  const fastSpotPrice = spotInfo.hasBinance ? spotInfo.binance : spotInfo.fallbackCurrent;
  const beatLabel =
    spotInfo.beatKind === "official"
      ? "Price to beat oficial"
      : spotInfo.beatKind === "rtds-derived"
      ? "Price to beat RTDS derivado"
      : spotInfo.hasAnchor
      ? "Price to beat sin oficial"
      : "Price to beat";
  const beatDeltaLabel =
    spotInfo.beatKind === "official"
      ? "Polymarket vs beat oficial"
      : spotInfo.beatKind === "rtds-derived"
      ? "Polymarket vs beat RTDS"
      : "Polymarket vs beat usado";
  const fastBeatDeltaLabel =
    spotInfo.beatKind === "official"
      ? "Spot rapido vs beat oficial"
      : spotInfo.beatKind === "rtds-derived"
      ? "Spot rapido vs beat RTDS"
      : "Spot rapido vs beat usado";
  const anchorDriftLabel =
    spotInfo.beatKind === "official"
      ? "Ancla propia vs oficial"
      : spotInfo.beatKind === "rtds-derived"
      ? "Ancla propia vs RTDS"
      : "Ancla propia (sin oficial)";
  const beatMeta =
    spotInfo.beatKind === "official"
      ? `beat oficial ${fmtBtcPrice(spotInfo.officialBeat)}`
      : spotInfo.beatKind === "rtds-derived"
      ? `beat RTDS ${fmtBtcPrice(spotInfo.anchor)}`
      : spotInfo.hasAnchor
      ? `ancla usada ${fmtBtcPrice(spotInfo.anchor)}`
      : "";

  document.getElementById("labModeValue").textContent = isVidarxLab(summary)
    ? `${timingLabel(summary)} / ${String(summary.strategy_price_mode || "sin banda").replaceAll("-", " ")}`
    : strategyLabel(summary);
  document.getElementById("labBudgetRemaining").textContent =
    `${fmtUsdPlain(remainingBudget, 2)} de ${fmtUsdPlain(cycleBudget, 2)}`;
  document.getElementById("labEffectiveMin").textContent =
    `${fmtUsdPlain(effectiveMinNotional, 2)}${budgetShortfall > 0 ? ` | faltan ${fmtUsdPlain(budgetShortfall, 2)}` : ""}`;
  document.getElementById("labBlockGate").textContent =
    Boolean(summary.strategy_operability_blocking)
      ? operability.label
      : String(summary.strategy_last_note || "").trim() || "sin bloqueo";
  document.getElementById("labWindowValue").textContent =
    String(summary.strategy_market_title || summary.strategy_market_slug || "-");
  document.getElementById("labFeedValue").textContent = spotInfo.referenceComparable ? feedInfo.label : `${feedInfo.label} | degradado`;
  document.getElementById("labOperabilityValue").textContent = operability.label;
  document.getElementById("labOperabilityReason").textContent = operability.reason;
  document.getElementById("labSpotCurrentLabel").textContent =
    spotInfo.priceMode === "lead-basis"
      ? "Spot comparable usado (lead+basis)"
      : spotInfo.priceMode === "lead"
      ? "Spot usado (rapido)"
      : "Spot comparable usado";
  document.getElementById("labFastSpotLabel").textContent = spotInfo.hasBinance ? "Spot externo rapido" : "Spot observado";
  document.getElementById("labBeatLabel").textContent = beatLabel;
  document.getElementById("labBeatDeltaLabel").textContent = beatDeltaLabel;
  document.getElementById("labFastBeatDeltaLabel").textContent = fastBeatDeltaLabel;
  document.getElementById("labAnchorDriftLabel").textContent = anchorDriftLabel;
  document.getElementById("labSpotCurrent").textContent =
    visibleComparablePrice > 0 ? fmtBtcPrice(visibleComparablePrice) : "-";
  document.getElementById("labChainlinkPrice").textContent = fastSpotPrice > 0 ? fmtBtcPrice(fastSpotPrice) : "-";
  document.getElementById("labSpotAnchor").textContent =
    spotInfo.beatKind === "official"
      ? fmtBtcPrice(spotInfo.officialBeat)
      : spotInfo.beatKind === "rtds-derived"
      ? `${fmtBtcPrice(spotInfo.anchor)} (RTDS derivado)`
      : spotInfo.hasAnchor
      ? `${fmtBtcPrice(spotInfo.anchor)} (sin oficial)`
      : "-";
  document.getElementById("labSpotDelta").textContent =
    visibleComparablePrice > 0 && spotInfo.hasAnchor
      ? `${fmtUsd(visibleDeltaUsd, 2)} | ${fmt(visibleDeltaBps, 1)}bps${spotInfo.referenceComparable ? "" : " | degradado"}`
      : spotInfo.hasCurrent || spotInfo.hasChainlink
      ? `degradado: ${spotInfo.referenceNote}`
      : "-";
  document.getElementById("labChainlinkDelta").textContent =
    fastSpotPrice > 0 && spotInfo.hasAnchor
      ? `${fmtUsd(spotInfo.hasBinance ? spotInfo.binanceDeltaUsd : spotInfo.deltaUsd, 2)} | ${fmt(spotInfo.hasBinance ? spotInfo.binanceDeltaBps : spotInfo.deltaMarketBps, 1)}bps${spotInfo.referenceComparable ? "" : " | degradado"}`
      : fastSpotPrice > 0
      ? "esperando beat oficial"
      : "-";
  document.getElementById("labAnchorDrift").textContent =
    spotInfo.hasLocalAnchor && spotInfo.hasAnchor
      ? `${fmtUsd(spotInfo.hasOfficialBeat ? spotInfo.anchorDriftUsd : spotInfo.anchorDriftRefUsd, 2)} | ${fmt(spotInfo.hasOfficialBeat ? spotInfo.anchorDriftBps : spotInfo.anchorDriftRefBps, 1)}bps${spotInfo.beatKind === "official" ? "" : " | derivado"}`
      : spotInfo.hasLocalAnchor
      ? `${fmtBtcPrice(spotInfo.localAnchor)} (sin oficial)`
      : "-";
  document.getElementById("labSpotFair").textContent =
    spotInfo.available
      ? `Sube ${fmtPct(spotInfo.fairUp * 100, 1)} / Baja ${fmtPct(spotInfo.fairDown * 100, 1)}`
      : spotInfo.hasCurrent || spotInfo.hasChainlink
      ? `degradado: ${spotInfo.referenceNote}`
      : "-";
  document.getElementById("labEdgeValue").textContent =
    edgeInfo.edgePct !== null
      ? `${edgeInfo.label} | ${fmt(edgeInfo.edgePct, 2)}%${edgeInfo.fairValue ? ` | fair ${fmt(edgeInfo.fairValue, 3)}` : ""}${spotInfo.anchorSource !== "-" ? ` | ancla ${spotInfo.anchorSource}` : ""}${spotInfo.referenceQuality ? ` | ref ${spotInfo.referenceQuality}` : ""}`
      : "-";
  document.getElementById("labWindowFill").style.width = `${windowPct}%`;
  document.getElementById("labExposureFill").style.width = `${exposurePct}%`;
  document.getElementById("labMeta").textContent = isVidarxLab(summary)
    ? `${windowSeconds}s | restan ${timingInfo.remaining}s | objetivo ${desiredRatioLabel(summary)} | actual ${actualRatioLabel(summary)} | ${bracketPhaseLabel(summary).toLowerCase()} | dinero ${fmtUsdPlain(deployed, 2)} | restante ${fmtUsdPlain(remainingBudget, 2)} | min ${fmtUsdPlain(effectiveMinNotional, 2)}${spotInfo.referenceComparable ? ` | ref ${spotInfo.referenceQuality}` : ` | degradado: ${spotInfo.referenceNote}`}`
    : `modo ${strategyLabel(summary)} | trigger ${summary.strategy_trigger_outcome || "-"} @ ${fmt(Number(summary.strategy_trigger_price_seen || 0), 3)}`;
}

function paintMicrostructurePanels(summary) {
  const pressureList = document.getElementById("microPressureList");
  const pressureMeta = document.getElementById("microPressureMeta");
  const pressureBadge = document.getElementById("microPressureBadge");
  const decisionList = document.getElementById("microDecisionList");
  const decisionMeta = document.getElementById("microDecisionMeta");
  const decisionBadge = document.getElementById("microDecisionBadge");
  const liquidationList = document.getElementById("microLiquidationList");
  const liquidationMeta = document.getElementById("microLiquidationMeta");
  const liquidationBadge = document.getElementById("microLiquidationBadge");
  const latencyList = document.getElementById("microLatencyList");
  const latencyMeta = document.getElementById("microLatencyMeta");
  const latencyBadge = document.getElementById("microLatencyBadge");

  if (isPublicRuntime() || isBackendDisconnectedRuntime()) {
    const fallbackText = isPublicRuntime()
      ? "requiere backend del NAS para microestructura en tiempo real"
      : "backend desconectado: sin telemetria de microestructura";
    pressureBadge.textContent = "off";
    decisionBadge.textContent = "off";
    liquidationBadge.textContent = "off";
    latencyBadge.textContent = "off";
    pressureList.innerHTML = `<li class="mini-item"><strong>Sin stream interno</strong><span>${fallbackText}</span></li>`;
    decisionList.innerHTML = `<li class="mini-item"><strong>Sin decision trace</strong><span>${fallbackText}</span></li>`;
    liquidationList.innerHTML = `<li class="mini-item"><strong>Sin liquidation feed</strong><span>${fallbackText}</span></li>`;
    latencyList.innerHTML = `<li class="mini-item"><strong>Sin latency trace</strong><span>${fallbackText}</span></li>`;
    pressureMeta.textContent = fallbackText;
    decisionMeta.textContent = fallbackText;
    liquidationMeta.textContent = fallbackText;
    latencyMeta.textContent = fallbackText;
    return;
  }

  const micro = summary?.microstructure_snapshot || {};
  const frame = micro?.frame || {};
  const decision = micro?.decision || {};
  const liquidations = summary?.liquidations_snapshot || {};
  const liquidationTotals = liquidations?.totals || {};
  const latency = summary?.latency_snapshot?.latencies || {};
  const recentLiquidations = Array.isArray(liquidations?.recent) ? liquidations.recent : [];
  const blockedBy = Array.isArray(summary?.strategy_decision_blocked_by)
    ? summary.strategy_decision_blocked_by
    : String(summary?.strategy_decision_blocked_by || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
  const timingInfo = windowTiming(summary);

  const internalBull5 = Number(summary?.strategy_internal_bullish_pressure_5s ?? frame.internal_bullish_pressure_5s ?? 0);
  const internalBear5 = Number(summary?.strategy_internal_bearish_pressure_5s ?? frame.internal_bearish_pressure_5s ?? 0);
  const externalSpot5 = Number(summary?.strategy_external_spot_pressure_5s ?? frame.external_spot_pressure_5s ?? 0);
  const cvd5 = Number(summary?.strategy_cvd_5s ?? frame.cvd_5s ?? 0);
  const cvd30 = Number(summary?.strategy_cvd_30s ?? frame.cvd_30s ?? 0);
  const liqBias = Number(summary?.strategy_liq_buy_notional_30s ?? 0) - Number(summary?.strategy_liq_sell_notional_30s ?? 0);
  const pressureTilt =
    internalBull5 > internalBear5 ? "bullish" : internalBear5 > internalBull5 ? "bearish" : "neutral";
  pressureBadge.textContent = pressureTilt;
  pressureList.innerHTML = [
    ["Pressure interna 5s", `bull ${fmtUsdPlain(internalBull5, 2)} | bear ${fmtUsdPlain(internalBear5, 2)}`],
    ["Spot pressure 5s", fmtBps(externalSpot5, 1)],
    ["CVD", `5s ${fmtUsd(cvd5, 2)} | 30s ${fmtUsd(cvd30, 2)}`],
    ["Spread Up / Down", `${fmtBps(summary?.strategy_spread_bps_up ?? frame.spread_bps_up ?? 0, 1)} / ${fmtBps(summary?.strategy_spread_bps_down ?? frame.spread_bps_down ?? 0, 1)}`],
    ["Pair sum / locked edge", `${fmtBps(frame.pair_sum_bps ?? 0, 1)} | edge ${fmtBps(frame.locked_edge_bps ?? 0, 1)}`],
    ["Liquidation bias 30s", fmtUsd(liqBias, 2)],
  ]
    .map(
      ([label, value]) => `
      <li class="mini-item">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(String(value))}</span>
      </li>
    `
    )
    .join("");
  pressureMeta.textContent = `snapshot ${isoText(summary?.microstructure_snapshot_generated_at)} | paired OFI ${fmt(frame.paired_ofi_z ?? 0, 2)} | paired CVD ${fmt(frame.paired_cvd ?? 0, 2)}`;

  const selectedExecution = String(summary?.strategy_selected_execution || decision.selected_execution || "no-trade");
  decisionBadge.textContent = selectedExecution || "no-trade";
  decisionList.innerHTML = [
    ["Readiness", `${fmtPct(summary?.strategy_readiness_score ?? frame.readiness_score ?? 0, 1)} | ${String(summary?.strategy_regime || frame.regime || "-")}`],
    ["Signal", `${String(summary?.strategy_signal_side || decision.signal_side || "-")} | edge ${fmtBps(summary?.strategy_expected_edge_bps ?? decision.expected_edge_bps ?? 0, 1)}`],
    ["EV maker / taker", `${fmtBps(summary?.strategy_maker_ev_bps ?? decision.maker_ev_bps ?? 0, 1)} / ${fmtBps(summary?.strategy_taker_ev_bps ?? decision.taker_ev_bps ?? 0, 1)}`],
    ["BBO Up", `${fmt(Number(summary?.strategy_best_bid_up ?? frame.best_bid_up ?? 0), 3)} / ${fmt(Number(summary?.strategy_best_ask_up ?? frame.best_ask_up ?? 0), 3)}`],
    ["BBO Down", `${fmt(Number(summary?.strategy_best_bid_down ?? frame.best_bid_down ?? 0), 3)} / ${fmt(Number(summary?.strategy_best_ask_down ?? frame.best_ask_down ?? 0), 3)}`],
    ["Tiempo / fase", `${timingInfo.remaining}s | ${String(summary?.strategy_window_third || frame.window_third || "-")}`],
    ["Blockers", blockedBy.length ? blockedBy.join(" | ") : "sin bloqueo activo"],
  ]
    .map(
      ([label, value]) => `
      <li class="mini-item">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(String(value))}</span>
      </li>
    `
    )
    .join("");
  decisionMeta.textContent = String(summary?.strategy_last_note || micro?.note || "sin nota de decision");

  const exchangeTotals = Object.entries(liquidationTotals?.by_exchange_5m || {});
  liquidationBadge.textContent = String(recentLiquidations.length || 0);
  const liquidationRows = [
    ["Burst 30s", `buy ${fmtUsdPlain(liquidationTotals?.buy_30s ?? 0, 2)} | sell ${fmtUsdPlain(liquidationTotals?.sell_30s ?? 0, 2)}`],
    ["Burst 5m", `buy ${fmtUsdPlain(liquidationTotals?.buy_5m ?? 0, 2)} | sell ${fmtUsdPlain(liquidationTotals?.sell_5m ?? 0, 2)}`],
    ["Z-score / near cluster", `${fmt(summary?.strategy_liq_burst_zscore ?? frame.liq_burst_zscore ?? 0, 2)} | ${fmtBps(summary?.strategy_near_liq_cluster_distance_bps ?? frame.near_liq_cluster_distance_bps ?? 0, 1)}`],
  ];
  exchangeTotals.slice(0, 2).forEach(([exchange, notional]) => {
    liquidationRows.push([`5m ${exchange}`, fmtUsdPlain(notional, 2)]);
  });
  recentLiquidations.slice(0, 3).forEach((item) => {
    liquidationRows.push([
      `${String(item.exchange || "-").toUpperCase()} ${String(item.side || "-")}`,
      `${fmtUsdPlain(item.notional || 0, 2)} @ ${fmtBtcPrice(item.price || 0)}`,
    ]);
  });
  liquidationList.innerHTML = liquidationRows
    .map(
      ([label, value]) => `
      <li class="mini-item">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(String(value))}</span>
      </li>
    `
    )
    .join("");
  liquidationMeta.textContent = `snapshot ${isoText(summary?.liquidations_snapshot_generated_at)} | ${recentLiquidations.length} eventos recientes`;

  const marketLag = Number(summary?.strategy_market_event_lag_ms ?? latency.market_event_lag_ms ?? 0);
  const spotAge = Number(summary?.strategy_spot_age_ms ?? latency.spot_age_ms ?? 0);
  const featureCompute = Number(latency.feature_compute_ms ?? 0);
  const decisionBlockers = Number(latency.decision_blockers ?? blockedBy.length ?? 0);
  latencyBadge.textContent = String(summary?.strategy_data_source || "idle");
  latencyList.innerHTML = [
    ["Feed", `${String(summary?.strategy_data_source || "-")} | ${Boolean(summary?.strategy_feed_connected) ? "connected" : "offline"}`],
    ["Assets / lag mercado", `${String(summary?.strategy_feed_tracked_assets ?? 0)} | ${fmt(marketLag, 1)} ms`],
    ["Edad spot / compute", `${spotAge} ms | ${fmt(featureCompute, 2)} ms`],
    ["Blockers runtime", String(decisionBlockers)],
    ["Execution mode", selectedExecution],
  ]
    .map(
      ([label, value]) => `
      <li class="mini-item">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(String(value))}</span>
      </li>
    `
    )
    .join("");
  latencyMeta.textContent = `snapshot ${isoText(summary?.latency_snapshot_generated_at)} | backend ${isoText(summary?.timestamp_utc)}`;
}

function paintSelectedWallets(items) {
  if (isPublicRuntime()) {
    const body = document.getElementById("selectedWalletsList");
    const latest = latestObservedExecution();
    const publicRows = [
      ["Wallet observada", shortWallet(watchedWallet)],
      ["Posiciones abiertas", String(lastSummary?.open_positions ?? lastPositions.length ?? 0)],
      ["Dinero visible", fmtUsdPlain(Number(lastSummary?.exposure ?? 0), 2)],
      ["P&L realizado visible", fmtUsd(Number(lastSummary?.realized_pnl ?? lastSummary?.cumulative_pnl ?? 0), 2)],
      ["Ultimo movimiento", latest ? publicLatestActionLabel() : "Sin movimientos recientes"],
    ];
    document.getElementById("selectedWalletsCount").textContent = String(publicRows.length);
    body.innerHTML = publicRows
      .map(
        ([label, value]) => `
      <li class="mini-item">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(value)}</span>
      </li>
    `
      )
      .join("");
    document.getElementById("selectedWalletsMeta").textContent =
      "Vista publica: util para seguir una wallet, no para ver el estado real del simulador del NAS.";
    return;
  }
  const body = document.getElementById("selectedWalletsList");
  if (isVidarxLab()) {
    const breakdown = currentBreakdown(lastSummary);
    const state = friendlyWindowState(lastSummary);
    const currentExposure = Number(lastSummary?.strategy_current_market_total_exposure ?? 0);
    const currentLivePnl = Number(lastSummary?.strategy_current_market_live_pnl || 0);
    const planRows = [
      ["Estado", state.label],
      ["Objetivo", desiredRatioLabel(lastSummary)],
      ["Actual", actualRatioLabel(lastSummary)],
      ["Fase", bracketPhaseLabel(lastSummary)],
      ["Reparto actual", currentWindowDirection(lastSummary)],
      ["Dinero metido", fmtUsdPlain(currentExposure, 2)],
      ["Acciones totales", fmt(Number(lastSummary?.strategy_current_market_total_shares || 0), 2)],
      ["PnL de esta ventana", fmtUsd(currentLivePnl, 2)],
      ...breakdown.map((item) => [
        friendlyOutcomeName(item.outcome),
        `${fmt(Number(item.shares || 0), 2)} acc. | ${fmtPct(item.share_pct, 0)} por acciones | ${fmtPct(Number(item.money_share_pct || 0), 0)} del dinero | ${fmtUsdPlain(Number(item.exposure || 0), 2)} | vivo ${fmtUsd(Number(item.unrealized_pnl || 0), 2)}`,
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
  if (isPublicRuntime()) {
    const body = document.getElementById("riskBlocksList");
    const items = (Array.isArray(lastExecutions) ? lastExecutions : []).slice(0, 6);
    document.getElementById("riskBlocksCount").textContent = String(items.length);
    if (!items.length) {
      body.innerHTML = `<li class="mini-item"><strong>Sin actividad visible</strong><span>La Data API aun no devuelve movimientos para esta wallet.</span></li>`;
      document.getElementById("riskBlocksMeta").textContent = "sin actividad publica reciente";
      return;
    }
    body.innerHTML = items
      .map(
        (item) => `
      <li class="mini-item">
        <strong>${escapeHtml(tsToIso(item.ts))}</strong>
        <span>${escapeHtml(publicLatestActionLabel(item))}</span>
        <span>${fmtUsdPlain(Number(item.notional || 0), 2)}</span>
      </li>
    `
      )
      .join("");
    document.getElementById("riskBlocksMeta").textContent =
      "Ultimos movimientos visibles en la Data API publica.";
    return;
  }
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
        <span>${escapeHtml(item.winning_outcome || "sin ganador")} | desplegado ${fmtUsdPlain(Number(item.deployed_notional || item.notional || 0), 2)}${Number(item.planned_budget || 0) > 0 ? ` | plan ${fmtUsdPlain(Number(item.planned_budget || 0), 2)}` : ""}</span>
        <span class="${Number(item.pnl || 0) > 0 ? "pnl-pos" : Number(item.pnl || 0) < 0 ? "pnl-neg" : "pnl-flat"}">${fmtUsd(Number(item.pnl || 0), 2)}</span>
      </li>
    `
      )
      .join("");
    document.getElementById("riskBlocksMeta").textContent = `hoy ${lastSummary?.strategy_resolution_count_today || 0} ventanas | ${fmtUsd(Number(lastSummary?.strategy_resolution_pnl_today || 0), 2)}${lastSummary?.strategy_incubation_recommendation_label ? ` | ${String(lastSummary.strategy_incubation_recommendation_label).toLowerCase()}` : ""}`;
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
  const leaderboard = Array.isArray(summary?.strategy_variant_leaderboard) ? summary.strategy_variant_leaderboard : [];
  if (leaderboard.length) {
    count.textContent = String(leaderboard.length);
    body.innerHTML = leaderboard
      .map((item) => {
        const pnl = Number(item.pnl || 0);
        const pnlClass = pnl > 0 ? "pnl-pos" : pnl < 0 ? "pnl-neg" : "pnl-flat";
        return `
          <li class="mini-item">
            <strong>#${Number(item.rank || 0)} ${escapeHtml(String(item.variant || "-"))}</strong>
            <span>edge ${fmt(Number(item.real_edge_bps || 0), 1)}bps | fill ${fmtPct(Number(item.fill_rate || 0) * 100, 0)} | hit ${fmtPct(Number(item.hit_rate || 0) * 100, 0)}</span>
            <span class="${pnlClass}">${escapeHtml(String(item.status || "-"))} | pnl ${fmtUsd(pnl, 2)} | dd ${fmtUsdPlain(Math.abs(Number(item.drawdown || 0)), 2)}</span>
          </li>
        `;
      })
      .join("");
    meta.textContent = `${variantBacktestMeta(summary)}${datasetMeta(summary) ? ` | ${datasetMeta(summary)}` : ""}`;
    return;
  }

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

function paintWalletHypotheses(summary) {
  const body = document.getElementById("walletHypothesesList");
  const count = document.getElementById("walletHypothesesCount");
  const meta = document.getElementById("walletHypothesesMeta");
  if (isPublicRuntime()) {
    count.textContent = "0";
    body.innerHTML = `<li class="mini-item"><strong>Sin backend</strong><span>Las hipotesis salen del miner del NAS y no de la Data API publica.</span></li>`;
    meta.textContent = "requiere backend del NAS";
    return;
  }
  const items = Array.isArray(summary?.strategy_wallet_hypotheses) ? summary.strategy_wallet_hypotheses : [];
  const patterns = Array.isArray(summary?.strategy_wallet_patterns) ? summary.strategy_wallet_patterns : [];
  count.textContent = String(items.length);
  if (!items.length) {
    body.innerHTML = `<li class="mini-item"><strong>Sin hipotesis aun</strong><span>Ejecuta el miner de wallets para extraer patrones convertibles en variantes.</span></li>`;
    meta.textContent = "sin artefacto de hypotheses";
    return;
  }
  body.innerHTML = items
    .map((item) => `
      <li class="mini-item">
        <strong>${escapeHtml(String(item.title || "-"))}</strong>
        <span>${escapeHtml(String(item.detail || ""))}</span>
      </li>
    `)
    .join("");
  meta.textContent = patterns.length
    ? patterns
        .slice(0, 2)
        .map((item) => `${String(item.label || "-").toLowerCase()}: ${String(item.value || "-")}`)
        .join(" | ")
    : "patrones derivados de wallets top";
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

  const currentExposure = Number(lastSummary?.strategy_current_market_total_exposure ?? btcSummary.exposure ?? 0);
  const currentLivePnl = Number(lastSummary?.strategy_current_market_live_pnl || btcSummary.unrealized || 0);
  document.getElementById("btcBucketCount").textContent = `${btcItems.length} ops.`;
  document.getElementById("btcBucketExposure").textContent = fmtUsdPlain(currentExposure, 2);
  document.getElementById("btcBucketPnl").textContent = fmtUsd(currentLivePnl, 2);

  if (isBackendDisconnectedRuntime()) {
    document.getElementById("generalBucketCount").textContent = "sin NAS";
    document.getElementById("generalBucketExposure").textContent = "-";
    document.getElementById("generalBucketPnl").textContent = "-";
  } else if (isPublicRuntime()) {
    document.getElementById("generalBucketCount").textContent = "sin NAS";
    document.getElementById("generalBucketExposure").textContent = "-";
    document.getElementById("generalBucketPnl").textContent = "-";
  } else if (isVidarxLab()) {
    const resolvedNotional = (Array.isArray(lastSummary?.strategy_recent_resolutions) ? lastSummary.strategy_recent_resolutions : []).reduce(
      (acc, item) => acc + Number(item.deployed_notional || item.notional || 0),
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
      paintWalletHypotheses(summary || {});
      return;
    }

    if (isBackendDisconnectedRuntime()) {
      const summary = disconnectedSummary();
      paintExecutions([]);
      paintSummary(summary, []);
      paintPositions([]);
      paintSignals([]);
      paintSelectedWallets([]);
      paintRiskBlocks({ items: [], hours: 24, blocked_total: 0 });
      paintExposureDonut(summary || {});
      paintOperationPnl([]);
      paintStrategySetups(summary || {});
      paintWalletHypotheses(summary || {});
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
      outcome: item.outcome || "",
      size: Number(item.size || 0),
      price: Number(item.price || 0),
      notional: Number(item.size || 0) * Number(item.price || 0),
      source_wallet: watchedWallet,
      title: item.title || item.market || item.slug || "",
      pnl_delta: 0,
      notes: item.title || item.market || item.slug || "-",
    }));

    const realized = positions.reduce((acc, item) => acc + Number(item.realized_pnl || 0), 0);
    const summary = {
      timestamp_utc: new Date().toISOString(),
      strategy_entry_mode: "",
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
    paintWalletHypotheses(summary);
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
  const value = normalizeApiBase(input.value || "");
  apiBase = value;
  input.value = apiBase;
  saveApiBase(apiBase);
  try {
    await getJson(withCacheBust(buildApiUrl("/api/health")));
    runtimeMode = "local";
    document.querySelector(".kicker").textContent = "Proyecto principal (Local DB)";
    document.getElementById("runtimeBadge").textContent = modeLabel();
    document.getElementById("lastUpdated").textContent = `Backend API guardado: ${apiBase || "local"}`;
    document.getElementById("resetBtn").disabled = false;
    document.getElementById("resetBtn").title = "";
    document.getElementById("resetCompareBtn").disabled = false;
    document.getElementById("resetCompareBtn").title = "";
  } catch (error) {
    runtimeMode = apiBase ? "backend-unreachable" : "backend-unreachable";
    document.querySelector(".kicker").textContent =
      "Proyecto principal (Backend NAS desconectado)";
    document.getElementById("runtimeBadge").textContent = modeLabel();
    document.getElementById("resetBtn").disabled = true;
    document.getElementById("resetBtn").title = "Solo disponible cuando el dashboard esta conectado al backend local";
    document.getElementById("resetCompareBtn").disabled = true;
    document.getElementById("resetCompareBtn").title =
      "Solo disponible cuando el dashboard esta conectado al backend local";
    document.getElementById("lastUpdated").textContent = apiBase
      ? `No conecta con ${apiBase}: ${error.message}. La web queda en modo backend desconectado.`
      : "Backend API borrado. La web queda en modo backend desconectado.";
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

  const runtimeLabel = String(lastSummary?.strategy_runtime_mode || "runtime actual").trim() || "runtime actual";
  const accepted = window.confirm(
    `Esto limpiara el ledger del runtime actual (${runtimeLabel}): posiciones, senales, ejecuciones, pnl diario y ventanas de estrategia. Mantiene el mercado y la referencia actual para que pueda seguir operando. No reinicia procesos ni toca otros runtimes. Continuar?`
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
    const windows = Number(deleted.strategy_windows || 0);
    const dailyPnl = Number(deleted.daily_pnl || 0);
    document.getElementById("lastUpdated").textContent =
      `Limpieza de ${runtimeLabel}: posiciones ${positions}, ejecuciones ${executions}, senales ${signals}, ventanas ${windows}, pnl diario ${dailyPnl}. Se mantiene el snapshot actual de mercado/referencia.`;
    await refreshAll();
  } catch (error) {
    document.getElementById("lastUpdated").textContent = `Error al limpiar: ${error.message}`;
  } finally {
    button.disabled = false;
    button.textContent = originalLabel || "Limpiar runtime";
  }
});

document.getElementById("resetCompareBtn").addEventListener("click", async () => {
  const button = document.getElementById("resetCompareBtn");
  if (runtimeMode !== "local") {
    document.getElementById("lastUpdated").textContent =
      "Reset compare no disponible en modo Public API. Usa la URL del backend local.";
    return;
  }

  const accepted = window.confirm(
    "Esto limpiara el ledger de paper, shadow y la base comparativa runtime_compare.db. Mantiene el snapshot actual de mercado/referencia y no toca live. No reinicia procesos. Continuar?"
  );
  if (!accepted) return;

  const originalLabel = button.textContent;
  button.disabled = true;
  button.textContent = "Limpiando compare...";
  try {
    const result = await postJson(withCacheBust(buildApiUrl("/api/reset-compare")), { confirm: "reset-compare" });
    const paper = result?.runtimes?.paper?.deleted || {};
    const shadow = result?.runtimes?.shadow?.deleted || {};
    const compareRemoved = result?.compare_files_removed || {};
    const compareDbRemoved = Boolean(compareRemoved["runtime_compare.db"]);
    document.getElementById("lastUpdated").textContent =
      `Compare limpiado: paper ${Number(paper.executions || 0)} ejecuciones / ${Number(paper.strategy_windows || 0)} ventanas, shadow ${Number(shadow.executions || 0)} ejecuciones / ${Number(shadow.strategy_windows || 0)} ventanas, db comparativa ${compareDbRemoved ? "reiniciada" : "sin archivo"}.`;
    await refreshAll();
  } catch (error) {
    document.getElementById("lastUpdated").textContent = `Error al limpiar compare: ${error.message}`;
  } finally {
    button.disabled = false;
    button.textContent = originalLabel || "Limpiar compare";
  }
});

async function sendLiveControlAction(action, buttonId, successPrefix) {
  const button = document.getElementById(buttonId);
  if (runtimeMode !== "local") {
    document.getElementById("lastUpdated").textContent =
      "Live control no disponible sin conexion con el backend local.";
    return;
  }
  const originalLabel = button.textContent;
  button.disabled = true;
  button.textContent = "Aplicando...";
  try {
    await postJson(withCacheBust(buildApiUrl("/api/live-control")), { action });
    document.getElementById("lastUpdated").textContent = `${successPrefix}.`;
    await refreshAll();
  } catch (error) {
    document.getElementById("lastUpdated").textContent = `Error live control: ${error.message}`;
  } finally {
    button.disabled = false;
    button.textContent = originalLabel;
  }
}

document.getElementById("armLiveBtn").addEventListener("click", async () => {
  await sendLiveControlAction("arm", "armLiveBtn", "Live armado");
});

document.getElementById("pauseLiveBtn").addEventListener("click", async () => {
  await sendLiveControlAction("pause", "pauseLiveBtn", "Live pausado");
});

document.getElementById("summaryNowBtn").addEventListener("click", async () => {
  await sendLiveControlAction("summary_now", "summaryNowBtn", "Resumen Telegram solicitado");
});

async function bootstrap() {
  const params = new URLSearchParams(window.location.search);
  watchedWallet = (params.get("wallet") || DEFAULT_WALLET).toLowerCase();
  const apiParam = normalizeApiBase(params.get("api") || "");
  const savedApiBase = loadSavedApiBase();
  const hostDefaultApi = normalizeApiBase(DEFAULT_REMOTE_API_BY_HOST[window.location.hostname] || "");
  apiBase = apiParam || savedApiBase || hostDefaultApi;
  document.getElementById("apiBaseInput").value = apiBase;
  saveApiBase(apiBase);

  try {
    await getJson(withCacheBust(buildApiUrl("/api/health")));
    runtimeMode = "local";
  } catch (error) {
    runtimeMode = "backend-unreachable";
    if (apiBase) {
      document.getElementById("lastUpdated").textContent =
        `No conecta con el backend del NAS (${apiBase}): ${error.message}.`;
    }
  }

  document.querySelector(".kicker").textContent =
    runtimeMode === "local"
      ? "Proyecto principal (Local DB)"
      : "Proyecto principal (Backend NAS desconectado)";
  document.getElementById("runtimeBadge").textContent = modeLabel();
  const resetBtn = document.getElementById("resetBtn");
  const resetCompareBtn = document.getElementById("resetCompareBtn");
  if (runtimeMode !== "local") {
    resetBtn.disabled = true;
    resetBtn.title = "Solo disponible cuando el dashboard esta conectado al backend local";
    resetCompareBtn.disabled = true;
    resetCompareBtn.title = "Solo disponible cuando el dashboard esta conectado al backend local";
  } else {
    resetBtn.disabled = false;
    resetBtn.title = "";
    resetCompareBtn.disabled = false;
    resetCompareBtn.title = "";
  }

  await refreshAll();
  configureAutoRefresh();
}

bootstrap();
