const GAMMA_API = "https://gamma-api.polymarket.com";
const CLOB_API = "https://clob.polymarket.com";
const SPORTS_WS = "wss://sports-api.polymarket.com/ws";
const PAPER_STORAGE_KEY = "polysainz_sports_paper_v1";
const REFRESH_MS = 60_000;
const LIVE_STALE_MS = 45_000;

const state = {
  sports: [],
  events: [],
  filteredEvents: [],
  selectedSeries: "",
  selectedEventId: "",
  search: "",
  marketType: "all",
  hasUserSelectedSeries: false,
  paper: readPaper(),
  lastUpdatedAt: 0,
  loading: false,
  error: "",
  sportsFeed: new Map(),
  books: new Map(),
  socket: null,
  socketRetry: 0,
  refreshTimer: null,
  requestId: 0,
};

const $ = (id) => document.getElementById(id);

function readPaper() {
  try {
    const raw = localStorage.getItem(PAPER_STORAGE_KEY);
    const parsed = JSON.parse(raw || "[]");
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function writePaper() {
  localStorage.setItem(PAPER_STORAGE_KEY, JSON.stringify(state.paper));
}

function unwrap(payload) {
  if (Array.isArray(payload)) return payload;
  if (payload && Array.isArray(payload.value)) return payload.value;
  if (payload && Array.isArray(payload.data)) return payload.data;
  return [];
}

function parseList(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== "string" || !value.trim()) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function numberOrNull(value) {
  if (value === null || value === undefined || (typeof value === "string" && value.trim() === "")) return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function formatPrice(value) {
  const number = numberOrNull(value);
  return number === null ? "-" : `${Math.round(number * 100)}¢`;
}

function formatUsd(value) {
  const number = numberOrNull(value);
  if (number === null) return "-";
  if (number >= 1_000_000) return `$${(number / 1_000_000).toFixed(1)}M`;
  if (number >= 1_000) return `$${(number / 1_000).toFixed(1)}k`;
  return `$${number.toFixed(0)}`;
}

function formatDate(value) {
  if (!value) return "Fecha no disponible";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return escapeHtml(String(value));
  return new Intl.DateTimeFormat("es-ES", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit" }).format(date);
}

function formatAgo(timestamp) {
  if (!timestamp) return "sin timestamp";
  const seconds = Math.max(0, Math.round((Date.now() - timestamp) / 1000));
  if (seconds < 5) return "ahora";
  if (seconds < 60) return `hace ${seconds}s`;
  return `hace ${Math.round(seconds / 60)}m`;
}

function formatMarketType(value) {
  const labels = {
    moneyline: "Ganador",
    spreads: "Hándicap",
    totals: "Totales",
    match_handicap: "Hándicap de partido",
    first_half_moneyline: "Ganador 1ª parte",
    first_half_spreads: "Hándicap 1ª parte",
    first_half_totals: "Totales 1ª parte",
  };
  return labels[value] || String(value || "Mercado deportivo").replaceAll("_", " ");
}

function normalizeSport(sport) {
  return {
    id: String(sport.id ?? sport.sport ?? sport.name),
    slug: String(sport.sport || sport.slug || "").toLowerCase(),
    name: sport.name || sport.sport || "Liga deportiva",
    series: String(sport.series || ""),
    image: sport.image || "",
    resolution: sport.resolution || "",
  };
}

function normalizeMarket(market, event) {
  const outcomes = parseList(market.outcomes);
  const prices = parseList(market.outcomePrices).map(numberOrNull);
  const tokens = parseList(market.clobTokenIds);
  const feeSchedule = market.feeSchedule || {};
  return {
    id: String(market.id || market.conditionId || market.slug),
    question: market.question || "Mercado sin título",
    slug: market.slug || "",
    type: market.sportsMarketType || "other",
    outcomes,
    prices,
    tokens,
    bestBid: numberOrNull(market.bestBid),
    bestAsk: numberOrNull(market.bestAsk),
    spread: numberOrNull(market.spread),
    liquidity: numberOrNull(market.liquidityNum ?? market.liquidity),
    volume: numberOrNull(market.volumeNum ?? market.volume),
    minSize: numberOrNull(market.orderMinSize),
    tickSize: numberOrNull(market.orderPriceMinTickSize),
    acceptingOrders: Boolean(market.acceptingOrders),
    clearBookOnStart: Boolean(market.clearBookOnStart),
    secondsDelay: numberOrNull(market.secondsDelay),
    feesEnabled: Boolean(market.feesEnabled),
    feeType: market.feeType || "desconocido",
    feeSchedule: {
      rate: numberOrNull(feeSchedule.rate),
      exponent: numberOrNull(feeSchedule.exponent),
      rebateRate: numberOrNull(feeSchedule.rebateRate),
      takerOnly: feeSchedule.takerOnly,
    },
    eventId: String(event.id),
  };
}

function normalizeEvent(event, sport) {
  const rawMarkets = Array.isArray(event.markets) ? event.markets : [];
  const markets = rawMarkets.map((market) => normalizeMarket(market, event));
  const teams = Array.isArray(event.teams) ? event.teams : [];
  return {
    id: String(event.id || event.slug),
    slug: event.slug || "",
    title: event.title || event.ticker || "Evento deportivo",
    description: event.description || "",
    sport: sport?.name || event.sport?.name || "Deportes",
    sportSlug: sport?.slug || event.sport?.sport || "",
    image: event.image || event.icon || sport?.image || "",
    resolutionSource: event.resolutionSource || sport?.resolution || "",
    startDate: event.startDate || event.startTime || event.gameStartTime || event.eventDate,
    gameId: event.gameId ? String(event.gameId) : "",
    liquidity: numberOrNull(event.liquidityClob ?? event.liquidity),
    volume24hr: numberOrNull(event.volume24hr),
    restricted: Boolean(event.restricted),
    teams,
    markets,
  };
}

function parseBookLevel(level) {
  if (Array.isArray(level)) return { price: numberOrNull(level[0]), size: numberOrNull(level[1]) };
  if (level && typeof level === "object") return { price: numberOrNull(level.price), size: numberOrNull(level.size) };
  return { price: null, size: null };
}

function bookLevel(book, side, index = 0) {
  const levels = Array.isArray(book?.[side]) ? book[side] : [];
  return parseBookLevel(levels[index]);
}

function bookDepth(book, side) {
  const levels = Array.isArray(book?.[side]) ? book[side] : [];
  return levels.slice(0, 3).reduce((total, level) => total + (parseBookLevel(level).size || 0), 0);
}

async function loadBookSnapshot(event) {
  if (!event) return;
  const markets = event.markets.filter(marketMatchesFilter).filter((market) => market.tokens[0]).slice(0, 8);
  await Promise.all(markets.map(async (market) => {
    try {
      const payload = await fetchJson(`${CLOB_API}/book?token_id=${encodeURIComponent(market.tokens[0])}`, "Libro CLOB");
      state.books.set(market.id, { book: payload, receivedAt: Date.now() });
    } catch {
      state.books.delete(market.id);
    }
  }));
}

async function fetchJson(url, label) {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), 15_000);
  try {
    const response = await fetch(url, { signal: controller.signal, headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error(`${label}: HTTP ${response.status}`);
    return await response.json();
  } finally {
    window.clearTimeout(timeout);
  }
}

function sortedSports(sports) {
  const preferred = ["nba", "nfl", "mlb", "epl", "lal", "ucl", "atp", "wta", "lol", "cs2"];
  return sports
    .filter((sport) => sport.series)
    .sort((a, b) => {
      const aRank = preferred.indexOf(a.slug);
      const bRank = preferred.indexOf(b.slug);
      if (aRank !== -1 || bRank !== -1) return (aRank === -1 ? 999 : aRank) - (bRank === -1 ? 999 : bRank);
      return a.name.localeCompare(b.name, "es");
    });
}

async function loadSports() {
  const payload = await fetchJson(`${GAMMA_API}/sports`, "Ligas");
  state.sports = sortedSports(unwrap(payload).map(normalizeSport));
  const select = $("leagueSelect");
  select.innerHTML = state.sports.map((sport) => `<option value="${escapeHtml(sport.series)}">${escapeHtml(sport.name)}</option>`).join("");
  const preferred = state.sports.find((sport) => ["nba", "nfl", "epl", "mlb", "lal"].includes(sport.slug));
  state.selectedSeries = preferred?.series || state.sports[0]?.series || "";
  select.value = state.selectedSeries;
}

async function loadEvents() {
  if (!state.selectedSeries) {
    state.events = [];
    return;
  }
  const requestId = ++state.requestId;
  const candidateSeries = state.hasUserSelectedSeries
    ? [state.selectedSeries]
    : [state.selectedSeries, ...state.sports.map((sport) => sport.series).filter((series) => series !== state.selectedSeries)].slice(0, 14);
  let selectedEvents = [];
  let selectedSeries = state.selectedSeries;
  for (const series of candidateSeries) {
    const params = new URLSearchParams({ series_id: series, limit: "100", active: "true", closed: "false" });
    let payload;
    try {
      payload = await fetchJson(`${GAMMA_API}/events?${params.toString()}`, "Eventos");
    } catch {
      // Some Gamma deployments reject the closed flag for older series; retry with the minimal query.
      payload = await fetchJson(`${GAMMA_API}/events?series_id=${encodeURIComponent(series)}&limit=100`, "Eventos");
    }
    const sport = state.sports.find((item) => item.series === series);
    const events = unwrap(payload)
      .map((event) => normalizeEvent(event, sport))
      .filter((event, index) => {
        const rawEvent = unwrap(payload)[index];
        return rawEvent?.active !== false && rawEvent?.closed !== true && rawEvent?.archived !== true;
      })
      .filter((event) => event.markets.length > 0)
      .sort((a, b) => (b.volume24hr || 0) - (a.volume24hr || 0));
    if (events.length > 0 || state.hasUserSelectedSeries) {
      selectedEvents = events;
      selectedSeries = series;
      break;
    }
  }
  if (requestId !== state.requestId) return;
  state.events = selectedEvents;
  state.selectedSeries = selectedSeries;
  $("leagueSelect").value = selectedSeries;
  if (!state.selectedEventId || !state.events.some((event) => event.id === state.selectedEventId)) {
    state.selectedEventId = state.events[0]?.id || "";
  }
}

function getScore(event) {
  if (!event.gameId) return null;
  const update = state.sportsFeed.get(event.gameId);
  if (!update) return null;
  const age = Date.now() - update.receivedAt;
  return age <= LIVE_STALE_MS ? update : { ...update, stale: true };
}

function eventState(event) {
  const score = getScore(event);
  if (score?.ended || /final|finished|ended/i.test(score?.status || "")) return { label: "FINAL", className: "state-final", score };
  if (score?.live || /inprogress|live/i.test(score?.status || "")) return { label: score.stale ? "LIVE STALE" : "EN DIRECT", className: score.stale ? "state-stale" : "state-live", score };
  const start = new Date(event.startDate).getTime();
  if (Number.isFinite(start) && start < Date.now()) return { label: "EN ESPERA", className: "state-waiting", score };
  return { label: "PRÓXIMO", className: "state-next", score };
}

function marketMatchesFilter(market) {
  if (state.marketType === "all") return true;
  return market.type === state.marketType || market.type.startsWith(`${state.marketType}_`);
}

function filteredEvents() {
  const query = state.search.trim().toLowerCase();
  return state.events.filter((event) => {
    const matchesSearch = !query || `${event.title} ${event.sport} ${event.description}`.toLowerCase().includes(query);
    const matchesType = event.markets.some(marketMatchesFilter);
    return matchesSearch && matchesType;
  });
}

function teamsLabel(event) {
  if (event.teams.length >= 2) return event.teams.map((team) => team.name).join(" vs ");
  return event.title;
}

function renderSummary(events) {
  const live = events.filter((event) => eventState(event).label.includes("DIRECT")).length;
  const liquidity = events.reduce((total, event) => total + (event.liquidity || 0), 0);
  $("eventCount").textContent = String(events.length);
  $("eventCountMeta").textContent = `${state.sports.find((sport) => sport.series === state.selectedSeries)?.name || "Liga"} seleccionada`;
  $("liveCount").textContent = String(live);
  $("liveCountMeta").textContent = live ? "Actualización de marcador" : "Sin eventos en directo";
  $("liquidityValue").textContent = formatUsd(liquidity);
  $("paperCount").textContent = String(state.paper.length);
  $("resultCount").textContent = `${events.length} eventos`;
}

function renderEvents() {
  const events = filteredEvents();
  state.filteredEvents = events;
  renderSummary(events);
  if (!events.length) {
    $("eventsList").innerHTML = `<div class="empty-feed"><div class="empty-icon">⌁</div><strong>No hay eventos con estos filtros</strong><p>Prueba otra liga, tipo de mercado o búsqueda. La fuente no se sustituye por datos inventados.</p></div>`;
    return;
  }
  $("eventsList").innerHTML = events.map((event) => {
    const status = eventState(event);
    const visibleMarkets = event.markets.filter(marketMatchesFilter).slice(0, 3);
    const selected = event.id === state.selectedEventId ? " is-selected" : "";
    const score = status.score?.score ? `<span class="score-chip">${escapeHtml(status.score.score)}</span>` : "";
    return `<article class="event-card${selected}" data-event-id="${escapeHtml(event.id)}">
      <button type="button" class="event-card-main" data-select-event="${escapeHtml(event.id)}">
        <div class="event-card-top"><span class="league-label">${escapeHtml(event.sport)}</span><span class="state-tag ${status.className}">${status.label}</span></div>
        <div class="event-title-row"><div><h3>${escapeHtml(teamsLabel(event))}</h3><p>${formatDate(event.startDate)} ${score}</p></div><span class="event-arrow">↗</span></div>
        <div class="event-meta"><span>Liquidez <b>${formatUsd(event.liquidity)}</b></span><span>24h <b>${formatUsd(event.volume24hr)}</b></span><span>${event.markets.length} mercados</span></div>
      </button>
      <div class="mini-markets">${visibleMarkets.map((market) => renderMiniMarket(market)).join("")}</div>
    </article>`;
  }).join("");
  document.querySelectorAll("[data-select-event]").forEach((button) => button.addEventListener("click", () => {
    state.selectedEventId = button.dataset.selectEvent;
    renderAll();
    loadBookSnapshot(state.events.find((event) => event.id === state.selectedEventId)).then(renderAll);
  }));
}

function renderMiniMarket(market) {
  const prices = market.prices;
  const yes = prices[0];
  const no = prices[1];
  return `<div class="mini-market"><span class="market-type">${escapeHtml(formatMarketType(market.type))}</span><span class="mini-question">${escapeHtml(market.question.replace(/^Will /i, ""))}</span><span class="price-pair"><b>${formatPrice(yes)}</b><span>/</span><b>${formatPrice(no)}</b></span></div>`;
}

function renderDetail() {
  const event = state.events.find((item) => item.id === state.selectedEventId);
  if (!event) {
    $("detailTitle").textContent = "Selecciona un evento";
    $("detailState").textContent = "-";
    $("detailState").className = "state-tag";
    $("detailPanel").className = "detail-panel empty-state";
    $("detailPanel").innerHTML = `<div class="empty-icon">◎</div><strong>Aquí aparecerá el libro de observación</strong><p>Verás precio, spread, liquidez, reglas y frescura. Las acciones de esta página son paper y no envían órdenes.</p>`;
    return;
  }
  const status = eventState(event);
  $("detailTitle").textContent = teamsLabel(event);
  $("detailState").textContent = status.label;
  $("detailState").className = `state-tag ${status.className}`;
  const scoreBlock = status.score ? `<div class="live-score"><span>Marcador público</span><strong>${escapeHtml(status.score.score || "-")}</strong><small>${escapeHtml(status.score.period || status.score.status || "Estado recibido")} · ${status.score.stale ? "stale" : formatAgo(status.score.receivedAt)}</small></div>` : `<div class="live-score muted"><span>Marcador público</span><strong>No disponible</strong><small>El evento aún no ha emitido estado en el feed</small></div>`;
  const markets = event.markets.filter(marketMatchesFilter);
  $("detailPanel").className = "detail-panel";
  $("detailPanel").innerHTML = `<div class="detail-intro"><div><span class="eyebrow">${escapeHtml(event.sport)}</span><p>${formatDate(event.startDate)} · ${event.restricted ? "Mercado con restricción indicada por Polymarket" : "Mercado público"}</p></div><a href="https://polymarket.com/event/${encodeURIComponent(event.slug)}" target="_blank" rel="noreferrer" class="external-link">Abrir Polymarket ↗</a></div>
    ${scoreBlock}
    <div class="market-table-head"><span>Mercado</span><span>Precios</span><span>Libro</span></div>
    <div class="market-table">${markets.map((market) => renderDetailMarket(market)).join("") || `<div class="table-empty">No hay mercados para este filtro.</div>`}</div>
    <div class="source-grid"><div><span>Resolución</span><a href="${escapeHtml(event.resolutionSource || "#")}" target="_blank" rel="noreferrer">Fuente oficial ↗</a></div><div><span>Liquidez Gamma</span><strong>${formatUsd(event.liquidity)}</strong></div><div><span>Datos</span><strong>Gamma + CLOB público</strong></div><div><span>Última lectura</span><strong>${formatAgo(state.lastUpdatedAt)}</strong></div></div>
    <p class="detail-note">Esto no es una recomendación ni una orden. El precio mostrado puede cambiar; no calculamos rentabilidad sin una fuente externa de cuotas y costes ejecutables.</p>`;
  document.querySelectorAll("[data-paper-market]").forEach((button) => button.addEventListener("click", () => savePaper(button.dataset.paperMarket, button.dataset.paperSide)));
}

function renderDetailMarket(market) {
  const yes = market.prices[0];
  const no = market.prices[1];
  const book = state.books.get(market.id)?.book;
  const bid = bookLevel(book, "bids");
  const ask = bookLevel(book, "asks");
  const bestBid = bid.price ?? market.bestBid;
  const bestAsk = ask.price ?? market.bestAsk;
  const depth = book ? `top3 ${bookDepth(book, "bids").toFixed(1)} / ${bookDepth(book, "asks").toFixed(1)}` : "Gamma snapshot";
  const feeLabel = market.feesEnabled ? `fee ${escapeHtml(market.feeType)}` : "fee no indicado";
  return `<div class="market-row"><div class="market-row-title"><strong>${escapeHtml(market.question)}</strong><span>${escapeHtml(formatMarketType(market.type))} · min ${market.minSize ?? "-"}</span></div><div class="market-row-prices"><span class="yes-price">SÍ <b>${formatPrice(yes)}</b></span><span class="no-price">NO <b>${formatPrice(no)}</b></span></div><div class="market-row-book"><span>bid/ask ${formatPrice(bestBid)} / ${formatPrice(bestAsk)}</span><span>spread ${bestBid !== null && bestAsk !== null ? `${((bestAsk - bestBid) * 100).toFixed(1)}¢` : "-"}</span><span>${depth}</span><span>${feeLabel}</span></div><div class="market-row-actions"><button type="button" data-paper-market="${escapeHtml(market.id)}" data-paper-side="YES" class="paper-button">+ Paper SÍ</button><button type="button" data-paper-market="${escapeHtml(market.id)}" data-paper-side="NO" class="paper-button paper-button-muted">+ Paper NO</button></div></div>`;
}

function renderPaper() {
  $("paperCount").textContent = String(state.paper.length);
  if (!state.paper.length) {
    $("paperLedger").innerHTML = `<div class="paper-empty"><span>Sin hipótesis todavía.</span><span>Usa “+ Paper” en un mercado para guardar una entrada local sin enviar dinero.</span></div>`;
    return;
  }
  $("paperLedger").innerHTML = state.paper.map((entry) => `<article class="paper-row"><div><span class="paper-side ${entry.side === "YES" ? "is-yes" : "is-no"}">${entry.side}</span><strong>${escapeHtml(entry.question)}</strong><small>${escapeHtml(entry.eventTitle)} · ${formatDate(entry.createdAt)}</small></div><div class="paper-price"><span>Precio registrado</span><b>${formatPrice(entry.price)}</b></div><button type="button" class="remove-paper" data-remove-paper="${escapeHtml(entry.id)}" aria-label="Eliminar hipótesis">×</button></article>`).join("");
  document.querySelectorAll("[data-remove-paper]").forEach((button) => button.addEventListener("click", () => {
    state.paper = state.paper.filter((entry) => entry.id !== button.dataset.removePaper);
    writePaper();
    renderPaper();
  }));
}

function savePaper(marketId, side) {
  const event = state.events.find((item) => item.id === state.selectedEventId);
  const market = event?.markets.find((item) => item.id === marketId);
  if (!event || !market) return;
  const price = side === "YES" ? market.prices[0] : market.prices[1];
  const entry = { id: `${market.id}-${side}`, marketId: market.id, side, price, question: market.question, eventTitle: teamsLabel(event), createdAt: new Date().toISOString() };
  state.paper = [entry, ...state.paper.filter((item) => item.id !== entry.id)].slice(0, 50);
  writePaper();
  renderPaper();
  const button = document.querySelector(`[data-paper-market="${CSS.escape(marketId)}"][data-paper-side="${side}"]`);
  if (button) {
    button.textContent = "Guardado";
    button.classList.add("is-saved");
    window.setTimeout(() => { button.textContent = `+ Paper ${side === "YES" ? "SÍ" : "NO"}`; button.classList.remove("is-saved"); }, 1400);
  }
}

function setFeedStatus(kind, label) {
  const badge = $("feedBadge");
  badge.className = `status-pill status-${kind}`;
  badge.innerHTML = `<span class="status-dot"></span> ${escapeHtml(label)}`;
}

function setAlert(title, message) {
  $("alertTitle").textContent = title;
  $("alertMessage").textContent = message;
  $("alertPanel").hidden = false;
}

function clearAlert() {
  state.error = "";
  $("alertPanel").hidden = true;
}

async function refresh() {
  if (state.loading) return;
  state.loading = true;
  setFeedStatus("loading", " ACTUALIZANDO");
  try {
    if (!state.sports.length) await loadSports();
    await loadEvents();
    await loadBookSnapshot(state.events.find((event) => event.id === state.selectedEventId));
    state.lastUpdatedAt = Date.now();
    $("lastUpdated").textContent = `Gamma actualizado ${formatAgo(state.lastUpdatedAt)}`;
    clearAlert();
    setFeedStatus("connected", " GAMMA OK");
    renderAll();
  } catch (error) {
    state.error = error instanceof Error ? error.message : "No se pudo cargar el feed";
    setFeedStatus("error", " FEED DEGRADADO");
    setAlert("No se pudo actualizar el feed", `${state.error}. Se conserva la última lectura disponible.`);
    renderAll();
  } finally {
    state.loading = false;
  }
}

function startSportsSocket() {
  if (!window.WebSocket || state.socket) return;
  try {
    const socket = new WebSocket(SPORTS_WS);
    state.socket = socket;
    socket.onopen = () => { state.socketRetry = 0; };
    socket.onmessage = (message) => {
      if (message.data === "ping") { socket.send("pong"); return; }
      try {
        const update = JSON.parse(message.data);
        if (!update?.gameId) return;
        state.sportsFeed.set(String(update.gameId), { ...update, receivedAt: Date.now() });
        renderAll();
      } catch { /* feed may contain a non-JSON heartbeat */ }
    };
    socket.onerror = () => socket.close();
    socket.onclose = () => {
      state.socket = null;
      const delay = Math.min(30_000, 2_000 * 2 ** state.socketRetry++);
      window.setTimeout(startSportsSocket, delay);
    };
  } catch {
    state.socket = null;
  }
}

function renderAll() {
  renderEvents();
  renderDetail();
  renderPaper();
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>'"]/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[character]);
}

function bindEvents() {
  $("leagueSelect").addEventListener("change", async (event) => { state.hasUserSelectedSeries = true; state.selectedSeries = event.target.value; state.selectedEventId = ""; await refresh(); });
  $("marketTypeSelect").addEventListener("change", (event) => { state.marketType = event.target.value; renderAll(); });
  $("searchInput").addEventListener("input", (event) => { state.search = event.target.value; renderAll(); });
  $("refreshBtn").addEventListener("click", refresh);
  $("alertRetryBtn").addEventListener("click", refresh);
  $("clearPaperBtn").addEventListener("click", () => { state.paper = []; writePaper(); renderPaper(); });
}

bindEvents();
startSportsSocket();
refresh();
state.refreshTimer = window.setInterval(refresh, REFRESH_MS);
