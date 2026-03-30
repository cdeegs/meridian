const DEFAULT_SYMBOLS = [
  "SPY", "QQQ", "IWM", "DIA",
  "AAPL", "MSFT", "NVDA", "AMZN", "META", "TSLA",
  "BTC-USD", "ETH-USD", "SOL-USD"
];
const THRESHOLD_CONDITIONS = new Set(["price_above", "price_below", "rsi_above", "rsi_below"]);
const ASSET_TYPES = ["equity", "etf", "crypto", "option", "macro", "custom"];
const conditionLabels = {
  price_above: "Price Above",
  price_below: "Price Below",
  rsi_above: "RSI Above",
  rsi_below: "RSI Below",
  macd_cross_up: "MACD Cross Up",
  macd_cross_down: "MACD Cross Down",
  study_profile_ready: "Study Profile Ready",
};
const EXPLAINERS = {
  insights: {
    trend: "Trend blends price location, moving-average structure, and short-term slope to show whether the market is behaving like an uptrend, downtrend, or chop.",
    momentum: "Momentum focuses on speed and thrust. It uses RSI and MACD to tell you whether pressure is building, fading, overheated, or washed out.",
    volatility: "Volatility compares current range behavior against what is normal for this symbol and timeframe, mainly through ATR percent and Bollinger bandwidth.",
    participation: "Participation is the volume context. Higher relative volume means the move has more attention and usually more conviction behind it.",
    stretch: "Stretch tells you whether price is still near a fair-value anchor like VWAP or already extended away from mean, where chasing gets riskier.",
  },
  indicators: {
    rsi: "RSI is best read relative to trend and timeframe, not with one hard rule. Strong uptrends can hold a higher floor than neutral markets.",
    macd: "MACD does not have a universal good number. The main read is whether it is above or below zero, above or below signal, and whether momentum is accelerating.",
    vwap: "VWAP is the short-term fair-value anchor. It matters most on intraday timeframes, where reclaiming or losing VWAP often changes the character of the session.",
    bollinger: "Bollinger Bands are most useful for squeeze and stretch, not for trend alone. Narrow bands hint at compression; touches at the edges hint at extension.",
    atr: "ATR percent tells you how much price normally moves relative to itself. It is useful for sizing expectations, stops, and how much noise a timeframe can tolerate.",
    volume: "Relative volume compares the current bar with its recent baseline. Around 1x is normal, while 1.5x or more usually means the move has attention behind it.",
  },
};
const palette = {
  close: "#edf2f8",
  sma20: "#89b79a",
  sma50: "#d8b06b",
  ema12: "#6ec5ff",
  ema26: "#a9b5ff",
  vwap: "#f0c36a",
  upper: "rgba(158, 173, 196, 0.58)",
  lower: "rgba(158, 173, 196, 0.58)",
  middle: "rgba(158, 173, 196, 0.28)",
  rsi: "#f0c36a",
  macd: "#8fc6ff",
  signal: "#f7a960",
  volume: "#5e7188",
  positive: "#22c38e",
  negative: "#ef7a7a",
  grid: "rgba(237, 242, 248, 0.06)",
  axis: "rgba(237, 242, 248, 0.54)",
  rsiBand: "rgba(240, 195, 106, 0.08)",
  threshold: "rgba(158, 173, 196, 0.78)"
};
const PLOT_PADDING = { top: 18, right: 78, bottom: 36, left: 68 };
const LOCAL_TIME_ZONE = Intl.DateTimeFormat().resolvedOptions().timeZone || "local timezone";
const TIMEFRAME_MINUTES = {
  "1m": 1,
  "5m": 5,
  "15m": 15,
  "30m": 30,
  "1h": 60,
  "2h": 120,
  "4h": 240,
  "6h": 360,
  "12h": 720,
  "1d": 1440,
  "2d": 2880,
  "1w": 10080,
};
const CHART_INSTANCE_OPTIONS = [30, 60, 90, 120, 180, 240, 360, 480, 720, 1000];
const CHART_WINDOW_PRESETS = {
  custom: null,
  "12h": { label: "12 Hours", minutes: 720 },
  "1d": { label: "1 Day", minutes: 1440 },
  "2d": { label: "2 Days", minutes: 2880 },
  "1w": { label: "1 Week", minutes: 10080 },
  "1m": { label: "1 Month", minutes: 43200 },
  "3m": { label: "3 Months", minutes: 129600 },
  "6m": { label: "6 Months", minutes: 259200 },
  "1y": { label: "1 Year", minutes: 525600 },
};
const PROFILE_TITLES = {
  responsive: "Responsive Tape",
  balanced: "Balanced Structure",
  trend: "Trend Continuation",
};
const WORKSPACE_STORAGE_KEY = "meridian.workspace.v1";
const VALID_TABS = new Set(["overview", "charts", "news", "alerts", "portfolios"]);
const VALID_TIMEFRAMES = new Set(Object.keys(TIMEFRAME_MINUTES));
const VALID_WINDOW_PRESETS = new Set(Object.keys(CHART_WINDOW_PRESETS));
const VALID_PROFILE_KEYS = new Set(Object.keys(PROFILE_TITLES));
const VALID_NEWS_FILTERS = new Set(["all", "macro", "geopolitics", "earnings", "analyst", "company", "stock", "crypto"]);
const VALID_NEWS_IMPACTS = new Set(["all", "high", "medium", "low"]);
const VALID_NEWS_BIASES = new Set(["all", "bullish", "bearish", "mixed", "unclear"]);
const VALID_NEWS_HORIZONS = new Set(["all", "intraday", "swing", "regime"]);
const VALID_NEWS_SORTS = new Set(["impact", "newest", "oldest", "source"]);

const state = {
  activeTab: "overview",
  ws: null,
  reconnectTimer: null,
  chartRefreshTimer: null,
  cards: new Map(),
  latestQuotes: new Map(),
  alerts: [],
  profileAlerts: [],
  recentTriggers: [],
  feeds: {},
  portfolios: [],
  telegramConfigured: false,
  selectedPortfolioId: null,
  assetDraftType: "equity",
  marketFilter: "all",
  knownSymbols: new Set(DEFAULT_SYMBOLS),
  news: {
    items: [],
    brief: null,
    sources: [],
    symbol: "",
    marketFilter: "all",
    impact: "all",
    bias: "all",
    horizon: "all",
    sort: "impact",
    selectedId: null,
    lastRefreshedAt: null,
  },
  charts: {
    assetFilter: "all",
    symbol: "BTC-USD",
    timeframe: "1m",
    windowPreset: "custom",
    studyProfile: null,
    studyConfig: null,
    limit: 120,
    data: null,
    matrix: [],
    matrixSymbol: null,
    matrixLoadedAt: 0,
    expandedStudies: {
      rsi: false,
      macd: false,
      volume: false,
    },
    overlays: {
      trend_sma: false,
      anchor_sma: false,
      fast_ema: true,
      slow_ema: true,
      vwap: true,
      bollinger: false,
    },
  },
};

const marketGrid = document.getElementById("market-grid");
const overviewMarketBrief = document.getElementById("overview-market-brief");
const overviewNewsTape = document.getElementById("overview-news-tape");
const alertBook = document.getElementById("alert-book");
const triggerFeed = document.getElementById("trigger-feed");
const overviewTriggerFeed = document.getElementById("overview-trigger-feed");
const symbolOptions = document.getElementById("symbol-options");
const chartSymbol = document.getElementById("chart-symbol");
const chartTimeframe = document.getElementById("chart-timeframe");
const chartWindow = document.getElementById("chart-window");
const chartLimit = document.getElementById("chart-limit");
const chartStatus = document.getElementById("chart-status");
const overlayToggles = document.getElementById("overlay-toggles");
const studyProfileList = document.getElementById("study-profile-list");
const alertForm = document.getElementById("alert-form");
const alertCondition = document.getElementById("alert-condition");
const alertThreshold = document.getElementById("alert-threshold");
const thresholdField = document.getElementById("threshold-field");
const profileAlertForm = document.getElementById("profile-alert-form");
const profileAlertSymbol = document.getElementById("profile-alert-symbol");
const profileAlertTimeframe = document.getElementById("profile-alert-timeframe");
const profileAlertProfile = document.getElementById("profile-alert-profile");
const profileAlertBook = document.getElementById("profile-alert-book");
const profileAlertStatusPill = document.getElementById("profile-alert-status-pill");
const profileAlertNote = document.getElementById("profile-alert-note");
const profileAlertSubmit = document.getElementById("profile-alert-submit");
const newsFeed = document.getElementById("news-feed");
const newsTape = document.getElementById("news-tape");
const newsDetail = document.getElementById("news-detail");
const newsMarketBrief = document.getElementById("news-market-brief");
const newsSourceDirectory = document.getElementById("news-source-directory");
const newsSymbol = document.getElementById("news-symbol");
const newsImpact = document.getElementById("news-impact");
const newsBias = document.getElementById("news-bias");
const newsHorizon = document.getElementById("news-horizon");
const newsSort = document.getElementById("news-sort");
const newsStatus = document.getElementById("news-status");
const newsLastUpdated = document.getElementById("news-last-updated");
const newsSummaryStrip = document.getElementById("news-summary-strip");
const portfolioForm = document.getElementById("portfolio-form");

async function fetchWithTimeout(url, options = {}) {
  const { timeoutMs = 4000, ...fetchOptions } = options;
  if (typeof AbortController === "undefined" || !timeoutMs) {
    return fetch(url, fetchOptions);
  }

  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...fetchOptions, signal: controller.signal });
  } finally {
    window.clearTimeout(timeoutId);
  }
}

function readWorkspaceState() {
  try {
    return JSON.parse(window.localStorage.getItem(WORKSPACE_STORAGE_KEY) || "null");
  } catch (_) {
    return null;
  }
}

function persistWorkspaceState() {
  try {
    window.localStorage.setItem(WORKSPACE_STORAGE_KEY, JSON.stringify({
      activeTab: state.activeTab,
      marketFilter: state.marketFilter,
      newsMarketFilter: state.news.marketFilter,
      newsImpact: state.news.impact,
      newsSymbol: state.news.symbol,
      newsBias: state.news.bias,
      newsHorizon: state.news.horizon,
      newsSort: state.news.sort,
      chartAssetFilter: state.charts.assetFilter,
      chartSymbol: state.charts.symbol,
      chartTimeframe: state.charts.timeframe,
      chartWindowPreset: state.charts.windowPreset,
      chartLimit: state.charts.limit,
      chartStudyProfile: state.charts.studyProfile,
      chartOverlays: state.charts.overlays,
      chartExpandedStudies: state.charts.expandedStudies,
    }));
  } catch (_) {}
}

function hydrateWorkspaceState() {
  const stored = readWorkspaceState();
  if (!stored || typeof stored !== "object") return;

  if (typeof stored.activeTab === "string" && VALID_TABS.has(stored.activeTab)) {
    state.activeTab = stored.activeTab;
  }
  if (typeof stored.marketFilter === "string") {
    state.marketFilter = stored.marketFilter;
  }
  if (typeof stored.newsMarketFilter === "string" && VALID_NEWS_FILTERS.has(stored.newsMarketFilter)) {
    state.news.marketFilter = stored.newsMarketFilter;
  }
  if (typeof stored.newsImpact === "string" && VALID_NEWS_IMPACTS.has(stored.newsImpact)) {
    state.news.impact = stored.newsImpact;
  }
  if (typeof stored.newsSymbol === "string") {
    state.news.symbol = stored.newsSymbol.trim().toUpperCase();
    if (state.news.symbol) {
      state.knownSymbols.add(state.news.symbol);
    }
  }
  if (typeof stored.newsBias === "string" && VALID_NEWS_BIASES.has(stored.newsBias)) {
    state.news.bias = stored.newsBias;
  }
  if (typeof stored.newsHorizon === "string" && VALID_NEWS_HORIZONS.has(stored.newsHorizon)) {
    state.news.horizon = stored.newsHorizon;
  }
  if (typeof stored.newsSort === "string" && VALID_NEWS_SORTS.has(stored.newsSort)) {
    state.news.sort = stored.newsSort;
  }
  if (typeof stored.chartAssetFilter === "string") {
    state.charts.assetFilter = stored.chartAssetFilter;
  }
  if (typeof stored.chartSymbol === "string" && stored.chartSymbol.trim()) {
    state.charts.symbol = stored.chartSymbol.trim().toUpperCase();
    state.knownSymbols.add(state.charts.symbol);
  }
  if (typeof stored.chartTimeframe === "string" && VALID_TIMEFRAMES.has(stored.chartTimeframe)) {
    state.charts.timeframe = stored.chartTimeframe;
  }
  if (typeof stored.chartWindowPreset === "string" && VALID_WINDOW_PRESETS.has(stored.chartWindowPreset)) {
    state.charts.windowPreset = stored.chartWindowPreset;
  }
  if (Number.isFinite(Number(stored.chartLimit))) {
    state.charts.limit = clampChartLimit(stored.chartLimit);
  }
  if (typeof stored.chartStudyProfile === "string" && VALID_PROFILE_KEYS.has(stored.chartStudyProfile)) {
    state.charts.studyProfile = stored.chartStudyProfile;
  }
  if (stored.chartOverlays && typeof stored.chartOverlays === "object") {
    state.charts.overlays = {
      ...state.charts.overlays,
      ...Object.fromEntries(Object.entries(stored.chartOverlays).map(([key, value]) => [key, Boolean(value)])),
    };
  }
  if (stored.chartExpandedStudies && typeof stored.chartExpandedStudies === "object") {
    state.charts.expandedStudies = {
      ...state.charts.expandedStudies,
      ...Object.fromEntries(Object.entries(stored.chartExpandedStudies).map(([key, value]) => [key, Boolean(value)])),
    };
  }
}

function hydrateQueryState() {
  const params = new URLSearchParams(window.location.search);
  const tab = params.get("tab");
  if (tab && VALID_TABS.has(tab)) {
    state.activeTab = tab;
  }

  const symbol = params.get("symbol");
  if (symbol && symbol.trim()) {
    state.charts.symbol = symbol.trim().toUpperCase();
    state.knownSymbols.add(state.charts.symbol);
  }

  const timeframe = params.get("timeframe");
  if (timeframe && VALID_TIMEFRAMES.has(timeframe)) {
    state.charts.timeframe = timeframe;
  }

  const windowPreset = params.get("window");
  if (windowPreset && VALID_WINDOW_PRESETS.has(windowPreset)) {
    state.charts.windowPreset = windowPreset;
  }

  const profileKey = params.get("profile");
  if (profileKey && VALID_PROFILE_KEYS.has(profileKey)) {
    state.charts.studyProfile = profileKey;
  }

  const instances = params.get("instances");
  if (instances && Number.isFinite(Number(instances))) {
    state.charts.limit = clampChartLimit(instances);
  }
}

function escapeHtml(value) {
  if (value === null || value === undefined) return "";
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function isCryptoSymbol(symbol) {
  return symbol.includes("-") || symbol.includes("/");
}

function assetBucket(symbol) {
  return isCryptoSymbol(symbol) ? "crypto" : "stock";
}

function setConnection(stateName, text) {
  const dot = document.getElementById("connection-dot");
  const label = document.getElementById("connection-text");
  dot.classList.remove("live", "dead");
  if (stateName === "live") dot.classList.add("live");
  if (stateName === "dead") dot.classList.add("dead");
  label.textContent = text;
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function digitsForPrice(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 2;
  const absolute = Math.abs(numeric);
  if (absolute >= 1000) return 2;
  if (absolute >= 1) return 2;
  if (absolute >= 0.1) return 4;
  return 6;
}

function formatPrice(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return formatNumber(value, digitsForPrice(value));
}

function formatMaybe(value, digits = 2) {
  if (value === null || value === undefined) return "-";
  return formatNumber(value, digits);
}

function formatSigned(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  const numeric = Number(value);
  const sign = numeric > 0 ? "+" : "";
  return `${sign}${formatNumber(numeric, digits)}`;
}

function formatCompact(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return Number(value).toLocaleString(undefined, {
    notation: "compact",
    maximumFractionDigits: 2,
  });
}

function formatDateTime(value, options = {}) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString(undefined, options);
}

function clampChartLimit(value) {
  const numeric = Math.round(Number(value) || 120);
  return Math.max(30, Math.min(1000, numeric));
}

function syncChartLimitOptions() {
  const selected = clampChartLimit(state.charts.limit);
  const optionValues = [...CHART_INSTANCE_OPTIONS];
  if (!optionValues.includes(selected)) optionValues.push(selected);
  optionValues.sort((left, right) => left - right);
  chartLimit.innerHTML = optionValues.map((value) => (
    `<option value="${value}">${value}</option>`
  )).join("");
  chartLimit.value = String(selected);
  state.charts.limit = selected;
}

function estimatedInstancesForWindow(timeframe, presetKey) {
  const preset = CHART_WINDOW_PRESETS[presetKey];
  if (!preset) return state.charts.limit;
  const timeframeMinutes = TIMEFRAME_MINUTES[timeframe] || 60;
  return Math.ceil(preset.minutes / timeframeMinutes);
}

function applyChartWindowPreset() {
  if (state.charts.windowPreset === "custom") {
    syncChartLimitOptions();
    return;
  }
  state.charts.limit = clampChartLimit(
    estimatedInstancesForWindow(state.charts.timeframe, state.charts.windowPreset)
  );
  syncChartLimitOptions();
}

function activeWindowLabel() {
  return CHART_WINDOW_PRESETS[state.charts.windowPreset]?.label || "Custom";
}

function chartRangeParams() {
  if (state.charts.windowPreset === "custom") return null;
  const preset = CHART_WINDOW_PRESETS[state.charts.windowPreset];
  if (!preset) return null;
  const end = new Date();
  const start = new Date(end.getTime() - preset.minutes * 60_000);
  return { start, end };
}

function fallbackStudyConfig() {
  return {
    key: "balanced",
    title: "Balanced Structure",
    studies: {
      fast_ema: { period: 12, key: "ema_12", label: "EMA 12" },
      slow_ema: { period: 26, key: "ema_26", label: "EMA 26" },
      trend_sma: { period: 50, key: "sma_50", label: "SMA 50" },
      anchor_sma: { period: 100, key: "sma_100", label: "SMA 100" },
      rsi: { period: 14, key: "rsi_14", label: "RSI 14" },
      macd: { fast: 12, slow: 26, signal: 9, key: "macd", label: "MACD 12/26/9" },
      bollinger: { period: 20, key: "bollinger_20", label: "Bollinger 20" },
      vwap: { key: "vwap", label: "VWAP" },
    },
    default_overlays: {
      fast_ema: true,
      slow_ema: true,
      trend_sma: true,
      anchor_sma: false,
      vwap: true,
      bollinger: false,
    },
  };
}

function studyProfileFromPayload(payload, key = null) {
  const profiles = payload?.insights?.study_profiles || [];
  if (!profiles.length) return fallbackStudyConfig();
  const selectedKey = key || state.charts.studyProfile || payload?.insights?.active_study_profile;
  return profiles.find((profile) => profile.key === selectedKey)
    || profiles.find((profile) => profile.key === payload?.insights?.active_study_profile)
    || profiles[0];
}

function applyStudyProfile(profileKey, payload, { resetOverlays = true } = {}) {
  const profile = studyProfileFromPayload(payload, profileKey);
  state.charts.studyProfile = profile.key;
  state.charts.studyConfig = profile;
  if (resetOverlays && profile.default_overlays) {
    state.charts.overlays = { ...profile.default_overlays };
  }
  persistWorkspaceState();
  return profile;
}

function currentStudyConfig(payload = state.charts.data) {
  return state.charts.studyConfig || studyProfileFromPayload(payload);
}

function overlayLabel(overlay, profile) {
  const studies = profile?.studies || {};
  if (overlay === "fast_ema") return studies.fast_ema?.label || "Fast EMA";
  if (overlay === "slow_ema") return studies.slow_ema?.label || "Slow EMA";
  if (overlay === "trend_sma") return studies.trend_sma?.label || "Trend SMA";
  if (overlay === "anchor_sma") return studies.anchor_sma?.label || "Anchor SMA";
  if (overlay === "bollinger") return studies.bollinger?.label || "Bollinger";
  if (overlay === "vwap") return studies.vwap?.label || "VWAP";
  return overlay;
}

function profileStudyLine(profile) {
  const studies = profile?.studies || {};
  return [
    studies.fast_ema?.label,
    studies.slow_ema?.label,
    studies.rsi?.label,
    studies.macd?.label,
    studies.bollinger?.label,
  ].filter(Boolean).join(" | ");
}

function profileTitle(profileKey) {
  return PROFILE_TITLES[profileKey] || profileKey.replaceAll("_", " ");
}

function profileAlertsForContext(symbol = state.charts.symbol, timeframe = state.charts.timeframe) {
  return state.profileAlerts.filter((alert) => (
    alert.symbol === symbol && alert.timeframe === timeframe
  ));
}

function profileAlertByKey(profileKey, symbol = state.charts.symbol, timeframe = state.charts.timeframe) {
  return profileAlertsForContext(symbol, timeframe).find((alert) => (
    alert.profile_key === profileKey && alert.status === "active"
  )) || null;
}

function alertConditionText(alert) {
  if (alert.condition === "study_profile_ready") {
    const bits = [conditionLabels.study_profile_ready];
    if (alert.profile_title) bits.push(alert.profile_title);
    if (alert.timeframe) bits.push(alert.timeframe);
    return bits.join(" | ");
  }
  return conditionLabels[alert.condition] || alert.condition || "Alert";
}

function relativeTime(value) {
  if (!value) return "waiting";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "waiting";
  const delta = Math.max(0, Math.round((Date.now() - date.getTime()) / 1000));
  if (delta < 60) return `${delta}s ago`;
  if (delta < 3600) return `${Math.round(delta / 60)}m ago`;
  return `${Math.round(delta / 3600)}h ago`;
}

function setProfileAlertFormFromCharts() {
  profileAlertSymbol.value = state.charts.symbol || "";
  profileAlertTimeframe.value = VALID_TIMEFRAMES.has(state.charts.timeframe)
    ? state.charts.timeframe
    : "1h";
  profileAlertProfile.value = VALID_PROFILE_KEYS.has(state.charts.studyProfile)
    ? state.charts.studyProfile
    : state.charts.data?.insights?.active_study_profile || "balanced";
}

function timeLabel(isoString, index = 0, total = 1) {
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) return "";
  const timeframe = state.charts.timeframe;
  const minutes = TIMEFRAME_MINUTES[timeframe] || 60;
  if (minutes >= 1440) {
    return date.toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
      year: minutes >= 10080 || index === 0 || index === total - 1 ? "2-digit" : undefined,
    });
  }
  if (minutes >= 60) {
    return date.toLocaleString(undefined, {
      month: index === 0 || index === total - 1 ? "short" : undefined,
      day: index === 0 || index === total - 1 ? "numeric" : undefined,
      hour: "numeric",
    });
  }
  return date.toLocaleString(undefined, {
    month: index === 0 || index === total - 1 ? "short" : undefined,
    day: index === 0 || index === total - 1 ? "numeric" : undefined,
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatChartWindow(candles) {
  if (!candles?.length) return "-";
  const first = candles[0]?.time;
  const last = candles[candles.length - 1]?.time;
  if (!first || !last) return "-";
  const minutes = TIMEFRAME_MINUTES[state.charts.timeframe] || 60;
  const options = minutes >= 1440
    ? { month: "short", day: "numeric", year: "2-digit" }
    : { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" };
  const firstLabel = formatDateTime(first, options);
  const lastLabel = formatDateTime(last, options);
  return `${firstLabel} to ${lastLabel}`;
}

function chartStatusSuffix() {
  return `Times shown in ${LOCAL_TIME_ZONE}.`;
}

function domainWithPadding(minValue, maxValue, padRatio = 0.07) {
  if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
    return [0, 1];
  }
  if (minValue === maxValue) {
    const delta = Math.max(Math.abs(minValue) * 0.02, 1);
    return [minValue - delta, maxValue + delta];
  }
  const padding = (maxValue - minValue) * padRatio;
  return [minValue - padding, maxValue + padding];
}

function formatAxisValue(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "-";
  return Math.abs(numeric) >= 10000 ? formatCompact(numeric) : formatPrice(numeric);
}

function updateTopStats() {
  document.getElementById("tracked-symbols").textContent = state.knownSymbols.size;
  document.getElementById("portfolio-count").textContent = state.portfolios.length;
  document.getElementById("active-tab-label").textContent =
    state.activeTab.charAt(0).toUpperCase() + state.activeTab.slice(1);
  const active = state.alerts.filter((alert) => alert.status === "active").length;
  document.getElementById("active-count").textContent = active;
}

function formatNewsTimestamp(value) {
  if (!value) return "Unknown time";
  const label = formatDateTime(value, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
  return `${label} | ${relativeTime(value)}`;
}

function toneFromBias(bias) {
  if (bias === "bullish") return "positive";
  if (bias === "bearish") return "negative";
  if (bias === "mixed") return "caution";
  return "neutral";
}

function newsFacetLabel(value) {
  const labels = {
    all: "All Flow",
    macro: "Macro",
    geopolitics: "War / Risk",
    earnings: "Earnings",
    analyst: "Analyst",
    company: "Company",
    regulation: "Regulation",
    security: "Security",
    stock: "Stocks",
    crypto: "Crypto",
  };
  return labels[value] || String(value || "").replace(/_/g, " ");
}

function newsSourceForItem(item) {
  return state.news.sources.find((source) => source.key === item.source_key) || null;
}

function safeUrl(value) {
  try {
    return new URL(value).toString();
  } catch (_) {
    return "";
  }
}

function newsSiteUrl(item, source = null) {
  const rawUrl = safeUrl(item?.url) || safeUrl(source?.url);
  if (!rawUrl) return "";
  try {
    const parsed = new URL(rawUrl);
    return `${parsed.protocol}//${parsed.host}`;
  } catch (_) {
    return "";
  }
}

function newsSourceDomain(item, source = null) {
  if (source?.domain) return source.domain.replace(/^www\./, "");
  const siteUrl = newsSiteUrl(item, source);
  if (!siteUrl) return "Source site";
  return siteUrl.replace(/^https?:\/\//, "").replace(/^www\./, "");
}

function renderNewsLinkButtons(item, options = {}) {
  const { className = "news-mini-link", includeSite = true, includeFeed = true } = options;
  const source = newsSourceForItem(item);
  const links = [];
  const articleUrl = safeUrl(item?.url);
  const siteUrl = newsSiteUrl(item, source);
  const feedUrl = includeFeed ? safeUrl(source?.url) : "";

  if (articleUrl) {
    links.push(`<a class="${className}" data-news-external="article" href="${escapeHtml(articleUrl)}" target="_blank" rel="noreferrer">Read Article</a>`);
  }
  if (includeSite && siteUrl) {
    links.push(`<a class="${className}" data-news-external="site" href="${escapeHtml(siteUrl)}" target="_blank" rel="noreferrer">Visit ${escapeHtml(newsSourceDomain(item, source))}</a>`);
  }
  if (feedUrl) {
    links.push(`<a class="${className}" data-news-external="feed" href="${escapeHtml(feedUrl)}" target="_blank" rel="noreferrer">Open Feed</a>`);
  }

  return links.join("");
}

function selectedNewsItem() {
  return state.news.items.find((item) => item.id === state.news.selectedId) || state.news.items[0] || null;
}

function selectNewsItem(newsId, switchToNews = false) {
  if (!newsId) return;
  state.news.selectedId = newsId;
  if (switchToNews) {
    switchTab("news");
  }
  renderOverviewMarketBrief();
  renderNewsMarketBrief();
  renderNewsTape();
  renderNewsFeed();
  renderNewsDetail();
}

function renderMarketBrief(container, options = {}) {
  if (!container) return;
  const { compact = false } = options;
  const brief = state.news.brief;
  if (!brief) {
    container.innerHTML = '<div class="empty">Market conditions will appear once the news engine refreshes.</div>';
    return;
  }

  const driverLimit = compact ? 2 : 3;
  const narrativeLimit = compact ? 2 : 3;
  container.innerHTML = `
    <div class="brief-top">
      <div>
        <p class="brief-kicker">${escapeHtml(brief.scope_label || "Cross-Market")}</p>
        <h3 class="brief-title">${escapeHtml(brief.statement)}</h3>
        <p class="muted">${escapeHtml(brief.summary)}</p>
      </div>
      <div class="brief-meta">
        <span class="pill ${toneClass(brief.confidence?.tone)}">Confidence ${escapeHtml(brief.confidence?.label || "Medium")}</span>
        <span class="pill neutral">${escapeHtml(`${brief.confidence?.score || 0}/100`)}</span>
      </div>
    </div>

    <div class="brief-condition-grid">
      ${(brief.conditions || []).map((condition) => `
        <article class="brief-card">
          <p class="brief-label">${escapeHtml(condition.label)}</p>
          <p class="brief-value">${escapeHtml(condition.value)}</p>
          <span class="pill ${toneClass(condition.tone)}">${escapeHtml(condition.value)}</span>
          <p class="muted">${escapeHtml(condition.detail)}</p>
        </article>
      `).join("")}
    </div>

    <div class="brief-footer">
      <section class="brief-drivers">
        <div class="section-head">
          <div>
            <h3 class="section-title">Top Catalysts</h3>
            <p class="note">Tap any driver to open the full impact read.</p>
          </div>
        </div>
        <div class="brief-driver-list">
          ${(brief.drivers || []).slice(0, driverLimit).map((driver) => `
            <button class="brief-driver" type="button" data-news-open="${escapeHtml(driver.id)}">
              <div class="card-head">
                <div>
                  <p class="guide-title">${escapeHtml(driver.title)}</p>
                  <p class="meta">${escapeHtml(driver.source_label)} | ${escapeHtml(newsFacetLabel(driver.market_bucket))}</p>
                </div>
                <div class="news-pill-stack">
                  <span class="pill ${driver.impact_level === "high" ? "caution" : driver.impact_level === "medium" ? "info" : "neutral"}">${escapeHtml(driver.impact_level)}</span>
                  <span class="pill ${toneClass(toneFromBias(driver.bias))}">${escapeHtml(driver.bias)}</span>
                </div>
              </div>
            </button>
          `).join("")}
        </div>
      </section>

      <section class="brief-summary-block">
        <div>
          <p class="brief-label">What Changed</p>
          <div class="brief-bullet-list">
            ${(brief.what_changed || []).map((line) => `
              <div class="brief-bullet">
                <span class="pill info">Now</span>
                <p class="muted">${escapeHtml(line)}</p>
              </div>
            `).join("")}
          </div>
        </div>
        <div>
          <p class="brief-label">Active Narratives</p>
          <div class="brief-narrative-list">
            ${(brief.narratives || []).slice(0, narrativeLimit).map((narrative) => `
              <button class="brief-driver" type="button" data-news-open="${escapeHtml(narrative.id)}">
                <div class="card-head">
                <div>
                  <p class="guide-title">${escapeHtml(narrative.label)}</p>
                  <p class="meta">${escapeHtml(`${narrative.count} headline${narrative.count === 1 ? "" : "s"} | ${newsFacetLabel(narrative.market_bucket)}`)}</p>
                </div>
                <div class="news-pill-stack">
                  <span class="pill ${toneClass(narrative.tone)}">${escapeHtml(narrative.tone)}</span>
                  <span class="pill ${narrative.impact_level === "high" ? "caution" : narrative.impact_level === "medium" ? "info" : "neutral"}">${escapeHtml(narrative.impact_level)}</span>
                </div>
                </div>
                <p class="muted">${escapeHtml(narrative.detail)}</p>
              </button>
            `).join("")}
          </div>
        </div>
        <div>
          <p class="brief-label">Watch Next</p>
          <p class="muted">${escapeHtml(brief.watch_next)}</p>
        </div>
        <div>
          <p class="brief-label">Desk Read</p>
          <p class="muted">This is a structured market-context view built from the current catalyst flow, not personal financial advice.</p>
        </div>
      </section>
    </div>
  `;
}

function renderOverviewMarketBrief() {
  renderMarketBrief(overviewMarketBrief, { compact: true });
}

function renderNewsMarketBrief() {
  renderMarketBrief(newsMarketBrief, { compact: false });
}

function renderNewsSources() {
  if (!newsSourceDirectory) return;
  if (!state.news.sources.length) {
    newsSourceDirectory.innerHTML = '<div class="empty">Source links will appear here once the feed refreshes.</div>';
    return;
  }

  newsSourceDirectory.innerHTML = state.news.sources.map((source) => `
    <a
      class="source-card"
      href="${escapeHtml(source.url)}"
      target="_blank"
      rel="noreferrer"
    >
      <div class="card-head">
        <div>
          <p class="guide-title">${escapeHtml(source.label)}</p>
          <p class="meta">${escapeHtml(source.domain)}</p>
        </div>
        <span class="pill neutral">${escapeHtml(newsFacetLabel(source.market_bucket))}</span>
      </div>
      <p class="muted">Open the live feed powering Meridian's news engine.</p>
    </a>
  `).join("");
}

function renderNewsSummary() {
  if (!newsSummaryStrip) return;
  if (!state.news.items.length && !state.news.sources.length) {
    newsSummaryStrip.innerHTML = '<div class="empty">News scope, source coverage, and refresh status will appear here once the feed loads.</div>';
    return;
  }

  const scopeLabel = newsFacetLabel(state.news.marketFilter);
  const impactLabel = state.news.impact === "all"
    ? "All impact"
    : `${state.news.impact.charAt(0).toUpperCase()}${state.news.impact.slice(1)}+`;
  const symbolLabel = state.news.symbol || "All tracked";
  const sortLabel = {
    impact: "Impact",
    newest: "Newest",
    oldest: "Oldest",
    source: "Source A-Z",
  }[state.news.sort] || "Impact";
  const refreshLabel = state.news.lastRefreshedAt ? relativeTime(state.news.lastRefreshedAt) : "Waiting";
  const biasLabel = state.news.bias === "all" ? "All bias" : `${state.news.bias.charAt(0).toUpperCase()}${state.news.bias.slice(1)} bias`;
  const horizonLabel = state.news.horizon === "all" ? "All horizons" : `${state.news.horizon.charAt(0).toUpperCase()}${state.news.horizon.slice(1)} horizon`;

  newsSummaryStrip.innerHTML = `
    <article class="news-summary-card">
      <p class="news-summary-label">Scope</p>
      <p class="news-summary-value">${escapeHtml(scopeLabel)}</p>
      <p class="muted">Symbol ${escapeHtml(symbolLabel)}</p>
    </article>
    <article class="news-summary-card">
      <p class="news-summary-label">Headlines</p>
      <p class="news-summary-value">${escapeHtml(String(state.news.items.length))}</p>
      <p class="muted">${escapeHtml(`${impactLabel} | ${biasLabel}`)}</p>
    </article>
    <article class="news-summary-card">
      <p class="news-summary-label">Sources</p>
      <p class="news-summary-value">${escapeHtml(String(state.news.sources.length))}</p>
      <p class="muted">${escapeHtml(`${horizonLabel} | live feed directory below`)}</p>
    </article>
    <article class="news-summary-card">
      <p class="news-summary-label">Sort / Refresh</p>
      <p class="news-summary-value">${escapeHtml(sortLabel)}</p>
      <p class="muted">${escapeHtml(`${refreshLabel} | direct article, site, and feed links`)}</p>
    </article>
  `;
}

function renderOverviewNewsTape() {
  if (!state.news.items.length) {
    overviewNewsTape.innerHTML = '<div class="empty">News flow will appear here once the feed refreshes.</div>';
    return;
  }

  overviewNewsTape.innerHTML = state.news.items.slice(0, 6).map((item) => `
    <button
      class="headline-chip"
      type="button"
      data-news-open="${escapeHtml(item.id)}"
    >
      <span class="pill ${item.impact_level === "high" ? "caution" : "info"}">${escapeHtml(item.impact_level)}</span>
      <span class="headline-chip-text">${escapeHtml(item.title)}</span>
    </button>
  `).join("");
}

function renderNewsTape() {
  if (!state.news.items.length) {
    newsTape.innerHTML = '<div class="empty">Headline tape will appear here once news loads.</div>';
    return;
  }

  newsTape.innerHTML = state.news.items.slice(0, 8).map((item) => `
    <button
      class="headline-chip ${item.id === state.news.selectedId ? "active" : ""}"
      type="button"
      data-news-open="${escapeHtml(item.id)}"
    >
      <span class="pill ${item.impact_level === "high" ? "caution" : item.impact_level === "medium" ? "info" : "neutral"}">${escapeHtml(item.impact_level)}</span>
      <span class="headline-chip-text">${escapeHtml(item.title)}</span>
    </button>
  `).join("");
}

function renderNewsFeed() {
  if (!state.news.items.length) {
    newsFeed.innerHTML = '<div class="empty">No headlines matched the current filters.</div>';
    return;
  }

  newsFeed.innerHTML = state.news.items.map((item) => `
    <article class="news-card ${item.id === state.news.selectedId ? "active" : ""}" data-news-open="${escapeHtml(item.id)}">
      <div class="card-head">
        <div>
          <p class="guide-title">${escapeHtml(item.title)}</p>
          <p class="meta">${escapeHtml(item.source_label)} | ${escapeHtml(formatNewsTimestamp(item.published_at))}</p>
        </div>
        <div class="news-pill-stack">
          <span class="pill ${item.impact_level === "high" ? "caution" : item.impact_level === "medium" ? "info" : "neutral"}">${escapeHtml(item.impact_level)} impact</span>
          <span class="pill ${toneClass(toneFromBias(item.bias))}">${escapeHtml(item.bias)}</span>
          <span class="pill neutral">${escapeHtml(newsFacetLabel(item.category))}</span>
        </div>
      </div>
      <p class="muted">${escapeHtml(item.summary || item.why_it_matters)}</p>
      <div class="news-tag-row">
        ${(item.tags || []).slice(0, 5).map((tag) => `<span class="pill neutral">${escapeHtml(tag)}</span>`).join("")}
      </div>
      <div class="news-card-footer">
        <div class="news-source-meta">
          <span class="pill neutral">${escapeHtml(item.source_label)}</span>
          <span class="muted">${escapeHtml(newsSourceDomain(item, newsSourceForItem(item)))}</span>
        </div>
        <div class="news-card-links">
          ${renderNewsLinkButtons(item)}
        </div>
      </div>
    </article>
  `).join("");
}

function renderNewsDetail() {
  const item = selectedNewsItem();
  if (!item) {
    newsDetail.innerHTML = `
      <div class="section-head">
        <div>
          <h2 class="section-title">Impact Read</h2>
          <p class="note">Potential directional read, time horizon, and follow-through cues.</p>
        </div>
      </div>
      <div class="empty">Select a headline to inspect its potential impact.</div>
    `;
    return;
  }

  const chartSymbolAction = item.affected_symbols?.[0]
    ? `
      <button
        class="button-secondary"
        type="button"
        data-action="focus-news-chart"
        data-symbol="${escapeHtml(item.affected_symbols[0])}"
      >
        Open ${escapeHtml(item.affected_symbols[0])} In Charts
      </button>
    `
    : "";
  const detailActions = chartSymbolAction
    ? `
      <div class="portfolio-actions">
        ${chartSymbolAction}
      </div>
    `
    : "";
  const source = newsSourceForItem(item);

  newsDetail.innerHTML = `
    <div class="section-head">
      <div>
        <h2 class="section-title">Impact Read</h2>
        <p class="note">${escapeHtml(item.source_label)} | ${escapeHtml(formatNewsTimestamp(item.published_at))}</p>
      </div>
      <div class="news-detail-links">
        <span class="pill ${item.impact_level === "high" ? "caution" : item.impact_level === "medium" ? "info" : "neutral"}">${escapeHtml(item.impact_level)} impact</span>
        ${renderNewsLinkButtons(item, { className: "news-mini-link", includeSite: true, includeFeed: true })}
      </div>
    </div>

    <div class="news-detail-stack">
      <h3 class="news-detail-title">${escapeHtml(item.title)}</h3>
      <p class="muted">${escapeHtml(item.summary || "No summary available.")}</p>

      <div class="overview-grid">
        <div class="overview-line">
          <p class="overview-kicker">Category</p>
          <p class="overview-value">${escapeHtml(newsFacetLabel(item.category))}</p>
        </div>
        <div class="overview-line">
          <p class="overview-kicker">Bias</p>
          <p class="overview-value">${escapeHtml(item.bias)}</p>
        </div>
        <div class="overview-line">
          <p class="overview-kicker">Horizon</p>
          <p class="overview-value">${escapeHtml(item.horizon)}</p>
        </div>
        <div class="overview-line">
          <p class="overview-kicker">Market</p>
          <p class="overview-value">${escapeHtml(newsFacetLabel(item.market_bucket))}</p>
        </div>
        <div class="overview-line">
          <p class="overview-kicker">Source Site</p>
          <p class="overview-value">${escapeHtml(newsSourceDomain(item, source))}</p>
        </div>
      </div>

      <div class="profile-note">
        <p class="profile-label">Why It Matters</p>
        <p class="muted">${escapeHtml(item.why_it_matters)}</p>
      </div>

      <div class="profile-note">
        <p class="profile-label">Watch Next</p>
        <p class="muted">${escapeHtml(item.watch_next)}</p>
      </div>

      <div class="profile-note">
        <p class="profile-label">Affected Symbols</p>
        <p class="muted">${escapeHtml((item.affected_symbols || []).join(", ") || "Broad tape")}</p>
      </div>

      <div class="news-tag-row">
        ${(item.tags || []).map((tag) => `<span class="pill neutral">${escapeHtml(tag)}</span>`).join("")}
      </div>

      ${detailActions}
    </div>
  `;
}

async function refreshNews() {
  newsStatus.textContent = "Refreshing headline flow...";
  try {
    const params = new URLSearchParams({
      market_bucket: state.news.marketFilter,
      impact: state.news.impact,
      bias: state.news.bias,
      horizon: state.news.horizon,
      sort: state.news.sort,
      limit: "50",
    });
    if (state.news.symbol.trim()) params.set("symbol", state.news.symbol.trim().toUpperCase());
    const response = await fetch(`/api/news?${params.toString()}`);
    if (!response.ok) throw new Error("news unavailable");
    const payload = await response.json();
    state.news.items = payload.news || [];
    state.news.brief = payload.brief || null;
    state.news.sources = payload.sources || [];
    state.news.lastRefreshedAt = payload.last_refreshed_at || null;
    if (!state.news.items.some((item) => item.id === state.news.selectedId)) {
      state.news.selectedId = state.news.items[0]?.id || null;
    }
    persistWorkspaceState();
    newsLastUpdated.className = "pill info";
    newsLastUpdated.textContent = state.news.lastRefreshedAt
      ? `Updated ${relativeTime(state.news.lastRefreshedAt)}`
      : "Waiting for refresh";
    newsStatus.textContent = state.news.items.length
      ? `Showing ${state.news.items.length} headlines sorted by ${state.news.sort}, with direct article, site, and feed links.`
      : "No headlines matched the current filters.";
  } catch (error) {
    state.news.items = [];
    state.news.brief = null;
    state.news.sources = [];
    state.news.selectedId = null;
    newsLastUpdated.className = "pill dead";
    newsLastUpdated.textContent = "News offline";
    newsStatus.textContent = `Unable to refresh headline flow: ${error.message}`;
  }

  renderOverviewMarketBrief();
  renderOverviewNewsTape();
  renderNewsSummary();
  renderNewsMarketBrief();
  renderNewsSources();
  renderNewsTape();
  renderNewsFeed();
  renderNewsDetail();
}

function filterSymbols(filterValue) {
  const sorted = [...state.knownSymbols].sort();
  if (filterValue === "all") return sorted;
  return sorted.filter((symbol) => assetBucket(symbol) === filterValue);
}

function rebuildSymbolControls() {
  const alertSymbols = [...state.knownSymbols].sort();
  symbolOptions.innerHTML = "";
  alertSymbols.forEach((symbol) => {
    const option = document.createElement("option");
    option.value = symbol;
    symbolOptions.appendChild(option);
  });

  const chartSymbols = filterSymbols(state.charts.assetFilter);
  const current = state.charts.symbol;
  chartSymbol.innerHTML = "";
  chartSymbols.forEach((symbol) => {
    const option = document.createElement("option");
    option.value = symbol;
    option.textContent = symbol;
    chartSymbol.appendChild(option);
  });

  if (!chartSymbols.includes(current)) {
    state.charts.symbol = chartSymbols[0] || current || "BTC-USD";
  }
  chartSymbol.value = state.charts.symbol;
  updateTopStats();
}

function registerSymbol(symbol) {
  const before = state.knownSymbols.size;
  state.knownSymbols.add(symbol);
  if (state.knownSymbols.size !== before) {
    rebuildSymbolControls();
    applyMarketFilter();
  }
}

function ensureCard(symbol) {
  if (state.cards.has(symbol)) return state.cards.get(symbol);
  const card = document.createElement("article");
  card.className = "market-card";
  card.dataset.symbol = symbol;
  card.dataset.bucket = assetBucket(symbol);
  card.innerHTML = `
    <div class="market-top">
      <div>
        <p class="market-symbol">${escapeHtml(symbol)}</p>
        <p class="meta" data-role="timestamp">Waiting for stream...</p>
      </div>
      <span class="pill live">${assetBucket(symbol)}</span>
    </div>
    <div>
      <p class="price" data-role="price">-</p>
    </div>
    <div class="subgrid">
      <div class="chip-block">
        <p class="chip-label">Bid / Ask</p>
        <p class="chip-value" data-role="bidask">-</p>
      </div>
      <div class="chip-block">
        <p class="chip-label">Spread</p>
        <p class="chip-value" data-role="spread">-</p>
      </div>
      <div class="chip-block">
        <p class="chip-label">RSI 14</p>
        <p class="chip-value" data-role="rsi">-</p>
      </div>
      <div class="chip-block">
        <p class="chip-label">VWAP</p>
        <p class="chip-value" data-role="vwap">-</p>
      </div>
      <div class="chip-block">
        <p class="chip-label">SMA 20</p>
        <p class="chip-value" data-role="sma20">-</p>
      </div>
      <div class="chip-block">
        <p class="chip-label">MACD Hist</p>
        <p class="chip-value" data-role="macdHist">-</p>
      </div>
    </div>
    <button class="market-quick" type="button" data-action="focus-chart" data-symbol="${escapeHtml(symbol)}">Open In Charts</button>
  `;
  marketGrid.appendChild(card);
  state.cards.set(symbol, card);
  return card;
}

function applyMarketFilter() {
  state.cards.forEach((card, symbol) => {
    const matches = state.marketFilter === "all" || assetBucket(symbol) === state.marketFilter;
    card.classList.toggle("hidden", !matches);
  });
  document.querySelectorAll("[data-market-filter]").forEach((button) => {
    button.classList.toggle("active", button.dataset.marketFilter === state.marketFilter);
  });
}

function setLiveQuote(symbol, price) {
  if (!symbol || !price) return;
  state.latestQuotes.set(symbol, price);
}

function quoteForSymbol(symbol) {
  return state.latestQuotes.get(symbol) || null;
}

function renderChartLiveQuote(symbol) {
  const quote = quoteForSymbol(symbol);
  document.getElementById("chart-live-price").textContent =
    quote?.price !== undefined ? formatPrice(quote.price) : "-";
  document.getElementById("chart-bidask").textContent =
    quote && (quote.bid !== undefined || quote.ask !== undefined)
      ? `${formatPrice(quote.bid)} / ${formatPrice(quote.ask)}`
      : "-";
}

async function refreshChartLivePrice(symbol) {
  try {
    const response = await fetch(`/api/prices/${encodeURIComponent(symbol)}`);
    if (!response.ok) throw new Error("quote unavailable");
    const payload = await response.json();
    setLiveQuote(symbol, payload);
  } catch (_) {
    if (!state.latestQuotes.has(symbol)) {
      renderChartLiveQuote(symbol);
    }
    return;
  }
  renderChartLiveQuote(symbol);
}

function updateMarketCard(message) {
  const symbol = message.symbol;
  registerSymbol(symbol);
  const card = ensureCard(symbol);
  const price = message.price || {};
  const indicators = message.indicators || {};
  setLiveQuote(symbol, price);

  card.querySelector('[data-role="price"]').textContent =
    price.price !== undefined ? formatPrice(price.price) : "-";
  card.querySelector('[data-role="timestamp"]').textContent =
    price.timestamp ? `Updated ${relativeTime(price.timestamp)}` : "Waiting for stream...";
  card.querySelector('[data-role="bidask"]').textContent =
    price.bid !== undefined || price.ask !== undefined
      ? `${formatPrice(price.bid)} / ${formatPrice(price.ask)}`
      : "-";
  card.querySelector('[data-role="spread"]').textContent = formatMaybe(price.spread, 4);
  card.querySelector('[data-role="rsi"]').textContent = formatMaybe(indicators.rsi_14?.v, 2);
  card.querySelector('[data-role="vwap"]').textContent = formatMaybe(indicators.vwap?.v, 2);
  card.querySelector('[data-role="sma20"]').textContent = formatMaybe(indicators.sma_20?.v, 2);
  card.querySelector('[data-role="macdHist"]').textContent = formatMaybe(indicators.macd?.histogram, 4);

  if (symbol === state.charts.symbol) {
    renderChartLiveQuote(symbol);
    scheduleChartRefresh();
  }
  applyMarketFilter();
}

function renderFeedStatus() {
  const feedGrid = document.getElementById("feed-grid");
  const feeds = state.feeds || {};
  const preferred = [
    { key: "coinbase", label: "Coinbase", hint: "Crypto stream" },
    { key: "alpaca", label: "Stocks", hint: "Equity stream" },
  ];

  feedGrid.innerHTML = preferred.map(({ key, label, hint }) => {
    const feed = feeds[key];
    const connected = Boolean(feed?.connected);
    const pillClass = connected ? "live" : "dead";
    const statusLabel = connected ? "Live" : "Offline";
    const detail = connected
      ? `${hint} | last tick ${relativeTime(feed?.last_tick)}`
      : key === "alpaca"
        ? "No stock feed configured yet. Schwab is pending approval."
        : "Waiting for public crypto stream.";

    return `
      <div class="feed-card">
        <div class="feed-top">
          <p class="feed-name">${escapeHtml(label)}</p>
          <span class="pill ${pillClass}">${statusLabel}</span>
        </div>
        <p class="note">${escapeHtml(detail)}</p>
      </div>
    `;
  }).join("");
}

async function refreshFeedStatus() {
  try {
    const response = await fetch("/api/feeds/status");
    if (!response.ok) throw new Error("feed status unavailable");
    const payload = await response.json();
    state.feeds = payload.feeds || {};
  } catch (error) {
    state.feeds = {};
  }
  renderFeedStatus();
}

function renderChartSummary(payload) {
  const summary = payload?.summary || {};
  const candles = payload?.candles || [];
  const lastCandle = candles[candles.length - 1];
  const minutes = TIMEFRAME_MINUTES[payload?.timeframe || state.charts.timeframe] || 60;
  const lastCandleOptions = minutes >= 1440
    ? { weekday: "short", month: "short", day: "numeric", year: "2-digit" }
    : { weekday: "short", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" };
  const visibleWindow = candles.length
    ? `${activeWindowLabel()} | ${candles.length} instances`
    : "-";
  renderChartLiveQuote(payload?.symbol || state.charts.symbol);
  document.getElementById("chart-last-close").textContent = formatPrice(summary.close);
  document.getElementById("chart-move").textContent =
    summary.change !== undefined && summary.change_pct !== undefined
      ? `${formatSigned(summary.change, 2)} (${formatSigned(summary.change_pct, 2)}%)`
      : "-";
  document.getElementById("chart-range").textContent =
    summary.low !== undefined && summary.high !== undefined
      ? `${formatPrice(summary.low)} - ${formatPrice(summary.high)}`
      : "-";
  document.getElementById("chart-volume").textContent = formatCompact(summary.volume);
  document.getElementById("chart-window-display").textContent = visibleWindow;
  document.getElementById("chart-last-candle").textContent = lastCandle?.time
    ? `${formatDateTime(lastCandle.time, lastCandleOptions)}`
    : "-";
  document.getElementById("chart-date-note").textContent = chartStatusSuffix();
  document.getElementById("price-caption").textContent = candles.length
    ? `${payload.symbol} ${payload.timeframe} structure across ${visibleWindow}. Visible dates run from ${formatChartWindow(candles)}, with extra hidden history loaded behind the scenes so the studies do not start cold.`
    : "Price context will appear here once chart data loads.";
}

function helpDot(text) {
  return `
    <span class="tooltip-anchor">
      <span class="tooltip-trigger">?</span>
      <span class="tooltip-bubble">${escapeHtml(text)}</span>
    </span>
  `;
}

function toneClass(tone) {
  return ["positive", "caution", "negative", "neutral", "info"].includes(tone) ? tone : "info";
}

function renderChartMatrix() {
  const container = document.getElementById("timeframe-matrix");
  const rows = state.charts.matrix || [];
  if (!rows.length) {
    container.innerHTML = '<div class="empty">Timeframe alignment will appear here once chart data loads.</div>';
    return;
  }

  container.innerHTML = rows.map((row) => {
    if (!row.available) {
      return `
        <article class="matrix-card unavailable">
          <div class="card-head">
            <p class="matrix-timeframe">${escapeHtml(row.timeframe)}</p>
            <span class="pill neutral">Waiting</span>
          </div>
          <p class="meta">${escapeHtml(row.label || row.timeframe)}</p>
          <p class="matrix-stat-line">${escapeHtml(row.detail || "No data yet")}</p>
        </article>
      `;
    }

    return `
      <article class="matrix-card ${row.timeframe === state.charts.timeframe ? "active" : ""}" data-matrix-timeframe="${escapeHtml(row.timeframe)}">
        <div class="card-head">
          <p class="matrix-timeframe">${escapeHtml(row.timeframe)}</p>
          <span class="pill ${toneClass(row.trend.tone)}">${escapeHtml(row.trend.label)}</span>
        </div>
        <p class="meta">${escapeHtml(row.label)}</p>
        <div class="matrix-chip-row">
          <span class="pill ${toneClass(row.momentum.tone)}">${escapeHtml(row.momentum.label)}</span>
          <span class="pill ${toneClass(row.participation.tone)}">${escapeHtml(row.participation.label)}</span>
        </div>
        <div class="matrix-stack">
          <p class="matrix-stat-line">${formatSigned(row.summary.change_pct, 2)}% move</p>
          <p class="matrix-stat-line">${escapeHtml(row.stretch.value)}</p>
        </div>
      </article>
    `;
  }).join("");
}

function renderChartInsights(payload) {
  const headline = document.getElementById("chart-headline");
  const timeframeContext = document.getElementById("timeframe-context");
  const aiOverview = document.getElementById("ai-overview");
  const insightGrid = document.getElementById("chart-insight-grid");
  const guideGrid = document.getElementById("indicator-guide-grid");
  const insights = payload?.insights;

  if (!insights) {
    state.charts.studyConfig = null;
    headline.textContent = "Chart interpretation will appear here once data loads.";
    timeframeContext.textContent = "Timeframe guidance will appear here.";
    aiOverview.innerHTML = '<p class="note">Meridian Read will appear here once chart data loads.</p>';
    insightGrid.innerHTML = "";
    guideGrid.innerHTML = "";
    studyProfileList.innerHTML = '<div class="empty">Study profiles will appear here once chart data loads.</div>';
    document.getElementById("rsi-study-title").textContent = "RSI 14";
    document.getElementById("rsi-legend-label").textContent = "RSI 14";
    document.getElementById("rsi-band-label").textContent = "70 / 30";
    document.getElementById("macd-study-title").textContent = "MACD";
    document.getElementById("macd-legend-label").textContent = "MACD 12/26/9";
    document.getElementById("rsi-study-note").innerHTML = '<p class="note">RSI context will appear here once chart data loads.</p>';
    document.getElementById("macd-study-note").innerHTML = '<p class="note">MACD context will appear here once chart data loads.</p>';
    document.getElementById("volume-study-note").innerHTML = '<p class="note">Volume context will appear here once chart data loads.</p>';
    renderOverlayToggles();
    return;
  }

  const studyProfile = applyStudyProfile(state.charts.studyProfile || insights.active_study_profile, payload, {
    resetOverlays: !state.charts.studyConfig || !state.charts.data,
  });

  headline.innerHTML = `
    <p class="stat-label">Market Read</p>
    <p class="muted">${escapeHtml(insights.headline)}</p>
  `;

  timeframeContext.innerHTML = `
    <p class="stat-label">${escapeHtml(insights.timeframe_context?.label || "Selected Window")}</p>
    <p class="muted">${escapeHtml(insights.timeframe_context?.summary || "Guidance unavailable.")}</p>
    ${insights.market_regime ? `<p class="guide-window-note">Regime: ${escapeHtml(insights.market_regime.label)}. ${escapeHtml(insights.market_regime.summary)}</p>` : ""}
  `;

  const overview = insights.ai_overview;
  aiOverview.innerHTML = overview ? `
    <div class="card-head">
      <div>
        <p class="stat-label">${escapeHtml(overview.title || "Meridian Read")}</p>
        <p class="overview-value">${escapeHtml(overview.summary)}</p>
      </div>
      <div class="overview-confidence">
        <span class="pill ${toneClass(overview.tone)}">${escapeHtml(overview.stance)}</span>
        <span class="pill info">${escapeHtml(String(overview.confidence_pct))}% confidence</span>
      </div>
    </div>
    <div class="overview-grid">
      <div class="overview-line">
        <p class="overview-kicker">Bias</p>
        <p class="overview-value">${escapeHtml(overview.action)}</p>
      </div>
      <div class="overview-line">
        <p class="overview-kicker">Risk To Respect</p>
        <p class="overview-value">${escapeHtml(overview.risk_note)}</p>
      </div>
      <div class="overview-line">
        <p class="overview-kicker">Watch Next</p>
        <p class="overview-value">${escapeHtml(overview.watch_next)}</p>
      </div>
    </div>
    <p class="meta">${escapeHtml(overview.disclaimer || "")}</p>
  ` : '<p class="note">Meridian Read will appear here once chart data loads.</p>';

  studyProfileList.innerHTML = (insights.study_profiles || []).map((profile) => {
    const activeProfileAlert = profileAlertByKey(profile.key);
    const alertMeta = activeProfileAlert
      ? `Telegram armed | last state ${activeProfileAlert.last_signal_label || "Not Ready"}`
      : state.telegramConfigured
        ? "Enable Telegram to get pinged when this profile flips constructive."
        : "Telegram is not configured yet for profile alerts.";

    return `
      <article class="guide-card profile-card ${profile.key === studyProfile.key ? "active" : ""}">
        <div class="card-head">
          <div>
            <p class="guide-title">${escapeHtml(profile.title)}</p>
            <p class="muted">${escapeHtml(profile.best_for || "")}</p>
          </div>
          <span class="pill ${toneClass(profile.recommended ? "positive" : profile.tone)}">
            ${escapeHtml(profile.key === studyProfile.key ? "Applied" : profile.recommended ? "Best Now" : profile.fit_label || "Ready")}
          </span>
        </div>
        <div class="profile-stack">
          <p class="profile-kv">${escapeHtml(profileStudyLine(profile))}</p>
          <div class="profile-metric-row">
            <span class="profile-metric">Fit ${escapeHtml(formatNumber(profile.fit_score_pct ?? 0, 0))}/100</span>
            <span class="profile-metric">${escapeHtml(profile.market_regime || "Mixed regime")}</span>
            <span class="profile-metric">${escapeHtml(profile.current_signal_label || "Not Ready")}</span>
            ${profile.backtest?.hit_rate_pct !== null && profile.backtest?.hit_rate_pct !== undefined
              ? `<span class="profile-metric">${escapeHtml(`${profile.backtest.hit_rate_pct}% hit`)}</span>`
              : ""}
            ${profile.backtest?.trades
              ? `<span class="profile-metric">${escapeHtml(`${profile.backtest.trades} signals`)}</span>`
              : ""}
          </div>
          ${profile.summary ? `<p class="muted">${escapeHtml(profile.summary)}</p>` : ""}
          ${profile.current_signal_summary ? `
            <div class="profile-note">
              <p class="profile-label">Signal State</p>
              <p class="muted">${escapeHtml(profile.current_signal_summary)}</p>
            </div>
          ` : ""}
          ${profile.why ? `
            <div class="profile-note">
              <p class="profile-label">Why It Fits</p>
              <p class="muted">${escapeHtml(profile.why)}</p>
            </div>
          ` : ""}
          ${profile.entry_guidance ? `
            <div class="profile-note">
              <p class="profile-label">Better Buy Setup</p>
              <p class="muted">${escapeHtml(profile.entry_guidance)}</p>
            </div>
          ` : ""}
          ${profile.timing_note ? `
            <div class="profile-note">
              <p class="profile-label">Current Timing</p>
              <p class="muted">${escapeHtml(profile.timing_note)}</p>
            </div>
          ` : ""}
          ${profile.fit_summary ? `<p class="guide-window-note">${escapeHtml(profile.fit_summary)}</p>` : ""}
          ${profile.backtest?.summary ? `<p class="guide-window-note">Recent sample: ${escapeHtml(profile.backtest.summary)}</p>` : ""}
          ${profile.fit_risks?.length ? `<p class="meta">Watch-outs: ${escapeHtml(profile.fit_risks.join("; "))}</p>` : ""}
        </div>
        <div class="profile-actions">
          <span class="meta">${escapeHtml(profile.studies?.vwap?.label ? `Includes ${profile.studies.vwap.label} | ${alertMeta}` : alertMeta)}</span>
          <div class="profile-button-row">
            <button class="button-secondary profile-apply" type="button" data-study-profile="${escapeHtml(profile.key)}">
              ${profile.key === studyProfile.key ? "Applied" : "Apply"}
            </button>
            <button
              class="${activeProfileAlert ? "button-danger" : "button-secondary"} profile-apply"
              type="button"
              data-profile-alert-toggle="${escapeHtml(profile.key)}"
              ${state.telegramConfigured ? "" : "disabled"}
              ${activeProfileAlert ? `data-profile-alert-id="${escapeHtml(activeProfileAlert.id)}"` : ""}
            >
              ${activeProfileAlert ? "Disable Telegram" : "Enable Telegram"}
            </button>
          </div>
        </div>
      </article>
    `;
  }).join("");

  insightGrid.innerHTML = (insights.cards || []).map((card) => `
    <article class="insight-card">
      <div class="card-head">
        <div class="title-with-help">
          <p class="insight-title">${escapeHtml(card.title)}</p>
          ${helpDot(EXPLAINERS.insights[card.key] || card.summary || "No explainer available yet.")}
        </div>
        <span class="pill ${toneClass(card.tone)}">${escapeHtml(card.label)}</span>
      </div>
      <p class="insight-value">${escapeHtml(card.value)}</p>
      <p class="muted">${escapeHtml(card.summary)}</p>
    </article>
  `).join("");

  guideGrid.innerHTML = Object.entries(insights.indicator_guides || {}).map(([key, guide]) => `
    <article class="guide-card">
      <div class="card-head">
        <div class="title-with-help">
          <p class="guide-title">${escapeHtml(guide.title)}</p>
          ${helpDot(EXPLAINERS.indicators[key] || guide.summary || "No explainer available yet.")}
        </div>
        <span class="pill ${toneClass(guide.tone)}">${escapeHtml(guide.current)}</span>
      </div>
      <div class="guide-stack">
        <p class="guide-good-range">${escapeHtml(guide.good_range)}</p>
        <p class="muted">${escapeHtml(guide.summary)}</p>
        ${guide.why_range ? `<p class="guide-why">${escapeHtml(guide.why_range)}</p>` : ""}
        ${guide.timeframe_note ? `<p class="guide-window-note">${escapeHtml(guide.timeframe_note)}</p>` : ""}
      </div>
    </article>
  `).join("");

  const rsiGuide = insights.indicator_guides?.rsi;
  const macdGuide = insights.indicator_guides?.macd;
  const volumeGuide = insights.indicator_guides?.volume;
  const activeStudies = studyProfile?.studies || {};

  document.getElementById("rsi-study-title").textContent = activeStudies.rsi?.label || "RSI 14";
  document.getElementById("rsi-legend-label").textContent = activeStudies.rsi?.label || "RSI 14";
  document.getElementById("rsi-band-label").textContent = "Guide Range";
  document.getElementById("macd-study-title").textContent = activeStudies.macd?.label || "MACD";
  document.getElementById("macd-legend-label").textContent = activeStudies.macd?.label || "MACD 12/26/9";

  document.getElementById("rsi-study-note").innerHTML = rsiGuide
    ? `<p class="note">${escapeHtml(rsiGuide.good_range)}</p><p class="muted">${escapeHtml(rsiGuide.summary)}</p>`
    : '<p class="note">RSI context will appear here once chart data loads.</p>';
  document.getElementById("macd-study-note").innerHTML = macdGuide
    ? `<p class="note">${escapeHtml(macdGuide.good_range)}</p><p class="muted">${escapeHtml(macdGuide.summary)}</p>`
    : '<p class="note">MACD context will appear here once chart data loads.</p>';
  document.getElementById("volume-study-note").innerHTML = volumeGuide
    ? `<p class="note">${escapeHtml(volumeGuide.good_range)}</p><p class="muted">${escapeHtml(volumeGuide.summary)}</p>`
    : '<p class="note">Volume context will appear here once chart data loads.</p>';
  renderOverlayToggles();
}

function renderStudyPanels() {
  document.querySelectorAll("[data-study-toggle]").forEach((button) => {
    const study = button.dataset.studyToggle;
    const expanded = Boolean(state.charts.expandedStudies[study]);
    button.textContent = expanded ? "Hide Study" : "Show Study";
    button.classList.toggle("active", expanded);
    const body = document.getElementById(`study-body-${study}`);
    if (body) body.classList.toggle("collapsed", !expanded);
  });
}

function setChartControls() {
  chartSymbol.value = state.charts.symbol;
  chartTimeframe.value = state.charts.timeframe;
  chartWindow.value = state.charts.windowPreset;
  syncChartLimitOptions();
  document.querySelectorAll("[data-chart-filter]").forEach((button) => {
    button.classList.toggle("active", button.dataset.chartFilter === state.charts.assetFilter);
  });
  renderChartMatrix();
  renderStudyPanels();
  renderOverlayToggles();
  persistWorkspaceState();
}

function clearCharts() {
  ["price-chart", "rsi-chart", "macd-chart", "volume-chart"].forEach((id) => {
    const canvas = document.getElementById(id);
    const { ctx, width, height } = setupCanvas(canvas);
    ctx.clearRect(0, 0, width, height);
  });
  document.getElementById("price-legend").innerHTML = "";
}

async function loadChartMatrix(force = false) {
  const symbol = state.charts.symbol;
  const now = Date.now();
  if (
    !force
    && state.charts.matrixSymbol === symbol
    && state.charts.matrix.length
    && now - state.charts.matrixLoadedAt < 45000
  ) {
    renderChartMatrix();
    return;
  }

  try {
    const response = await fetch(`/api/charts/${encodeURIComponent(symbol)}/matrix`);
    if (!response.ok) throw new Error("matrix unavailable");
    const payload = await response.json();
    state.charts.matrix = payload.matrix || [];
    state.charts.matrixSymbol = symbol;
    state.charts.matrixLoadedAt = Date.now();
  } catch (_) {
    state.charts.matrix = [];
  }
  renderChartMatrix();
}

async function loadChartData() {
  const symbol = state.charts.symbol;
  const timeframe = state.charts.timeframe;
  const limit = state.charts.limit;
  const range = chartRangeParams();
  if (!symbol) {
    chartStatus.textContent = "No symbols available for this asset filter yet.";
    document.getElementById("chart-date-note").textContent = chartStatusSuffix();
    state.charts.matrix = [];
    clearCharts();
    renderChartMatrix();
    renderChartInsights(null);
    return;
  }

  chartStatus.textContent = state.charts.windowPreset === "custom"
    ? `Loading ${symbol} ${timeframe} chart...`
    : `Loading ${symbol} ${timeframe} chart for the ${activeWindowLabel()} window...`;

  try {
    const params = new URLSearchParams({
      timeframe,
      limit: String(limit),
    });
    if (range) {
      params.set("start", range.start.toISOString());
      params.set("end", range.end.toISOString());
    }
    const response = await fetch(`/api/charts/${encodeURIComponent(symbol)}?${params.toString()}`);
    if (!response.ok) {
      let detail = `Chart data unavailable (${response.status})`;
      try {
        const errorPayload = await response.json();
        if (errorPayload?.detail) detail = errorPayload.detail;
      } catch (_) {}
      throw new Error(detail);
    }
    const payload = await response.json();
    state.charts.data = payload;
    await refreshChartLivePrice(payload.symbol);
    await refreshProfileAlerts();
    const coverage = payload.coverage || {};
    const warmupNote = coverage.history_candles && coverage.warmup_candles
      && coverage.history_candles < coverage.warmup_candles
      ? " Some slower indicators are still warming up and will fill in as more history accumulates."
      : "";
    const rangeNote = state.charts.windowPreset === "custom"
      ? ""
      : ` across the ${activeWindowLabel()} window`;
    chartStatus.textContent =
      `Showing ${payload.symbol} on ${payload.timeframe}${rangeNote} with ${payload.candles.length} visible instances `
      + `and ${coverage.history_candles ?? payload.candles.length} loaded for warmup and backfill.${warmupNote}`;
    renderChartSummary(payload);
    await loadChartMatrix(state.charts.matrixSymbol !== payload.symbol);
    renderChartInsights(payload);
    renderCharts();
    if (state.activeTab === "alerts") {
      setProfileAlertFormFromCharts();
    }
  } catch (error) {
    state.charts.data = null;
    renderChartSummary(null);
    renderChartInsights(null);
    clearCharts();
    chartStatus.textContent = `Unable to load chart data: ${error.message}`;
    document.getElementById("chart-date-note").textContent = chartStatusSuffix();
  }
}

function scheduleChartRefresh() {
  clearTimeout(state.chartRefreshTimer);
  state.chartRefreshTimer = setTimeout(loadChartData, 2500);
}

function setupCanvas(canvas) {
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, rect.width, rect.height);
  return { ctx, width: rect.width, height: rect.height };
}

function xForIndex(index, total, width, padding = PLOT_PADDING) {
  const innerWidth = width - padding.left - padding.right;
  return padding.left + (innerWidth * index) / Math.max(total - 1, 1);
}

function projectY(value, height, minValue, maxValue, padding = PLOT_PADDING) {
  const innerHeight = height - padding.top - padding.bottom;
  const ratio = maxValue === minValue ? 0.5 : (value - minValue) / (maxValue - minValue);
  return padding.top + innerHeight - ratio * innerHeight;
}

function drawFrame(ctx, width, height, padding = PLOT_PADDING) {
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;
  ctx.strokeStyle = palette.grid;
  ctx.lineWidth = 1;
  for (let index = 0; index < 5; index += 1) {
    const y = padding.top + (innerHeight * index) / 4;
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(width - padding.right, y);
    ctx.stroke();
  }
  for (let index = 0; index < 3; index += 1) {
    const x = padding.left + (innerWidth * index) / 2;
    ctx.beginPath();
    ctx.moveTo(x, padding.top);
    ctx.lineTo(x, height - padding.bottom);
    ctx.stroke();
  }
}

function projectSeries(values, height, width, minValue, maxValue, padding = PLOT_PADDING) {
  return values.map((value, index) => {
    const x = xForIndex(index, values.length, width, padding);
    const y = projectY(value, height, minValue, maxValue, padding);
    return [x, y];
  });
}

function drawLine(ctx, points, color, lineWidth = 2) {
  if (points.length < 2) return;
  ctx.strokeStyle = color;
  ctx.lineWidth = lineWidth;
  ctx.beginPath();
  points.forEach(([x, y], index) => {
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function drawNullableSeries(ctx, series, height, width, minValue, maxValue, color, lineWidth = 2, padding = PLOT_PADDING) {
  ctx.strokeStyle = color;
  ctx.lineWidth = lineWidth;
  ctx.beginPath();
  let started = false;

  series.forEach((value, index) => {
    if (value === null || value === undefined) {
      started = false;
      return;
    }
    const x = xForIndex(index, series.length, width, padding);
    const y = projectY(value, height, minValue, maxValue, padding);
    if (!started) {
      ctx.moveTo(x, y);
      started = true;
    } else {
      ctx.lineTo(x, y);
    }
  });

  ctx.stroke();
}

function drawCandlesticks(ctx, candles, width, height, minValue, maxValue, padding = PLOT_PADDING) {
  const innerWidth = width - padding.left - padding.right;
  const bodyWidth = Math.max(3, Math.min(12, (innerWidth / Math.max(candles.length, 1)) * 0.72));

  candles.forEach((candle, index) => {
    const x = xForIndex(index, candles.length, width, padding);
    const yHigh = projectY(candle.high, height, minValue, maxValue, padding);
    const yLow = projectY(candle.low, height, minValue, maxValue, padding);
    const yOpen = projectY(candle.open, height, minValue, maxValue, padding);
    const yClose = projectY(candle.close, height, minValue, maxValue, padding);
    const rising = candle.close >= candle.open;
    const color = rising ? palette.positive : palette.negative;

    ctx.strokeStyle = color;
    ctx.lineWidth = 1.2;
    ctx.beginPath();
    ctx.moveTo(x, yHigh);
    ctx.lineTo(x, yLow);
    ctx.stroke();

    const top = Math.min(yOpen, yClose);
    const bodyHeight = Math.max(1.5, Math.abs(yClose - yOpen));
    ctx.fillStyle = color;
    ctx.fillRect(x - bodyWidth / 2, top, bodyWidth, bodyHeight);
  });
}

function drawTextAxis(ctx, width, height, labels, minValue, maxValue, padding = PLOT_PADDING) {
  ctx.fillStyle = palette.axis;
  ctx.font = "11px IBM Plex Mono, monospace";
  ctx.textAlign = "left";
  ctx.fillText(formatAxisValue(maxValue), 10, padding.top - 4);
  ctx.fillText(formatAxisValue(minValue), 10, height - padding.bottom + 12);
  if (labels.length) {
    const labelIndexes = [...new Set([
      0,
      Math.floor((labels.length - 1) / 3),
      Math.floor(((labels.length - 1) * 2) / 3),
      labels.length - 1,
    ])];
    labelIndexes.forEach((labelIndex, index) => {
      const align = index === 0 ? "left" : index === labelIndexes.length - 1 ? "right" : "center";
      const x = index === 0
        ? padding.left
        : index === labelIndexes.length - 1
          ? width - padding.right
          : xForIndex(labelIndex, labels.length, width, padding);
      const text = labels[labelIndex];
      ctx.textAlign = align;
      ctx.fillText(text, x, height - 10);
    });
    ctx.textAlign = "left";
  }
}

function roundedRectPath(ctx, x, y, width, height, radius) {
  const clamped = Math.min(radius, width / 2, height / 2);
  ctx.beginPath();
  ctx.moveTo(x + clamped, y);
  ctx.lineTo(x + width - clamped, y);
  ctx.quadraticCurveTo(x + width, y, x + width, y + clamped);
  ctx.lineTo(x + width, y + height - clamped);
  ctx.quadraticCurveTo(x + width, y + height, x + width - clamped, y + height);
  ctx.lineTo(x + clamped, y + height);
  ctx.quadraticCurveTo(x, y + height, x, y + height - clamped);
  ctx.lineTo(x, y + clamped);
  ctx.quadraticCurveTo(x, y, x + clamped, y);
  ctx.closePath();
}

function drawPriceMarker(ctx, width, height, value, minValue, maxValue, color, padding = PLOT_PADDING) {
  if (!Number.isFinite(value)) return;
  const y = projectY(value, height, minValue, maxValue, padding);
  const label = formatAxisValue(value);
  ctx.save();
  ctx.font = "11px IBM Plex Mono, monospace";
  const markerX = width - padding.right + 10;
  const markerWidth = Math.max(48, ctx.measureText(label).width + 12);
  const markerHeight = 18;
  ctx.setLineDash([5, 4]);
  ctx.strokeStyle = color;
  ctx.globalAlpha = 0.45;
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padding.left, y);
  ctx.lineTo(width - padding.right + 4, y);
  ctx.stroke();
  ctx.restore();

  ctx.save();
  roundedRectPath(ctx, markerX, y - markerHeight / 2, markerWidth, markerHeight, 8);
  ctx.fillStyle = "rgba(7, 12, 18, 0.96)";
  ctx.fill();
  ctx.strokeStyle = color;
  ctx.lineWidth = 1;
  ctx.stroke();
  ctx.fillStyle = color;
  ctx.font = "11px IBM Plex Mono, monospace";
  ctx.textAlign = "left";
  ctx.fillText(label, markerX + 6, y + 4);
  ctx.restore();
}

function drawThresholdLine(ctx, width, height, minValue, maxValue, threshold, color, padding = PLOT_PADDING) {
  const points = projectSeries([threshold, threshold], height, width, minValue, maxValue, padding);
  drawLine(ctx, points, color, 1.2);
}

function drawZeroLine(ctx, width, height, minValue, maxValue, padding = PLOT_PADDING) {
  const points = projectSeries([0, 0], height, width, minValue, maxValue, padding);
  drawLine(ctx, points, palette.axis, 1);
}

function drawBars(ctx, values, width, height, minValue, maxValue, padding = PLOT_PADDING) {
  const innerWidth = width - padding.left - padding.right;
  const baseline = projectSeries([0, 0], height, width, minValue, maxValue, padding)[0][1];
  const barWidth = Math.max(2, innerWidth / Math.max(values.length, 1) - 2);

  values.forEach((value, index) => {
    if (value === null || value === undefined) return;
    const x = padding.left + (innerWidth * index) / Math.max(values.length, 1);
    const y = projectY(value, height, minValue, maxValue, padding);
    ctx.fillStyle = value >= 0 ? palette.positive : palette.negative;
    ctx.fillRect(x, Math.min(y, baseline), barWidth, Math.abs(baseline - y));
  });
}

function drawBand(ctx, width, height, minValue, maxValue, lower, upper, color, padding = PLOT_PADDING) {
  const yTop = projectY(upper, height, minValue, maxValue, padding);
  const yBottom = projectY(lower, height, minValue, maxValue, padding);
  ctx.fillStyle = color;
  ctx.fillRect(
    padding.left,
    Math.min(yTop, yBottom),
    width - padding.left - padding.right,
    Math.abs(yBottom - yTop),
  );
}

function alignSeries(candles, series, field) {
  const byTime = new Map(series.map((entry) => [entry.time, entry[field]]));
  return candles.map((candle) => byTime.get(candle.time) ?? null);
}

function lastDefined(values) {
  for (let index = values.length - 1; index >= 0; index -= 1) {
    if (values[index] !== null && values[index] !== undefined) {
      return values[index];
    }
  }
  return null;
}

function legendItem(color, text, value = null) {
  const suffix = value === null || value === undefined ? "" : ` ${formatNumber(value, 2)}`;
  return `<span class="legend-item"><span class="legend-swatch" style="background:${color};"></span><span class="meta">${escapeHtml(text)}${escapeHtml(suffix)}</span></span>`;
}

function renderCharts() {
  const payload = state.charts.data;
  if (!payload || !payload.candles?.length) return;
  const candles = payload.candles;
  const indicators = payload.indicators || {};
  const labels = candles.map((candle, index) => timeLabel(candle.time, index, candles.length));
  const study = currentStudyConfig(payload);
  renderStudyPanels();
  drawPriceChart(candles, indicators, labels, study);
  if (state.charts.expandedStudies.rsi) drawRsiChart(indicators[study.studies.rsi.key] || [], labels);
  if (state.charts.expandedStudies.macd) drawMacdChart(indicators[study.studies.macd.key] || [], labels);
  if (state.charts.expandedStudies.volume) drawVolumeChart(candles, labels);
}

function drawPriceChart(candles, indicators, labels, study) {
  const canvas = document.getElementById("price-chart");
  const { ctx, width, height } = setupCanvas(canvas);
  drawFrame(ctx, width, height);

  const studies = study?.studies || fallbackStudyConfig().studies;
  const trendSma = alignSeries(candles, indicators[studies.trend_sma?.key] || [], "value");
  const anchorSma = alignSeries(candles, indicators[studies.anchor_sma?.key] || [], "value");
  const fastEma = alignSeries(candles, indicators[studies.fast_ema?.key] || [], "value");
  const slowEma = alignSeries(candles, indicators[studies.slow_ema?.key] || [], "value");
  const vwap = alignSeries(candles, indicators.vwap || [], "value");
  const bollUpper = alignSeries(candles, indicators[studies.bollinger?.key] || [], "upper");
  const bollLower = alignSeries(candles, indicators[studies.bollinger?.key] || [], "lower");
  const bollMiddle = alignSeries(candles, indicators[studies.bollinger?.key] || [], "middle");

  const active = state.charts.overlays;
  const candidates = [
    candles.map((item) => item.high),
    candles.map((item) => item.low),
    active.trend_sma ? trendSma : [],
    active.anchor_sma ? anchorSma : [],
    active.fast_ema ? fastEma : [],
    active.slow_ema ? slowEma : [],
    active.vwap ? vwap : [],
    active.bollinger ? bollUpper : [],
    active.bollinger ? bollLower : [],
    active.bollinger ? bollMiddle : [],
  ]
    .flat()
    .filter((value) => value !== null && value !== undefined);
  const [minValue, maxValue] = domainWithPadding(Math.min(...candidates), Math.max(...candidates), 0.08);

  drawCandlesticks(ctx, candles, width, height, minValue, maxValue);
  if (active.trend_sma) drawNullableSeries(ctx, trendSma, height, width, minValue, maxValue, palette.sma20, 2);
  if (active.anchor_sma) drawNullableSeries(ctx, anchorSma, height, width, minValue, maxValue, palette.sma50, 2);
  if (active.fast_ema) drawNullableSeries(ctx, fastEma, height, width, minValue, maxValue, palette.ema12, 1.8);
  if (active.slow_ema) drawNullableSeries(ctx, slowEma, height, width, minValue, maxValue, palette.ema26, 1.8);
  if (active.vwap) drawNullableSeries(ctx, vwap, height, width, minValue, maxValue, palette.vwap, 1.8);
  if (active.bollinger) {
    drawNullableSeries(ctx, bollUpper, height, width, minValue, maxValue, palette.upper, 1.2);
    drawNullableSeries(ctx, bollLower, height, width, minValue, maxValue, palette.lower, 1.2);
    drawNullableSeries(ctx, bollMiddle, height, width, minValue, maxValue, palette.middle, 1.2);
  }
  drawTextAxis(ctx, width, height, labels, minValue, maxValue);
  const lastCandle = candles[candles.length - 1];
  drawPriceMarker(
    ctx,
    width,
    height,
    lastCandle?.close,
    minValue,
    maxValue,
    lastCandle && lastCandle.close >= lastCandle.open ? palette.positive : palette.negative,
  );

  const legend = [];
  legend.push(legendItem(palette.close, "Candles", candles[candles.length - 1]?.close));
  if (active.trend_sma) legend.push(legendItem(palette.sma20, studies.trend_sma?.label || "Trend SMA", lastDefined(trendSma)));
  if (active.anchor_sma) legend.push(legendItem(palette.sma50, studies.anchor_sma?.label || "Anchor SMA", lastDefined(anchorSma)));
  if (active.fast_ema) legend.push(legendItem(palette.ema12, studies.fast_ema?.label || "Fast EMA", lastDefined(fastEma)));
  if (active.slow_ema) legend.push(legendItem(palette.ema26, studies.slow_ema?.label || "Slow EMA", lastDefined(slowEma)));
  if (active.vwap) legend.push(legendItem(palette.vwap, "VWAP", lastDefined(vwap)));
  if (active.bollinger) legend.push(legendItem(palette.upper, studies.bollinger?.label || "Bollinger", lastDefined(bollMiddle)));
  document.getElementById("price-legend").innerHTML = legend.join("");
}

function drawRsiChart(series, labels) {
  const canvas = document.getElementById("rsi-chart");
  const { ctx, width, height } = setupCanvas(canvas);
  drawFrame(ctx, width, height);
  const values = alignSeries(state.charts.data.candles, series, "value");
  drawBand(ctx, width, height, 0, 100, 30, 70, palette.rsiBand);
  drawThresholdLine(ctx, width, height, 0, 100, 70, palette.threshold);
  drawThresholdLine(ctx, width, height, 0, 100, 30, palette.threshold);
  drawNullableSeries(ctx, values, height, width, 0, 100, palette.rsi, 2.2);
  drawTextAxis(ctx, width, height, labels, 0, 100);
}

function drawMacdChart(series, labels) {
  const canvas = document.getElementById("macd-chart");
  const { ctx, width, height } = setupCanvas(canvas);
  drawFrame(ctx, width, height);

  const macd = alignSeries(state.charts.data.candles, series, "macd");
  const signal = alignSeries(state.charts.data.candles, series, "signal");
  const histogram = alignSeries(state.charts.data.candles, series, "histogram");
  const candidates = [...macd, ...signal, ...histogram].filter((value) => value !== null && value !== undefined);
  const [minValue, maxValue] = domainWithPadding(Math.min(...candidates, 0), Math.max(...candidates, 0), 0.12);

  drawZeroLine(ctx, width, height, minValue, maxValue);
  drawBars(ctx, histogram, width, height, minValue, maxValue);
  drawNullableSeries(ctx, macd, height, width, minValue, maxValue, palette.macd, 2);
  drawNullableSeries(ctx, signal, height, width, minValue, maxValue, palette.signal, 2);
  drawTextAxis(ctx, width, height, labels, minValue, maxValue);
}

function drawVolumeChart(candles, labels) {
  const canvas = document.getElementById("volume-chart");
  const { ctx, width, height } = setupCanvas(canvas);
  drawFrame(ctx, width, height);

  const values = candles.map((item) => item.volume || 0);
  const maxValue = Math.max(...values, 1);
  const innerWidth = width - PLOT_PADDING.left - PLOT_PADDING.right;
  const innerHeight = height - PLOT_PADDING.top - PLOT_PADDING.bottom;
  const barWidth = Math.max(2, innerWidth / Math.max(values.length, 1) - 2);

  ctx.fillStyle = palette.volume;
  values.forEach((value, index) => {
    const x = PLOT_PADDING.left + (innerWidth * index) / Math.max(values.length, 1);
    const barHeight = (value / maxValue) * innerHeight;
    const y = height - PLOT_PADDING.bottom - barHeight;
    ctx.fillRect(x, y, barWidth, barHeight);
  });

  drawTextAxis(ctx, width, height, labels, 0, maxValue);
}

function renderOverlayToggles() {
  const profile = currentStudyConfig();
  overlayToggles.querySelectorAll("[data-overlay]").forEach((button) => {
    const overlay = button.dataset.overlay;
    button.textContent = overlayLabel(overlay, profile);
    button.classList.toggle("active", Boolean(state.charts.overlays[overlay]));
  });
}

function switchTab(tabName) {
  state.activeTab = tabName;
  document.querySelectorAll(".tab-button").forEach((button) => {
    button.classList.toggle("active", button.dataset.tab === tabName);
  });
  document.querySelectorAll(".tab-panel").forEach((panel) => {
    panel.classList.toggle("active", panel.id === `tab-${tabName}`);
  });
  updateTopStats();
  persistWorkspaceState();
  if (tabName === "charts") {
    if (state.charts.data) {
      renderCharts();
    } else {
      loadChartData();
    }
  }
  if (tabName === "news") {
    newsSymbol.value = state.news.symbol;
    newsImpact.value = state.news.impact;
    newsBias.value = state.news.bias;
    newsHorizon.value = state.news.horizon;
    newsSort.value = state.news.sort;
    document.querySelectorAll("[data-news-filter]").forEach((button) => {
      button.classList.toggle("active", button.dataset.newsFilter === state.news.marketFilter);
    });
    if (state.news.items.length) {
      renderNewsSummary();
      renderNewsTape();
      renderNewsFeed();
      renderNewsDetail();
    } else {
      refreshNews();
    }
  }
  if (tabName === "alerts") {
    setProfileAlertFormFromCharts();
  }
}

function renderAlertBook() {
  if (!state.alerts.length) {
    alertBook.innerHTML = '<div class="empty">No alerts yet. Create one to start seeing trigger events.</div>';
  } else {
    alertBook.innerHTML = state.alerts.map((alert) => {
      const thresholdText = alert.threshold === null || alert.threshold === undefined
        ? "No threshold"
        : `Threshold ${formatMaybe(alert.threshold, 2)}`;
      const timestamp = alert.triggered_at || alert.created_at;
      return `
        <article class="alert-card">
          <div class="card-head">
            <div>
              <strong>${escapeHtml(alert.symbol)}</strong>
              <p class="meta">${escapeHtml(conditionLabels[alert.condition] || alert.condition)}</p>
            </div>
            <span class="pill ${escapeHtml(alert.status)}">${escapeHtml(alert.status)}</span>
          </div>
          <p class="muted">${escapeHtml(thresholdText)}</p>
          <p class="meta">${timestamp ? new Date(timestamp).toLocaleString() : "Pending"}</p>
          <div class="portfolio-actions">
            ${alert.status === "active"
              ? `<button class="button-danger" data-action="disable-alert" data-id="${escapeHtml(alert.id)}">Disable</button>`
              : `<button class="button-secondary" data-action="activate-alert" data-id="${escapeHtml(alert.id)}">Reactivate</button>`}
          </div>
        </article>
      `;
    }).join("");
  }
  updateTopStats();
}

function renderTriggerFeed() {
  const markup = !state.recentTriggers.length
    ? '<div class="empty">Triggered alerts will appear here in real time.</div>'
    : state.recentTriggers.map((alert) => `
        <article class="alert-card">
          <div class="card-head">
            <div>
              <strong>${escapeHtml(alert.symbol)}</strong>
              <p class="meta">${escapeHtml(alertConditionText(alert))}</p>
            </div>
            <span class="pill triggered">Triggered</span>
          </div>
          <p class="muted">${escapeHtml(alert.message)}</p>
          <p class="meta">${alert.triggered_at ? new Date(alert.triggered_at).toLocaleString() : "Just now"}</p>
        </article>
      `).join("");

  triggerFeed.innerHTML = markup;
  overviewTriggerFeed.innerHTML = markup;
}

async function refreshAlerts() {
  const response = await fetch("/api/alerts");
  if (!response.ok) return;
  const payload = await response.json();
  state.alerts = payload.alerts || [];
  renderAlertBook();
}

function renderProfileAlertManager() {
  profileAlertStatusPill.className = `pill ${state.telegramConfigured ? "live" : "dead"}`;
  profileAlertStatusPill.textContent = state.telegramConfigured ? "Telegram Ready" : "Telegram Missing";
  profileAlertSubmit.disabled = !state.telegramConfigured;
  profileAlertNote.textContent = state.telegramConfigured
    ? "This arms a Telegram alert when the selected study profile flips constructive."
    : "Telegram is not configured in this Meridian session yet, so new profile alerts cannot be armed.";

  if (!state.profileAlerts.length) {
    profileAlertBook.innerHTML = '<div class="empty">No study-profile alerts yet. Use the form or chart cards to arm your first Telegram setup.</div>';
    return;
  }

  const orderedAlerts = [...state.profileAlerts].sort((left, right) => {
    const statusRank = { active: 0, triggered: 1, disabled: 2 };
    return (statusRank[left.status] ?? 9) - (statusRank[right.status] ?? 9)
      || String(left.symbol).localeCompare(String(right.symbol))
      || String(left.timeframe).localeCompare(String(right.timeframe));
  });

  profileAlertBook.innerHTML = orderedAlerts.map((alert) => `
    <article class="alert-card">
      <div class="card-head">
        <div>
          <strong>${escapeHtml(alert.symbol)}</strong>
          <p class="meta">${escapeHtml(alert.profile_title || profileTitle(alert.profile_key))} | ${escapeHtml(alert.timeframe)}</p>
        </div>
        <span class="pill ${escapeHtml(alert.status)}">${escapeHtml(alert.status)}</span>
      </div>
      <div class="chart-summary">
        <div class="stat">
          <p class="stat-label">Signal State</p>
          <p class="stat-value">${escapeHtml(alert.last_signal_label || "Waiting")}</p>
        </div>
        <div class="stat">
          <p class="stat-label">Delivery</p>
          <p class="stat-value">${escapeHtml(alert.delivery || "Telegram")}</p>
        </div>
      </div>
      <p class="muted">
        ${escapeHtml(alert.status === "active"
          ? "Armed and watching for the next constructive transition on this study profile."
          : "Paused. Reactivate when you want Telegram pings for this profile again.")}
      </p>
      <p class="meta">
        Last triggered ${escapeHtml(relativeTime(alert.last_triggered_at))} | last evaluated ${escapeHtml(relativeTime(alert.last_evaluated_at))}
      </p>
      <div class="portfolio-actions">
        <button
          class="button-secondary"
          type="button"
          data-action="focus-profile-chart"
          data-symbol="${escapeHtml(alert.symbol)}"
          data-timeframe="${escapeHtml(alert.timeframe)}"
          data-profile-key="${escapeHtml(alert.profile_key)}"
        >
          Open In Charts
        </button>
        <button
          class="${alert.status === "active" ? "button-danger" : "button-secondary"}"
          type="button"
          data-action="${alert.status === "active" ? "disable-profile-alert" : "activate-profile-alert"}"
          data-id="${escapeHtml(alert.id)}"
        >
          ${alert.status === "active" ? "Disable" : "Reactivate"}
        </button>
      </div>
    </article>
  `).join("");
}

async function refreshProfileAlerts() {
  try {
    const response = await fetch("/api/profile-alerts");
    if (!response.ok) throw new Error("profile alert fetch failed");
    const payload = await response.json();
    state.profileAlerts = payload.profile_alerts || [];
    state.telegramConfigured = Boolean(payload.telegram_configured);
  } catch (_) {
    state.profileAlerts = [];
    state.telegramConfigured = false;
  }
  renderProfileAlertManager();
}

async function refreshPortfolios() {
  try {
    const response = await fetch("/api/portfolios");
    if (!response.ok) throw new Error("portfolio fetch failed");
    const payload = await response.json();
    state.portfolios = payload.portfolios || [];
  } catch (_) {
    state.portfolios = [];
  }

  if (!state.portfolios.length) {
    state.selectedPortfolioId = null;
  } else if (!state.portfolios.some((portfolio) => portfolio.id === state.selectedPortfolioId)) {
    state.selectedPortfolioId = state.portfolios[0].id;
  }
  renderPortfolioList();
  renderPortfolioDetail();
  updateTopStats();
}

function renderPortfolioList() {
  const container = document.getElementById("portfolio-list");
  if (!state.portfolios.length) {
    container.innerHTML = '<div class="empty">No portfolio profiles yet. Create one to start building named sleeves and strategy playbooks.</div>';
    return;
  }

  container.innerHTML = state.portfolios.map((portfolio) => `
    <article class="portfolio-card ${portfolio.id === state.selectedPortfolioId ? "active" : ""}" data-portfolio-id="${escapeHtml(portfolio.id)}">
      <div class="portfolio-head">
        <div>
          <p class="portfolio-name">${escapeHtml(portfolio.name)}</p>
          <p class="meta">${escapeHtml(portfolio.strategy || "No strategy tagline yet")}</p>
        </div>
        <span class="pill asset">${portfolio.asset_count} assets</span>
      </div>
      <div class="chart-summary">
        <div class="stat">
          <p class="stat-label">Allocation</p>
          <p class="stat-value">${formatMaybe(portfolio.allocation_pct, 0)}%</p>
        </div>
        <div class="stat">
          <p class="stat-label">Updated</p>
          <p class="stat-value">${escapeHtml(relativeTime(portfolio.updated_at))}</p>
        </div>
      </div>
    </article>
  `).join("");
}

function selectedPortfolio() {
  return state.portfolios.find((portfolio) => portfolio.id === state.selectedPortfolioId) || null;
}

function renderPortfolioDetail() {
  const container = document.getElementById("portfolio-detail");
  const portfolio = selectedPortfolio();
  if (!portfolio) {
    container.className = "empty";
    container.innerHTML = "Create a portfolio to start organizing assets and strategies.";
    return;
  }

  container.className = "sheet";
  container.innerHTML = `
    <form id="portfolio-detail-form">
      <div class="portfolio-detail-grid">
        <label>
          Portfolio Name
          <input id="portfolio-detail-name" value="${escapeHtml(portfolio.name)}" required />
        </label>
        <label>
          Strategy Tagline
          <input id="portfolio-detail-strategy" value="${escapeHtml(portfolio.strategy || "")}" placeholder="Trend, carry, event-driven, quality..." />
        </label>
      </div>
      <label>
        Notes
        <textarea id="portfolio-detail-notes" placeholder="Risk rules, catalysts, rebalance cadence, constraints.">${escapeHtml(portfolio.notes || "")}</textarea>
      </label>
      <div class="portfolio-actions">
        <button class="button-primary" type="submit">Save Profile</button>
        <button class="button-danger" type="button" id="delete-portfolio-button">Delete Portfolio</button>
      </div>
    </form>

    <div class="section-head">
      <div>
        <h3 class="section-title">Assets</h3>
        <p class="note">Add symbols with type, allocation, and role inside this profile.</p>
      </div>
      <span class="pill chart">${portfolio.asset_count} holdings</span>
    </div>

    <form id="portfolio-asset-form">
      <div class="form-grid">
        <label>
          Symbol
          <input id="portfolio-asset-symbol" placeholder="AAPL or BTC-USD" required />
        </label>
        <label>
          Allocation %
          <input id="portfolio-asset-allocation" type="number" step="0.1" min="0" max="100" placeholder="12.5" />
        </label>
      </div>
      <div>
        <p class="stat-label">Asset Type</p>
        <div class="asset-type-row" id="asset-type-row">
          ${ASSET_TYPES.map((type) => `
            <button class="asset-chip ${type === state.assetDraftType ? "active" : ""}" type="button" data-asset-type="${type}">
              ${type}
            </button>
          `).join("")}
        </div>
      </div>
      <div class="form-grid">
        <label>
          Asset Strategy
          <input id="portfolio-asset-strategy" placeholder="Core trend, hedging leg, tactical momentum..." />
        </label>
        <label>
          Notes
          <input id="portfolio-asset-notes" placeholder="Thesis, timing, catalysts, stop logic..." />
        </label>
      </div>
      <button class="button-secondary" type="submit">Add Asset</button>
    </form>

    <div class="asset-grid" id="portfolio-assets">
      ${portfolio.assets.length
        ? portfolio.assets.map((asset) => `
            <article class="asset-card">
              <div class="asset-head">
                <div>
                  <strong>${escapeHtml(asset.symbol)}</strong>
                  <p class="meta">${escapeHtml(asset.strategy || "No asset strategy yet")}</p>
                </div>
                <span class="pill asset">${escapeHtml(asset.asset_type)}</span>
              </div>
              <div class="chart-summary">
                <div class="stat">
                  <p class="stat-label">Allocation</p>
                  <p class="stat-value">${asset.allocation_pct !== null && asset.allocation_pct !== undefined ? `${formatMaybe(asset.allocation_pct, 1)}%` : "-"}</p>
                </div>
                <div class="stat">
                  <p class="stat-label">Added</p>
                  <p class="stat-value">${escapeHtml(relativeTime(asset.created_at))}</p>
                </div>
              </div>
              <p class="muted">${escapeHtml(asset.notes || "No notes yet.")}</p>
              <div class="portfolio-actions">
                <button class="button-secondary" type="button" data-action="focus-chart" data-symbol="${escapeHtml(asset.symbol)}">Open In Charts</button>
                <button class="button-danger" type="button" data-action="delete-asset" data-portfolio-id="${escapeHtml(portfolio.id)}" data-asset-id="${escapeHtml(asset.id)}">Remove</button>
              </div>
            </article>
          `).join("")
        : '<div class="empty">No assets in this portfolio yet. Add symbols and strategy notes to make the profile useful.</div>'}
    </div>
  `;
}

async function hydrateSymbols() {
  try {
    const response = await fetchWithTimeout("/api/symbols", { timeoutMs: 2500 });
    if (!response.ok) throw new Error("No symbols");
    const symbols = await response.json();
    symbols.forEach((item) => state.knownSymbols.add(item.symbol));
  } catch (_) {
    DEFAULT_SYMBOLS.forEach((symbol) => state.knownSymbols.add(symbol));
  }
  rebuildSymbolControls();
  [...state.knownSymbols].forEach((symbol) => ensureCard(symbol));
  applyMarketFilter();
}

function updateThresholdVisibility() {
  const needsThreshold = THRESHOLD_CONDITIONS.has(alertCondition.value);
  thresholdField.style.display = needsThreshold ? "grid" : "none";
  alertThreshold.required = needsThreshold;
  if (!needsThreshold) alertThreshold.value = "";
}

async function handleAlertSubmit(event) {
  event.preventDefault();
  const payload = {
    symbol: document.getElementById("alert-symbol").value.trim().toUpperCase(),
    condition: alertCondition.value,
  };
  if (THRESHOLD_CONDITIONS.has(payload.condition)) {
    payload.threshold = Number(alertThreshold.value);
  }
  const response = await fetch("/api/alerts", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    alert("Unable to create alert. Check the symbol and threshold.");
    return;
  }
  alertForm.reset();
  updateThresholdVisibility();
  await refreshAlerts();
  document.getElementById("alert-symbol").focus();
}

async function handleAlertAction(event) {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  const action = button.dataset.action;
  if (action !== "activate-alert" && action !== "disable-alert") return;
  const alertId = button.dataset.id;
  const endpoint = `/api/alerts/${alertId}/${action === "activate-alert" ? "activate" : "disable"}`;
  const response = await fetch(endpoint, { method: "POST" });
  if (!response.ok) {
    alert("Unable to update alert state.");
    return;
  }
  await refreshAlerts();
}

async function handleProfileAlertSubmit(event) {
  event.preventDefault();
  const payload = {
    symbol: profileAlertSymbol.value.trim().toUpperCase(),
    timeframe: profileAlertTimeframe.value,
    profile_key: profileAlertProfile.value,
  };
  const response = await fetch("/api/profile-alerts", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    let detail = "Unable to enable profile alert.";
    try {
      const errorPayload = await response.json();
      if (errorPayload?.detail) detail = errorPayload.detail;
    } catch (_) {}
    alert(detail);
    return;
  }
  registerSymbol(payload.symbol);
  profileAlertNote.textContent = `${profileTitle(payload.profile_key)} is now armed for ${payload.symbol} on ${payload.timeframe}.`;
  await refreshProfileAlerts();
  if (state.charts.data) renderChartInsights(state.charts.data);
}

async function handleProfileAlertAction(event) {
  const button = event.target.closest("[data-action]");
  if (!button) return;

  if (button.dataset.action === "focus-profile-chart") {
    state.charts.symbol = button.dataset.symbol;
    state.charts.timeframe = button.dataset.timeframe;
    state.charts.studyProfile = button.dataset.profileKey;
    state.charts.assetFilter = assetBucket(state.charts.symbol);
    rebuildSymbolControls();
    setChartControls();
    persistWorkspaceState();
    switchTab("charts");
    await loadChartData();
    return;
  }

  if (button.dataset.action !== "activate-profile-alert" && button.dataset.action !== "disable-profile-alert") {
    return;
  }

  const alertId = button.dataset.id;
  const endpoint = `/api/profile-alerts/${alertId}/${button.dataset.action === "activate-profile-alert" ? "activate" : "disable"}`;
  const response = await fetch(endpoint, { method: "POST" });
  if (!response.ok) {
    let detail = "Unable to update profile alert.";
    try {
      const errorPayload = await response.json();
      if (errorPayload?.detail) detail = errorPayload.detail;
    } catch (_) {}
    alert(detail);
    return;
  }

  await refreshProfileAlerts();
  if (state.charts.data) renderChartInsights(state.charts.data);
}

async function handlePortfolioCreate(event) {
  event.preventDefault();
  const payload = {
    name: document.getElementById("portfolio-name").value.trim(),
    strategy: document.getElementById("portfolio-strategy").value.trim(),
    notes: document.getElementById("portfolio-notes").value.trim(),
  };
  const response = await fetch("/api/portfolios", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    alert("Unable to create portfolio.");
    return;
  }
  portfolioForm.reset();
  await refreshPortfolios();
  switchTab("portfolios");
}

async function handlePortfolioDetailSubmit(event) {
  event.preventDefault();
  const portfolio = selectedPortfolio();
  if (!portfolio) return;
  const payload = {
    name: document.getElementById("portfolio-detail-name").value.trim(),
    strategy: document.getElementById("portfolio-detail-strategy").value.trim(),
    notes: document.getElementById("portfolio-detail-notes").value.trim(),
  };
  const response = await fetch(`/api/portfolios/${portfolio.id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    alert("Unable to update portfolio.");
    return;
  }
  await refreshPortfolios();
}

async function handlePortfolioDelete() {
  const portfolio = selectedPortfolio();
  if (!portfolio) return;
  if (!window.confirm(`Delete portfolio "${portfolio.name}"?`)) {
    return;
  }
  const response = await fetch(`/api/portfolios/${portfolio.id}`, { method: "DELETE" });
  if (!response.ok) {
    alert("Unable to delete portfolio.");
    return;
  }
  await refreshPortfolios();
}

async function handlePortfolioAssetSubmit(event) {
  event.preventDefault();
  const portfolio = selectedPortfolio();
  if (!portfolio) return;
  const payload = {
    symbol: document.getElementById("portfolio-asset-symbol").value.trim().toUpperCase(),
    asset_type: state.assetDraftType,
    allocation_pct: document.getElementById("portfolio-asset-allocation").value
      ? Number(document.getElementById("portfolio-asset-allocation").value)
      : null,
    strategy: document.getElementById("portfolio-asset-strategy").value.trim(),
    notes: document.getElementById("portfolio-asset-notes").value.trim(),
  };
  const response = await fetch(`/api/portfolios/${portfolio.id}/assets`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    alert("Unable to add asset.");
    return;
  }
  event.target.reset();
  state.assetDraftType = "equity";
  await refreshPortfolios();
  registerSymbol(payload.symbol);
}

async function handlePortfolioActions(event) {
  const card = event.target.closest("[data-portfolio-id]");
  if (card && !event.target.closest("[data-action]")) {
    state.selectedPortfolioId = card.dataset.portfolioId;
    renderPortfolioList();
    renderPortfolioDetail();
    return;
  }

  const button = event.target.closest("[data-action]");
  if (!button) return;

  if (button.dataset.action === "delete-asset") {
    const portfolioId = button.dataset.portfolioId;
    const assetId = button.dataset.assetId;
    const symbol = button.closest(".asset-card")?.querySelector("strong")?.textContent || "this asset";
    if (!window.confirm(`Remove ${symbol} from this portfolio?`)) {
      return;
    }
    const response = await fetch(`/api/portfolios/${portfolioId}/assets/${assetId}`, { method: "DELETE" });
    if (!response.ok) {
      alert("Unable to remove asset.");
      return;
    }
    await refreshPortfolios();
  }

  if (button.dataset.action === "focus-chart") {
    state.charts.symbol = button.dataset.symbol;
    state.charts.assetFilter = assetBucket(button.dataset.symbol);
    rebuildSymbolControls();
    setChartControls();
    persistWorkspaceState();
    switchTab("charts");
    await loadChartData();
  }
}

function bindTabs() {
  document.getElementById("tabs").addEventListener("click", (event) => {
    const button = event.target.closest("[data-tab]");
    if (!button) return;
    switchTab(button.dataset.tab);
  });
}

function bindOverview() {
  document.querySelectorAll("[data-market-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      state.marketFilter = button.dataset.marketFilter;
      applyMarketFilter();
      persistWorkspaceState();
    });
  });

  marketGrid.addEventListener("click", async (event) => {
    const button = event.target.closest('[data-action="focus-chart"]');
    if (!button) return;
    state.charts.symbol = button.dataset.symbol;
    state.charts.assetFilter = assetBucket(button.dataset.symbol);
    rebuildSymbolControls();
    setChartControls();
    persistWorkspaceState();
    switchTab("charts");
    await loadChartData();
  });

  overviewNewsTape.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-news-open]");
    if (!button) return;
    state.news.symbol = "";
    newsSymbol.value = "";
    selectNewsItem(button.dataset.newsOpen, true);
  });

  overviewMarketBrief.addEventListener("click", (event) => {
    const button = event.target.closest("[data-news-open]");
    if (!button) return;
    selectNewsItem(button.dataset.newsOpen, true);
  });
}

function bindAlertsSection() {
  profileAlertForm.addEventListener("submit", handleProfileAlertSubmit);
  profileAlertBook.addEventListener("click", handleProfileAlertAction);
}

function bindNewsSection() {
  document.querySelectorAll("[data-news-filter]").forEach((button) => {
    button.addEventListener("click", async () => {
      state.news.marketFilter = button.dataset.newsFilter;
      document.querySelectorAll("[data-news-filter]").forEach((chip) => {
        chip.classList.toggle("active", chip.dataset.newsFilter === state.news.marketFilter);
      });
      persistWorkspaceState();
      await refreshNews();
    });
  });

  newsSymbol.addEventListener("change", async () => {
    state.news.symbol = newsSymbol.value.trim().toUpperCase();
    persistWorkspaceState();
    await refreshNews();
  });

  newsImpact.addEventListener("change", async () => {
    state.news.impact = newsImpact.value;
    persistWorkspaceState();
    await refreshNews();
  });

  newsBias.addEventListener("change", async () => {
    state.news.bias = newsBias.value;
    persistWorkspaceState();
    await refreshNews();
  });

  newsHorizon.addEventListener("change", async () => {
    state.news.horizon = newsHorizon.value;
    persistWorkspaceState();
    await refreshNews();
  });

  newsSort.addEventListener("change", async () => {
    state.news.sort = newsSort.value;
    persistWorkspaceState();
    await refreshNews();
  });

  document.getElementById("news-refresh").addEventListener("click", refreshNews);

  newsTape.addEventListener("click", (event) => {
    const button = event.target.closest("[data-news-open]");
    if (!button) return;
    selectNewsItem(button.dataset.newsOpen);
  });

  newsFeed.addEventListener("click", (event) => {
    if (event.target.closest("[data-news-external]")) return;
    const card = event.target.closest("[data-news-open]");
    if (!card) return;
    selectNewsItem(card.dataset.newsOpen);
  });

  newsMarketBrief.addEventListener("click", (event) => {
    const button = event.target.closest("[data-news-open]");
    if (!button) return;
    selectNewsItem(button.dataset.newsOpen);
  });

  newsDetail.addEventListener("click", async (event) => {
    if (event.target.closest("[data-news-external]")) return;
    const button = event.target.closest('[data-action="focus-news-chart"]');
    if (!button) return;
    state.charts.symbol = button.dataset.symbol;
    state.charts.assetFilter = assetBucket(button.dataset.symbol);
    rebuildSymbolControls();
    setChartControls();
    persistWorkspaceState();
    switchTab("charts");
    await loadChartData();
  });
}

function bindChartControls() {
  chartSymbol.addEventListener("change", async () => {
    state.charts.symbol = chartSymbol.value;
    persistWorkspaceState();
    await loadChartData();
  });
  chartTimeframe.addEventListener("change", async () => {
    state.charts.timeframe = chartTimeframe.value;
    if (state.charts.windowPreset !== "custom") {
      applyChartWindowPreset();
      setChartControls();
    }
    persistWorkspaceState();
    await loadChartData();
  });
  chartWindow.addEventListener("change", async () => {
    state.charts.windowPreset = chartWindow.value;
    applyChartWindowPreset();
    setChartControls();
    persistWorkspaceState();
    await loadChartData();
  });
  chartLimit.addEventListener("change", async () => {
    state.charts.limit = clampChartLimit(chartLimit.value);
    state.charts.windowPreset = "custom";
    setChartControls();
    persistWorkspaceState();
    await loadChartData();
  });
  document.getElementById("chart-refresh").addEventListener("click", loadChartData);
  document.querySelectorAll("[data-chart-filter]").forEach((button) => {
    button.addEventListener("click", async () => {
      state.charts.assetFilter = button.dataset.chartFilter;
      rebuildSymbolControls();
      setChartControls();
      persistWorkspaceState();
      await loadChartData();
    });
  });
  overlayToggles.addEventListener("click", (event) => {
    const button = event.target.closest("[data-overlay]");
    if (!button) return;
    const overlay = button.dataset.overlay;
    state.charts.overlays[overlay] = !state.charts.overlays[overlay];
    renderOverlayToggles();
    persistWorkspaceState();
    if (state.charts.data) renderCharts();
  });
  studyProfileList.addEventListener("click", async (event) => {
    const toggleButton = event.target.closest("[data-profile-alert-toggle]");
    if (toggleButton && state.charts.data) {
      const profileKey = toggleButton.dataset.profileAlertToggle;
      const alertId = toggleButton.dataset.profileAlertId;
      let response;
      if (alertId) {
        response = await fetch(`/api/profile-alerts/${alertId}/disable`, { method: "POST" });
      } else {
        response = await fetch("/api/profile-alerts", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            symbol: state.charts.symbol,
            timeframe: state.charts.timeframe,
            profile_key: profileKey,
          }),
        });
      }

      if (!response.ok) {
        let detail = "Unable to update study profile alert.";
        try {
          const payload = await response.json();
          if (payload?.detail) detail = payload.detail;
        } catch (_) {}
        alert(detail);
        return;
      }

      await refreshProfileAlerts();
      renderChartInsights(state.charts.data);
      return;
    }

    const button = event.target.closest("[data-study-profile]");
    if (!button || !state.charts.data) return;
    const nextKey = button.dataset.studyProfile;
    applyStudyProfile(nextKey, state.charts.data, { resetOverlays: true });
    renderChartInsights(state.charts.data);
    renderCharts();
  });
  document.getElementById("timeframe-matrix").addEventListener("click", async (event) => {
    const card = event.target.closest("[data-matrix-timeframe]");
    if (!card) return;
    state.charts.timeframe = card.dataset.matrixTimeframe;
    setChartControls();
    persistWorkspaceState();
    await loadChartData();
  });
  document.querySelector(".chart-grid").addEventListener("click", (event) => {
    const button = event.target.closest("[data-study-toggle]");
    if (!button) return;
    const study = button.dataset.studyToggle;
    state.charts.expandedStudies[study] = !state.charts.expandedStudies[study];
    renderStudyPanels();
    persistWorkspaceState();
    if (state.charts.data && state.charts.expandedStudies[study]) {
      requestAnimationFrame(() => renderCharts());
    }
  });
}

function bindPortfolioSection() {
  portfolioForm.addEventListener("submit", handlePortfolioCreate);
  document.getElementById("portfolio-list").addEventListener("click", handlePortfolioActions);
  document.getElementById("portfolio-detail").addEventListener("click", async (event) => {
    if (event.target.id === "delete-portfolio-button") {
      await handlePortfolioDelete();
      return;
    }
    if (event.target.closest("[data-action]")) {
      await handlePortfolioActions(event);
      return;
    }
    const typeButton = event.target.closest("[data-asset-type]");
    if (typeButton) {
      state.assetDraftType = typeButton.dataset.assetType;
      document.querySelectorAll("[data-asset-type]").forEach((button) => {
        button.classList.toggle("active", button.dataset.assetType === state.assetDraftType);
      });
    }
  });
  document.getElementById("portfolio-detail").addEventListener("submit", async (event) => {
    if (event.target.id === "portfolio-detail-form") {
      await handlePortfolioDetailSubmit(event);
    }
    if (event.target.id === "portfolio-asset-form") {
      await handlePortfolioAssetSubmit(event);
    }
  });
}

function connectSocket() {
  clearTimeout(state.reconnectTimer);
  setConnection("idle", "Connecting...");
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const socketUrl = `${protocol}://${window.location.host}/ws/stream`;
  document.getElementById("socket-url").textContent = socketUrl;

  const ws = new WebSocket(socketUrl);
  state.ws = ws;

  ws.addEventListener("open", () => {
    setConnection("live", "Live stream connected");
    ws.send(JSON.stringify({ action: "subscribe", symbols: ["*"] }));
  });

  ws.addEventListener("message", (event) => {
    const message = JSON.parse(event.data);
    if (message.type === "market_update") {
      updateMarketCard(message);
    } else if (message.type === "alert_triggered") {
      state.recentTriggers.unshift(message.alert);
      state.recentTriggers = state.recentTriggers.slice(0, 12);
      renderTriggerFeed();
      refreshAlerts();
      if (message.alert?.condition === "study_profile_ready") {
        refreshProfileAlerts().then(() => {
          if (state.activeTab === "charts" && message.symbol === state.charts.symbol && state.charts.data) {
            renderChartInsights(state.charts.data);
          }
        });
      }
    }
  });

  ws.addEventListener("close", () => {
    setConnection("dead", "Stream reconnecting...");
    state.reconnectTimer = setTimeout(connectSocket, 2500);
  });

  ws.addEventListener("error", () => {
    ws.close();
  });
}

async function bootstrap() {
  hydrateWorkspaceState();
  hydrateQueryState();
  bindTabs();
  bindOverview();
  bindAlertsSection();
  bindNewsSection();
  bindChartControls();
  bindPortfolioSection();
  alertCondition.addEventListener("change", updateThresholdVisibility);
  alertForm.addEventListener("submit", handleAlertSubmit);
  alertBook.addEventListener("click", handleAlertAction);
  window.addEventListener("resize", () => {
    if (state.charts.data && state.activeTab === "charts") renderCharts();
  });

  await Promise.allSettled([
    hydrateSymbols(),
    refreshFeedStatus(),
    refreshAlerts(),
    refreshProfileAlerts(),
    refreshNews(),
    refreshPortfolios(),
  ]);
  renderTriggerFeed();
  updateThresholdVisibility();
  setChartControls();
  setProfileAlertFormFromCharts();
  switchTab(state.activeTab);
  updateTopStats();
  connectSocket();
  loadChartData().catch((error) => {
    chartStatus.textContent = `Unable to load chart data: ${error.message}`;
  });
  setInterval(refreshFeedStatus, 15000);
  setInterval(refreshNews, 60000);
}

bootstrap();
