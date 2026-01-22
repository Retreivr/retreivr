const state = {
  config: null,
  timers: {},
  pollingPaused: false,
  configDirty: false,
  inputFocused: false,
  suppressDirty: false,
  lastLogsText: null,
  lastHistoryKey: null,
  lastFilesKey: null,
  configNoticeTimer: null,
  configNoticeClearable: false,
  currentPage: "home",
  actionButtons: null,
  runtimeInfo: null,
  watcherStatus: null,
  searchSelectedRequestId: null,
  searchSelectedItemId: null,
  lastSearchRequestsKey: null,
  lastSearchItemsKey: null,
  lastSearchCandidatesKey: null,
  lastSearchQueueKey: null,
  searchRequestsSort: "desc",
  homeSearchRequestId: null,
  homeResultsTimer: null,
  homeSearchMode: "searchOnly",
  homeRequestContext: {},
  homeBestScores: {},
  homeCandidateCache: {},
  homeCandidatesLoading: {},
  homeSearchPollStart: null,
  homeSearchControlsEnabled: true,
  pendingAdvancedRequestId: null,
  spotifyPlaylistStatus: {},
  homeNoCandidateStreaks: {},
  homeDestinationInvalid: false,
  homeDirectPreview: null,
  homeDirectJob: null,
  homeDirectJobTimer: null,
};
const browserState = {
  open: false,
  root: "downloads",
  mode: "dir",
  ext: "",
  path: "",
  currentAbs: "",
  selected: "",
  target: null,
  renderToken: 0,
  limit: 500,
};
const oauthState = {
  open: false,
  sessionId: null,
  authUrl: "",
  account: "",
};
const BROWSE_DEFAULTS = {
  configDir: "",
  mediaRoot: "",
  tokensDir: "",
};
const GITHUB_REPO = "Retreivr/retreivr";
const GITHUB_RELEASE_URL = `https://api.github.com/repos/${GITHUB_REPO}/releases/latest`;
const GITHUB_RELEASE_PAGE = "https://github.com/Retreivr/retreivr/releases";
const RELEASE_CHECK_KEY = "yt_archiver_release_checked_at";
const RELEASE_CACHE_KEY = "yt_archiver_release_cache";
const RELEASE_VERSION_KEY = "yt_archiver_release_app_version";
const HOME_SOURCE_PRIORITY_MAP = {
  auto: null,
  youtube: ["youtube"],
  youtube_music: ["youtube_music"],
  soundcloud: ["soundcloud"],
  bandcamp: ["bandcamp"],
};
const HOME_GENERIC_SOURCE_PRIORITY = ["youtube_music", "soundcloud", "bandcamp", "youtube"];
const HOME_VIDEO_SOURCE_PRIORITY = ["youtube", "youtube_music", "soundcloud", "bandcamp"];
const HOME_VIDEO_KEYWORDS = ["show", "podcast", "episode", "interview"];
const HOME_STATUS_LABELS = {
  queued: "Searching",
  searching: "Searching",
  candidate_found: "Matched",
  selected: "Matched",
  enqueued: "Queued",
  claimed: "Downloading",
  downloading: "Downloading",
  postprocessing: "Downloading",
  completed: "Completed",
  failed: "Failed",
  skipped: "Failed",
};
const HOME_STATUS_CLASS_MAP = {
  queued: "searching",
  searching: "searching",
  candidate_found: "matched",
  selected: "matched",
  enqueued: "queued",
  claimed: "queued",
  downloading: "queued",
  postprocessing: "queued",
  completed: "matched",
  failed: "failed",
  skipped: "failed",
};
const HOME_FINAL_STATUSES = new Set(["completed", "completed_with_skips", "failed"]);
const HOME_RESULT_TIMEOUT_MS = 18000;
const DIRECT_URL_PLAYLIST_ERROR =
  "Playlist URLs are not supported in Direct URL mode. Please add this playlist via Scheduler or Playlist settings.";

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

function normalizePageName(page) {
  if (!page) {
    return "home";
  }
  if (page === "search") {
    return "advanced";
  }
  if (["downloads", "history", "logs"].includes(page)) {
    return "status";
  }
  return page;
}

function setNotice(el, message, isError = false) {
  if (!el) return;
  el.textContent = message;
  el.style.color = isError ? "#ff7b7b" : "#59b0ff";
}

function clearConfigNotice() {
  if (!state.configNoticeClearable) {
    return;
  }
  const el = $("#config-message");
  if (el) {
    el.textContent = "";
  }
  state.configNoticeClearable = false;
  if (state.configNoticeTimer) {
    clearTimeout(state.configNoticeTimer);
    state.configNoticeTimer = null;
  }
}

function setConfigNotice(message, isError = false, autoClear = false) {
  const el = $("#config-message");
  setNotice(el, message, isError);
  if (state.configNoticeTimer) {
    clearTimeout(state.configNoticeTimer);
    state.configNoticeTimer = null;
  }
  state.configNoticeClearable = !!autoClear;
  if (autoClear) {
    state.configNoticeTimer = setTimeout(clearConfigNotice, 20000);
  }
}

function setPage(page) {
  const normalized = normalizePageName(page);
  const allowed = new Set(["home", "config", "status", "advanced"]);
  const target = allowed.has(normalized) ? normalized : "home";
  state.currentPage = target;
  if (target === "home") {
    if (state.homeSearchRequestId) {
      startHomeResultPolling(state.homeSearchRequestId);
    }
    updateHomeViewAdvancedLink();
  } else {
    stopHomeResultPolling();
    updateHomeViewAdvancedLink();
  }
  document.body.classList.remove("nav-open");
  // Home-only root class for scoping styles
  document.body.classList.toggle("home-page", target === "home");
  document.body.dataset.page = target;
  const navToggle = $("#nav-toggle");
  if (navToggle) {
    navToggle.setAttribute("aria-expanded", "false");
  }
  const sections = $$("section[data-page]");
  sections.forEach((section) => {
    const show = section.dataset.page === target;
    section.classList.toggle("page-hidden", !show);
  });
  $$(".nav-button").forEach((button) => {
    button.classList.toggle("active", button.dataset.page === target);
  });
  if (target === "status") {
    refreshStatus();
    refreshMetrics();
    refreshVersion();
    refreshDownloads();
    refreshHistory();
    refreshLogs();
  } else if (target === "config") {
    if (!state.config || !state.configDirty) {
      loadConfig();
    }
    refreshSchedule();
  } else if (target === "advanced") {
    if (!state.config) {
      fetchJson("/api/config")
        .then((cfg) => {
          state.config = cfg;
          updateSearchDestinationDisplay();
        })
        .catch(() => {});
    } else {
      updateSearchDestinationDisplay();
    }
    const handoffRequestId = state.pendingAdvancedRequestId;
    state.pendingAdvancedRequestId = null;
    refreshSearchRequests(handoffRequestId)
      .then(() => {
        const requestIdToLoad = handoffRequestId || state.searchSelectedRequestId;
        if (requestIdToLoad) {
          return refreshSearchRequestDetails(requestIdToLoad);
        }
      })
      .catch(() => {});
    refreshSearchQueue();
  }
}

function isValidHttpUrl(value) {
  if (!value) return false;
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch (err) {
    return false;
  }
}

function extractPlaylistIdFromUrl(value) {
  if (!value) return null;
  try {
    const parsed = new URL(value, window.location.origin);
    const listParam = parsed.searchParams.get("list");
    if (listParam) {
      return listParam;
    }
    const segments = parsed.pathname.split("/").filter(Boolean);
    if (segments.includes("playlist")) {
      const index = segments.indexOf("playlist");
      if (segments[index + 1]) {
        return segments[index + 1];
      }
    }
    if (segments[0] && segments[0].startsWith("PL")) {
      return segments[0];
    }
  } catch (err) {
    return null;
  }
  return null;
}

function computeResolvedDestinationPath(raw) {
  const base = BROWSE_DEFAULTS.mediaRoot || "/downloads";
  const trimmed = (raw || "").trim();
  if (!trimmed) {
    return base || "/downloads";
  }
  if (trimmed.startsWith("/")) {
    return trimmed;
  }
  const normalizedBase = base.endsWith("/") ? base.slice(0, -1) : base;
  const cleaned = trimmed.replace(/^\/+/, "");
  if (!normalizedBase) {
    return `/${cleaned}`;
  }
  return `${normalizedBase}/${cleaned}`;
}

function shouldPreferVideoAdapters(raw) {
  if (!raw) return false;
  const normalized = raw.toLowerCase();
  const words = normalized.split(/\s+/).filter(Boolean);
  if (words.length >= 3) {
    return true;
  }
  return HOME_VIDEO_KEYWORDS.some((keyword) => normalized.includes(keyword));
}

function hasInvalidDestinationValue(value) {
  if (!value) return false;
  const trimmed = value.trim();
  if (!trimmed) return false;
  if (trimmed.includes("..") || trimmed.includes("\\") || trimmed.includes("~")) {
    return true;
  }
  return false;
}

function updateHomeDestinationResolved() {
  const display = $("#home-destination-resolved");
  if (!display) return;
  const value = $("#home-destination")?.value;
  const resolved = computeResolvedDestinationPath(value);
  display.textContent = `Resolved: ${resolved}`;
}

function setupNavActions() {
  const topActions = $("#top-actions");
  const navActions = $("#nav-actions");
  if (!topActions || !navActions) {
    return;
  }
  if (!state.actionButtons) {
    state.actionButtons = Array.from(topActions.children);
  }
  const mql = window.matchMedia("(max-width: 900px)");
  const sync = () => {
    const target = mql.matches ? navActions : topActions;
    state.actionButtons.forEach((button) => {
      if (button.parentElement !== target) {
        target.appendChild(button);
      }
    });
  };
  sync();
  if (mql.addEventListener) {
    mql.addEventListener("change", sync);
  } else if (mql.addListener) {
    mql.addListener(sync);
  }
}

function updatePollingState() {
  state.pollingPaused = browserState.open || oauthState.open || state.configDirty || state.inputFocused;
}

function withPollingGuard(fn) {
  if (state.pollingPaused) {
    return;
  }
  fn();
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes)) return "";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = bytes;
  let idx = 0;
  while (size >= 1024 && idx < units.length - 1) {
    size /= 1024;
    idx += 1;
  }
  return `${size.toFixed(idx === 0 ? 0 : 1)} ${units[idx]}`;
}

function formatSpeed(speed) {
  if (!Number.isFinite(speed)) return "-";
  return `${formatBytes(speed)}/s`;
}

function formatDuration(seconds) {
  if (!Number.isFinite(seconds)) return "-";
  const total = Math.max(0, Math.floor(seconds));
  const mins = Math.floor(total / 60);
  const secs = total % 60;
  if (mins > 0) {
    return `${mins}m ${secs}s`;
  }
  return `${secs}s`;
}

function formatCountdown(serverTime, targetTime) {
  if (!serverTime || !targetTime) return "";
  const now = new Date(serverTime).getTime();
  const target = new Date(targetTime).getTime();
  if (Number.isNaN(now) || Number.isNaN(target)) return "";
  const diff = Math.max(0, Math.floor((target - now) / 1000));
  return formatDuration(diff);
}

function normalizeVersionTag(tag) {
  if (!tag) return "";
  return tag.trim().replace(/^v/i, "");
}

function sanitizeVersionTag(tag) {
  return (tag || "").replace(/[^0-9A-Za-z._-]/g, "");
}

function parseVersion(tag) {
  const clean = normalizeVersionTag(tag);
  const parts = clean.split(".");
  return parts.map((part) => parseInt(part, 10) || 0);
}

function compareVersions(current, latest) {
  const a = parseVersion(current);
  const b = parseVersion(latest);
  const len = Math.max(a.length, b.length);
  for (let i = 0; i < len; i += 1) {
    const left = a[i] || 0;
    const right = b[i] || 0;
    if (left > right) return 1;
    if (left < right) return -1;
  }
  return 0;
}

function formatTimestamp(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function downloadUrl(fileId) {
  return `/api/files/${encodeURIComponent(fileId)}/download`;
}

function resolveTheme() {
  const saved = localStorage.getItem("yt_archiver_theme");
  if (saved === "light" || saved === "dark") {
    return saved;
  }
  return "dark";
}

function applyTheme(theme) {
  const root = document.documentElement;
  if (theme === "light") {
    root.dataset.theme = "light";
  } else {
    delete root.dataset.theme;
  }
  const button = $("#toggle-theme");
  if (button) {
    button.textContent = theme === "light" ? "Dark mode" : "Light mode";
  }
  localStorage.setItem("yt_archiver_theme", theme);
}

async function copyText(text) {
  if (!text) return false;
  if (navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      /* fall through */
    }
  }
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  let ok = false;
  try {
    ok = document.execCommand("copy");
  } catch (err) {
    ok = false;
  }
  document.body.removeChild(textarea);
  return ok;
}

function displayPath(path, baseDir, showInternal) {
  if (!path) return "";
  if (showInternal) {
    return path;
  }
  if (baseDir) {
    const normalized = baseDir.endsWith("/") ? baseDir : `${baseDir}/`;
    if (path.startsWith(normalized)) {
      return path.slice(normalized.length);
    }
  }
  return path;
}

function normalizeDownloadsRelative(value) {
  const raw = (value || "").trim();
  if (!raw) return "";
  if (raw === "." || raw === "./") return ".";
  if (raw.startsWith("./")) {
    return raw.slice(2);
  }

  const base = BROWSE_DEFAULTS.mediaRoot || "/downloads";
  const normalizedBase = base.endsWith("/") ? base : `${base}/`;
  if (raw === base || raw === "/downloads") {
    return ".";
  }
  if (raw.startsWith(normalizedBase)) {
    return raw.slice(normalizedBase.length);
  }
  if (raw.startsWith("/downloads/")) {
    return raw.slice("/downloads/".length);
  }
  return raw;
}

function resolveBrowseStart(rootKey, value) {
  const raw = (value || "").trim();
  if (!raw) return "";
  if (raw.startsWith("..")) return "";
  if (raw.startsWith("./")) {
    return raw.slice(2);
  }

  let base = "";
  if (rootKey === "downloads") {
    base = BROWSE_DEFAULTS.mediaRoot || "";
  } else if (rootKey === "config") {
    base = BROWSE_DEFAULTS.configDir || "";
  } else if (rootKey === "tokens") {
    base = BROWSE_DEFAULTS.tokensDir || "";
  }

  if (!base) {
    return raw.startsWith("/") ? "" : raw;
  }

  const normalizedBase = base.endsWith("/") ? base : `${base}/`;
  if (raw === base) {
    return "";
  }
  if (raw.startsWith(normalizedBase)) {
    return raw.slice(normalizedBase.length);
  }
  if (!raw.startsWith("/")) {
    return raw;
  }
  return "";
}


async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${text}`);
  }
  return response.json();
}

// Helper to cancel a job by ID
async function cancelJob(jobId) {
  if (!jobId) return;
  return fetchJson(`/api/jobs/${encodeURIComponent(jobId)}/cancel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ reason: "User cancelled" }),
  });
}

function updateVersionDisplay(info) {
  if (!info) return;
  const appVersion = normalizeVersionTag(info.app_version || "") || "0.0.0";
  const ytDlpVersion = info.yt_dlp_version || "-";
  const pyVersion = info.python_version || "-";
  const appEl = $("#status-version-app");
  const ytdlpEl = $("#status-version-ytdlp");
  const pyEl = $("#status-version-python");
  if (appEl) appEl.textContent = `App ${appVersion}`;
  if (ytdlpEl) ytdlpEl.textContent = `yt-dlp ${ytDlpVersion}`;
  if (pyEl) pyEl.textContent = `Py ${pyVersion}`;
}

function applyReleaseStatus(currentVersion, latestTag) {
  const updateEl = $("#status-update");
  if (!updateEl) return;
  const latest = normalizeVersionTag(latestTag);
  const current = normalizeVersionTag(currentVersion || "");
  if (!latest) {
    updateEl.textContent = "-";
    return;
  }
  const safeTag = sanitizeVersionTag(latest);
  const link = document.createElement("a");
  link.href = GITHUB_RELEASE_PAGE;
  link.target = "_blank";
  link.rel = "noopener";
  link.textContent = `v${safeTag}`;

  updateEl.textContent = "";
  const cmp = compareVersions(current, latest);
  if (cmp < 0) {
    updateEl.append("App update: ");
    updateEl.appendChild(link);
    return;
  }
  if (!current || current === "0.0.0") {
    updateEl.append("Latest: ");
    updateEl.appendChild(link);
    return;
  }
  updateEl.append("Up to date: ");
  updateEl.appendChild(link);
}

async function checkRelease(currentVersion) {
  const now = Date.now();
  const lastCheck = parseInt(localStorage.getItem(RELEASE_CHECK_KEY) || "0", 10);
  const cachedVersion = localStorage.getItem(RELEASE_VERSION_KEY) || "";
  const cachedRaw = localStorage.getItem(RELEASE_CACHE_KEY);
  let cached = null;
  if (cachedRaw) {
    try {
      cached = JSON.parse(cachedRaw);
    } catch (err) {
      cached = null;
    }
  }

  const normalizedVersion = normalizeVersionTag(currentVersion || "");
  const versionChanged = cachedVersion !== normalizedVersion;
  if (versionChanged) {
    localStorage.removeItem(RELEASE_CHECK_KEY);
    localStorage.removeItem(RELEASE_CACHE_KEY);
  }

  if (lastCheck && now - lastCheck < 24 * 60 * 60 * 1000 && cached && !versionChanged) {
    applyReleaseStatus(currentVersion, cached.tag);
    return;
  }

  try {
    const response = await fetch(GITHUB_RELEASE_URL, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    const tag = data.tag_name || "";
    localStorage.setItem(RELEASE_CHECK_KEY, String(now));
    localStorage.setItem(RELEASE_CACHE_KEY, JSON.stringify({ tag }));
    localStorage.setItem(RELEASE_VERSION_KEY, normalizedVersion);
    applyReleaseStatus(currentVersion, tag);
  } catch (err) {
    if (cached) {
      applyReleaseStatus(currentVersion, cached.tag);
    }
  }
}

async function refreshVersion() {
  try {
    const info = await fetchJson("/api/version");
    state.runtimeInfo = info;
    updateVersionDisplay(info);
    await checkRelease(info.app_version || "");
  } catch (err) {
    const versionEl = $("#status-version");
    if (versionEl) {
      versionEl.textContent = "-";
    }
  }
}

async function loadPaths() {
  try {
    const data = await fetchJson("/api/paths");
    BROWSE_DEFAULTS.configDir = data.config_dir || "";
    BROWSE_DEFAULTS.mediaRoot = data.downloads_dir || "";
    BROWSE_DEFAULTS.tokensDir = data.tokens_dir || "";
  } catch (err) {
    setConfigNotice(`Path load error: ${err.message}`, true);
  }
}

function openBrowser(target, root, mode = "dir", ext = "", startPath = "") {
  browserState.open = true;
  browserState.root = root;
  browserState.mode = mode;
  browserState.ext = ext;
  browserState.path = "";
  browserState.currentAbs = "";
  browserState.selected = "";
  browserState.target = target;
  updatePollingState();
  $("#browser-modal").classList.remove("hidden");
  $("#browser-select").textContent = mode === "dir" ? "Use this folder" : "Use selected file";
  refreshBrowser(startPath || "");
}

function closeBrowser() {
  browserState.open = false;
  $("#browser-modal").classList.add("hidden");
  browserState.target = null;
  browserState.selected = "";
  updatePollingState();
}

async function refreshBrowser(path, allowFallback = true) {
  const params = new URLSearchParams();
  params.set("root", browserState.root);
  if (path) {
    params.set("path", path);
  }
  params.set("mode", browserState.mode);
  if (browserState.ext) {
    params.set("ext", browserState.ext);
  }
  if (browserState.limit) {
    params.set("limit", String(browserState.limit));
  }

  const list = $("#browser-list");
  list.textContent = "";
  const loading = document.createElement("div");
  loading.className = "browser-item empty";
  loading.textContent = "Loading...";
  list.appendChild(loading);
  const renderToken = ++browserState.renderToken;

  try {
    const data = await fetchJson(`/api/browse?${params.toString()}`);
    if (renderToken !== browserState.renderToken) {
      return;
    }
    browserState.path = data.path || "";
    browserState.currentAbs = data.abs_path || "";
    browserState.selected = "";
    $("#browser-path").textContent = data.abs_path || "/";
    if (browserState.mode === "dir") {
      $("#browser-selected").textContent = browserState.currentAbs ? `Current: ${browserState.currentAbs}` : "Select a folder";
    } else {
      $("#browser-selected").textContent = "No selection";
    }
    const hasParent = data.parent !== null && data.parent !== undefined;
    $("#browser-up").disabled = !hasParent;
    $("#browser-up").dataset.path = data.parent || "";
    const canSelect = browserState.mode === "dir" ? !!browserState.currentAbs : !!browserState.selected;
    $("#browser-select").disabled = !canSelect;

    list.textContent = "";

    if (!data.entries.length) {
      const empty = document.createElement("div");
      empty.className = "browser-item empty";
      empty.textContent = "No entries";
      list.appendChild(empty);
      return;
    }

    const entries = data.entries;
    const chunkSize = 100;
    let index = 0;

    const createItem = (entry) => {
      const item = document.createElement("button");
      item.className = "browser-item";
      item.type = "button";
      item.dataset.path = entry.path;
      item.dataset.absPath = entry.abs_path || "";
      item.dataset.type = entry.type;
      item.textContent = entry.type === "dir" ? `${entry.name}/` : entry.name;
      return item;
    };

    const renderChunk = () => {
      if (renderToken !== browserState.renderToken) {
        return;
      }
      const fragment = document.createDocumentFragment();
      if (index === 0 && browserState.limit && entries.length >= browserState.limit) {
        const notice = document.createElement("div");
        notice.className = "browser-item empty";
        notice.textContent = `Showing first ${browserState.limit} entries`;
        fragment.appendChild(notice);
      }
      for (let i = 0; i < chunkSize && index < entries.length; i += 1, index += 1) {
        fragment.appendChild(createItem(entries[index]));
      }
      list.appendChild(fragment);
      if (index < entries.length) {
        requestAnimationFrame(renderChunk);
      }
    };

    renderChunk();
  } catch (err) {
    if (allowFallback && path) {
      refreshBrowser("", false);
      return;
    }
    list.textContent = "";
    const errorItem = document.createElement("div");
    errorItem.className = "browser-item error";
    errorItem.textContent = `Failed to load: ${err.message}`;
    list.appendChild(errorItem);
  }
}

function applyBrowserSelection() {
  if (!browserState.target) return;
  if (browserState.mode === "dir") {
    if (!browserState.currentAbs) {
      return;
    }
    const rel = browserState.path ? browserState.path : ".";
    const targetId = browserState.target.id;
    browserState.target.value = rel;
    console.info("Directory selected", { root: browserState.root, path: rel });
    closeBrowser();
    if (targetId === "home-destination") {
      updateHomeDestinationResolved();
    } else if (targetId === "search-destination") {
      updateSearchDestinationDisplay();
    }
    return;
  }
  if (browserState.selected) {
    const targetId = browserState.target.id;
    browserState.target.value = browserState.selected;
    console.info("File selected", { root: browserState.root, path: browserState.selected });
    closeBrowser();
    if (targetId === "home-destination") {
      updateHomeDestinationResolved();
    } else if (targetId === "search-destination") {
      updateSearchDestinationDisplay();
    }
  }
}

function openOauthModal() {
  oauthState.open = true;
  $("#oauth-modal").classList.remove("hidden");
  updatePollingState();
}

function closeOauthModal() {
  oauthState.open = false;
  $("#oauth-modal").classList.add("hidden");
  oauthState.sessionId = null;
  oauthState.authUrl = "";
  oauthState.account = "";
  updatePollingState();
}

async function startOauthForRow(row) {
  const account = row.querySelector(".account-name").value.trim();
  const clientSecret = row.querySelector(".account-client").value.trim();
  const tokenOut = row.querySelector(".account-token").value.trim();
  if (!account) {
    setConfigNotice("Account name is required for OAuth.", true);
    return;
  }
  if (!clientSecret || !tokenOut) {
    setConfigNotice("Client secret and token paths are required for OAuth.", true);
    return;
  }
  try {
    setConfigNotice("Starting OAuth...", false);
    const data = await fetchJson("/api/oauth/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        account,
        client_secret: clientSecret,
        token_out: tokenOut,
      }),
    });
    oauthState.sessionId = data.session_id;
    oauthState.authUrl = data.auth_url || "";
    oauthState.account = account;
    $("#oauth-account").textContent = account;
    $("#oauth-url").value = oauthState.authUrl;
    $("#oauth-code").value = "";
    setNotice($("#oauth-message"), "", false);
    openOauthModal();
    if (oauthState.authUrl) {
      window.open(oauthState.authUrl, "_blank", "noopener");
    }
  } catch (err) {
    setConfigNotice(`OAuth start failed: ${err.message}`, true);
  }
}

async function completeOauth() {
  const code = $("#oauth-code").value.trim();
  if (!oauthState.sessionId) {
    setNotice($("#oauth-message"), "No active OAuth session.", true);
    return;
  }
  if (!code) {
    setNotice($("#oauth-message"), "Authorization code is required.", true);
    return;
  }
  try {
    setNotice($("#oauth-message"), "Completing OAuth...", false);
    await fetchJson("/api/oauth/complete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        session_id: oauthState.sessionId,
        code,
      }),
    });
    setNotice($("#oauth-message"), "Token saved.", false);
  } catch (err) {
    setNotice($("#oauth-message"), `OAuth failed: ${err.message}`, true);
  }
}

async function refreshConfigPath() {
  try {
    const data = await fetchJson("/api/config/path");
    $("#config-path").value = data.path || "";
  } catch (err) {
    setConfigNotice(`Config path error: ${err.message}`, true);
  }
}

async function setConfigPath() {
  const path = $("#config-path").value.trim();
  if (!path) {
    setConfigNotice("Config path is required", true);
    return;
  }
  try {
    await fetchJson("/api/config/path", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ path }),
    });
    await loadConfig();
    setConfigNotice("Config path updated", false);
  } catch (err) {
    setConfigNotice(`Config path error: ${err.message}`, true);
  }
}

async function refreshStatus() {
  try {
    const data = await fetchJson("/api/status");
    const runningChip = $("#status-running");
    if (data.running) {
      runningChip.textContent = "running";
      runningChip.classList.add("running");
      runningChip.classList.remove("idle");
    } else {
      runningChip.textContent = "idle";
      runningChip.classList.add("idle");
      runningChip.classList.remove("running");
    }

    $("#status-run-id").textContent = `run: ${data.run_id || "-"}`;
    $("#status-started").textContent = formatTimestamp(data.started_at) || "-";
    $("#status-finished").textContent = formatTimestamp(data.finished_at) || "-";
    const watcher = data.watcher || {};
    const scheduler = data.scheduler || {};
    const watcherText = watcher.enabled
      ? (watcher.paused ? "paused (downtime)" : "enabled")
      : "disabled";
    $("#status-watcher").textContent = watcherText;
    $("#status-scheduler").textContent = scheduler.enabled ? "enabled" : "disabled";
    state.watcherStatus = watcher;
    const scheduleNote = $("#schedule-watcher-note");
    if (scheduleNote) {
      scheduleNote.style.display = watcher.enabled ? "block" : "none";
    }

    const status = data.status || {};
    const failures = status.run_failures || [];
    const watcherErrors = data.watcher_errors || [];
    let errorText = data.error || status.last_error_message || "";
    if (!errorText) {
      if (status.single_download_ok === false) {
        errorText = "Single download failed";
      } else if (failures.length) {
        errorText = failures[failures.length - 1];
      } else if (watcherErrors.length) {
        const last = watcherErrors[watcherErrors.length - 1];
        errorText = `Watcher: ${last.playlist_id} (${last.last_error})`;
      }
    }
    $("#status-error").textContent = errorText || "-";
    $("#status-success").textContent = (status.run_successes || []).length;
    $("#status-failed").textContent = failures.length;
    $("#status-playlist").textContent = status.current_playlist_id || "-";
    $("#status-video").textContent = status.current_video_title || status.current_video_id || "-";
    $("#status-phase").textContent = status.current_phase || "-";
    const watcherStatus = data.watcher_status || {};
    const watcherStateMap = {
      idle: "Idle",
      polling: "Polling",
      waiting_quiet_window: "Waiting (quiet window)",
      batch_ready: "Batch ready",
      running_batch: "Running batch",
      disabled: "Disabled",
    };
    const watcherState = watcherStatus.state || (watcher.enabled ? "idle" : "disabled");
    $("#watcher-state").textContent = watcherStateMap[watcherState] || watcherState;
    const pendingCount = Number.isFinite(watcherStatus.pending_playlists_count)
      ? watcherStatus.pending_playlists_count
      : 0;
    $("#watcher-pending").textContent = String(pendingCount);
    $("#watcher-batch").textContent = watcherStatus.batch_active ? "Active" : "Inactive";
    $("#watcher-last-poll").textContent = watcherStatus.last_poll_ts
      ? formatTimestamp(watcherStatus.last_poll_ts)
      : "-";
    if (watcherStatus.next_poll_ts) {
      const countdown = formatCountdown(data.server_time, watcherStatus.next_poll_ts);
      const suffix = countdown ? ` (in ${countdown})` : "";
      $("#watcher-next-poll").textContent = `${formatTimestamp(watcherStatus.next_poll_ts)}${suffix}`;
    } else {
      $("#watcher-next-poll").textContent = "-";
    }
    if (Number.isFinite(watcherStatus.quiet_window_remaining_sec)) {
      $("#watcher-quiet-remaining").textContent = formatDuration(watcherStatus.quiet_window_remaining_sec);
    } else {
      $("#watcher-quiet-remaining").textContent = "-";
    }
    if (status.last_completed) {
      const suffix = status.last_completed_at ? ` (${formatTimestamp(status.last_completed_at)})` : "";
      $("#status-last-completed").textContent = `${status.last_completed}${suffix}`;
    } else {
      $("#status-last-completed").textContent = "-";
    }
    if (Number.isFinite(status.progress_total) && Number.isFinite(status.progress_current)) {
      const percent = Number.isFinite(status.progress_percent)
        ? status.progress_percent
        : (status.progress_total > 0
          ? Math.round((status.progress_current / status.progress_total) * 100)
          : 0);
      $("#status-playlist-progress-text").textContent =
        `${status.progress_current}/${status.progress_total} (${percent}%)`;
      $("#status-playlist-progress-bar").style.width = `${Math.max(0, Math.min(100, percent))}%`;
    } else {
      $("#status-playlist-progress-text").textContent = "-";
      $("#status-playlist-progress-bar").style.width = "0%";
    }

    const videoContainer = $("#status-video-progress");
    const downloaded = status.video_downloaded_bytes;
    const total = status.video_total_bytes;
    let videoPercent = status.video_progress_percent;
    if (!Number.isFinite(videoPercent) && Number.isFinite(downloaded) && Number.isFinite(total) && total > 0) {
      videoPercent = Math.round((downloaded / total) * 100);
    }
    const hasVideoProgress = data.running && (
      Number.isFinite(videoPercent) ||
      Number.isFinite(downloaded) ||
      Number.isFinite(total)
    );
    if (hasVideoProgress) {
      videoContainer.classList.remove("hidden");
      $("#status-video-progress-text").textContent =
        Number.isFinite(videoPercent) ? `${videoPercent}%` : "-";
      $("#status-video-progress-bar").style.width =
        Number.isFinite(videoPercent) ? `${Math.max(0, Math.min(100, videoPercent))}%` : "0%";
      const downloadedText = Number.isFinite(downloaded) ? formatBytes(downloaded) : "-";
      const totalText = Number.isFinite(total) ? formatBytes(total) : "-";
      const speedText = formatSpeed(status.video_speed);
      const etaText = formatDuration(status.video_eta);
      $("#status-video-progress-meta").textContent =
        `${downloadedText} / ${totalText} · ${speedText} · ETA ${etaText}`;
    } else {
      videoContainer.classList.add("hidden");
      $("#status-video-progress-text").textContent = "-";
      $("#status-video-progress-bar").style.width = "0%";
      $("#status-video-progress-meta").textContent = "-";
    }

    const singleLink = $("#run-single-download");
    if (singleLink) {
      const clientDeliveryId = status.client_delivery_id;
      const fileId = status.last_completed_file_id;
      if (clientDeliveryId) {
        singleLink.href = `/api/deliveries/${clientDeliveryId}/download`;
        singleLink.textContent = "Download to device";
        singleLink.setAttribute("aria-disabled", "false");
      } else if (fileId) {
        singleLink.href = downloadUrl(fileId);
        singleLink.textContent = "Download last";
        singleLink.setAttribute("aria-disabled", "false");
      } else {
        singleLink.href = "#";
        singleLink.textContent = "Download last";
        singleLink.setAttribute("aria-disabled", "true");
      }
    }
    const cancelBtn = $("#status-cancel");
    if (cancelBtn) {
      cancelBtn.disabled = !data.running;
      cancelBtn.onclick = async () => {
        const jobId = data.current_job_id || data.run_id;
        if (!jobId) return;
        cancelBtn.disabled = true;
        try {
          await cancelJob(jobId);
          await refreshStatus();
        } catch (err) {
          setNotice($("#run-message"), `Cancel failed: ${err.message}`, true);
        }
      };
    }
  } catch (err) {
    setNotice($("#run-message"), `Status error: ${err.message}`, true);
  }
}

async function refreshLogs() {
  const lines = parseInt($("#logs-lines").value, 10) || 200;
  try {
    const response = await fetch(`/api/logs?lines=${lines}`);
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`${response.status} ${text}`);
    }
    const text = await response.text();
    if (text !== state.lastLogsText) {
      $("#logs-output").textContent = text;
      state.lastLogsText = text;
    }
  } catch (err) {
    $("#logs-output").textContent = `Failed to load logs: ${err.message}`;
  }
}

async function refreshHistory() {
  const limit = parseInt($("#history-limit").value, 10) || 50;
  try {
    const params = new URLSearchParams();
    params.set("limit", String(limit));
    const search = $("#history-search").value.trim();
    if (search) {
      params.set("search", search);
    }
    const playlist = $("#history-playlist").value.trim();
    if (playlist) {
      params.set("playlist_id", playlist);
    }
    const dateFrom = $("#history-from").value;
    if (dateFrom) {
      params.set("date_from", dateFrom);
    }
    const dateTo = $("#history-to").value;
    if (dateTo) {
      params.set("date_to", dateTo);
    }
    const sortBy = $("#history-sort").value;
    if (sortBy) {
      params.set("sort_by", sortBy);
    }
    const sortDir = $("#history-dir").value;
    if (sortDir) {
      params.set("sort_dir", sortDir);
    }

    const rows = await fetchJson(`/api/history?${params.toString()}`);
    const key = JSON.stringify(rows);
    if (key === state.lastHistoryKey) {
      return;
    }
    state.lastHistoryKey = key;
    const body = $("#history-body");
    body.textContent = "";
    const showPaths = $("#history-show-paths").checked;
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      const downloadHref = row.file_id ? downloadUrl(row.file_id) : "";
      const downloadButton = row.file_id
        ? `<a class="button ghost small" href="${downloadHref}">⬇ Download</a>`
        : `<span class="meta">-</span>`;
      const videoUrl = row.video_url || (row.video_id ? `https://www.youtube.com/watch?v=${row.video_id}` : "");
      const copyUrlButton = videoUrl
        ? `<button class="button ghost small" data-copy="url" data-value="${encodeURIComponent(videoUrl)}">Copy URL</button>`
        : "";
      const pathDisplay = displayPath(row.filepath || "", BROWSE_DEFAULTS.mediaRoot, showPaths);
      const copyPathButton = showPaths && row.filepath
        ? `<button class="button ghost small" data-copy="path" data-value="${encodeURIComponent(row.filepath)}">Copy Path</button>`
        : "";
      const jsonPayload = encodeURIComponent(JSON.stringify(row, null, 2));
      tr.innerHTML = `
        <td>${row.video_id || ""}</td>
        <td>${row.playlist_id || ""}</td>
        <td>${formatTimestamp(row.downloaded_at) || ""}</td>
        <td>${pathDisplay}</td>
        <td>
          <div class="action-group">
            ${downloadButton}
            ${copyUrlButton}
            ${copyPathButton}
            <button class="button ghost small" data-copy="json" data-value="${jsonPayload}">Copy JSON</button>
          </div>
        </td>
      `;
      body.appendChild(tr);
    });
  } catch (err) {
    const body = $("#history-body");
    body.textContent = "";
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="5">Failed to load history: ${err.message}</td>`;
    body.appendChild(tr);
  }
}

async function refreshMetrics() {
  try {
    const data = await fetchJson("/api/metrics");
    $("#metrics-downloads-count").textContent = data.downloads_files ?? "-";
    $("#metrics-downloads-size").textContent = formatBytes(data.downloads_bytes);
    const free = formatBytes(data.disk_free_bytes);
    const total = formatBytes(data.disk_total_bytes);
    const percent = Number.isFinite(data.disk_free_percent) ? ` (${data.disk_free_percent}%)` : "";
    $("#metrics-disk-free").textContent = free ? `${free}${percent}` : "-";
    $("#metrics-disk-total").textContent = total || "-";
    const message = $("#metrics-message");
    message.classList.remove("warn", "critical");
    if (Number.isFinite(data.disk_free_percent)) {
      if (data.disk_free_percent < 5) {
        message.textContent = "Warning: disk space below 5%";
        message.classList.add("critical");
      } else if (data.disk_free_percent < 10) {
        message.textContent = "Warning: disk space below 10%";
        message.classList.add("warn");
      } else {
        message.textContent = "";
      }
    } else {
      message.textContent = "";
    }
  } catch (err) {
    const message = $("#metrics-message");
    message.classList.remove("warn", "critical");
    message.textContent = `Metrics error: ${err.message}`;
  }
}

async function refreshSchedule() {
  try {
    const data = await fetchJson("/api/schedule");
    const schedule = data.schedule || {};
    $("#schedule-enabled").checked = !!schedule.enabled;
    $("#schedule-interval").value = schedule.interval_hours ?? 6;
    $("#schedule-startup").checked = !!schedule.run_on_startup;
    $("#schedule-last-run").textContent = data.last_run ? formatTimestamp(data.last_run) : "-";
    $("#schedule-next-run").textContent = data.next_run ? formatTimestamp(data.next_run) : "-";
    setNotice($("#schedule-message"), "", false);
  } catch (err) {
    setNotice($("#schedule-message"), `Schedule error: ${err.message}`, true);
  }
}

async function saveSchedule() {
  const interval = parseInt($("#schedule-interval").value, 10);
  const payload = {
    enabled: $("#schedule-enabled").checked,
    mode: "interval",
    interval_hours: Number.isFinite(interval) ? interval : 1,
    run_on_startup: $("#schedule-startup").checked,
  };
  try {
    await fetchJson("/api/schedule", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setNotice($("#schedule-message"), "Schedule updated", false);
    await refreshSchedule();
  } catch (err) {
    setNotice($("#schedule-message"), `Schedule update failed: ${err.message}`, true);
  }
}

async function runScheduleNow() {
  try {
    setNotice($("#schedule-message"), "Starting run...", false);
    await fetchJson("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    setNotice($("#schedule-message"), "Run started", false);
    await refreshStatus();
  } catch (err) {
    setNotice($("#schedule-message"), `Run failed: ${err.message}`, true);
  }
}

async function refreshDownloads() {
  try {
    const search = ($("#downloads-search")?.value || "").trim().toLowerCase();
    const limitRaw = parseInt($("#downloads-limit")?.value, 10);
    const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? limitRaw : 50;
    const rows = await fetchJson("/api/files");
    const key = JSON.stringify({ rows, search, limit });
    if (key === state.lastFilesKey) {
      return;
    }
    state.lastFilesKey = key;
    const body = $("#downloads-body");
    body.textContent = "";
    const filtered = search
      ? rows.filter((row) => {
        const hay = `${row.relative_path || ""} ${row.name || ""}`.toLowerCase();
        return hay.includes(search);
      })
      : rows;
    const sliced = filtered.slice(0, limit);
    if (!sliced.length) {
      const tr = document.createElement("tr");
      const label = search ? "No downloads match this filter." : "No downloads found.";
      tr.innerHTML = `<td colspan="4">${label}</td>`;
      body.appendChild(tr);
      return;
    }
    sliced.forEach((row) => {
      const tr = document.createElement("tr");
      const downloadHref = downloadUrl(row.id);
      const copyUrl = encodeURIComponent(downloadHref);
      tr.innerHTML = `
        <td>${row.relative_path || row.name || ""}</td>
        <td>${formatTimestamp(row.modified_at) || ""}</td>
        <td>${formatBytes(row.size_bytes)}</td>
        <td>
          <div class="action-group">
            <a class="button ghost small" href="${downloadHref}">⬇ Download</a>
            <button class="button ghost small" data-copy="url" data-value="${copyUrl}">Copy URL</button>
          </div>
        </td>
      `;
      body.appendChild(tr);
    });
  } catch (err) {
    const body = $("#downloads-body");
    body.textContent = "";
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="4">Failed to load downloads: ${err.message}</td>`;
    body.appendChild(tr);
  }
}



function getSearchSourcePriority() {
  const list = $("#search-source-list");
  if (!list) return [];
  return Array.from(list.querySelectorAll(".source-priority-row"))
    .map((row) => {
      const enabled = row.querySelector(".source-enabled");
      return enabled && enabled.checked ? row.dataset.source : null;
    })
    .filter(Boolean);
}

function updateSearchSortLabel() {
  const button = $("#search-requests-sort");
  if (!button) return;
  const label = state.searchRequestsSort === "asc" ? "Sort: Oldest" : "Sort: Newest";
  button.textContent = label;
}

function moveSourcePriorityRow(row, direction) {
  if (!row) return;
  const parent = row.parentElement;
  if (!parent) return;
  if (direction === "up") {
    const prev = row.previousElementSibling;
    if (prev) {
      parent.insertBefore(row, prev);
    }
  } else if (direction === "down") {
    const next = row.nextElementSibling;
    if (next) {
      parent.insertBefore(next, row);
    }
  }
}

function buildSearchRequestPayload(sources, { autoEnqueue = true } = {}) {
  const artist = $("#search-artist").value.trim();
  const album = $("#search-album").value.trim();
  const track = $("#search-track").value.trim();
  const intent = $("#search-intent").value || "track";
  const minScoreRaw = parseFloat($("#search-min-score").value);
  const maxCandidatesRaw = parseInt($("#search-max-candidates").value, 10);
  const destination = $("#search-destination").value.trim();

  return {
    intent,
    media_type: "music",
    artist,
    album: album || null,
    track: track || null,
    destination_dir: destination || null,
    include_albums: $("#search-include-albums").checked,
    include_singles: $("#search-include-singles").checked,
    lossless_only: $("#search-lossless-only").checked,
    auto_enqueue: autoEnqueue,
    min_match_score: Number.isFinite(minScoreRaw) ? minScoreRaw : 0.92,
    max_candidates_per_source: Number.isFinite(maxCandidatesRaw) ? maxCandidatesRaw : 5,
    source_priority: sources && sources.length ? sources : null,
  };
}



function getSearchDefaultDestination() {
  if (state.config && state.config.music_download_folder) {
    return state.config.music_download_folder;
  }
  if (state.config && state.config.single_download_folder) {
    return state.config.single_download_folder;
  }
  return "";
}

function updateSearchDestinationDisplay() {
  const el = $("#search-default-destination");
  if (!el) return;
  const value = getSearchDefaultDestination();
  el.textContent = value || "-";
  const resolvedEl = $("#search-destination-resolved");
  if (resolvedEl) {
    const customValue = $("#search-destination")?.value.trim();
    const resolved = computeResolvedDestinationPath(customValue);
    resolvedEl.textContent = `Resolved: ${resolved}`;
  }
}
function setSearchSelectedRequest(requestId) {
  state.searchSelectedRequestId = requestId || null;
  const label = $("#search-selected-request");
  if (label) {
    label.textContent = requestId || "-";
  }
}

function setSearchSelectedItem(itemId) {
  state.searchSelectedItemId = itemId || null;
  const label = $("#search-selected-item");
  if (label) {
    label.textContent = itemId || "-";
  }
}

function renderSearchEmptyRow(body, colspan, message) {
  if (!body) return;
  body.textContent = "";
  const tr = document.createElement("tr");
  tr.innerHTML = `<td colspan="${colspan}">${message}</td>`;
  body.appendChild(tr);
}

function getHomeSourcePriority() {
  const panel = $("#home-source-panel");
  if (!panel) return null;

  const checked = Array.from(
    panel.querySelectorAll("input[type=checkbox][data-source]:checked")
  ).map((el) => el.dataset.source);

  if (!checked.length) {
    return null;
  }

  // If all sources selected, treat as auto (null)
  const allSources = Array.from(
    panel.querySelectorAll("input[type=checkbox][data-source]")
  ).map((el) => el.dataset.source);

  if (checked.length === allSources.length) {
    return null;
  }

  return checked;
}

function parseHomeSearchQuery(value, preferAlbum) {
  const trimmed = (value || "").trim();
  if (!trimmed) {
    return null;
  }
  const isUrl = /^https?:\/\//i.test(trimmed);
  let artist = trimmed;
  let album = null;
  let track = trimmed;
  let intent = preferAlbum ? "album" : "track";
  const separators = [" - ", " / ", ":"];
  const separator = separators.find((sep) => trimmed.includes(sep));
  if (separator && !isUrl) {
    const [first, second] = trimmed.split(separator);
    artist = first.trim() || trimmed;
    const remainder = second.trim();
    if (preferAlbum) {
      intent = "album";
      album = remainder || trimmed;
      track = "";
    } else {
      intent = "track";
      track = remainder || trimmed;
    }
  } else if (isUrl) {
    intent = "track";
    artist = trimmed;
    track = trimmed;
  } else {
    if (preferAlbum) {
      intent = "album";
      album = trimmed;
      track = "";
    } else {
      intent = "track";
      track = trimmed;
    }
  }
  if (intent === "track" && !track) {
    track = artist || trimmed;
  }
  return {
    intent,
    artist: artist || trimmed,
    album: album || null,
    track: track || null,
  };
}

function buildHomeSearchPayload(autoEnqueue, rawQuery = "") {
  const preferAlbum = $("#home-prefer-albums")?.checked;
  const parsed = parseHomeSearchQuery($("#home-search-input")?.value, preferAlbum);
  if (!parsed) {
    throw new Error("Enter an artist, track, album, or playlist URL");
  }
  const minScoreRaw = parseFloat($("#home-min-score")?.value);
  const destination = $("#home-destination")?.value.trim();
  const treatAsMusic = $("#home-treat-music")?.checked ?? false;
  const rawText = rawQuery || $("#home-search-input")?.value || "";
  const selectedSource = $("#home-search-source")?.value;
  let sources = getHomeSourcePriority();
  if (!sources && selectedSource === "auto" && !treatAsMusic) {
    const preferVideo = shouldPreferVideoAdapters(rawText);
    sources = preferVideo ? HOME_VIDEO_SOURCE_PRIORITY : HOME_GENERIC_SOURCE_PRIORITY;
  }
  return {
    intent: parsed.intent,
    media_type: treatAsMusic ? "music" : "generic",
    artist: parsed.artist,
    album: parsed.album,
    track: parsed.track,
    destination_dir: destination || null,
    include_albums: 1,
    include_singles: preferAlbum ? 0 : 1,
    min_match_score: Number.isFinite(minScoreRaw) ? minScoreRaw : 0.92,
    lossless_only: treatAsMusic ? 1 : 0,
    auto_enqueue: autoEnqueue,
    source_priority: sources && sources.length ? sources : null,
    max_candidates_per_source: 5,
  };
}

function showHomeResults(visible) {
  const section = $("#home-results");
  if (!section) return;
  section.classList.toggle("hidden", !visible);
  section.classList.remove("has-results");
}

function updateHomeViewAdvancedLink() {
  const button = $("#home-view-advanced");
  if (!button) return;
  const enabled = !!state.homeSearchRequestId;
  button.disabled = !enabled;
  button.setAttribute("aria-disabled", (!enabled).toString());
}

function handleHomeViewAdvanced() {
  const requestId = state.homeSearchRequestId;
  if (!requestId) {
    return;
  }
  state.pendingAdvancedRequestId = requestId;
  window.location.hash = "#advanced";
}

function setHomeSearchControlsEnabled(enabled) {
  state.homeSearchControlsEnabled = enabled;
  ["#home-search-download", "#home-search-only"].forEach((selector) => {
    const el = $(selector);
    if (!el) return;
    el.disabled = !enabled;
    el.setAttribute("aria-disabled", (!enabled).toString());
  });
}

function maybeReleaseHomeSearchControls(requestId, requestStatus) {
  if (!requestId) return;
  if (state.homeSearchRequestId !== requestId) {
    return;
  }
  if (HOME_FINAL_STATUSES.has(requestStatus)) {
    setHomeSearchControlsEnabled(true);
  }
}

function stopHomeResultPolling() {
  if (state.homeResultsTimer) {
    clearInterval(state.homeResultsTimer);
    state.homeResultsTimer = null;
  }
  state.homeSearchPollStart = null;
}

function setHomeResultsStatus(text) {
  const statusEl = $("#home-results-status-text");
  if (statusEl) {
    statusEl.textContent = text;
  }
}

function setHomeResultsDetail(text, isError = false) {
  const detailEl = $("#home-results-detail");
  if (!detailEl) {
    return;
  }
  if (!text) {
    detailEl.textContent = "";
    detailEl.classList.remove("home-results-error");
    detailEl.classList.add("hidden");
    return;
  }
  detailEl.textContent = text;
  detailEl.classList.toggle("home-results-error", isError);
  detailEl.classList.remove("hidden");
}

function buildHomeResultsStatusInfo(requestId) {
  const context = state.homeRequestContext[requestId];
  if (!context) {
    return {
      text: "Searching sources…",
      detail: "Results appear as soon as each source responds.",
      isError: false
    };
  }
  const request = context.request || {};
  const items = context.items || [];
  const requestStatus = request.status || "pending";
  const adaptersTotal = Number.isFinite(request.adapters_total)
    ? request.adapters_total
    : null;
  const adaptersCompleted = Number.isFinite(request.adapters_completed)
    ? request.adapters_completed
    : null;
  const error = request.error || "";
  const minScore = Number.isFinite(request.min_match_score) ? request.min_match_score : null;
  const thresholdText = Number.isFinite(minScore) ? minScore.toFixed(2) : "0.00";
  const hasCandidates = items.some((item) => item.candidate_count > 0);
  const allowQueued = state.homeSearchMode !== "searchOnly";
  const hasQueued = allowQueued && items.some((item) => ["enqueued", "skipped"].includes(item.status));

  if (requestStatus === "failed" && error && error !== "no_items_enqueued") {
    return { text: "FAILED", detail: error, isError: true, status: requestStatus };
  }

  if (requestStatus === "failed" && error === "no_items_enqueued") {
    if (hasCandidates) {
      const detail = `Results found below the auto-download threshold. Use Advanced Search to enqueue a result.`;
      return { text: "Results found (below auto-download threshold)", detail, isError: false, status: requestStatus };
    }
    return {
      text: "No results",
      detail: "No candidates were returned for this search.",
      isError: false,
      status: requestStatus,
    };
  }

  if (!items.length) {
    const searchingStates = new Set(["pending", "resolving"]);
    if (searchingStates.has(requestStatus)) {
      if (adaptersTotal !== null && adaptersCompleted !== null) {
        return {
          text: `Searching sources (${adaptersCompleted}/${adaptersTotal})`,
          detail: "Results stream in as they are found.",
          isError: false,
          status: requestStatus,
        };
      } else {
        return {
          text: "Searching sources…",
          detail: "Results stream in as they are found.",
          isError: false,
          status: requestStatus,
        };
      }
    }
    const fallback = formatHomeRequestStatus(requestStatus);
    return { text: fallback, detail: "", isError: false, status: requestStatus };
  }

  if (hasQueued || requestStatus === "completed_with_skips") {
    const detail =
      hasQueued && requestStatus === "completed_with_skips"
        ? "Some matches were skipped because they already exist."
        : requestStatus === "completed_with_skips"
        ? "Matches were skipped, but the request resolved successfully."
        : "Items have been scheduled for download.";
    return {
      text: "Results queued",
      detail,
      isError: false,
      status: requestStatus,
    };
  }

  if (requestStatus === "resolving" && adaptersTotal !== null && adaptersCompleted !== null) {
    return {
      text: `Searching sources (${adaptersCompleted}/${adaptersTotal})`,
      detail: "More results may still appear.",
      isError: false,
      status: requestStatus,
    };
  }

  const detail =
    requestStatus === "completed_with_skips"
      ? "Some entries already existed in the queue."
      : "";
  return { text: formatHomeRequestStatus(requestStatus), detail, isError: false, status: requestStatus };
}

function updateHomeResultsStatusForRequest(requestId) {
  if (!requestId) {
    setHomeResultsStatus("Ready to discover media");
    setHomeResultsDetail("Search Only is the default discovery action; use Search & Download to enqueue jobs.", false);
    return;
  }
  const info = buildHomeResultsStatusInfo(requestId);
  setHomeResultsStatus(info.text);
  setHomeResultsDetail(info.detail, info.isError);
  maybeReleaseHomeSearchControls(requestId, info.status);
}

function recordHomeCandidateScore(requestId, score) {
  if (!requestId || !Number.isFinite(score)) {
    return;
  }
  const current = state.homeBestScores[requestId];
  if (!Number.isFinite(current) || score > current) {
    state.homeBestScores[requestId] = score;
    updateHomeResultsStatusForRequest(requestId);
  }
}

function formatHomeRequestStatus(status) {
  if (!status) return "Searching for media…";
  if (status === "resolving" || status === "pending") return "Searching for media…";
  if (status === "completed") return "Results found";
  if (status === "completed_with_skips") return "Results queued";
  if (status === "failed") return "Search failed";
  return status[0]?.toUpperCase() + status.slice(1);
}

function renderHomeStatusBadge(status) {
  const normalized = status || "queued";
  const label = HOME_STATUS_LABELS[normalized] || normalized;
  const className = HOME_STATUS_CLASS_MAP[normalized] || "searching";
  const badge = document.createElement("span");
  badge.className = `home-result-badge ${className} long`;
  badge.textContent = label;
  return badge;
}

const HOME_CANDIDATE_STATE_LABELS = {
  queued: "Queued",
  searching: "Searching",
  candidate_found: "Matched",
  selected: "Selected",
  enqueued: "Queued",
  failed: "Failed",
  skipped: "Already queued",
};
function getHomeCandidateStateInfo(status, { searchOnly = false } = {}) {
  const key = status || "queued";
  const label = HOME_CANDIDATE_STATE_LABELS[key] || key;
  const className = {
    queued: "queued",
    searching: "searching",
    candidate_found: "matched",
    selected: "matched",
    enqueued: "queued",
    failed: "failed",
    skipped: "skipped",
  }[key] || "queued";
  if (searchOnly && key === "enqueued") {
    return { label: "Matched", className: "matched" };
  }
  return { label, className };
}

function renderHomeResultItem(item) {
  const card = document.createElement("article");
  card.className = "home-result-card";
  if (item.id) {
    card.dataset.itemId = item.id;
  }
  const header = document.createElement("div");
  header.className = "home-result-header";
  const title = document.createElement("div");
  const summary = [item.artist, item.album, item.track].filter(Boolean).join(" / ") || "-";
  title.innerHTML = `<strong>${summary}</strong>`;
  header.appendChild(title);
  header.appendChild(renderHomeStatusBadge(item.status));
  card.appendChild(header);
  // Remove destination line for Home page result cards (visual polish)
  // No destination line
  const candidateList = document.createElement("div");
  candidateList.className = "home-candidate-list";
  candidateList.dataset.itemId = item.id || "";
  const placeholder = document.createElement("div");
  placeholder.className = "home-results-empty";
  placeholder.textContent = "Searching for candidates…";
  candidateList.appendChild(placeholder);
  card.appendChild(candidateList);
  return card;
}

function updateHomeResultItemCard(card, item) {
  if (!card || !item) {
    return;
  }
  const summary = [item.artist, item.album, item.track].filter(Boolean).join(" / ") || "-";
  const title = card.querySelector(".home-result-header strong");
  if (title) {
    title.textContent = summary;
  }
  const header = card.querySelector(".home-result-header");
  const existingBadge = header?.querySelector(".home-result-badge");
  const newBadge = renderHomeStatusBadge(item.status);
  if (existingBadge && existingBadge.parentElement) {
    existingBadge.replaceWith(newBadge);
  } else if (header) {
    header.appendChild(newBadge);
  }
  const detail = card.querySelector(".home-candidate-title");
  if (detail) {
    detail.textContent = `Source: ${item.media_type || "generic"} · ${item.position ? `Item ${item.position}` : ""}`.trim();
  }
  const resolvedDestination = state.homeRequestContext[item.request_id]?.request?.resolved_destination;
  let destinationEl = card.querySelector(".home-result-destination");
  if (resolvedDestination) {
    if (!destinationEl) {
      destinationEl = document.createElement("div");
      destinationEl.className = "home-result-destination";
      const candidateList = card.querySelector(".home-candidate-list");
      card.insertBefore(destinationEl, candidateList);
    }
    destinationEl.textContent = `Destination: ${resolvedDestination}`;
  } else if (destinationEl) {
    destinationEl.remove();
  }
  const candidateList = card.querySelector(".home-candidate-list");
  if (candidateList) {
    scheduleHomeCandidateRefresh(item, candidateList);
  }
}

function scheduleHomeCandidateRefresh(item, container) {
  if (!item || !item.id || !container || state.homeCandidatesLoading[item.id]) {
    return;
  }
  loadHomeCandidates(item, container);
}

async function loadHomeCandidates(item, container) {
  if (!item || !container) {
    return;
  }
  if (!item.id) {
    return;
  }
  if (state.homeCandidatesLoading[item.id]) {
    return;
  }
  state.homeCandidatesLoading[item.id] = true;
  try {
    let placeholder = container.querySelector(".home-results-empty");
    if (!placeholder) {
      placeholder = document.createElement("div");
      placeholder.className = "home-results-empty";
      container.appendChild(placeholder);
    }
    placeholder.textContent = "Fetching candidates…";
    const data = await fetchJson(`/api/search/items/${encodeURIComponent(item.id)}/candidates`);
    const candidates = data.candidates || [];
    if (!candidates.length) {
      placeholder.textContent = "Searching…";
      return;
    }
    if (placeholder.parentElement) {
      placeholder.remove();
    }
    const resultsSection = $("#home-results");
    if (resultsSection) {
      resultsSection.classList.add("has-results");
    }
    let rendered = state.homeCandidateCache[item.id];
    if (!rendered) {
      rendered = new Set();
    }
    const requestId = item.request_id || state.homeSearchRequestId;
    let bestScore = Number.NEGATIVE_INFINITY;
    candidates.forEach((candidate) => {
      if (rendered.has(candidate.id)) {
        return;
      }
      rendered.add(candidate.id);
      if (candidate.final_score !== undefined) {
        const score = Number(candidate.final_score);
        if (Number.isFinite(score) && score > bestScore) {
          bestScore = score;
        }
      }
      const row = renderHomeCandidateRow(candidate, item);
      container.appendChild(row);
    });
    state.homeCandidateCache[item.id] = rendered;
    if (Number.isFinite(bestScore)) {
      recordHomeCandidateScore(requestId, bestScore);
    }
  } catch (err) {
    container.textContent = "";
    const errorEl = document.createElement("div");
    errorEl.className = "home-results-empty";
    errorEl.textContent = `Failed to load candidates: ${err.message}`;
    container.appendChild(errorEl);
  } finally {
    state.homeCandidatesLoading[item.id] = false;
  }
}

function renderHomeCandidateRow(candidate, item) {
  const row = document.createElement("div");
  row.className = "home-candidate-row";

  const artworkUrl =
    candidate.thumbnail_url ||
    candidate.artwork_url ||
    null;
  const artwork = document.createElement("div");
  artwork.className = "home-candidate-artwork";
  if (artworkUrl) {
    const img = document.createElement("img");
    img.src = artworkUrl;
    img.alt = candidate.source || "";
    artwork.appendChild(img);
  }
  row.appendChild(artwork);

  const info = document.createElement("div");
  info.className = "home-candidate-info";
  const sourceLabelMap = {
    youtube: "YouTube",
    youtube_music: "YouTube Music",
    soundcloud: "SoundCloud",
    bandcamp: "Bandcamp",
  };
  const sourceKey = (candidate.source || "").toLowerCase();
  const sourceLabel = sourceLabelMap[sourceKey] || candidate.source || "Unknown";
  const title = candidate.title || candidate.source || "-";
  const detail = [candidate.artist_detected, candidate.album_detected, candidate.track_detected]
    .filter(Boolean)
    .join(" / ");
  const titleEl = document.createElement("div");
  titleEl.className = "home-candidate-title";
  titleEl.textContent = title;
  info.appendChild(titleEl);
  if (detail) {
    const metaEl = document.createElement("div");
    metaEl.className = "home-candidate-meta";
    metaEl.textContent = detail;
    info.appendChild(metaEl);
  }
  const sourceEl = document.createElement("span");
  sourceEl.className = "home-candidate-source-tag";
  sourceEl.textContent = sourceLabel;
  info.appendChild(sourceEl);
  row.appendChild(info);

  const action = document.createElement("div");
  action.className = "home-candidate-action";
  const allowDownload = (candidate.allow_download ?? item.allow_download);
  const canDownload = allowDownload !== false;
  const isSearchOnly = state.homeSearchMode === "searchOnly";
  const isSearchResult = Boolean(item.request_id);
  if (isSearchOnly && isSearchResult) {
    const button = document.createElement("button");
    button.className = "button ghost small";
    button.dataset.action = "home-download";
    button.dataset.itemId = item.id || "";
    button.dataset.candidateId = candidate.id || "";
    button.textContent = "Download";

    const disabled =
      !canDownload || !candidate.url || !item.id || !candidate.id;

    button.disabled = disabled;

    if (disabled) {
      button.title = "Not downloadable";
      button.setAttribute("aria-disabled", "true");
    }

    action.appendChild(button);
  } else if (isSearchOnly && canDownload) {
    const jobStatus = candidate.job_status || "";
    const queued = ["queued", "claimed", "downloading", "postprocessing", "completed"].includes(jobStatus);
    const button = document.createElement("button");
    button.className = "button ghost small";
    button.dataset.action = "home-direct-download";
    button.dataset.directUrl = candidate.url || "";
    button.textContent = queued ? "Queued" : "Download";
    button.disabled = queued || !candidate.url;
    action.appendChild(button);
  } else if (isSearchOnly) {
    const button = document.createElement("button");
    button.className = "button ghost small";
    button.textContent = "Search only";
    button.disabled = true;
    action.appendChild(button);
  } else {
    const label = document.createElement("span");
    label.textContent = "Auto";
    label.className = "meta";
    action.appendChild(label);
  }
  if (candidate.url) {
    const openLink = document.createElement("a");
    openLink.className = "button ghost small home-candidate-open";
    openLink.textContent = "Open source";
    openLink.href = candidate.url;
    openLink.target = "_blank";
    openLink.rel = "noopener noreferrer";
    action.appendChild(openLink);
  }
  row.appendChild(action);

  return row;
}

function renderHomeDirectUrlCard(preview, status) {
  const card = document.createElement("article");
  card.className = "home-result-card";
  card.dataset.directUrl = preview.url || "";
  const header = document.createElement("div");
  header.className = "home-result-header";
  const title = document.createElement("div");
  const summary = preview.title || preview.url || "Direct URL";
  title.innerHTML = `<strong>${summary}</strong>`;
  header.appendChild(title);
  header.appendChild(renderHomeStatusBadge(status));
  card.appendChild(header);
  const detail = document.createElement("div");
  detail.className = "home-candidate-title";
  detail.textContent = `Source: ${preview.source || "direct"}${preview.uploader ? ` · ${preview.uploader}` : ""}`;
  card.appendChild(detail);
  const candidateList = document.createElement("div");
  candidateList.className = "home-candidate-list";
  // Compose candidate and item
  const candidate = {
    id: "direct-url-candidate",
    title: preview.title,
    artist_detected: preview.uploader,
    album_detected: null,
    track_detected: null,
    final_score: null,
    source: preview.source || "direct",
    url: preview.url,
    thumbnail_url: preview.thumbnail_url,
    allow_download: preview.allow_download !== false,
    job_status: preview.job_status || "",
  };
  const item = {
    id: "direct-url-item",
    status,
    allow_download: preview.allow_download !== false,
  };
  // Insert candidate row, with Cancel button if needed
  const row = renderHomeCandidateRow(candidate, item);
  // Add Cancel button for active direct jobs
  if (
    ["queued", "claimed", "downloading", "postprocessing"].includes(status)
  ) {
    const cancelBtn = document.createElement("button");
    cancelBtn.className = "button ghost small";
    cancelBtn.textContent = "Cancel";
    cancelBtn.onclick = async () => {
      try {
        cancelBtn.disabled = true;
        await cancelJob(state.homeDirectJob?.runId || state.homeDirectJob?.jobId);
        stopHomeDirectJobPolling();
        state.homeDirectPreview.job_status = "cancelled";
        const container = $("#home-results-list");
        if (container) {
          container.textContent = "";
          container.appendChild(renderHomeDirectUrlCard(state.homeDirectPreview, "cancelled"));
        }
        setHomeResultsStatus("Cancelled");
        setHomeResultsDetail("Download cancelled by user", false);
        setHomeSearchControlsEnabled(true);
      } catch (err) {
        setHomeResultsDetail(`Cancel failed: ${err.message}`, true);
      }
    };
    row.querySelector(".home-candidate-action")?.appendChild(cancelBtn);
  }
  candidateList.appendChild(row);
  card.appendChild(candidateList);
  return card;
}

function showHomeDirectUrlError(url, message, messageEl) {
  const text = message || DIRECT_URL_PLAYLIST_ERROR;
  if (messageEl) {
    setNotice(messageEl, text, true);
  }
  stopHomeDirectJobPolling();
  stopHomeResultPolling();
  state.homeSearchMode = "searchOnly";
  state.homeDirectJob = null;
  state.homeDirectPreview = {
    title: url,
    url,
    source: "direct",
    uploader: null,
    thumbnail_url: null,
    allow_download: false,
    job_status: "failed",
  };
  state.homeSearchRequestId = null;
  state.homeRequestContext = {};
  state.homeBestScores = {};
  state.homeCandidateCache = {};
  state.homeCandidatesLoading = {};
  showHomeResults(true);
  setHomeResultsStatus("Failed");
  setHomeResultsDetail(text, true);
  const container = $("#home-results-list");
  if (container) {
    container.textContent = "";
    container.appendChild(renderHomeDirectUrlCard(state.homeDirectPreview, "failed"));
  }
  const resultsSection = $("#home-results");
  if (resultsSection) {
    resultsSection.classList.add("has-results");
  }
}

function showHomeDirectUrlPreview(preview) {
  const container = $("#home-results-list");
  if (!container) return;
  state.homeSearchMode = "searchOnly";
  stopHomeDirectJobPolling();
  state.homeDirectPreview = preview;
  state.homeSearchRequestId = null;
  state.homeRequestContext = {};
  state.homeBestScores = {};
  state.homeCandidateCache = {};
  state.homeCandidatesLoading = {};
  showHomeResults(true);
  setHomeResultsStatus("Direct URL preview");
  setHomeResultsDetail("Review the metadata and click Download to enqueue.", false);
  stopHomeResultPolling();
  container.textContent = "";
  const card = renderHomeDirectUrlCard(preview, "candidate_found");
  container.appendChild(card);
  const resultsSection = $("#home-results");
  if (resultsSection) {
    resultsSection.classList.add("has-results");
  }
}

function stopHomeDirectJobPolling() {
  if (state.homeDirectJobTimer) {
    clearInterval(state.homeDirectJobTimer);
    state.homeDirectJobTimer = null;
  }
}

async function refreshHomeDirectJobStatus() {
  if (!state.homeDirectJob) {
    return;
  }
  const container = $("#home-results-list");
  if (!container) return;
  try {
    const data = await fetchJson("/api/download_jobs?limit=50");
    const jobs = data.jobs || [];
    const job = jobs.find((entry) => {
      if (state.homeDirectJob.playlistId) {
        return entry.origin === "playlist" && entry.origin_id === state.homeDirectJob.playlistId;
      }
      return entry.url === state.homeDirectJob.url;
    });
    if (job) {
      state.homeDirectJob.status = job.status;
      state.homeDirectPreview = {
        ...state.homeDirectPreview,
        job_status: job.status,
      };
      container.textContent = "";
      const card = renderHomeDirectUrlCard(state.homeDirectPreview, job.status);
      container.appendChild(card);
      setHomeResultsStatus(formatDirectJobStatus(job.status));
      setHomeResultsDetail(job.last_error || "", Boolean(job.last_error));
      if (["completed", "failed"].includes(job.status)) {
        stopHomeDirectJobPolling();
        setHomeSearchControlsEnabled(true);
      }
      return;
    }
    if (!state.homeDirectJob.runId) {
      return;
    }
    const runData = await fetchJson(`/api/status?run_id=${encodeURIComponent(state.homeDirectJob.runId)}`);
    if (runData.run_id !== state.homeDirectJob.runId) {
      return;
    }
    let runStatus = "queued";
    let runError = "";
    if (runData.state === "error" || runData.error) {
      runStatus = "failed";
      runError = runData.error || "";
    } else if (runData.state === "completed") {
      runStatus = "completed";
    } else if (runData.running || runData.state === "running") {
      runStatus = "downloading";
    } else {
      runStatus = "completed";
    }
    state.homeDirectJob.status = runStatus;
    state.homeDirectPreview = {
      ...state.homeDirectPreview,
      job_status: runStatus,
    };
    container.textContent = "";
    const card = renderHomeDirectUrlCard(state.homeDirectPreview, runStatus);
    container.appendChild(card);
    setHomeResultsStatus(formatDirectJobStatus(runStatus));
    setHomeResultsDetail(runError || "", Boolean(runError));
    if (["completed", "failed"].includes(runStatus)) {
      stopHomeDirectJobPolling();
      setHomeSearchControlsEnabled(true);
    }
  } catch (err) {
    setHomeResultsStatus("Direct URL status error");
    setHomeResultsDetail(`Failed to load job status: ${err.message}`, true);
    stopHomeDirectJobPolling();
    setHomeSearchControlsEnabled(true);
  }
}

function startHomeDirectJobPolling() {
  stopHomeDirectJobPolling();
  const tick = async () => {
    await refreshHomeDirectJobStatus();
  };
  state.homeDirectJobTimer = setInterval(tick, 4000);
  tick();
}

function formatDirectJobStatus(status) {
  if (status === "cancelled") return "Cancelled";
  if (!status) return "Queued";
  if (status === "claimed" || status === "downloading" || status === "postprocessing") {
    return "Downloading";
  }
  if (status === "queued") return "Queued";
  if (status === "completed") return "Completed";
  if (status === "failed") return "Failed";
  return status[0]?.toUpperCase() + status.slice(1);
}

async function refreshHomeResults(requestId) {
  const container = $("#home-results-list");
  if (!container) return null;
  try {
    const data = await fetchJson(`/api/search/requests/${encodeURIComponent(requestId)}`);
    const requestStatus = data.request?.status || "queued";
    const items = data.items || [];
    state.homeRequestContext[requestId] = {
      request: data.request || {},
      items,
    };
    updateHomeResultsStatusForRequest(requestId);
    const existingCards = new Map();
    container.querySelectorAll(".home-result-card").forEach((card) => {
      const itemId = card.dataset.itemId;
      if (itemId) {
        existingCards.set(itemId, card);
      }
    });
    if (!items.length) {
      container.textContent = "";
      const placeholder = document.createElement("div");
      placeholder.className = "home-results-empty";
      placeholder.textContent = "Searching sources…";
      container.appendChild(placeholder);
      const resultsSection = $("#home-results");
      const terminal = ["completed", "completed_with_skips", "failed"].includes(requestStatus);
      if (resultsSection) {
        resultsSection.classList.toggle("has-results", terminal);
      }
      return requestStatus;
    }
    const currentIds = new Set();
    items.forEach((item) => {
      if (!item.id) {
        return;
      }
      currentIds.add(item.id);
      let card = existingCards.get(item.id);
      if (card) {
        existingCards.delete(item.id);
        updateHomeResultItemCard(card, item);
      } else {
        card = renderHomeResultItem(item);
        container.appendChild(card);
      }
      const candidateList = card.querySelector(".home-candidate-list");
      if (candidateList) {
        scheduleHomeCandidateRefresh(item, candidateList);
      }
    });
    existingCards.forEach((card) => {
      card.remove();
    });
    const hasResults = items.some((entry) => entry.candidate_count > 0);
    const terminal = ["completed", "completed_with_skips", "failed"].includes(requestStatus);
    const resultsSection = $("#home-results");
    if (resultsSection) {
      resultsSection.classList.toggle("has-results", hasResults || terminal);
    }
    Object.keys(state.homeCandidateCache).forEach((key) => {
      if (!currentIds.has(key)) {
        delete state.homeCandidateCache[key];
      }
    });
    guardHomeSearchNoCandidates(requestId, requestStatus, items);
    return requestStatus;
  } catch (err) {
    container.textContent = "";
    const errorEl = document.createElement("div");
    errorEl.className = "home-results-empty";
    errorEl.textContent = `Failed to refresh results: ${err.message}`;
    container.appendChild(errorEl);
    const resultsSection = $("#home-results");
    if (resultsSection) {
      resultsSection.classList.add("has-results");
    }
    setHomeResultsStatus("FAILED");
    setHomeResultsDetail(`Failed to refresh results: ${err.message}`, true);
    setHomeSearchControlsEnabled(true);
    stopHomeResultPolling();
    return null;
  }
}

function guardHomeSearchNoCandidates(requestId, requestStatus, items) {
  if (!requestId || !items.length) {
    return;
  }
  const watchingStates = new Set(["pending", "resolving"]);
  const hasCandidates = items.some((item) => item.candidate_count > 0);
  if (watchingStates.has(requestStatus) && !hasCandidates) {
    const streak = (state.homeNoCandidateStreaks[requestId] || 0) + 1;
    state.homeNoCandidateStreaks[requestId] = streak;
    if (streak >= 3) {
      stopHomeResultPolling();
      setHomeResultsStatus("No candidates returned");
      setHomeResultsDetail(
        "Adapters are not returning results for this query. Try refining the search or use Advanced Search.",
        true
      );
      setHomeSearchControlsEnabled(true);
    }
    return;
  }
  state.homeNoCandidateStreaks[requestId] = 0;
}

function abortHomeResultPolling(message) {
  stopHomeResultPolling();
  setHomeResultsStatus("Search timed out");
  setHomeResultsDetail(message || "Adapters took too long; try again or use Advanced Search.", true);
  setHomeSearchControlsEnabled(true);
}

function startHomeResultPolling(requestId) {
  stopHomeResultPolling();
  showHomeResults(true);
  state.homeSearchPollStart = Date.now();
  const tick = async () => {
    const status = await refreshHomeResults(requestId);
    if (status === null) {
      stopHomeResultPolling();
      return;
    }
    const elapsed = state.homeSearchPollStart ? Date.now() - state.homeSearchPollStart : 0;
    if (elapsed >= HOME_RESULT_TIMEOUT_MS) {
      abortHomeResultPolling("No adapters responded in time. Please retry or use Advanced Search.");
      return;
    }
    if (status && ["completed", "completed_with_skips", "failed"].includes(status)) {
      stopHomeResultPolling();
    }
  };
  state.homeResultsTimer = setInterval(tick, 4000);
  tick();
}

async function submitHomeSearch(autoEnqueue) {
  const messageEl = $("#home-search-message");
  const inputValue = $("#home-search-input")?.value.trim() || "";
  const destinationValue = $("#home-destination")?.value.trim() || "";
  if (!state.homeSearchControlsEnabled) {
    return;
  }
  setHomeSearchControlsEnabled(false);
  stopHomeDirectJobPolling();
  state.homeDirectJob = null;
  state.homeDirectPreview = null;
  if (hasInvalidDestinationValue(destinationValue)) {
    setNotice(messageEl, "Destination path is invalid; please select a folder within downloads.", true);
    setHomeSearchControlsEnabled(true);
    return;
  }
  try {
    if (isValidHttpUrl(inputValue)) {
      if (!autoEnqueue) {
        await handleHomeDirectUrlPreview(inputValue, destinationValue, messageEl);
        return;
      }
      await handleHomeDirectUrl(inputValue, destinationValue, messageEl);
      return;
    }
    const payload = buildHomeSearchPayload(autoEnqueue, inputValue);
    const modeLabel = autoEnqueue ? "Search & Download" : "Search Only";
    setNotice(messageEl, `${modeLabel}: creating request...`, false);
    const data = await fetchJson("/api/search/requests", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    state.homeRequestContext = {};
    state.homeBestScores = {};
    state.homeCandidateCache = {};
    state.homeCandidatesLoading = {};
    state.homeSearchRequestId = data.request_id;
    state.homeSearchMode = autoEnqueue ? "download" : "searchOnly";
    updateHomeViewAdvancedLink();
    setNotice(messageEl, `${modeLabel}: created ${data.request_id}`, false);
    showHomeResults(true);
    startHomeResultPolling(data.request_id);
    await runSearchResolutionOnce({ preferRequestId: data.request_id, showMessage: false });
  } catch (err) {
    setHomeSearchControlsEnabled(true);
    setNotice(messageEl, `Search failed: ${err.message}`, true);
  }
}

async function handleHomeDirectUrl(url, destination, messageEl) {
  if (!messageEl) return;
  const formatOverride = $("#home-format")?.value.trim();
  const treatAsMusic = $("#home-treat-music")?.checked ?? false;
  const playlistId = extractPlaylistIdFromUrl(url);
  if (playlistId) {
    showHomeDirectUrlError(url, DIRECT_URL_PLAYLIST_ERROR, messageEl);
    return;
  }
  const payload = {};
  payload.single_url = url;
  if (destination) {
    payload.destination = destination;
  }
  if (formatOverride) {
    payload.final_format_override = formatOverride;
  }
  payload.music_mode = treatAsMusic;
  setNotice(messageEl, "Direct URL download requested...", false);
  try {
    const runInfo = await startRun(payload);
    state.homeSearchMode = "download";
    state.homeDirectJob = {
      url,
      playlistId: playlistId || null,
      startedAt: new Date().toISOString(),
      runId: runInfo?.run_id || null,
      status: "queued",
    };
    state.homeDirectPreview = {
      title: url,
      url,
      source: playlistId ? "playlist" : "direct",
      uploader: null,
      thumbnail_url: null,
      job_status: "queued",
    };
    showHomeResults(true);
    setHomeResultsStatus("Queued");
    setHomeResultsDetail("Direct URL download queued.", false);
    const container = $("#home-results-list");
    if (container) {
      container.textContent = "";
      container.appendChild(renderHomeDirectUrlCard(state.homeDirectPreview, "enqueued"));
    }
    startHomeDirectJobPolling();
    setNotice(messageEl, "Direct URL download started", false);
  } catch (err) {
    setNotice(messageEl, `Direct download failed: ${err.message}`, true);
  } finally {
    setHomeSearchControlsEnabled(true);
  }
}

async function handleHomeDirectUrlPreview(url, destination, messageEl) {
  if (!messageEl) return;
  const playlistId = extractPlaylistIdFromUrl(url);
  if (playlistId) {
    showHomeDirectUrlError(url, DIRECT_URL_PLAYLIST_ERROR, messageEl);
    return;
  }
  setNotice(messageEl, "Fetching URL metadata...", false);
  try {
    const data = await fetchJson("/api/direct_url_preview", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url }),
    });
    const preview = data.preview || {};
    preview.url = preview.url || url;
    showHomeDirectUrlPreview(preview);
    setNotice(messageEl, "Direct URL preview ready", false);
  } catch (err) {
    setNotice(messageEl, `Preview failed: ${err.message}`, true);
  } finally {
    setHomeSearchControlsEnabled(true);
  }
}

async function createSearchRequest(autoEnqueue = true) {
  const messageEl = $("#search-create-message");
  const destinationValue = $("#search-destination")?.value.trim() || "";
  if (hasInvalidDestinationValue(destinationValue)) {
    setNotice(messageEl, "Destination path is invalid; please select a folder within downloads.", true);
    return;
  }
  try {
    const sources = getSearchSourcePriority();
    if (!sources.length) {
      setNotice(messageEl, "Select at least one source", true);
      return;
    }
    const payload = buildSearchRequestPayload(sources, { autoEnqueue });
    const modeLabel = autoEnqueue ? "Search & Download" : "Search Only";
    setNotice(messageEl, `${modeLabel}: creating request...`, false);
    const data = await fetchJson("/api/search/requests", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setNotice(messageEl, `${modeLabel}: created ${data.request_id}`, false);
    await runSearchResolutionOnce({ preferRequestId: data.request_id, showMessage: false });
  } catch (err) {
    setNotice(messageEl, `Create failed: ${err.message}`, true);
  }
}


async function runSearchResolutionOnce({ preferRequestId = null, showMessage = true } = {}) {
  const messageEl = $("#search-requests-message");
  try {
    if (showMessage) {
      setNotice(messageEl, "Running resolution...", false);
    }
    const data = await fetchJson("/api/search/resolve/once", { method: "POST" });
    const resolvedId = data.request_id || preferRequestId || null;
    if (showMessage) {
      if (data.request_id) {
        setNotice(messageEl, `Resolved request ${data.request_id}`, false);
      } else {
        setNotice(messageEl, "No queued requests", false);
      }
    } else if (messageEl) {
      messageEl.textContent = "";
    }
    await refreshSearchRequests(resolvedId);
    if (resolvedId) {
      await refreshSearchRequestDetails(resolvedId);
    }
    await refreshSearchQueue();
  } catch (err) {
    if (showMessage) {
      setNotice(messageEl, `Resolution failed: ${err.message}`, true);
    }
  }
}


async function enqueueSearchCandidate(itemId, candidateId, options = {}) {
  if (!itemId || !candidateId) return;
  const messageEl = options.messageEl || $("#search-requests-message");
  try {
    setNotice(messageEl, `Enqueuing candidate ${candidateId}...`, false);
    const data = await fetchJson(`/api/search/items/${encodeURIComponent(itemId)}/enqueue`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ candidate_id: candidateId }),
    });
    if (data.created) {
      setNotice(messageEl, `Enqueued job ${data.job_id}`, false);
    } else {
      setNotice(messageEl, "Job already queued", false);
    }
    await refreshSearchRequestDetails(state.searchSelectedRequestId);
    await refreshSearchQueue();
  } catch (err) {
    setNotice(messageEl, `Enqueue failed: ${err.message}`, true);
  }
}
async function cancelSearchRequest(requestId) {
  if (!requestId) return;
  const messageEl = $("#search-requests-message");
  try {
    setNotice(messageEl, `Canceling request ${requestId}...`, false);
    await fetchJson(`/api/search/requests/${encodeURIComponent(requestId)}/cancel`, { method: "POST" });
    setNotice(messageEl, `Canceled request ${requestId}`, false);
    await refreshSearchRequests(requestId);
    if (state.searchSelectedRequestId === requestId) {
      await refreshSearchRequestDetails(requestId);
    }
  } catch (err) {
    setNotice(messageEl, `Cancel failed: ${err.message}`, true);
  }
}

async function refreshSearchRequests(preferRequestId = null) {
  const body = $("#search-requests-body");
  const messageEl = $("#search-requests-message");
  try {
    updateSearchSortLabel();
    const data = await fetchJson("/api/search/requests");
    const requests = (data.requests || []).slice();
    const sortDir = state.searchRequestsSort === "asc" ? "asc" : "desc";
    requests.sort((a, b) => {
      const aTime = Date.parse(a.created_at || "");
      const bTime = Date.parse(b.created_at || "");
      if (Number.isNaN(aTime) || Number.isNaN(bTime)) {
        const aKey = String(a.created_at || "");
        const bKey = String(b.created_at || "");
        return sortDir === "desc" ? bKey.localeCompare(aKey) : aKey.localeCompare(bKey);
      }
      return sortDir === "desc" ? bTime - aTime : aTime - bTime;
    });

    body.textContent = "";
    if (!requests.length) {
      renderSearchEmptyRow(body, 7, "No search requests found.");
      setSearchSelectedRequest(null);
      setSearchSelectedItem(null);
      renderSearchEmptyRow($("#search-items-body"), 6, "Select a request to view items.");
      renderSearchEmptyRow($("#search-candidates-body"), 8, "Select an item to view candidates.");
      if (messageEl) messageEl.textContent = "";
      return;
    }

    let selectedRequestId = preferRequestId || state.searchSelectedRequestId;
    if (selectedRequestId && !requests.some((req) => req.id === selectedRequestId)) {
      selectedRequestId = null;
    }
    setSearchSelectedRequest(selectedRequestId);

    requests.forEach((req) => {
      const tr = document.createElement("tr");
      tr.dataset.requestId = req.id || "";
      if (req.id && req.id === selectedRequestId) {
        tr.classList.add("selected");
      }
      const summary = [req.artist, req.album, req.track].filter(Boolean).join(" / ") || "-";
      const status = req.status || "";
      const cancelDisabled = ["completed", "completed_with_skips", "failed"].includes(status);
      tr.innerHTML = `
        <td>${req.id || ""}</td>
        <td>${summary}</td>
        <td>${req.intent || ""}</td>
        <td>${status}</td>
        <td>${formatTimestamp(req.created_at) || ""}</td>
        <td>${req.error || ""}</td>
        <td>
          <div class="action-group">
            <button class="button ghost small" data-action="cancel" data-request-id="${req.id}" ${cancelDisabled ? "disabled" : ""}>Cancel</button>
          </div>
        </td>
      `;
      body.appendChild(tr);
    });

    if (!selectedRequestId) {
      setSearchSelectedItem(null);
      renderSearchEmptyRow($("#search-items-body"), 6, "Select a request to view items.");
      renderSearchEmptyRow($("#search-candidates-body"), 8, "Select an item to view candidates.");
    }
    if (messageEl) messageEl.textContent = "";
  } catch (err) {
    renderSearchEmptyRow(body, 7, `Failed to load requests: ${err.message}`);
    setNotice(messageEl, `Failed to load requests: ${err.message}`, true);
  }
}

async function refreshSearchRequestDetails(requestId) {
  const itemsBody = $("#search-items-body");
  const candidatesBody = $("#search-candidates-body");
  if (!requestId) {
    renderSearchEmptyRow(itemsBody, 6, "Select a request to view items.");
    renderSearchEmptyRow(candidatesBody, 8, "Select an item to view candidates.");
    setSearchSelectedItem(null);
    updateSearchDestinationDisplay();
    return;
  }
  try {
    const data = await fetchJson(`/api/search/requests/${encodeURIComponent(requestId)}`);
    const destEl = $("#search-default-destination");
    if (destEl) {
      destEl.textContent =
        data.request?.resolved_destination || getSearchDefaultDestination() || "-";
    }
    const items = data.items || [];
    const requestStatus = data.request ? data.request.status : null;
    itemsBody.textContent = "";

    if (!items.length) {
      renderSearchEmptyRow(itemsBody, 6, "No items found for this request.");
      renderSearchEmptyRow(candidatesBody, 8, "Select an item to view candidates.");
      setSearchSelectedItem(null);
      return;
    }

    let selectedItemId = state.searchSelectedItemId;
    if (selectedItemId && !items.some((item) => item.id === selectedItemId)) {
      selectedItemId = null;
    }
    if (!selectedItemId && items.length && ["completed", "failed"].includes(requestStatus)) {
      selectedItemId = items[0].id;
    }
    setSearchSelectedItem(selectedItemId);

    items.forEach((item) => {
      const tr = document.createElement("tr");
      tr.dataset.itemId = item.id || "";
      if (item.id && item.id === selectedItemId) {
        tr.classList.add("selected");
      }
      const summary = [item.artist, item.album, item.track].filter(Boolean).join(" / ") || "-";
      const score = Number.isFinite(item.chosen_score) ? item.chosen_score.toFixed(3) : "";
      tr.innerHTML = `
        <td>${item.id || ""}</td>
        <td>${item.position ?? ""}</td>
        <td>${summary}</td>
        <td>${item.status || ""}</td>
        <td>${item.chosen_source || ""}</td>
        <td>${score}</td>
      `;
      itemsBody.appendChild(tr);
    });

    if (selectedItemId) {
      await refreshSearchCandidates(selectedItemId);
    } else {
      renderSearchEmptyRow(candidatesBody, 8, "Select an item to view candidates.");
    }
  } catch (err) {
    renderSearchEmptyRow(itemsBody, 6, `Failed to load items: ${err.message}`);
    renderSearchEmptyRow(candidatesBody, 8, "Select an item to view candidates.");
    setSearchSelectedItem(null);
  }
}

async function refreshSearchCandidates(itemId) {
  const body = $("#search-candidates-body");
  if (!itemId) {
    renderSearchEmptyRow(body, 8, "Select an item to view candidates.");
    return;
  }
  try {
    const data = await fetchJson(`/api/search/items/${encodeURIComponent(itemId)}/candidates`);
    const candidates = data.candidates || [];
    body.textContent = "";

    if (!candidates.length) {
      renderSearchEmptyRow(body, 8, "No candidates found for this item.");
      return;
    }

    candidates.forEach((candidate) => {
      const tr = document.createElement("tr");
      const canonical = candidate.canonical_metadata || {};
      const canonicalTitle = canonical.track || canonical.album || "";
      const canonicalDetailParts = [canonical.artist, canonical.album].filter(Boolean);
      const canonicalDetail = canonicalDetailParts.join(" / ");
      const canonicalType = canonical.album_type ? (canonicalDetail ? ` · ${canonical.album_type}` : canonical.album_type) : "";
      const canonicalYear = canonical.release_year ? ` (${canonical.release_year})` : "";
      const canonicalHtml = canonicalTitle
        ? `<div class="cell-title">${canonicalTitle}</div><div class="meta">${canonicalDetail}${canonicalType}${canonicalYear}</div>`
        : `<span class="meta">-</span>`;
      const artworkUrl = Array.isArray(canonical.artwork) && canonical.artwork.length
        ? canonical.artwork[0].url
        : "";
      const artworkHtml = artworkUrl
        ? `<img class="candidate-artwork" src="${artworkUrl}" alt="">`
        : `<span class="meta">-</span>`;
      const detected = [candidate.artist_detected, candidate.album_detected, candidate.track_detected]
        .filter(Boolean)
        .join(" / ");
      const sourceTitle = candidate.title || "";
      const sourceHtml = sourceTitle
        ? `<div class="cell-title">${sourceTitle}</div>${detected ? `<div class=\"meta\">${detected}</div>` : ""}`
        : `<span class="meta">-</span>`;
      const duration = Number.isFinite(candidate.duration_sec)
        ? formatDuration(candidate.duration_sec)
        : "";
      const score = Number.isFinite(candidate.final_score) ? candidate.final_score.toFixed(3) : "";
    const downloadDisabled = !candidate.id || !candidate.url;
    const sourceUrl = candidate.url ? candidate.url.replace(/"/g, "&quot;") : "";
    const openHtml = sourceUrl
      ? `<a class="button ghost small" href="${sourceUrl}" target="_blank" rel="noopener noreferrer">Open source</a>`
      : "";
    const actionHtml = `<div class="action-group">${openHtml}<button class="button ghost small" data-action="download" data-item-id="${itemId}" data-candidate-id="${candidate.id || ""}" ${downloadDisabled ? "disabled" : ""}>Download</button></div>`;
    tr.innerHTML = `
        <td>${candidate.source || ""}</td>
        <td>${canonicalHtml}</td>
        <td>${artworkHtml}</td>
        <td>${sourceHtml}</td>
        <td>${duration}</td>
        <td>${score}</td>
        <td>${candidate.rank ?? ""}</td>
        <td>${actionHtml}</td>
      `;
      body.appendChild(tr);
    });
  } catch (err) {
    renderSearchEmptyRow(body, 8, `Failed to load candidates: ${err.message}`);
  }
}

async function refreshSearchQueue() {
  const body = $("#search-queue-body");
  const messageEl = $("#search-queue-message");
  try {
    const limitRaw = parseInt($("#search-queue-limit").value, 10);
    const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? limitRaw : 100;
    const data = await fetchJson(`/api/download_jobs?limit=${limit}`);
    const jobs = data.jobs || [];

    body.textContent = "";
    if (!jobs.length) {
      renderSearchEmptyRow(body, 8, "No queued jobs found.");
      if (messageEl) messageEl.textContent = "";
      return;
    }

    jobs.forEach((job) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${job.id || ""}</td>
        <td>${job.origin || ""}</td>
        <td>${job.source || ""}</td>
        <td>${job.media_intent || ""}</td>
        <td>${job.status || ""}</td>
        <td>${job.attempts ?? ""}</td>
        <td>${formatTimestamp(job.created_at) || ""}</td>
        <td>${job.last_error || ""}</td>
      `;
      body.appendChild(tr);
    });
    if (messageEl) messageEl.textContent = "";
  } catch (err) {
    renderSearchEmptyRow(body, 8, `Failed to load queue: ${err.message}`);
    setNotice(messageEl, `Failed to load queue: ${err.message}`, true);
  }
}

async function cleanupTemp() {
  const ok = window.confirm("Clear temporary files? This does not affect completed downloads.");
  if (!ok) {
    return;
  }
  try {
    setNotice($("#downloads-message"), "Cleaning temp files...", false);
    const data = await fetchJson("/api/cleanup", { method: "POST" });
    const bytes = formatBytes(data.deleted_bytes || 0);
    setNotice($("#downloads-message"), `Removed ${data.deleted_files || 0} files (${bytes}).`, false);
  } catch (err) {
    setNotice($("#downloads-message"), `Cleanup failed: ${err.message}`, true);
  }
}

function addAccountRow(name = "", data = {}) {
  const row = document.createElement("div");
  row.className = "row account-row";
  row.dataset.original = JSON.stringify(data || {});
  row.innerHTML = `
    <input class="account-name" type="text" placeholder="name" value="${name}">
    <label class="field">
      <span>Client Secret</span>
      <div class="row tight">
        <input class="account-client" type="text" placeholder="tokens/client_secret.json" value="${data.client_secret || ""}">
        <button class="button ghost small browse-client" type="button">Browse</button>
      </div>
    </label>
    <label class="field">
      <span>Token</span>
      <div class="row tight">
        <input class="account-token" type="text" placeholder="tokens/token.json" value="${data.token || ""}">
        <button class="button ghost small browse-token" type="button">Browse</button>
      </div>
    </label>
    <div class="account-actions">
      <button class="button ghost small oauth-run" type="button">Run OAuth</button>
      <button class="button ghost remove">Remove</button>
    </div>
  `;
  row.querySelector(".remove").addEventListener("click", () => {
    if (!window.confirm("Remove this account?")) {
      return;
    }
    row.remove();
  });
  row.querySelector(".oauth-run").addEventListener("click", () => {
    startOauthForRow(row);
  });
  row.querySelector(".browse-client").addEventListener("click", () => {
    const input = row.querySelector(".account-client");
    openBrowser(input, "tokens", "file", ".json", resolveBrowseStart("tokens", input.value));
  });
  row.querySelector(".browse-token").addEventListener("click", () => {
    const input = row.querySelector(".account-token");
    openBrowser(input, "tokens", "file", ".json", resolveBrowseStart("tokens", input.value));
  });
  $("#accounts-list").appendChild(row);
}

function addPlaylistRow(entry = {}) {
  const folderValue = normalizeDownloadsRelative(entry.folder || entry.directory || "");
  const row = document.createElement("div");
  row.className = "row playlist-row";
  row.dataset.original = JSON.stringify(entry || {});
  row.innerHTML = `
    <input class="playlist-name" type="text" placeholder="name" value="${entry.name || ""}">
    <input class="playlist-id" type="text" placeholder="playlist id" value="${entry.playlist_id || entry.id || ""}">
    <input class="playlist-folder" type="text" placeholder="folder" value="${folderValue}">
    <button class="button ghost small browse-folder" type="button">Browse</button>
    <input class="playlist-account" type="text" placeholder="account" value="${entry.account || ""}">
    <select class="playlist-format">
      <option value="">(default)</option>
      <option value="webm">webm</option>
      <option value="mp4">mp4</option>
      <option value="mkv">mkv</option>
      <option value="mp3">mp3</option>
    </select>
    <label class="field inline">
      <span>Music mode</span>
      <input class="playlist-music" type="checkbox" ${entry.music_mode ? "checked" : ""}>
    </label>
    <label class="field inline">
      <span>Only download new videos (subscribe mode)</span>
      <input class="playlist-subscribe" type="checkbox" ${entry.mode === "subscribe" ? "checked" : ""}>
    </label>
    <label class="field inline">
      <span>Remove after</span>
      <input class="playlist-remove" type="checkbox" ${entry.remove_after_download ? "checked" : ""}>
    </label>
    <button class="button ghost remove">Remove</button>
  `;
  const separator = document.createElement("div");
  separator.className = "playlist-separator";
  row.appendChild(separator);
  row.querySelector(".remove").addEventListener("click", () => {
    if (!window.confirm("Remove this playlist?")) {
      return;
    }
    row.remove();
  });
  row.querySelector(".browse-folder").addEventListener("click", () => {
    const target = row.querySelector(".playlist-folder");
    openBrowser(target, "downloads", "dir", "", resolveBrowseStart("downloads", target.value));
  });
  row.querySelector(".playlist-format").value = entry.final_format || "";
  $("#playlists-list").appendChild(row);
}

function renderConfig(cfg) {
  state.suppressDirty = true;
  $("#cfg-upload-date-format").value = cfg.upload_date_format ?? "";
  $("#cfg-filename-template").value = cfg.filename_template ?? "";
  $("#cfg-final-format").value = cfg.final_format ?? "";
  $("#cfg-js-runtime").value = cfg.js_runtime ?? "";
  $("#cfg-single-download-folder").value = normalizeDownloadsRelative(cfg.single_download_folder ?? "");
  $("#cfg-music-template").value = cfg.music_filename_template ?? "";
  $("#cfg-yt-dlp-cookies").value = cfg.yt_dlp_cookies ?? "";
  const musicMetaDefaults = {
    enabled: true,
    confidence_threshold: 70,
    use_acoustid: false,
    acoustid_api_key: "",
    embed_artwork: true,
    allow_overwrite_tags: true,
    max_artwork_size_px: 1500,
    rate_limit_seconds: 1.5,
    dry_run: false,
  };
  const musicMeta = cfg.music_metadata || {};
  $("#cfg-music-meta-enabled").checked = typeof musicMeta.enabled === "boolean"
    ? musicMeta.enabled
    : musicMetaDefaults.enabled;
  $("#cfg-music-meta-threshold").value = Number.isFinite(musicMeta.confidence_threshold)
    ? musicMeta.confidence_threshold
    : musicMetaDefaults.confidence_threshold;
  $("#cfg-music-meta-acoustid").checked = typeof musicMeta.use_acoustid === "boolean"
    ? musicMeta.use_acoustid
    : musicMetaDefaults.use_acoustid;
  $("#cfg-music-meta-acoustid-key").value = musicMeta.acoustid_api_key ?? "";
  $("#cfg-music-meta-artwork").checked = typeof musicMeta.embed_artwork === "boolean"
    ? musicMeta.embed_artwork
    : musicMetaDefaults.embed_artwork;
  $("#cfg-music-meta-overwrite").checked = typeof musicMeta.allow_overwrite_tags === "boolean"
    ? musicMeta.allow_overwrite_tags
    : musicMetaDefaults.allow_overwrite_tags;
  $("#cfg-music-meta-artwork-size").value = Number.isFinite(musicMeta.max_artwork_size_px)
    ? musicMeta.max_artwork_size_px
    : musicMetaDefaults.max_artwork_size_px;
  $("#cfg-music-meta-rate").value = Number.isFinite(musicMeta.rate_limit_seconds)
    ? musicMeta.rate_limit_seconds
    : musicMetaDefaults.rate_limit_seconds;
  $("#cfg-music-meta-dry-run").checked = typeof musicMeta.dry_run === "boolean"
    ? musicMeta.dry_run
    : musicMetaDefaults.dry_run;
  const watcher = cfg.watcher || {};
  const watcherEnabled = typeof watcher.enabled === "boolean" ? watcher.enabled : true;
  const watcherToggle = $("#cfg-watcher-enabled");
  if (watcherToggle) {
    watcherToggle.checked = watcherEnabled;
  }

  const defaultPolicy = {
    min_interval_minutes: 5,
    max_interval_minutes: 360,
    idle_backoff_factor: 2,
    active_reset_minutes: 5,
    downtime: {
      enabled: false,
      start: "23:00",
      end: "09:00",
      timezone: "local",
    },
  };
  const policy = cfg.watch_policy || defaultPolicy;
  const minInterval = Number.isFinite(policy.min_interval_minutes)
    ? policy.min_interval_minutes
    : defaultPolicy.min_interval_minutes;
  const maxInterval = Number.isFinite(policy.max_interval_minutes)
    ? policy.max_interval_minutes
    : defaultPolicy.max_interval_minutes;
  $("#cfg-watcher-min-interval").value = minInterval;
  $("#cfg-watcher-max-interval").value = maxInterval;
  $("#cfg-watcher-idle-backoff").value = Number.isFinite(policy.idle_backoff_factor)
    ? policy.idle_backoff_factor
    : defaultPolicy.idle_backoff_factor;
  $("#cfg-watcher-active-reset").value = Number.isFinite(policy.active_reset_minutes)
    ? policy.active_reset_minutes
    : defaultPolicy.active_reset_minutes;
  const downtime = policy.downtime || defaultPolicy.downtime;
  $("#cfg-watcher-downtime-enabled").checked = !!downtime.enabled;
  $("#cfg-watcher-downtime-start").value = downtime.start || defaultPolicy.downtime.start;
  $("#cfg-watcher-downtime-end").value = downtime.end || defaultPolicy.downtime.end;
  $("#cfg-watcher-downtime-timezone").value = downtime.timezone || defaultPolicy.downtime.timezone;

  const telegram = cfg.telegram || {};
  $("#cfg-telegram-token").value = telegram.bot_token ?? "";
  $("#cfg-telegram-chat").value = telegram.chat_id ?? "";

  $("#accounts-list").textContent = "";
  const accounts = cfg.accounts || {};
  Object.keys(accounts).forEach((name) => addAccountRow(name, accounts[name] || {}));

  $("#playlists-list").textContent = "";
  const playlists = cfg.playlists || [];
  playlists.forEach((entry) => addPlaylistRow(entry));
  renderSpotifyPlaylists(cfg);
  refreshSpotifyPlaylistStatus();

  const opts = cfg.yt_dlp_opts || {};
  $("#cfg-yt-dlp-opts").value = Object.keys(opts).length ? JSON.stringify(opts, null, 2) : "";
  state.suppressDirty = false;
}

function renderSpotifyPlaylists(cfg) {
  const container = $("#spotify-playlists-list");
  if (!container) return;
  container.textContent = "";
  const entries = cfg.spotify_playlists || [];
  if (!entries.length) {
    const note = document.createElement("div");
    note.className = "notice";
    note.textContent = "No Spotify playlists configured.";
    container.appendChild(note);
    return;
  }
  entries.forEach((entry) => {
    const row = document.createElement("div");
    row.className = "row spotify-playlist-row";
    row.dataset.playlistUrl = entry.playlist_url || "";

    const info = document.createElement("div");
    info.className = "spotify-playlist-info";
    const title = document.createElement("div");
    title.className = "spotify-playlist-title";
    title.textContent = entry.name || "Spotify Playlist";
    const summary = document.createElement("div");
    summary.className = "meta spotify-playlist-summary";
    const summaryParts = [];
    if (entry.destination) {
      summaryParts.push(`Destination: ${entry.destination}`);
    } else {
      summaryParts.push("Destination: default");
    }
    summaryParts.push(`Auto-download: ${entry.auto_download === false ? "off" : "on"}`);
    const minScore = Number.isFinite(entry.min_match_score) ? entry.min_match_score.toFixed(2) : "0.65";
    summaryParts.push(`Min score: ${minScore}`);
    summary.textContent = summaryParts.join(" · ");
    const status = document.createElement("div");
    status.className = "spotify-playlist-status";
    const url = document.createElement("div");
    url.className = "meta";
    url.textContent = entry.playlist_url || "";
    info.append(title, summary, status, url);

    const button = document.createElement("button");
    button.type = "button";
    button.className = "button ghost small spotify-import-button";
    button.textContent = "Import Playlist";
    button.addEventListener("click", () => importSpotifyPlaylist(entry, row, button));

    container.append(row);
    row.append(info, button);
    updateSpotifyPlaylistRowStatus(row, entry.playlist_url);
  });
}

async function refreshSpotifyPlaylistStatus() {
  try {
    const data = await fetchJson("/api/spotify/playlists/status");
    state.spotifyPlaylistStatus = data.statuses || {};
    updateSpotifyPlaylistStatusDisplay();
  } catch (err) {
    // ignore
  }
}

function updateSpotifyPlaylistStatusDisplay() {
  $$(".spotify-playlist-row").forEach((row) => {
    const url = row.dataset.playlistUrl;
    updateSpotifyPlaylistRowStatus(row, url);
  });
}

function updateSpotifyPlaylistRowStatus(row, playlistUrl) {
  const statusEl = row.querySelector(".spotify-playlist-status");
  if (!statusEl) return;
  const status = state.spotifyPlaylistStatus[playlistUrl] || null;
  if (!status) {
    statusEl.textContent = "Status: Idle";
    return;
  }

  let statusLabel = "Status: Idle";
  if (status.state === "importing") {
    statusLabel = "Status: Importing...";
  } else if (status.state === "completed") {
    if (status.tracks_queued > 0) {
      statusLabel = "✅ Imported successfully";
    } else if (status.tracks_skipped > 0) {
      statusLabel = "🔁 Imported, tracks already queued";
    } else if (status.tracks_discovered > 0) {
      statusLabel = "⚠ Imported, no matches met threshold";
    } else {
      statusLabel = "⚠ Imported, no tracks found";
    }
  } else if (status.state === "error") {
    statusLabel = "❌ Import error";
  } else {
    statusLabel = `Status: ${status.state || "idle"}`;
  }

  const parts = [statusLabel];
  if (status.destination) {
    parts.push(`Destination: ${status.destination}`);
  }
  if (status.tracks_discovered != null) {
    parts.push(`Tracks discovered: ${status.tracks_discovered}`);
  }
  if (status.tracks_queued != null && status.tracks_queued > 0) {
    parts.push(`Queued: ${status.tracks_queued}`);
  }
  if (status.tracks_skipped != null && status.tracks_skipped > 0) {
    parts.push(`Duplicates skipped: ${status.tracks_skipped}`);
  }
  if (status.tracks_failed != null && status.tracks_failed > 0) {
    parts.push(`No matches: ${status.tracks_failed}`);
  }
  if (status.error) {
    parts.push(`Error: ${status.error}`);
  } else if (status.message && status.state === "importing") {
    parts.push(status.message);
  }
  statusEl.textContent = parts.join(" · ");
}

async function importSpotifyPlaylist(entry, row, button) {
  const playlistUrl = (entry.playlist_url || "").trim();
  if (!playlistUrl) {
    return;
  }
  button.disabled = true;
  state.spotifyPlaylistStatus[playlistUrl] = {
    state: "importing",
    message: "Importing playlist metadata...",
    tracks_discovered: 0,
    tracks_queued: 0,
    error: null,
    updated_at: new Date().toISOString(),
  };
  updateSpotifyPlaylistRowStatus(row, playlistUrl);
  try {
    const result = await fetchJson("/api/spotify/playlists/import", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ playlist_url: playlistUrl }),
    });
    state.spotifyPlaylistStatus[playlistUrl] = result.status || state.spotifyPlaylistStatus[playlistUrl];
    updateSpotifyPlaylistRowStatus(row, playlistUrl);
  } catch (err) {
    state.spotifyPlaylistStatus[playlistUrl] = {
      state: "error",
      message: "Import failed",
      error: err.message,
      tracks_discovered: 0,
      tracks_queued: 0,
      updated_at: new Date().toISOString(),
    };
    updateSpotifyPlaylistRowStatus(row, playlistUrl);
  } finally {
    button.disabled = false;
    await refreshSpotifyPlaylistStatus();
  }
}

async function loadConfig() {
  try {
    await refreshConfigPath();
    const cfg = await fetchJson("/api/config");
    state.config = cfg;
    renderConfig(cfg);
    updateSearchDestinationDisplay();
    state.configDirty = false;
    updatePollingState();
    setConfigNotice("Config loaded", false);
  } catch (err) {
    setConfigNotice(`Config error: ${err.message}`, true);
  }
}

function buildConfigFromForm() {
  const base = state.config ? JSON.parse(JSON.stringify(state.config)) : {};
  const errors = [];

  const uploadFmt = $("#cfg-upload-date-format").value.trim();
  if (uploadFmt) {
    base.upload_date_format = uploadFmt;
  } else {
    delete base.upload_date_format;
  }

  const filenameTemplate = $("#cfg-filename-template").value.trim();
  if (filenameTemplate) {
    base.filename_template = filenameTemplate;
  } else {
    delete base.filename_template;
  }

  const finalFormat = $("#cfg-final-format").value.trim();
  if (finalFormat) {
    base.final_format = finalFormat;
  } else {
    delete base.final_format;
  }

  const jsRuntime = $("#cfg-js-runtime").value.trim();
  if (jsRuntime) {
    base.js_runtime = jsRuntime;
  } else {
    delete base.js_runtime;
  }

  const musicTemplate = $("#cfg-music-template").value.trim();
  if (musicTemplate) {
    base.music_filename_template = musicTemplate;
  } else {
    delete base.music_filename_template;
  }

  const cookiesPath = $("#cfg-yt-dlp-cookies").value.trim();
  if (cookiesPath) {
    base.yt_dlp_cookies = cookiesPath;
  } else {
    delete base.yt_dlp_cookies;
  }

  const metaDefaults = {
    enabled: true,
    confidence_threshold: 70,
    use_acoustid: false,
    acoustid_api_key: "",
    embed_artwork: true,
    allow_overwrite_tags: true,
    max_artwork_size_px: 1500,
    rate_limit_seconds: 1.5,
    dry_run: false,
  };
  const metaThreshold = parseInt($("#cfg-music-meta-threshold").value, 10);
  const metaArtworkSize = parseInt($("#cfg-music-meta-artwork-size").value, 10);
  const metaRate = parseFloat($("#cfg-music-meta-rate").value);
  base.music_metadata = {
    enabled: $("#cfg-music-meta-enabled").checked,
    confidence_threshold: Number.isInteger(metaThreshold) ? metaThreshold : metaDefaults.confidence_threshold,
    use_acoustid: $("#cfg-music-meta-acoustid").checked,
    acoustid_api_key: $("#cfg-music-meta-acoustid-key").value.trim(),
    embed_artwork: $("#cfg-music-meta-artwork").checked,
    allow_overwrite_tags: $("#cfg-music-meta-overwrite").checked,
    max_artwork_size_px: Number.isInteger(metaArtworkSize) ? metaArtworkSize : metaDefaults.max_artwork_size_px,
    rate_limit_seconds: Number.isFinite(metaRate) ? metaRate : metaDefaults.rate_limit_seconds,
    dry_run: $("#cfg-music-meta-dry-run").checked,
  };

  const watcherEnabled = $("#cfg-watcher-enabled").checked;
  base.watcher = { enabled: watcherEnabled };
  const watcherPolicy = {
    min_interval_minutes: parseInt($("#cfg-watcher-min-interval").value, 10),
    max_interval_minutes: parseInt($("#cfg-watcher-max-interval").value, 10),
    idle_backoff_factor: parseInt($("#cfg-watcher-idle-backoff").value, 10),
    active_reset_minutes: parseInt($("#cfg-watcher-active-reset").value, 10),
    downtime: {
      enabled: $("#cfg-watcher-downtime-enabled").checked,
      start: $("#cfg-watcher-downtime-start").value.trim(),
      end: $("#cfg-watcher-downtime-end").value.trim(),
      timezone: $("#cfg-watcher-downtime-timezone").value.trim(),
    },
  };
  const policyErrors = [];
  if (!Number.isInteger(watcherPolicy.min_interval_minutes) || watcherPolicy.min_interval_minutes < 1) {
    policyErrors.push("Watcher min interval must be an integer >= 1");
  }
  if (!Number.isInteger(watcherPolicy.max_interval_minutes) || watcherPolicy.max_interval_minutes < 1) {
    policyErrors.push("Watcher max interval must be an integer >= 1");
  }
  if (Number.isInteger(watcherPolicy.min_interval_minutes)
      && Number.isInteger(watcherPolicy.max_interval_minutes)
      && watcherPolicy.max_interval_minutes < watcherPolicy.min_interval_minutes) {
    policyErrors.push("Watcher max interval must be >= min interval");
  }
  if (!Number.isInteger(watcherPolicy.idle_backoff_factor) || watcherPolicy.idle_backoff_factor < 1) {
    policyErrors.push("Watcher idle backoff factor must be an integer >= 1");
  }
  if (!Number.isInteger(watcherPolicy.active_reset_minutes) || watcherPolicy.active_reset_minutes < 1) {
    policyErrors.push("Watcher active reset must be an integer >= 1");
  }
  if (!watcherPolicy.downtime.start) {
    watcherPolicy.downtime.start = "23:00";
  }
  if (!watcherPolicy.downtime.end) {
    watcherPolicy.downtime.end = "09:00";
  }
  if (!watcherPolicy.downtime.timezone) {
    watcherPolicy.downtime.timezone = "local";
  }
  if (policyErrors.length) {
    errors.push(...policyErrors);
  } else {
    base.watch_policy = watcherPolicy;
  }

  let singleFolder = $("#cfg-single-download-folder").value.trim();
  singleFolder = normalizeDownloadsRelative(singleFolder);
  if (singleFolder) {
    base.single_download_folder = singleFolder;
  } else {
    delete base.single_download_folder;
  }

  const telegramToken = $("#cfg-telegram-token").value.trim();
  const telegramChat = $("#cfg-telegram-chat").value.trim();
  if (telegramToken || telegramChat) {
    base.telegram = {
      bot_token: telegramToken,
      chat_id: telegramChat,
    };
  } else {
    delete base.telegram;
  }

  const accounts = {};
  $$(".account-row").forEach((row) => {
    const name = row.querySelector(".account-name").value.trim();
    if (!name) {
      return;
    }
    const original = row.dataset.original ? JSON.parse(row.dataset.original) : {};
    original.client_secret = row.querySelector(".account-client").value.trim();
    original.token = row.querySelector(".account-token").value.trim();
    accounts[name] = original;
  });
  base.accounts = accounts;

  const playlists = [];
  $$(".playlist-row").forEach((row, idx) => {
    const name = row.querySelector(".playlist-name").value.trim();
    const playlistId = row.querySelector(".playlist-id").value.trim();
    let folder = row.querySelector(".playlist-folder").value.trim();
    folder = normalizeDownloadsRelative(folder);
    if (!playlistId && !folder) {
      return;
    }
    if (!playlistId || !folder) {
      errors.push(`Playlist ${idx + 1} missing playlist_id or folder`);
      return;
    }
    const original = row.dataset.original ? JSON.parse(row.dataset.original) : {};
    if (name) {
      original.name = name;
    } else {
      delete original.name;
    }
    original.playlist_id = playlistId;
    delete original.id;
    original.folder = folder;
    delete original.directory;
    const account = row.querySelector(".playlist-account").value.trim();
    if (account) {
      original.account = account;
    } else {
      delete original.account;
    }
    const format = row.querySelector(".playlist-format").value.trim();
    if (format) {
      original.final_format = format;
    } else {
      delete original.final_format;
    }
    if (row.querySelector(".playlist-music").checked) {
      original.music_mode = true;
    } else {
      delete original.music_mode;
    }
    if (row.querySelector(".playlist-subscribe").checked) {
      original.mode = "subscribe";
    } else {
      delete original.mode;
    }
    original.remove_after_download = row.querySelector(".playlist-remove").checked;
    playlists.push(original);
  });
  base.playlists = playlists;

  const optsRaw = $("#cfg-yt-dlp-opts").value.trim();
  if (optsRaw) {
    try {
      base.yt_dlp_opts = JSON.parse(optsRaw);
    } catch (err) {
      errors.push(`yt-dlp options JSON error: ${err.message}`);
    }
  } else {
    delete base.yt_dlp_opts;
  }

  return { config: base, errors };
}

async function saveConfig() {
  const result = buildConfigFromForm();
  if (result.errors.length) {
    setConfigNotice(result.errors.join("; "), true);
    return;
  }

  try {
    await fetchJson("/api/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.config),
    });
    setConfigNotice("Config saved", false, true);
    state.config = result.config;
    state.configDirty = false;
    updatePollingState();
  } catch (err) {
    setConfigNotice(`Save failed: ${err.message}`, true);
  }
}

async function updateYtdlp() {
  try {
    setNotice($("#ytdlp-update-message"), "Starting yt-dlp update...", false);
    await fetchJson("/api/yt-dlp/update", { method: "POST" });
    setNotice($("#ytdlp-update-message"), "Update started. Restart container after completion.", false);
  } catch (err) {
    setNotice($("#ytdlp-update-message"), `Update failed: ${err.message}`, true);
  }
}

async function startRun(payload) {
  try {
    setNotice($("#run-message"), "Starting run...", false);
    const data = await fetchJson("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setNotice($("#run-message"), "Run started", false);
    await refreshStatus();
    return data;
  } catch (err) {
    setNotice($("#run-message"), `Run failed: ${err.message}`, true);
    return null;
  }
}

function buildRunPayload() {
  const payload = {};
  const singleUrl = $("#run-single-url").value.trim();
  const deliveryMode = getSingleDeliveryMode();
  if (singleUrl) {
    payload.single_url = singleUrl;
    payload.delivery_mode = deliveryMode;
  }
  const destination = $("#run-destination").value.trim();
  if (destination && deliveryMode !== "client") {
    payload.destination = destination;
  }
  const finalFormat = $("#run-format").value.trim();
  if (finalFormat) {
    payload.final_format_override = finalFormat;
  }
  const jsRuntime = $("#run-js-runtime").value.trim();
  if (jsRuntime) {
    payload.js_runtime = jsRuntime;
  }
  const musicMode = $("#run-music-mode");
  if (musicMode && musicMode.checked) {
    payload.music_mode = true;
  }
  return payload;
}

function getSingleDeliveryMode() {
  const selected = document.querySelector('input[name="run-delivery-mode"]:checked');
  return selected ? selected.value : "server";
}

function applySingleDeliveryMode() {
  const mode = getSingleDeliveryMode();
  const destInput = $("#run-destination");
  const destLabel = $("#run-destination-label");
  const browseBtn = $("#browse-run-destination");
  if (mode === "client") {
    destInput.value = "";
    destInput.disabled = true;
    destInput.placeholder = "Download to this device";
    browseBtn.disabled = true;
    destLabel.textContent = "Download destination (client)";
  } else {
    destInput.disabled = false;
    destInput.placeholder = "downloads";
    browseBtn.disabled = false;
    destLabel.textContent = "Destination (single runs)";
  }
}

function buildPlaylistPayload() {
  const payload = {};
  const playlistValue = $("#run-playlist-id").value.trim();
  if (playlistValue) {
    payload.playlist_id = playlistValue;
  }
  const account = $("#run-playlist-account").value.trim();
  if (account) {
    payload.playlist_account = account;
  }
  const destination = $("#run-destination").value.trim();
  if (destination) {
    payload.destination = destination;
  }
  const finalFormat = $("#run-format").value.trim();
  if (finalFormat) {
    payload.final_format_override = finalFormat;
  }
  const jsRuntime = $("#run-js-runtime").value.trim();
  if (jsRuntime) {
    payload.js_runtime = jsRuntime;
  }
  const musicMode = $("#run-music-mode");
  if (musicMode && musicMode.checked) {
    payload.music_mode = true;
  }
  return payload;
}

async function handleCopy(event, noticeEl) {
  const button = event.target.closest("button[data-copy]");
  if (!button) return;
  const raw = button.dataset.value || "";
  let text = "";
  try {
    text = decodeURIComponent(raw);
  } catch (err) {
    text = raw;
  }
  const ok = await copyText(text);
  const label = button.dataset.copy || "value";
  if (ok) {
    setNotice(noticeEl, `${label} copied`, false);
  } else {
    setNotice(noticeEl, `Copy failed for ${label}`, true);
  }
}

function setupTimers() {
  if (state.timers.status) {
    clearInterval(state.timers.status);
  }
  state.timers.status = setInterval(() => {
    withPollingGuard(refreshStatus);
  }, 3000);

  if (state.timers.metrics) {
    clearInterval(state.timers.metrics);
  }
  state.timers.metrics = setInterval(() => {
    withPollingGuard(refreshMetrics);
  }, 8000);

  if (state.timers.schedule) {
    clearInterval(state.timers.schedule);
  }
  state.timers.schedule = setInterval(() => {
    withPollingGuard(refreshSchedule);
  }, 8000);

  if (state.timers.logs) {
    clearInterval(state.timers.logs);
  }
  state.timers.logs = setInterval(() => {
    const logsAuto = $("#logs-auto");
    if (!logsAuto || !logsAuto.checked) {
      return;
    }
    if (state.currentPage !== "logs") {
      return;
    }
    withPollingGuard(refreshLogs);
  }, 4000);
}

function bindEvents() {
  const navToggle = $("#nav-toggle");
  if (navToggle) {
    navToggle.addEventListener("click", () => {
      const isOpen = document.body.classList.toggle("nav-open");
      navToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
    });
  }
  $$(".filters-toggle").forEach((button) => {
    button.addEventListener("click", () => {
      const targetId = button.dataset.target;
      if (!targetId) return;
      const block = document.getElementById(targetId);
      if (!block) return;
      const open = block.classList.toggle("open");
      button.textContent = open ? "Hide filters" : "Filters";
    });
  });
  $$(".nav-button").forEach((button) => {
    button.addEventListener("click", () => {
      const page = button.dataset.page || "home";
      setPage(page);
      window.location.hash = page;
    });
  });

  $("#refresh-all").addEventListener("click", async () => {
    await refreshStatus();
    await refreshSchedule();
    await refreshMetrics();
    await refreshLogs();
    await refreshHistory();
    await refreshDownloads();
  });

  $("#logs-refresh").addEventListener("click", refreshLogs);
  $("#logs-auto").addEventListener("change", () => {
    if ($("#logs-auto").checked) {
      refreshLogs();
    }
  });
  $("#downloads-refresh").addEventListener("click", refreshDownloads);
  $("#downloads-apply").addEventListener("click", refreshDownloads);
  $("#downloads-clear").addEventListener("click", async () => {
    $("#downloads-search").value = "";
    $("#downloads-limit").value = 50;
    await refreshDownloads();
  });
  $("#cleanup-temp").addEventListener("click", cleanupTemp);
  $("#history-refresh").addEventListener("click", refreshHistory);
  $("#history-apply").addEventListener("click", refreshHistory);
  $("#history-clear").addEventListener("click", async () => {
    $("#history-search").value = "";
    $("#history-playlist").value = "";
    $("#history-from").value = "";
    $("#history-to").value = "";
    $("#history-limit").value = 50;
    $("#history-sort").value = "date";
    $("#history-dir").value = "desc";
    await refreshHistory();
  });
  $("#history-show-paths").addEventListener("change", refreshHistory);
  $("#history-body").addEventListener("click", async (event) => {
    await handleCopy(event, $("#history-message"));
  });
  $("#downloads-body").addEventListener("click", async (event) => {
    await handleCopy(event, $("#downloads-message"));
  });
  $("#search-create-download").addEventListener("click", () => createSearchRequest(true));
  $("#search-create-only").addEventListener("click", () => createSearchRequest(false));
  $("#search-requests-refresh").addEventListener("click", refreshSearchRequests);
  $("#search-requests-sort").addEventListener("click", () => {
    state.searchRequestsSort = state.searchRequestsSort === "asc" ? "desc" : "asc";
    updateSearchSortLabel();
    refreshSearchRequests();
  });
  $("#search-queue-refresh").addEventListener("click", refreshSearchQueue);
  const sourceList = $("#search-source-list");
  if (sourceList) {
    sourceList.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-action]");
      if (!button) return;
      const row = button.closest(".source-priority-row");
      if (!row) return;
      moveSourcePriorityRow(row, button.dataset.action);
    });
  }
  const homeSearchDownload = $("#home-search-download");
  if (homeSearchDownload) {
    homeSearchDownload.addEventListener("click", () => submitHomeSearch(true));
  }
  const homeSearchOnly = $("#home-search-only");
  if (homeSearchOnly) {
    homeSearchOnly.addEventListener("click", () => submitHomeSearch(false));
  }
const homeAdvancedToggle = $("#home-advanced-toggle");
const homeAdvancedPanel = $("#home-advanced-options");
if (homeAdvancedToggle && homeAdvancedPanel) {
  const advancedLabel = "Advanced options (optional)";
  homeAdvancedPanel.classList.remove("open");
  homeAdvancedToggle.textContent = advancedLabel;
  homeAdvancedToggle.setAttribute("aria-expanded", "false");
  homeAdvancedToggle.addEventListener("click", () => {
    const open = homeAdvancedPanel.classList.toggle("open");
    homeAdvancedToggle.textContent = open ? "Hide advanced options" : advancedLabel;
    homeAdvancedToggle.setAttribute("aria-expanded", open ? "true" : "false");
  });
}
  const homeViewAdvanced = $("#home-view-advanced");
  if (homeViewAdvanced) {
    homeViewAdvanced.addEventListener("click", handleHomeViewAdvanced);
    updateHomeViewAdvancedLink();
  }
  const homeResultsList = $("#home-results-list");
  if (homeResultsList) {
    homeResultsList.addEventListener("click", async (event) => {
      const directButton = event.target.closest('button[data-action="home-direct-download"]');
      if (directButton) {
        if (directButton.disabled) return;
        const directUrl = directButton.dataset.directUrl;
        if (!directUrl) return;
        directButton.disabled = true;
        await handleHomeDirectUrl(directUrl, $("#home-destination")?.value.trim() || "", $("#home-search-message"));
        return;
      }
      const button = event.target.closest('button[data-action="home-download"]');
      if (!button) return;
      const itemId = button.dataset.itemId;
      const candidateId = button.dataset.candidateId;
      if (!itemId || !candidateId) return;
      if (button.disabled) return;
      button.disabled = true;
      const originalText = button.textContent;
      button.textContent = "Queued";
      await enqueueSearchCandidate(itemId, candidateId, { messageEl: $("#home-search-message") });
      await refreshHomeResults(state.homeSearchRequestId);
      button.disabled = false;
      button.textContent = originalText;
    });
  }
  $("#search-requests-body").addEventListener("click", async (event) => {
    const actionButton = event.target.closest("button[data-action]");
    if (actionButton) {
      const requestId = actionButton.dataset.requestId;
      const action = actionButton.dataset.action;
      if (action === "cancel") {
        await cancelSearchRequest(requestId);
      }
      return;
    }
    const row = event.target.closest("tr[data-request-id]");
    if (!row) return;
    const requestId = row.dataset.requestId;
    if (!requestId) return;
    setSearchSelectedRequest(requestId);
    setSearchSelectedItem(null);
    $$("#search-requests-body tr.selected").forEach((el) => el.classList.remove("selected"));
    row.classList.add("selected");
    await refreshSearchRequestDetails(requestId);
  });
  $("#search-items-body").addEventListener("click", async (event) => {
    const row = event.target.closest("tr[data-item-id]");
    if (!row) return;
    const itemId = row.dataset.itemId;
    if (!itemId) return;
    setSearchSelectedItem(itemId);
    $$("#search-items-body tr.selected").forEach((el) => el.classList.remove("selected"));
    row.classList.add("selected");
    await refreshSearchCandidates(itemId);
  });

  $("#search-candidates-body").addEventListener("click", async (event) => {
    const button = event.target.closest('button[data-action="download"]');
    if (!button) return;
    const itemId = button.dataset.itemId || state.searchSelectedItemId;
    const candidateId = button.dataset.candidateId;
    await enqueueSearchCandidate(itemId, candidateId);
  });

  $("#schedule-save").addEventListener("click", saveSchedule);
  $("#schedule-run-now").addEventListener("click", runScheduleNow);
  $("#save-config").addEventListener("click", saveConfig);
  const ytdlpUpdate = $("#ytdlp-update");
  if (ytdlpUpdate) {
    ytdlpUpdate.addEventListener("click", updateYtdlp);
  }
  $("#reset-config").addEventListener("click", async () => {
    await loadConfig();
    setConfigNotice("Config reloaded", false);
  });
  $("#load-config-path").addEventListener("click", setConfigPath);
  $("#browse-config-path").addEventListener("click", () => {
    const input = $("#config-path");
    openBrowser(input, "config", "file", ".json", resolveBrowseStart("config", input.value));
  });
  $("#browse-single-download").addEventListener("click", () => {
    const input = $("#cfg-single-download-folder");
    openBrowser(input, "downloads", "dir", "", resolveBrowseStart("downloads", input.value));
  });
  $("#browse-run-destination").addEventListener("click", () => {
    const input = $("#run-destination");
    openBrowser(input, "downloads", "dir", "", resolveBrowseStart("downloads", input.value));
  });
  const homeBrowse = $("#home-destination-browse");
  if (homeBrowse) {
    homeBrowse.addEventListener("click", () => {
      const target = $("#home-destination");
      if (!target) return;
      openBrowser(target, "downloads", "dir", "", resolveBrowseStart("downloads", target.value));
    });
  }
  const searchBrowse = $("#search-destination-browse");
  if (searchBrowse) {
    searchBrowse.addEventListener("click", () => {
      const target = $("#search-destination");
      if (!target) return;
      openBrowser(target, "downloads", "dir", "", resolveBrowseStart("downloads", target.value));
    });
  }
  const homeDestinationInput = $("#home-destination");
  if (homeDestinationInput) {
    homeDestinationInput.addEventListener("input", () => {
      updateHomeDestinationResolved();
    });
  }
  const searchDestinationInput = $("#search-destination");
  if (searchDestinationInput) {
    searchDestinationInput.addEventListener("input", () => {
      updateSearchDestinationDisplay();
    });
  }
  $("#browse-yt-dlp-cookies").addEventListener("click", () => {
    const input = $("#cfg-yt-dlp-cookies");
    openBrowser(input, "tokens", "file", ".txt", resolveBrowseStart("tokens", input.value));
  });

  $("#toggle-telegram-token").addEventListener("click", () => {
    const input = $("#cfg-telegram-token");
    if (input.type === "password") {
      input.type = "text";
      $("#toggle-telegram-token").textContent = "Hide";
    } else {
      input.type = "password";
      $("#toggle-telegram-token").textContent = "Show";
    }
  });

  $("#browser-close").addEventListener("click", closeBrowser);
  $("#browser-up").addEventListener("click", (event) => {
    const next = event.currentTarget.dataset.path;
    if (next !== undefined) {
      refreshBrowser(next);
    }
  });
  $("#browser-select").addEventListener("click", applyBrowserSelection);
  $("#browser-list").addEventListener("click", (event) => {
    const item = event.target.closest(".browser-item");
    if (!item || item.classList.contains("empty") || item.classList.contains("error")) {
      return;
    }
    const type = item.dataset.type;
    const relPath = item.dataset.path || "";
    const absPath = item.dataset.absPath || "";
    if (type === "dir") {
      refreshBrowser(relPath);
      return;
    }
    if (type === "file" && browserState.mode === "file") {
      browserState.selected = absPath;
      $("#browser-selected").textContent = absPath;
      $$(".browser-item.selected").forEach((el) => el.classList.remove("selected"));
      item.classList.add("selected");
      $("#browser-select").disabled = false;
    }
  });

  $("#oauth-close").addEventListener("click", closeOauthModal);
  $("#oauth-open").addEventListener("click", () => {
    if (oauthState.authUrl) {
      window.open(oauthState.authUrl, "_blank", "noopener");
    }
  });
  $("#oauth-complete").addEventListener("click", completeOauth);

  $("#add-account").addEventListener("click", () => addAccountRow("", {}));
  $("#add-playlist").addEventListener("click", () => addPlaylistRow({}));

  $("#run-playlists").addEventListener("click", () => {
    const jsRuntime = $("#run-js-runtime").value.trim();
    const payload = jsRuntime ? { js_runtime: jsRuntime } : {};
    startRun(payload);
  });
  $("#status-cancel").addEventListener("click", async () => {
    const ok = confirm("Are you sure you want to kill downloads in progress?");
    if (!ok) {
      return;
    }
    try {
      await fetchJson("/api/cancel", { method: "POST" });
      setNotice($("#run-message"), "Cancel requested", false);
      await refreshStatus();
    } catch (err) {
      setNotice($("#run-message"), `Cancel failed: ${err.message}`, true);
    }
  });
  $$('input[name="run-delivery-mode"]').forEach((input) => {
    input.addEventListener("change", applySingleDeliveryMode);
  });
  $("#run-single").addEventListener("click", () => {
    const payload = buildRunPayload();
    if (!payload.single_url) {
      setNotice($("#run-message"), "Single URL is required", true);
      return;
    }
    startRun(payload);
  });
  $("#run-playlist-once").addEventListener("click", () => {
    const payload = buildPlaylistPayload();
    if (!payload.playlist_id) {
      setNotice($("#run-message"), "Playlist URL or ID is required", true);
      return;
    }
    startRun(payload);
  });

  $("#toggle-theme").addEventListener("click", () => {
    const next = resolveTheme() === "light" ? "dark" : "light";
    applyTheme(next);
  });

  document.addEventListener("focusin", (event) => {
    const tag = (event.target && event.target.tagName) || "";
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") {
      state.inputFocused = true;
      updatePollingState();
    }
  });
  document.addEventListener("focusout", (event) => {
    const tag = (event.target && event.target.tagName) || "";
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") {
      state.inputFocused = false;
      updatePollingState();
    }
  });
  document.addEventListener("click", clearConfigNotice);
  document.addEventListener("input", clearConfigNotice);

  const configPanel = $("#config-panel");
  if (configPanel) {
    configPanel.addEventListener("input", () => {
      if (state.suppressDirty) {
        return;
      }
      state.configDirty = true;
      updatePollingState();
    });
  }

  applySingleDeliveryMode();
}

async function init() {
  applyTheme(resolveTheme());
  bindEvents();
  setupNavActions();
  await loadPaths();
  const initialPage = (window.location.hash || "#home").replace("#", "");
  setPage(initialPage || "home");
  window.addEventListener("hashchange", () => {
    const next = (window.location.hash || "#home").replace("#", "");
    setPage(next || "home");
  });
  setupTimers();
  const logsAuto = $("#logs-auto");
  if (logsAuto) {
    logsAuto.checked = true;
    logsAuto.disabled = false;
  }
}

window.addEventListener("DOMContentLoaded", init);

// Add Enter key handler for home search input to trigger "Search Only"
document.addEventListener("DOMContentLoaded", () => {
  const homeSearchInput = $("#home-search-input");
  if (homeSearchInput) {
    homeSearchInput.addEventListener("keydown", (event) => {
      if (
        event.key === "Enter" &&
        !event.shiftKey &&
        !event.ctrlKey &&
        !event.altKey &&
        !event.metaKey
      ) {
        event.preventDefault();
        const searchOnlyBtn = $("#home-search-only");
        if (searchOnlyBtn && !searchOnlyBtn.disabled) {
          searchOnlyBtn.click();
        }
      }
    });
  }
});


(function ensureHomeClass() {
  const homeSection = document.querySelector('section[data-page="home"]');
  if (!homeSection) return;

  const observer = new MutationObserver(() => {
    const visible = !homeSection.classList.contains("page-hidden");
    document.body.classList.toggle("home-page", visible);
    if (visible) {
      document.body.dataset.page = "home";
    } else if (state.currentPage) {
      document.body.dataset.page = state.currentPage;
    }
  });

  observer.observe(homeSection, { attributes: true, attributeFilter: ["class"] });

  // initial state
  const visible = !homeSection.classList.contains("page-hidden");
  document.body.classList.toggle("home-page", visible);
  if (visible) {
    document.body.dataset.page = "home";
  } else if (state.currentPage) {
    document.body.dataset.page = state.currentPage;
  }
})();
