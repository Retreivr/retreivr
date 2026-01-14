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
const GITHUB_REPO = "z3ro-2/youtube-archiver";
const GITHUB_RELEASE_URL = `https://api.github.com/repos/${GITHUB_REPO}/releases/latest`;
const GITHUB_RELEASE_PAGE = "https://github.com/z3ro-2/youtube-archiver/releases";
const RELEASE_CHECK_KEY = "yt_archiver_release_checked_at";
const RELEASE_CACHE_KEY = "yt_archiver_release_cache";
const RELEASE_VERSION_KEY = "yt_archiver_release_app_version";

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

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
  const allowed = new Set(["home", "config", "downloads", "history", "logs"]);
  const target = allowed.has(page) ? page : "home";
  state.currentPage = target;
  document.body.classList.remove("nav-open");
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
  if (target === "home") {
    refreshStatus();
    refreshSchedule();
    refreshMetrics();
    refreshVersion();
  } else if (target === "config") {
    if (!state.config || !state.configDirty) {
      loadConfig();
    }
  } else if (target === "downloads") {
    refreshDownloads();
  } else if (target === "history") {
    refreshHistory();
  } else if (target === "logs") {
    refreshLogs();
  }
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
    browserState.target.value = rel;
    console.info("Directory selected", { root: browserState.root, path: rel });
    closeBrowser();
    return;
  }
  if (browserState.selected) {
    browserState.target.value = browserState.selected;
    console.info("File selected", { root: browserState.root, path: browserState.selected });
    closeBrowser();
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

  const opts = cfg.yt_dlp_opts || {};
  $("#cfg-yt-dlp-opts").value = Object.keys(opts).length ? JSON.stringify(opts, null, 2) : "";
  state.suppressDirty = false;
}

async function loadConfig() {
  try {
    await refreshConfigPath();
    const cfg = await fetchJson("/api/config");
    state.config = cfg;
    renderConfig(cfg);
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
    await fetchJson("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setNotice($("#run-message"), "Run started", false);
    await refreshStatus();
  } catch (err) {
    setNotice($("#run-message"), `Run failed: ${err.message}`, true);
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
