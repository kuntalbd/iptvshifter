// Shared frontend helpers for the M3U Processor web UI.
// Loaded from base.html; every page relies on these.

// ---------- auth / api ----------
const AUTH_KEY = "m3u_token";

function getToken() {
  return localStorage.getItem(AUTH_KEY) || "";
}
function setToken(t) {
  if (t) localStorage.setItem(AUTH_KEY, t);
  else localStorage.removeItem(AUTH_KEY);
}

// apiFetch wraps fetch() with the Bearer token and consistent JSON handling.
// On 401 it asks for a token (single prompt per page) so pages work without a
// page reload after auth is enabled.
let _authPromptShown = false;

async function apiFetch(url, opts = {}) {
  const headers = Object.assign({}, opts.headers || {});
  const t = getToken();
  if (t) headers["Authorization"] = "Bearer " + t;
  opts = Object.assign({}, opts, { headers });
  const res = await fetch(url, opts);
  if (res.status === 401) {
    const need = !_authPromptShown;
    _authPromptShown = true;
    if (need) {
      const entered = prompt("Enter web UI auth token:");
      _authPromptShown = false;
      if (entered) {
        setToken(entered);
        return apiFetch(url, opts);
      }
    }
    throw new Error("unauthorized");
  }
  return res;
}

async function apiGet(url) {
  const res = await apiFetch(url);
  return res.json();
}

async function apiPost(url, body) {
  const res = await apiFetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  return res.json();
}

// ---------- dom / escape ----------
function esc(s) {
  return (s == null ? "" : String(s)).replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[
        c
      ])
  );
}

let _toastEl = null;
function toast(msg, ms) {
  if (!_toastEl) {
    _toastEl = document.createElement("div");
    _toastEl.id = "app-toast";
    _toastEl.style.cssText =
      "position:fixed;bottom:20px;right:20px;padding:10px 18px;border-radius:6px;" +
      "background:#16a34a;color:#fff;font-size:0.85rem;display:none;z-index:999;";
    document.body.appendChild(_toastEl);
  }
  _toastEl.textContent = msg;
  _toastEl.style.display = "block";
  setTimeout(() => {
    _toastEl.style.display = "none";
  }, ms || 2000);
}

function debounce(fn, ms) {
  let t = null;
  return function () {
    clearTimeout(t);
    t = setTimeout(fn, ms);
  };
}

// ---------- time formatting ----------
function fmtDate(s) {
  if (!s) return "";
  try {
    const d = new Date(s);
    if (isNaN(d)) return s;
    const diff = (Date.now() - d.getTime()) / 1000;
    if (diff < 60) return "just now";
    if (diff < 3600) return Math.floor(diff / 60) + "m ago";
    if (diff < 86400) return Math.floor(diff / 3600) + "h ago";
    if (diff < 604800) return Math.floor(diff / 86400) + "d ago";
    return (
      d.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" }) +
      " " +
      d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" })
    );
  } catch (e) {
    return s;
  }
}

function fmtTime(t) {
  if (!t) return "—";
  return new Date(t).toLocaleString();
}

function fmtDur(s) {
  if (s == null) return "—";
  s = Math.round(s);
  const h = Math.floor(s / 3600),
    m = Math.floor((s % 3600) / 60),
    x = s % 60;
  return (h ? h + "h " : "") + (m ? m + "m " : "") + x + "s";
}

function fmtElapsed(start) {
  if (!start) return "—";
  const s = new Date(start).getTime();
  let sec = Math.floor((Date.now() - s) / 1000);
  const h = Math.floor(sec / 3600),
    m = Math.floor((sec % 3600) / 60),
    x = sec % 60;
  return (h ? h + "h " : "") + (m ? m + "m " : "") + x + "s";
}

// ---------- selection helpers (batch checkboxes) ----------
function selectedIds() {
  return [...document.querySelectorAll(".sel:checked")].map((c) => +c.dataset.id);
}
function selAll(el) {
  document.querySelectorAll(".sel").forEach((c) => (c.checked = el.checked));
  refreshSelCount();
}
function refreshSelCount() {
  const el = document.getElementById("sel-count");
  const box = document.getElementById("batch-box");
  if (!el) return;
  const n = selectedIds().length;
  el.textContent = n + " selected";
  if (box) box.classList.toggle("hidden", n === 0);
}
document.addEventListener("change", (e) => {
  if (e.target.classList && e.target.classList.contains("sel")) refreshSelCount();
});