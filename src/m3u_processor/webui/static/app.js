// M3U Processor web UI — shared framework.
// Every page uses these helpers so behavior looks identical everywhere.

// ---------- auth / api ----------
const AUTH_KEY = "m3u_token";

function getToken() {
  return localStorage.getItem(AUTH_KEY) || "";
}
function setToken(t) {
  if (t) localStorage.setItem(AUTH_KEY, t);
  else localStorage.removeItem(AUTH_KEY);
}

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

async function apiDelete(url, body) {
  const res = await apiFetch(url, {
    method: "DELETE",
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
function toast(msg, ok = true) {
  if (!_toastEl) {
    _toastEl = document.createElement("div");
    _toastEl.id = "app-toast";
    _toastEl.style.cssText =
      "position:fixed;bottom:20px;right:20px;padding:10px 18px;border-radius:8px;" +
      "font-size:13px;display:none;z-index:999;box-shadow:0 4px 16px rgba(0,0,0,.4);";
    document.body.appendChild(_toastEl);
  }
  _toastEl.textContent = msg;
  _toastEl.style.background = ok ? "#16a34a" : "#b91c1c";
  _toastEl.style.color = "#fff";
  _toastEl.style.display = "block";
  setTimeout(() => {
    _toastEl.style.display = "none";
  }, 2200);
}

function debounce(fn, ms) {
  let t = null;
  return function () {
    clearTimeout(t);
    t = setTimeout(fn, ms);
  };
}

function confirmDialog(msg) {
  return window.confirm(msg);
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

// ---------- generic table component ----------
// One DataTable powers streams / providers / blacklist / favorites / errors /
// schedules so every table sorts, filters and paginates identically.
//
// new DataTable({
//   el: selector,                 // required: container (a <table> is built inside)
//   columns: [{
//     key,                        // row field for sorting
//     label,                      // header text
//     sortable: true,             // clickable header (default true)
//     render: (r) => html,        // cell content (esc() yourself)
//     cls: "num",                 // td class (num/center)
//     thCls: "num",
//   }],
//   fetch: (params) => Promise,   // returns array of rows (client data)
//   searchable: true,             // show a search box that filters client-side
//   searchKeys: ["name"],         // default: all render text / string fields
//   searchFn: (r, q) => bool,     // custom client filter (overrides searchKeys)
//   pageSize: 50,                 // 0 = no pagination
//   selectable: true,             // checkbox column
//   selData: (r) => ({id: r.id}), // row data stored on checkbox
//   rowClick: (r) => {},          // click on a data row
//   rowAttrs: (r) => ({}),        // extra <tr> attrs
//   emptyText: "no rows",
//   onLoad: (rows) => {},         // after render
//   onSelChange: (ids) => {},     // selection changed
//   autoRefresh: ms,              // reload on interval
// })
class DataTable {
  constructor(o) {
    this.o = o;
    this.rows = [];
    this.search = "";
    this.sortKey = o.sortKey || null;
    this.sortAsc = o.sortAsc !== false;
    this.page = 1;
    this.el = document.querySelector(o.el);
    this._sel = new Map();

    const wrap = document.createElement("div");
    wrap.className = "table-wrap";
    this.wrap = wrap;
    this.el.appendChild(wrap);

    const table = document.createElement("table");
    table.className = "data-table";
    this.table = table;
    wrap.appendChild(table);

    this.thead = document.createElement("thead");
    this.tbody = document.createElement("tbody");
    table.appendChild(this.thead);
    table.appendChild(this.tbody);

    this._buildHead();

    if (o.searchable) {
      this._buildSearch();
    }

    if (o.autoRefresh) {
      setInterval(() => this.reload(), o.autoRefresh);
    }

    this.reload();
  }

  _buildSearch() {
    const toolbar = document.createElement("div");
    toolbar.className = "toolbar";
    toolbar.innerHTML =
      `<div class="field"><span>Filter</span>` +
      `<input type="search" placeholder="${esc(this.o.searchPlaceholder || "Search…")}"></div>`;
    const input = toolbar.querySelector("input");
    input.addEventListener(
      "input",
      debounce(() => {
        this.search = input.value.trim().toLowerCase();
        this.page = 1;
        this._render();
      }, 250)
    );
    this.el.insertBefore(toolbar, this.wrap);
  }

  _buildHead() {
    let head = "";
    if (this.o.selectable) {
      head += `<th class="sel-cell"><input type="checkbox" class="sel-all"></th>`;
    }
    for (const c of this.o.columns) {
      const cls = c.sortable === false ? "" : "sortable";
      const thCls = c.thCls ? ` ${c.thCls}` : "";
      let sortCls = "";
      if (this.sortKey === c.key) sortCls = this.sortAsc ? " sort-asc" : " sort-desc";
      head += `<th class="${cls}${thCls}${sortCls}" data-key="${esc(c.key)}">${esc(c.label)}</th>`;
    }
    this.thead.innerHTML = `<tr>${head}</tr>`;

    this.thead.querySelectorAll("th.sortable").forEach((th) => {
      th.addEventListener("click", () => {
        const k = th.dataset.key;
        if (this.sortKey === k) this.sortAsc = !this.sortAsc;
        else {
          this.sortKey = k;
          this.sortAsc = true;
        }
        this._render();
      });
    });

    const selAll = this.thead.querySelector(".sel-all");
    if (selAll) {
      selAll.addEventListener("change", () => {
        const on = selAll.checked;
        this._visibleRows().forEach((r) => {
          const key = this._rowKey(r);
          if (on) this._sel.set(key, r);
          else this._sel.delete(key);
        });
        this._syncSelBoxes();
        this._emitSel();
      });
    }
  }

  _rowKey(r) {
    const d = this.o.selData ? this.o.selData(r) : r;
    return d.id != null ? String(d.id) : JSON.stringify(r).slice(0, 80);
  }

  async reload() {
    const data = await this.o.fetch({ sortKey: this.sortKey, sortAsc: this.sortAsc });
    this.rows = Array.isArray(data) ? data : data.rows || [];
    this.page = 1;
    if (this.o.onLoad) this.o.onLoad(this.rows);
    this._render();
  }

  _visibleRows() {
    let rows = this.rows;
    if (this.o.searchFn) {
      if (this.search) rows = rows.filter((r) => this.o.searchFn(r, this.search));
    } else if (this.search) {
      const keys = this.o.searchKeys || [];
      rows = rows.filter((r) => {
        const hay = keys.length
          ? keys.map((k) => String(r[k] == null ? "" : r[k])).join(" ").toLowerCase()
          : JSON.stringify(r).toLowerCase();
        return hay.includes(this.search);
      });
    }
    return rows;
  }

  _sortRows(rows) {
    if (!this.sortKey) return rows;
    const c = this.o.columns.find((x) => x.key === this.sortKey);
    const key = this.sortKey;
    return [...rows].sort((a, b) => {
      let av = a[key],
        bv = b[key];
      if (av == null) av = "";
      if (bv == null) bv = "";
      if (typeof av === "number" && typeof bv === "number") {
        return this.sortAsc ? av - bv : bv - av;
      }
      av = String(av).toLowerCase();
      bv = String(bv).toLowerCase();
      return this.sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
    });
  }

  _render() {
    let rows = this._sortRows(this._visibleRows());
    const pageSize = this.o.pageSize || 0;
    const total = rows.length;
    const pages = pageSize ? Math.max(1, Math.ceil(total / pageSize)) : 1;
    if (this.page > pages) this.page = pages;
    const start = pageSize ? (this.page - 1) * pageSize : 0;
    const slice = pageSize ? rows.slice(start, start + pageSize) : rows;

    let html = "";
    if (!slice.length) {
      html = `<tr><td colspan="${this._colCount()}" class="empty">${esc(this.o.emptyText || "no rows")}</td></tr>`;
    } else {
      html = slice
        .map((r) => {
          let td = "";
          if (this.o.selectable) {
            const key = this._rowKey(r);
            const checked = this._sel.has(key) ? " checked" : "";
            td += `<td class="sel-cell"><input type="checkbox" class="row-sel" data-key="${esc(key)}"${checked}></td>`;
          }
          for (const c of this.o.columns) {
            const cls = c.cls ? ` class="${c.cls}"` : "";
            td += `<td${cls}>${c.render ? c.render(r) : esc(r[c.key])}</td>`;
          }
          const attrs = this.o.rowAttrs ? this.o.rowAttrs(r) : {};
          const attrStr = Object.entries(attrs)
            .map(([k, v]) => ` ${k}="${esc(v)}"`)
            .join("");
          const clickable = this.o.rowClick ? " clickable" : "";
          return `<tr data-key="${esc(this._rowKey(r))}"${attrStr} class="${clickable}">${td}</tr>`;
        })
        .join("");
    }
    this.tbody.innerHTML = html;

    if (this.o.selectable) {
      this._syncSelBoxes();
      this.tbody.querySelectorAll(".row-sel").forEach((cb) => {
        cb.addEventListener("change", () => {
          const key = cb.dataset.key;
          const row = this.rows.find((r) => this._rowKey(r) === key);
          if (cb.checked) this._sel.set(key, row);
          else this._sel.delete(key);
          this._syncSelBoxes();
          this._emitSel();
        });
      });
    }

    if (this.o.rowClick) {
      this.tbody.querySelectorAll("tr.clickable").forEach((tr) => {
        tr.addEventListener("click", (e) => {
          if (e.target.closest("button, a, input, select")) return;
          const row = this.rows.find((r) => this._rowKey(r) === tr.dataset.key);
          if (row) this.o.rowClick(row);
        });
      });
    }

    this._renderPagination(total, pages);
  }

  _syncSelBoxes() {
    const all = this._visibleRows().map((r) => this._rowKey(r));
    const allChecked = all.length > 0 && all.every((k) => this._sel.has(k));
    const selAll = this.thead.querySelector(".sel-all");
    if (selAll) {
      selAll.checked = allChecked;
      selAll.indeterminate = all.length > 0 && !allChecked && all.some((k) => this._sel.has(k));
    }
  }

  _emitSel() {
    if (this.o.onSelChange) this.o.onSelChange([...this._sel.values()]);
  }

  selectedRows() {
    return [...this._sel.values()];
  }

  setSearch(val) {
    const input = this.el.querySelector(".toolbar input[type='search']");
    if (input) {
      input.value = val || "";
      this.search = (val || "").trim().toLowerCase();
      this.page = 1;
      this._render();
    }
  }

  _colCount() {
    return this.o.columns.length + (this.o.selectable ? 1 : 0);
  }

  _renderPagination(total, pages) {
    if (!this.o.pageSize) return; // pageSize 0: no client pagination (providers uses its own pager)
    let el = this.el.querySelector(".pagination");
    if (!el) {
      el = document.createElement("div");
      el.className = "pagination";
      this.el.appendChild(el);
    }
    if (pages <= 1) {
      el.innerHTML = "";
      return;
    }
    const cur = this.page;
    const btn = (label, p, cls, disabled) =>
      `<button ${disabled ? "disabled" : ""} class="${cls || ""}" data-pg="${p}">${label}</button>`;
    let html = btn("« Prev", cur - 1, "", cur <= 1);
    const start = Math.max(1, cur - 3);
    const end = Math.min(pages, cur + 3);
    if (start > 1) html += btn("1", 1, "", false);
    if (start > 2) html += `<span>…</span>`;
    for (let i = start; i <= end; i++) html += btn(String(i), i, i === cur ? "active" : "", false);
    if (end < pages - 1) html += `<span>…</span>`;
    if (end < pages) html += btn(String(pages), pages, "", false);
    html += btn("Next »", cur + 1, "", cur >= pages);
    el.innerHTML = html;
    el.querySelectorAll("button").forEach((b) => {
      b.addEventListener("click", () => {
        this.page = +b.dataset.pg;
        this._render();
      });
    });
  }
}

// ---------- shared rendering helpers ----------
function statusPill(st) {
  const m = { completed: "ok", stopped: "warn", running: "run", discarded: "bad" };
  return `<span class="pill ${m[st] || ""}">${esc(st)}</span>`;
}

function healthIcon(t) {
  return { healthy: "⭐", medium: "⚠️", slow: "🐢", unknown: "?" }[t] || "?";
}

function stateText(on) {
  return `<span class="${on ? "state-enabled" : "state-disabled"}">${on ? "ENABLED" : "DISABLED"}</span>`;
}

function checkmark(v) {
  if (v === 1) return "✔";
  if (v === 0) return "✘";
  return "?";
}