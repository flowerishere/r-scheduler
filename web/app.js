
const $ = (id) => document.getElementById(id);
const statuses = {active: "运行中", paused: "已暂停", completed: "已生成完毕", cancelled: "已取消", error: "规则错误", pending: "待执行", running: "投递中", succeeded: "成功", dead: "失败终止", failed: "失败", lease_expired: "租约过期"};
const triggerNames = {once: "ONCE", delay: "DELAY", cron: "CRON", rrule: "RRULE"};
const state = {key: "", session: 0, view: "schedules", offset: 0, filters: {}, request: 0, detail: 0, editor: 0, preview: 0, editing: null, creation: null, timer: null, toast: null, controllers: new Set()};
const PAGE_SIZE = 20;
const dateFormat = new Intl.DateTimeFormat("zh-CN", {year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false});

function node(tag, text, className) {
  const element = document.createElement(tag);
  if (text !== undefined && text !== null) element.textContent = String(text);
  if (className) element.className = className;
  return element;
}
function date(value) { return value ? dateFormat.format(new Date(value)) : "—"; }
function showError(id, error) { $(id).textContent = error?.message || String(error); $(id).hidden = false; }
function hideError(id) { $(id).textContent = ""; $(id).hidden = true; }
function toast(message) { clearTimeout(state.toast); $("toast").textContent = message; $("toast").hidden = false; state.toast = setTimeout(() => { $("toast").hidden = true; }, 4000); }
function badge(status) { return node("span", statuses[status] || status, `badge ${status}`); }
function action(text, handler, className = "secondary compact") {
  const button = node("button", text, className); button.type = "button";
  button.addEventListener("click", () => void handler(button)); return button;
}
function jsonBox(value) { return node("pre", JSON.stringify(value, null, 2), "json-box"); }
function silent(error) { return error?.name === "AbortError"; }

async function api(path, {method = "GET", body, idempotency} = {}) {
  const session = state.session;
  const controller = new AbortController();
  state.controllers.add(controller);
  const timeout = setTimeout(() => controller.abort(), 20000);
  try {
    const headers = {Authorization: `Bearer ${state.key}`};
    if (body !== undefined) headers["Content-Type"] = "application/json";
    if (idempotency) headers["Idempotency-Key"] = idempotency;
    const response = await fetch(`/v1${path}`, {method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal: controller.signal, cache: "no-store", credentials: "omit", redirect: "error"});
    const text = await response.text();
    if (session !== state.session) throw new DOMException("Session changed", "AbortError");
    let data;
    try { data = JSON.parse(text); } catch { data = {error: text || `HTTP ${response.status}`}; }
    if (!response.ok) {
      const error = new Error(response.status === 409 ? `版本或状态冲突：${data.error}` : data.error || `HTTP ${response.status}`);
      error.status = response.status;
      if (response.status === 401) logout("密钥无效或已被撤销，请重新连接。");
      throw error;
    }
    return data;
  } catch (error) {
    if (session !== state.session) throw new DOMException("Session changed", "AbortError");
    if (controller.signal.aborted) throw new Error("请求超时，请检查连接后重试。");
    if (error instanceof TypeError) throw new Error("无法连接调度服务，请检查网络后重试。");
    throw error;
  } finally { clearTimeout(timeout); state.controllers.delete(controller); }
}

function logout(message = "") {
  state.key = ""; state.session++; state.request++; state.detail++;
  clearInterval(state.timer); state.timer = null; clearTimeout(state.toast);
  for (const controller of state.controllers) controller.abort();
  state.controllers.clear();
  for (const dialog of document.querySelectorAll("dialog[open]")) dialog.close();
  $("workspace").hidden = true; $("login-screen").hidden = false;
  $("api-key").value = ""; $("tenant-name").textContent = ""; $("connect-button").disabled = false;
  $("table-body").replaceChildren();
  $("filters").reset();
  state.editing = null; state.creation = null; state.offset = 0; state.filters = {};
  $("toast").hidden = true; hideError("list-error");
  for (const id of ["stat-active", "stat-pending", "stat-succeeded", "stat-dead", "schedule-count"]) $(id).textContent = "—";
  if (message) showError("login-error", message); else hideError("login-error");
  $("api-key").focus();
}

$("endpoint").textContent = location.origin;
document.querySelector(".timezone-label").textContent = `时间：${Intl.DateTimeFormat().resolvedOptions().timeZone}`;
$("login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if ($("connect-button").disabled) return;
  hideError("login-error");
  state.key = $("api-key").value.trim(); $("api-key").value = "";
  if (!state.key) { showError("login-error", "请输入 API key。"); return; }
  const session = ++state.session;
  $("connect-button").disabled = true;
  try {
    const identity = await api("/me");
    $("tenant-name").textContent = identity.tenant_id;
    $("login-screen").hidden = true; $("workspace").hidden = false;
    state.view = "schedules"; state.offset = 0; $("auto-refresh").checked = true;
    configureView(); await refresh();
    if (session !== state.session || !state.key) return;
    $("main-content").focus();
    clearInterval(state.timer);
    state.timer = setInterval(() => {
      if (state.key && $("auto-refresh").checked && !document.hidden && !document.querySelector("dialog[open]")) void refresh(true);
    }, 10000);
  } catch (error) { if (session === state.session && !silent(error)) logout(error.message); }
  finally { if (session === state.session) $("connect-button").disabled = false; }
});
$("logout").addEventListener("click", () => logout());
// Clear credentials before a document can enter the browser's back/forward cache.
window.addEventListener("pagehide", () => logout());

function configureView() {
  const schedules = state.view === "schedules";
  const title = schedules ? "调度计划" : "执行记录";
  $("page-title").textContent = title; $("breadcrumb").textContent = title;
  $("page-description").textContent = schedules ? "安排下一次执行，掌握每一个周期。" : "查看每次投递的结果，定位错误并追踪重试。";
  $("list-title").textContent = schedules ? "全部计划" : "执行历史";
  $("search-field").hidden = !schedules; $("run-schedule-field").hidden = schedules;
  $("status-filter").replaceChildren(new Option("全部状态", ""), ...(schedules ? ["active", "paused", "completed", "error", "cancelled"] : ["pending", "running", "succeeded", "dead", "cancelled"]).map((s) => new Option(statuses[s], s)));
  for (const button of document.querySelectorAll("[data-view]")) {
    const active = button.dataset.view === state.view;
    button.classList.toggle("active", active);
    if (active) button.setAttribute("aria-current", "page"); else button.removeAttribute("aria-current");
  }
  const headers = schedules ? ["计划 / 目标", "触发方式", "状态", "下次触发", "ID"] : ["计划 / 实例", "尝试次数", "状态", "原计划时间", "ID"];
  const row = node("tr");
  for (const title of headers) { const th = node("th", title); th.scope = "col"; row.append(th); }
  $("table-head").replaceChildren(row);
  $("table-body").replaceChildren(); $("table-wrap").hidden = true; $("empty-state").hidden = true;
}

async function switchView(view, scheduleId = "") {
  state.view = view; state.offset = 0; $("filters").reset(); configureView();
  $("run-schedule-id").value = scheduleId;
  applyFilters();
  await refresh();
}
for (const button of document.querySelectorAll("[data-view]")) button.addEventListener("click", () => void switchView(button.dataset.view));
$("refresh").addEventListener("click", () => void refresh());
function applyFilters() {
  state.filters = {status: $("status-filter").value};
  if (state.view === "schedules") {
    state.filters.q = $("search").value.trim();
  }
  else state.filters.schedule_id = $("run-schedule-id").value.trim();
  state.offset = 0;
}
$("filters").addEventListener("submit", (event) => {
  event.preventDefault();
  applyFilters(); void refresh();
});
$("clear-filters").addEventListener("click", () => { $("filters").reset(); applyFilters(); void refresh(); });
$("previous-page").addEventListener("click", () => { state.offset = Math.max(0, state.offset - PAGE_SIZE); void refresh(); });
$("next-page").addEventListener("click", () => { state.offset += PAGE_SIZE; void refresh(); });

async function refresh(background = false) {
  if (!state.key) return;
  const request = ++state.request, session = state.session, view = state.view;
  const current = () => request === state.request && session === state.session;
  const query = new URLSearchParams({limit: String(PAGE_SIZE + 1), offset: String(state.offset)});
  for (const [name, value] of Object.entries(state.filters)) if (value) query.set(name, value);
  hideError("list-error");
  $("refresh").disabled = true;
  if (!background) { $("loading").hidden = false; $("table-wrap").hidden = true; $("empty-state").hidden = true; }
  try {
    const [items, stats] = await Promise.all([api(`/${view}?${query}`), api("/stats")]);
    if (!current()) return;
    $("stat-active").textContent = stats.schedules.active || 0;
    $("stat-pending").textContent = stats.runs.pending || 0;
    $("stat-succeeded").textContent = stats.runs.succeeded || 0;
    $("stat-dead").textContent = stats.runs.dead || 0;
    $("schedule-count").textContent = Object.values(stats.schedules).reduce((a, b) => a + Number(b), 0);
    renderRows(items.slice(0, PAGE_SIZE));
    $("previous-page").disabled = state.offset === 0;
    $("next-page").disabled = items.length <= PAGE_SIZE || state.offset + PAGE_SIZE > 100000;
    $("page-summary").textContent = items.length ? `第 ${state.offset + 1}–${state.offset + Math.min(items.length, PAGE_SIZE)} 条 · 每页 ${PAGE_SIZE} 条` : "当前条件下没有记录";
    $("last-refreshed").textContent = `更新于 ${new Date().toLocaleTimeString("zh-CN", {hour12: false})}`;
    $("connection-state").classList.remove("offline"); $("connection-state").replaceChildren(node("i"), document.createTextNode(" 已连接"));
  } catch (error) {
    if (current() && !silent(error)) {
      showError("list-error", error); $("previous-page").disabled = true; $("next-page").disabled = true;
      $("connection-state").classList.add("offline"); $("connection-state").replaceChildren(node("i"), document.createTextNode(" 刷新失败"));
    }
  } finally { if (current()) { $("loading").hidden = true; $("refresh").disabled = false; } }
}

function renderRows(items) {
  $("table-body").replaceChildren(); $("table-wrap").hidden = items.length === 0; $("empty-state").hidden = items.length !== 0;
  const filtered = Object.values(state.filters).some(Boolean) || state.offset;
  $("empty-title").textContent = filtered ? "没有匹配的记录" : state.view === "schedules" ? "还没有调度计划" : "还没有执行记录";
  $("empty-description").textContent = filtered ? "调整筛选条件，或者清除后查看全部记录。" : "通过 API 创建计划后，可在这里查看执行情况。";
  for (const item of items) {
    const row = node("tr"), title = node("td"), type = node("td"), status = node("td"), time = node("td"), identity = node("td");
    const schedule = state.view === "schedules";
    title.append(node("strong", item.spec.name, "row-title"), node("span", new URL(item.spec.target.url).host, "row-subtitle"));
    type.append(node("span", schedule ? triggerNames[item.spec.trigger.type] : `${item.attempt_count} 次`, "trigger-badge"));
    status.append(badge(item.status));
    const timestamp = schedule ? item.next_fire_at : item.scheduled_at;
    const stamp = node("span", date(timestamp), "time-value"); stamp.title = timestamp || "没有后续触发时间"; time.append(stamp);
    const shortId = node("code", item.id.slice(0, 8)); shortId.title = item.id; identity.append(shortId);
    row.append(title, type, status, time, identity); $("table-body").append(row);
  }
}
