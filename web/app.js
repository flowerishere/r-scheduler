
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
  $("table-body").replaceChildren(); $("detail-content").replaceChildren();
  $("detail-actions").replaceChildren(); $("preview-result").replaceChildren();
  $("schedule-form").reset(); $("filters").reset();
  state.editing = null; state.creation = null; state.offset = 0; state.filters = {};
  $("toast").hidden = true; hideError("editor-error"); hideError("list-error");
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
  const headers = schedules ? ["计划 / 目标", "触发方式", "状态", "下次触发", "操作"] : ["计划 / 实例", "尝试次数", "状态", "原计划时间", "操作"];
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
  $("empty-description").textContent = filtered ? "调整筛选条件，或者清除后查看全部记录。" : state.view === "schedules" ? "创建第一个计划，安排一次延迟任务或周期执行。" : "计划到期并生成实例后，执行结果会显示在这里。";
  $("empty-create").hidden = Boolean(filtered) || state.view !== "schedules";
  for (const item of items) {
    const row = node("tr"), title = node("td"), type = node("td"), status = node("td"), time = node("td"), actions = node("td");
    const isSchedule = state.view === "schedules";
    const open = () => void openDetail(isSchedule ? "schedule" : "run", item.id);
    const name = action(item.spec.name, open, "row-title");
    name.setAttribute("aria-label", `查看${isSchedule ? "计划" : "执行"}：${item.spec.name}`);
    let subtitle = item.id.slice(0, 8);
    if (isSchedule) { try { subtitle = new URL(item.spec.target.url).host; } catch { /* API validates URLs. */ } }
    title.append(name, node("span", subtitle, "row-subtitle"));
    type.append(isSchedule ? node("span", triggerNames[item.spec.trigger.type], "trigger-badge") : node("span", `${item.attempt_count} 次`));
    status.append(badge(item.status));
    const value = isSchedule ? item.next_fire_at : item.scheduled_at;
    const stamp = node("span", date(value), "time-value"); stamp.title = value || "没有后续触发时间"; time.append(stamp);
    const links = node("div", null, "row-actions");
    links.append(action("详情", open, ""));
    if (isSchedule) {
      if (item.status !== "cancelled") links.append(action("编辑", () => void editSchedule(item.id), ""));
      if (["active", "completed"].includes(item.status)) links.append(action("暂停", (button) => mutateSchedule(item.id, "pause", button), ""));
      if (["paused", "error"].includes(item.status)) links.append(action("恢复", (button) => mutateSchedule(item.id, "resume", button), ""));
    } else if (item.status === "dead") links.append(action("重放", (button) => replayRun(item, button), ""));
    actions.append(links); row.append(title, type, status, time, actions); $("table-body").append(row);
  }
}

function invalidatePreview() {
  state.preview++; $("preview-result").replaceChildren(); $("preview-trigger").disabled = false;
}
function triggerFields() {
  const type = $("trigger-type").value;
  for (const section of document.querySelectorAll("[data-trigger]")) {
    section.hidden = section.dataset.trigger !== type;
    for (const input of section.querySelectorAll("input,textarea")) input.disabled = section.hidden;
  }
  invalidatePreview();
}
$("trigger-type").addEventListener("change", triggerFields);
for (const id of ["delay-seconds", "once-at", "cron-expression", "cron-timezone", "rrule-value"]) $(id).addEventListener("input", invalidatePreview);
function openEditor(schedule = null) {
  state.editor++; $("save-schedule").disabled = false;
  $("schedule-form").reset(); $("advanced").open = false; state.editing = schedule; state.creation = null;
  hideError("editor-error"); $("preview-result").replaceChildren();
  $("editor-title").textContent = schedule ? "编辑计划" : "新建计划";
  $("save-schedule").textContent = schedule ? "保存修改" : "创建计划";
  $("edit-notice").hidden = !schedule; $("reload-editor").hidden = !schedule;
  const tomorrow = new Date(Date.now() + 86400000).toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
  $("rrule-value").value = `DTSTART:${tomorrow}\nRRULE:FREQ=DAILY`;
  $("once-at").value = new Date(Date.now() + 3600000).toISOString();
  if (schedule) {
    const spec = schedule.spec;
    $("job-name").value = spec.name; $("trigger-type").value = spec.trigger.type;
    if (spec.trigger.type === "once") $("once-at").value = spec.trigger.at;
    if (spec.trigger.type === "delay") $("delay-seconds").value = spec.trigger.seconds;
    if (spec.trigger.type === "cron") { $("cron-expression").value = spec.trigger.expression; $("cron-timezone").value = spec.trigger.timezone; }
    if (spec.trigger.type === "rrule") $("rrule-value").value = spec.trigger.value;
    $("target-url").value = spec.target.url; $("target-headers").value = JSON.stringify(spec.target.headers, null, 2);
    $("payload").value = JSON.stringify(spec.payload, null, 2); $("timeout-seconds").value = spec.target.timeout_seconds;
    $("max-attempts").value = spec.retry.max_attempts; $("initial-delay").value = spec.retry.initial_delay_seconds;
    $("max-delay").value = spec.retry.max_delay_seconds; $("max-age").value = spec.retry.max_age_seconds ?? "";
    $("misfire").value = spec.misfire; $("misfire-grace").value = spec.misfire_grace_seconds; $("concurrency").value = spec.concurrency;
  }
  triggerFields(); if (!$("editor").open) $("editor").showModal(); $("job-name").focus();
}
for (const id of ["create-schedule", "empty-create"]) $(id).addEventListener("click", () => openEditor());
async function editSchedule(id) {
  const editor = ++state.editor, detail = state.detail, fromDetail = $("detail").open;
  const current = () => editor === state.editor && (!fromDetail || (detail === state.detail && $("detail").open));
  try {
    const schedule = await api(`/schedules/${id}`);
    if (!current()) return;
    if (schedule.status === "cancelled") throw new Error("计划已被取消，不能继续编辑。");
    $("detail").close(); openEditor(schedule);
  } catch (error) { if (current() && !silent(error) && state.key) reportActionError(error); }
}
$("reload-editor").addEventListener("click", async () => {
  if (!state.editing) return;
  if (await confirm("重新载入计划", "这会放弃当前未保存的修改，重新读取最新版本。", "重新载入")) await editSchedule(state.editing.id);
});

function integer(id) { const value = Number($(id).value); if (!$(id).value.trim() || !Number.isSafeInteger(value)) throw new Error("请输入有效的整数。"); return value; }
function readTrigger() {
  const type = $("trigger-type").value;
  if (type === "delay") return {type, seconds: integer("delay-seconds")};
  if (type === "once") {
    const value = $("once-at").value.trim();
    if (!/(Z|[+-]\d{2}:\d{2})$/i.test(value) || Number.isNaN(Date.parse(value))) throw new Error("执行时间需要包含时区，例如 2026-10-01T09:00:00+08:00。");
    // Preserve sub-millisecond precision and let the API validate the actual
    // calendar date. Date.toISOString() silently normalizes e.g. February 30.
    return {type, at: value};
  }
  if (type === "cron") return {type, expression: $("cron-expression").value.trim(), timezone: $("cron-timezone").value.trim()};
  return {type, value: $("rrule-value").value.trim()};
}
function parseJson(id, label) {
  try {
    return JSON.parse($(id).value);
  } catch (error) {
    if (error instanceof RangeError) throw new Error(`${label} 中的数字超出支持范围，请改用字符串传递。`);
    throw new Error(`${label} 不是有效的 JSON。`);
  }
}
function readSpec() {
  const headers = parseJson("target-headers", "请求头");
  if (!headers || Array.isArray(headers) || typeof headers !== "object" || Object.values(headers).some((value) => typeof value !== "string")) throw new Error("请求头必须是值为字符串的 JSON 对象。");
  const retry = {max_attempts: integer("max-attempts"), initial_delay_seconds: integer("initial-delay"), max_delay_seconds: integer("max-delay")};
  if ($("max-age").value) retry.max_age_seconds = integer("max-age");
  return {name: $("job-name").value.trim(), trigger: readTrigger(), target: {url: $("target-url").value.trim(), headers, timeout_seconds: integer("timeout-seconds")}, payload: parseJson("payload", "Payload"), retry, misfire: $("misfire").value, misfire_grace_seconds: integer("misfire-grace"), concurrency: $("concurrency").value};
}
$("preview-trigger").addEventListener("click", async () => {
  const editor = state.editor, previewId = ++state.preview;
  const current = () => editor === state.editor && previewId === state.preview && $("editor").open;
  const button = $("preview-trigger"); button.disabled = true; $("preview-result").replaceChildren();
  try {
    const preview = await api("/preview", {method: "POST", body: {trigger: readTrigger(), count: 5}});
    if (!current()) return;
    const list = node("ol"); for (const value of preview.dates) { const item = node("li", date(value)); item.title = value; list.append(item); }
    $("preview-result").append(list);
    if (!preview.dates.length) $("preview-result").append(node("p", "当前时间之后没有触发点。0 秒延迟创建后仍会立即进入调度。", "field-help"));
    else if (preview.exhausted) $("preview-result").append(node("p", "以上为全部剩余触发时间。", "field-help"));
  } catch (error) { if (current() && !silent(error)) $("preview-result").append(node("p", error.message, "error")); }
  finally { if (current()) button.disabled = false; }
});
$("schedule-form").addEventListener("invalid", (event) => {
  const details = event.target.closest("details");
  if (details) details.open = true;
}, true);
$("schedule-form").addEventListener("submit", async (event) => {
  const editor = state.editor, editing = state.editing, session = state.session;
  event.preventDefault(); hideError("editor-error"); $("save-schedule").disabled = true;
  try {
    const spec = readSpec();
    if (editing) await api(`/schedules/${editing.id}`, {method: "PUT", body: {expected_revision: editing.revision, spec}});
    else {
      const encoded = JSON.stringify(spec);
      if (!state.creation || state.creation.encoded !== encoded) state.creation = {encoded, key: `console-${Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte) => byte.toString(16).padStart(2, "0")).join("")}`};
      await api("/schedules", {method: "POST", body: spec, idempotency: state.creation.key});
    }
    if (session !== state.session) return;
    toast(editing ? "计划已更新" : "计划已创建");
    if (editor === state.editor) {
      $("editor").close();
      await switchView("schedules");
    }
    else await refresh(true);
  } catch (error) {
    if (!silent(error) && state.key && editor === state.editor) { showError("editor-error", error); $("editor-error").scrollIntoView({block: "nearest"}); }
  } finally { if (editor === state.editor) $("save-schedule").disabled = false; }
});
$("editor").addEventListener("close", () => { state.editor++; invalidatePreview(); });

for (const button of document.querySelectorAll("[data-close]")) button.addEventListener("click", () => $(button.dataset.close).close());
function confirm(title, description, label) {
  return new Promise((resolve) => {
    $("confirm-title").textContent = title; $("confirm-description").textContent = description; $("confirm-yes").textContent = label;
    const dialog = $("confirm"); dialog.returnValue = "no";
    const closed = () => { dialog.removeEventListener("close", closed); resolve(dialog.returnValue === "yes"); };
    dialog.addEventListener("close", closed); dialog.showModal(); $("confirm-no").focus();
  });
}
$("confirm-no").addEventListener("click", () => $("confirm").close("no"));
$("confirm-yes").addEventListener("click", () => $("confirm").close("yes"));

async function mutateSchedule(id, actionName, button) {
  const detail = state.detail;
  button.disabled = true;
  try {
    if (actionName === "cancel" && !await confirm("取消这个计划？", "取消后不能恢复。尚未开始的实例会被取消，已经开始的投递仍可能完成。", "取消计划")) return;
    if (!state.key) return;
    await api(`/schedules/${id}/${actionName}`, {method: "POST"});
    toast({pause: "计划已暂停", resume: "计划已恢复", cancel: "计划已取消"}[actionName]);
    if ($("detail").open && state.detail === detail) await openDetail("schedule", id);
    await refresh();
  } catch (error) { if (!silent(error) && state.key) reportActionError(error); }
  finally { button.disabled = false; }
}
async function replayRun(run, button) {
  const detail = state.detail;
  button.disabled = true;
  try {
    if (!await confirm("重新投递这个实例？", "实例将立即重新入队，保留原 run_id 和投递历史。接收端可能再次收到请求。", "确认重放")) return;
    if (!state.key) return;
    await api(`/runs/${run.id}/replay`, {method: "POST"}); toast("实例已重新入队");
    if ($("detail").open && state.detail === detail) await openDetail("run", run.id);
    await refresh();
  } catch (error) { if (!silent(error) && state.key) reportActionError(error); }
  finally { button.disabled = false; }
}

function reportActionError(error) {
  if ($("detail").open) { const message = node("p", error.message, "error"); message.setAttribute("role", "alert"); $("detail-content").prepend(message); message.scrollIntoView({block: "nearest"}); }
  else toast(error.message);
}

function fact(grid, title, value) { const group = node("div"); const description = node("dd"); description.append(value instanceof Node ? value : document.createTextNode(String(value ?? "—"))); group.append(node("dt", title), description); grid.append(group); }
function section(title, content) { const wrapper = node("section", null, "detail-section"); wrapper.append(node("h3", title), content); return wrapper; }
function attemptHistory(_id, rows, _detail) {
  const timeline = node("div");
  if (!rows.length) timeline.append(node("p", "尚无投递记录。", "muted"));
  for (const attempt of rows) {
    const card = node("article", null, "attempt-card"), title = node("div", null, "attempt-title");
    title.append(node("strong", `第 ${attempt.number} 次尝试`), badge(attempt.status));
    card.append(title, node("p", `${date(attempt.started_at)} → ${date(attempt.finished_at)}`));
    if (attempt.http_status) card.append(node("p", `HTTP ${attempt.http_status}`));
    if (attempt.error) card.append(node("p", attempt.error));
    if (attempt.response_excerpt) card.append(node("pre", attempt.response_excerpt));
    timeline.append(card);
  }
  return timeline;
}
async function openDetail(kind, id) {
  const request = ++state.detail;
  $("detail-title").textContent = "正在读取…"; $("detail-eyebrow").textContent = kind === "schedule" ? "SCHEDULE" : "RUN";
  $("detail-content").replaceChildren(); $("detail-actions").replaceChildren();
  if (!$("detail").open) $("detail").showModal();
  try {
    const item = await api(`/${kind === "schedule" ? "schedules" : "runs"}/${id}`);
    const attempts = kind === "run" ? await api(`/runs/${id}/attempts`) : [];
    if (request !== state.detail || !$("detail").open) return;
    $("detail-title").textContent = item.spec.name;
    const grid = node("dl", null, "detail-grid");
    fact(grid, kind === "schedule" ? "计划 ID" : "实例 ID", item.id); fact(grid, "状态", badge(item.status));
    fact(grid, "规则版本", `v${item.revision}`); fact(grid, "创建时间", date(item.created_at));
    if (kind === "schedule") {
      fact(grid, "下次触发", date(item.next_fire_at)); fact(grid, "更新时间", date(item.updated_at));
      $("detail-actions").append(action("执行记录", async () => { $("detail").close(); await switchView("runs", id); }));
      if (item.status !== "cancelled") {
        $("detail-actions").append(action("编辑计划", () => editSchedule(id)));
        const verb = ["paused", "error"].includes(item.status) ? "resume" : "pause";
        $("detail-actions").append(action(verb === "resume" ? "恢复计划" : "暂停计划", (button) => mutateSchedule(id, verb, button)));
        $("detail-actions").append(action("取消计划", (button) => mutateSchedule(id, "cancel", button), "danger compact"));
      }
    } else {
      fact(grid, "所属计划", action(item.schedule_id, () => openDetail("schedule", item.schedule_id), "text-button compact"));
      fact(grid, "原计划时间", date(item.scheduled_at)); fact(grid, "允许认领时间", date(item.available_at));
      fact(grid, "认领截止时间", item.expires_at ? date(item.expires_at) : "不限制");
      fact(grid, "尝试次数 / 本轮", `${item.attempt_count} / ${item.cycle_attempts}`); fact(grid, "完成时间", date(item.finished_at));
      fact(grid, "租约截止时间", date(item.lease_until));
      if (item.status === "dead") {
        const replay = action("重放实例", (button) => replayRun(item, button), "primary compact");
        if (item.expires_at && new Date(item.expires_at).getTime() <= Date.now()) { replay.disabled = true; replay.title = "实例已超过认领截止时间"; }
        $("detail-actions").append(replay);
      }
    }
    $("detail-actions").append(action("刷新详情", () => openDetail(kind, id)));
    $("detail-content").append(grid);
    if (item.last_error) $("detail-content").append(node("p", item.last_error, "error"));
    if (kind === "run") {
      $("detail-content").append(section("投递尝试", attemptHistory(id, attempts, request)));
    }
    $("detail-content").append(section(kind === "run" ? "本次执行的计划快照" : "计划配置", jsonBox(item.spec)));
  } catch (error) { if (!silent(error) && request === state.detail && $("detail").open) $("detail-content").append(node("p", error.message, "error")); }
}
$("detail").addEventListener("close", () => { state.detail++; });
