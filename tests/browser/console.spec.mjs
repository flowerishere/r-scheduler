import { test, expect } from "@playwright/test";

const key = "console-browser-test-key";
const otherKey = "other-browser-test-key";
const auth = (token = key) => ({Authorization: `Bearer ${token}`});
let callback;

test.beforeAll(async ({request}) => {
  await expect.poll(async () => {
    const response = await request.get("/v1/schedules?q=browser-test-callback", {headers: auth()});
    const schedules = await response.json();
    callback = schedules[0]?.spec.target.url;
    return Boolean(callback);
  }).toBe(true);
});

test.beforeEach(async ({page}) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  test.info()._browserErrors = errors;
});
test.afterEach(async () => { expect(test.info()._browserErrors).toEqual([]); });

async function login(page, token = key) {
  await page.goto("/console/");
  await page.getByLabel("API key", {exact: true}).fill(token);
  await page.getByRole("button", {name: "连接工作空间"}).click();
  await expect(page.locator("#workspace")).toBeVisible();
  await expect(page.locator("#loading")).toBeHidden();
}
async function create(request, name, extra = {}, token = key) {
  const response = await request.post("/v1/schedules", {headers: auth(token), data: {
    name, trigger: {type: "delay", seconds: 86400}, target: {url: callback}, ...extra,
  }});
  expect(response.ok()).toBeTruthy();
  return response.json();
}
async function search(page, name) {
  await page.getByPlaceholder("搜索计划名称或完整 ID").fill(name);
  await page.getByRole("button", {name: "筛选", exact: true}).click();
  await expect(page.locator("#loading")).toBeHidden();
}
async function getSchedule(request, id) {
  const response = await request.get(`/v1/schedules/${id}`, {headers: auth()});
  return response.json();
}

async function pendingRetry(request, name) {
  const schedule = await create(request, name, {
    trigger: {type: "delay", seconds: 0}, target: {url: callback.replace(/\/ok$/, "/fail")},
    retry: {max_attempts: 3, initial_delay_seconds: 86400, max_delay_seconds: 86400, max_age_seconds: 172800},
  });
  let run;
  await expect.poll(async () => {
    const rows = await (await request.get(`/v1/runs?schedule_id=${schedule.id}`, {headers: auth()})).json();
    run = rows[0];
    return run?.status === "pending" && run?.attempt_count === 1;
  }).toBe(true);
  expect((await request.post(`/v1/schedules/${schedule.id}/pause`, {headers: auth()})).ok()).toBeTruthy();
  return {schedule, run};
}

async function openRun(page, run) {
  await login(page);
  await page.getByRole("button", {name: "执行记录", exact: true}).click();
  await page.locator("#run-schedule-id").fill(run.schedule_id);
  await page.getByRole("button", {name: "筛选", exact: true}).click();
  const row = page.locator("tbody tr").filter({hasText: run.spec.name});
  await row.getByRole("button", {name: "详情", exact: true}).click();
  await expect(page.getByRole("heading", {name: "管理待执行实例", exact: true})).toBeVisible();
}

test("login, memory-only credentials, tenant isolation and reload", async ({page, request}) => {
  await page.goto("/console");
  await page.getByLabel("API key", {exact: true}).fill("invalid-key");
  await page.getByRole("button", {name: "连接工作空间"}).click();
  await expect(page.locator("#login-error")).toBeVisible();
  await expect(page.locator("#workspace")).toBeHidden();
  await login(page);
  await expect(page.locator("#tenant-name")).toHaveText("console");
  const privateSchedule = await create(request, "tenant-private-example");
  await search(page, privateSchedule.spec.name);
  await expect(page.getByRole("button", {name: `查看计划：${privateSchedule.spec.name}`})).toBeVisible();
  expect(await page.evaluate(() => [localStorage.length, sessionStorage.length])).toEqual([0, 0]);
  await page.getByRole("button", {name: "退出工作空间"}).click();
  await expect(page.locator("#table-body")).toBeEmpty();
  await login(page, otherKey);
  await expect(page.locator("#tenant-name")).toHaveText("other");
  await expect(page.locator("#empty-state")).toBeVisible();
  await page.reload();
  await expect(page.locator("#login-screen")).toBeVisible();
  await expect(page.getByLabel("API key", {exact: true})).toHaveValue("");
  await login(page);
  await page.goto("/");
  await page.goBack();
  await expect(page.locator("#login-screen")).toBeVisible();
});

test("create, preview, pause, edit, resume and cancel a schedule", async ({page, request}) => {
  await login(page);
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("console-cron-example");
  await page.getByLabel("触发方式").selectOption("cron");
  await page.getByRole("button", {name: "预览未来 5 次触发"}).click();
  await expect(page.locator("#preview-result li")).toHaveCount(5);
  await page.getByLabel("目标 URL").fill(callback);
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect(page.locator("#editor")).toBeHidden();
  await search(page, "console-cron-example");
  const row = page.locator("tbody tr").filter({hasText: "console-cron-example"});
  await row.getByRole("button", {name: "暂停", exact: true}).click();
  await expect(row).toContainText("已暂停");
  await row.getByRole("button", {name: "编辑", exact: true}).click();
  await expect(page.getByLabel("计划名称", {exact: true})).toHaveValue("console-cron-example");
  await page.getByLabel("计划名称", {exact: true}).fill("console-cron-edited");
  await page.getByLabel("Payload（JSON）").fill('{"note":"<img src=x onerror=alert(1)>"}');
  await page.getByRole("button", {name: "保存修改"}).click();
  await expect(page.locator("#editor")).toBeHidden();
  await search(page, "console-cron-edited");
  const edited = page.locator("tbody tr").filter({hasText: "console-cron-edited"});
  await expect(edited).toContainText("已暂停");
  await edited.getByRole("button", {name: "恢复", exact: true}).click();
  await expect(edited).toContainText("运行中");
  await edited.getByRole("button", {name: "详情", exact: true}).click();
  await expect(page.locator("#detail-content")).toContainText("<img src=x onerror=alert(1)>");
  await expect(page.locator("#detail-content img")).toHaveCount(0);
  await page.getByRole("button", {name: "取消计划", exact: true}).click();
  await page.locator("#confirm").getByRole("button", {name: "取消计划", exact: true}).click();
  await expect(page.locator("#detail-content")).toContainText("已取消");
  await page.getByRole("button", {name: "关闭详情"}).click();
  const schedules = await (await request.get("/v1/schedules?q=console-cron-edited", {headers: auth()})).json();
  expect(schedules[0].status).toBe("cancelled");
  expect(schedules[0].revision).toBe(2);
});

test("RRULE preview and input errors leave the form intact", async ({page}) => {
  await login(page);
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("rrule-form-example");
  await page.getByLabel("触发方式").selectOption("rrule");
  await page.getByLabel("RRULE 规则集").fill("DTSTART:20300101T090000Z\nRRULE:FREQ=DAILY;COUNT=3");
  await page.getByRole("button", {name: "预览未来 5 次触发"}).click();
  await expect(page.locator("#preview-result li")).toHaveCount(3);
  await page.getByLabel("目标 URL").fill(callback);
  await page.getByLabel("Payload（JSON）").fill("{broken");
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect(page.locator("#editor-error")).toContainText("Payload 不是有效的 JSON");
  await expect(page.getByLabel("计划名称", {exact: true})).toHaveValue("rrule-form-example");
  await page.getByLabel("Payload（JSON）").fill("{}");
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect(page.locator("#editor")).toBeHidden();
  await search(page, "rrule-form-example");
  await expect(page.locator("tbody")).toContainText("RRULE");
});

test("concurrent edits surface a revision conflict and can reload", async ({page, request}) => {
  const original = await create(request, "revision-browser-example");
  await login(page); await search(page, original.spec.name);
  await page.locator("tbody").getByRole("button", {name: "编辑", exact: true}).click();
  await expect(page.locator("#editor")).toBeVisible();
  const external = {...original.spec, name: "changed-by-another-client"};
  const response = await request.put(`/v1/schedules/${original.id}`, {headers: auth(), data: {expected_revision: 1, spec: external}});
  expect(response.ok()).toBeTruthy();
  await page.getByLabel("计划名称", {exact: true}).fill("my-uncommitted-edit");
  await page.getByRole("button", {name: "保存修改"}).click();
  await expect(page.locator("#editor-error")).toContainText("版本或状态冲突");
  expect((await getSchedule(request, original.id)).spec.name).toBe("changed-by-another-client");
  await page.getByRole("button", {name: "重新载入最新版本"}).click();
  await page.locator("#confirm").getByRole("button", {name: "重新载入", exact: true}).click();
  await expect(page.getByLabel("计划名称", {exact: true})).toHaveValue("changed-by-another-client");
  await page.getByLabel("计划名称", {exact: true}).fill("revision-merged");
  await page.getByRole("button", {name: "保存修改"}).click();
  await expect(page.locator("#editor")).toBeHidden();
  expect((await getSchedule(request, original.id)).revision).toBe(3);
});

test("real delivery history and failed run replay", async ({page, request}) => {
  const schedule = await create(request, "browser-delivery-failure", {
    trigger: {type: "delay", seconds: 0}, target: {url: callback.replace("/ok", "/fail")},
    retry: {max_attempts: 1, initial_delay_seconds: 1, max_delay_seconds: 1},
  });
  let run;
  await expect.poll(async () => {
    const runs = await (await request.get(`/v1/runs?schedule_id=${schedule.id}`, {headers: auth()})).json();
    run = runs[0]; return run?.status;
  }).toBe("dead");
  await login(page); await search(page, schedule.id);
  await page.locator("tbody").getByRole("button", {name: "详情", exact: true}).click();
  await page.locator("#detail-actions").getByRole("button", {name: "执行记录", exact: true}).click();
  await page.locator("tbody").getByRole("button", {name: "详情", exact: true}).click();
  await expect(page.locator("#detail-content")).toContainText("HTTP 503");
  await expect(page.locator(".attempt-card")).toHaveCount(1);
  await page.getByRole("button", {name: "重放实例", exact: true}).click();
  await page.getByRole("button", {name: "确认重放", exact: true}).click();
  await expect.poll(async () => (await (await request.get(`/v1/runs/${run.id}`, {headers: auth()})).json()).attempt_count).toBe(2);
  await page.getByRole("button", {name: "刷新详情", exact: true}).click();
  await expect(page.locator(".attempt-card")).toHaveCount(2);
});

test("literal search, filters and pagination", async ({page, request}) => {
  for (let index = 0; index < 22; index++) await create(request, `pagination%_${String(index).padStart(2, "0")}`);
  await create(request, "paginationXX_should-not-match");
  await login(page); await search(page, "pagination%_");
  await expect(page.locator("tbody tr")).toHaveCount(20);
  await page.getByRole("button", {name: "下一页"}).click();
  await expect(page.locator("tbody tr")).toHaveCount(2);
  await expect(page.getByRole("button", {name: "下一页"})).toBeDisabled();
  await page.getByRole("button", {name: "上一页"}).click();
  await expect(page.locator("tbody tr")).toHaveCount(20);
  await page.getByRole("combobox", {name: "状态筛选"}).selectOption("paused");
  await page.getByRole("button", {name: "筛选", exact: true}).click();
  await expect(page.locator("#empty-title")).toHaveText("没有匹配的记录");
});

test("a response arriving after logout cannot restore tenant data", async ({page}) => {
  await login(page);
  let release, intercepted;
  const waiting = new Promise((resolve) => { release = resolve; });
  const started = new Promise((resolve) => { intercepted = resolve; });
  await page.route("**/v1/schedules?**", async (route) => {
    const response = await route.fetch(); intercepted(); await waiting;
    await route.fulfill({response}).catch(() => {});
  });
  await page.getByRole("button", {name: "刷新", exact: true}).click();
  await started;
  await page.getByRole("button", {name: "退出工作空间"}).click();
  release();
  await expect(page.locator("#workspace")).toBeHidden();
  await expect(page.locator("#table-body")).toBeEmpty();
  await expect(page.locator("#stat-active")).toHaveText("—");
  await page.unrouteAll({behavior: "wait"});
  await login(page, otherKey);
  await expect(page.locator("#empty-state")).toBeVisible();
});

test("mobile layout, semantic controls and screenshot", async ({page}) => {
  await page.setViewportSize({width: 390, height: 844});
  await login(page);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await expect(page.getByRole("button", {name: "新建计划", exact: true})).toBeInViewport();
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await expect(page.getByRole("dialog", {name: "新建计划"})).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.locator("#editor")).toBeHidden();
  await page.evaluate(() => { document.activeElement.blur(); window.scrollTo(0, 0); });
  await page.screenshot({path: "test-results/console-mobile.png", fullPage: true});
  await page.setViewportSize({width: 1440, height: 1000});
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({path: "test-results/console-desktop.png", fullPage: true});
});

test("an old preview does not populate a newly opened form", async ({page}) => {
  await login(page);
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  let release, intercepted;
  const waiting = new Promise((resolve) => { release = resolve; });
  const started = new Promise((resolve) => { intercepted = resolve; });
  await page.route("**/v1/preview", async (route) => {
    const response = await route.fetch(); intercepted(); await waiting; await route.fulfill({response});
  });
  await page.getByRole("button", {name: "预览未来 5 次触发"}).click(); await started;
  await page.getByRole("button", {name: "关闭计划编辑"}).click();
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("触发方式").selectOption("cron");
  release(); await page.unrouteAll({behavior: "wait"});
  await expect(page.locator("#preview-result")).toBeEmpty();
  await expect(page.getByRole("button", {name: "预览未来 5 次触发"})).toBeEnabled();
  await page.getByRole("button", {name: "预览未来 5 次触发"}).click();
  await expect(page.locator("#preview-result li")).toHaveCount(5);
});

test("a late save cannot close or replace a newer draft", async ({page}) => {
  await login(page);
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("late-save-original");
  await page.getByLabel("目标 URL").fill(callback);
  let release, intercepted;
  const waiting = new Promise((resolve) => { release = resolve; });
  const started = new Promise((resolve) => { intercepted = resolve; });
  await page.route("**/v1/schedules", async (route) => {
    const response = await route.fetch(); intercepted(); await waiting; await route.fulfill({response});
  });
  await page.getByRole("button", {name: "创建计划", exact: true}).click(); await started;
  await page.getByRole("button", {name: "关闭计划编辑"}).click();
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("new-unsaved-draft");
  release(); await page.unrouteAll({behavior: "wait"});
  await expect(page.locator("#editor")).toBeVisible();
  await expect(page.getByLabel("计划名称", {exact: true})).toHaveValue("new-unsaved-draft");
  await expect(page.getByRole("button", {name: "创建计划", exact: true})).toBeEnabled();
});

test("a delayed edit load cannot overwrite a new draft", async ({page, request}) => {
  const schedule = await create(request, "slow-edit-existing-plan");
  await login(page); await search(page, schedule.id);
  let release, intercepted;
  const waiting = new Promise((resolve) => { release = resolve; });
  const started = new Promise((resolve) => { intercepted = resolve; });
  await page.route(`**/v1/schedules/${schedule.id}`, async (route) => {
    const response = await route.fetch(); intercepted(); await waiting; await route.fulfill({response});
  });
  await page.locator("tbody").getByRole("button", {name: "编辑", exact: true}).click(); await started;
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("new-draft-must-survive");
  const delivered = page.waitForResponse((response) => response.url().endsWith(`/v1/schedules/${schedule.id}`));
  release(); await delivered; await page.unrouteAll({behavior: "wait"});
  await expect(page.getByLabel("计划名称", {exact: true})).toHaveValue("new-draft-must-survive");
  await expect(page.locator("#editor-title")).toHaveText("新建计划");
});

test("refresh keeps applied filters until the next submit", async ({page, request}) => {
  await create(request, "filter-applied-one");
  await create(request, "filter-draft-two");
  await login(page); await search(page, "filter-applied-one");
  await expect(page.locator("tbody tr")).toHaveCount(1);
  await page.getByPlaceholder("搜索计划名称或完整 ID").fill("filter-draft-two");
  await page.getByRole("button", {name: "刷新", exact: true}).click();
  await expect(page.locator("#loading")).toBeHidden();
  await expect(page.locator("tbody")).toContainText("filter-applied-one");
  await page.getByRole("button", {name: "筛选", exact: true}).click();
  await expect(page.locator("tbody")).toContainText("filter-draft-two");
});

test("editing once preserves sub-millisecond precision and invalid dates are rejected", async ({page, request}) => {
  const at = "2030-06-01T09:00:00.123456Z";
  const schedule = await create(request, "precise-once-example", {trigger: {type: "once", at}});
  await login(page); await search(page, schedule.id);
  await page.locator("tbody").getByRole("button", {name: "编辑", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("precise-once-renamed");
  await page.getByRole("button", {name: "保存修改"}).click();
  await expect(page.locator("#editor")).toBeHidden();
  expect((await getSchedule(request, schedule.id)).spec.trigger.at).toBe(at);
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("invalid-february-date");
  await page.getByLabel("触发方式").selectOption("once");
  await page.getByLabel("执行时间（带时区）").fill("2030-02-30T09:00:00Z");
  await page.getByLabel("目标 URL").fill(callback);
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect(page.locator("#editor-error")).toBeVisible();
  await expect(page.locator("#editor")).toBeVisible();
});

test("logging out during the initial load leaves no refresh timer", async ({page}) => {
  await page.addInitScript(() => {
    const originalSet = window.setInterval, originalClear = window.clearInterval;
    window.__refreshTimers = new Set();
    window.setInterval = (...args) => { const id = originalSet(...args); window.__refreshTimers.add(id); return id; };
    window.clearInterval = (id) => { window.__refreshTimers.delete(id); originalClear(id); };
  });
  let release, intercepted;
  const waiting = new Promise((resolve) => { release = resolve; });
  const started = new Promise((resolve) => { intercepted = resolve; });
  await page.route("**/v1/schedules?**", async (route) => {
    const response = await route.fetch(); intercepted(); await waiting;
    await route.fulfill({response}).catch(() => {});
  });
  await page.goto("/console/");
  await page.getByLabel("API key", {exact: true}).fill(key);
  await page.getByRole("button", {name: "连接工作空间"}).click(); await started;
  await page.getByRole("button", {name: "退出工作空间"}).click();
  release(); await page.unrouteAll({behavior: "wait"});
  await expect(page.getByRole("button", {name: "连接工作空间"})).toBeEnabled();
  expect(await page.evaluate(() => window.__refreshTimers.size)).toBe(0);
  await page.getByLabel("API key", {exact: true}).fill(key);
  await page.getByRole("button", {name: "连接工作空间"}).click();
  await expect(page.locator("#workspace")).toBeVisible();
  await expect(page.locator("#refresh")).toBeEnabled();
  expect(await page.evaluate(() => window.__refreshTimers.size)).toBe(1);
  await page.getByRole("button", {name: "退出工作空间"}).click();
  expect(await page.evaluate(() => window.__refreshTimers.size)).toBe(0);
});

test("invalid fields in collapsed advanced options are revealed", async ({page}) => {
  await login(page);
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("invalid-hidden-options");
  await page.getByLabel("目标 URL").fill(callback);
  await page.locator("#advanced summary").click();
  await page.getByLabel("最大尝试次数", {exact: true}).fill("0");
  await page.locator("#advanced summary").click();
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect(page.getByLabel("最大尝试次数", {exact: true})).toBeVisible();
  await expect(page.getByLabel("最大尝试次数", {exact: true})).toBeFocused();
  await page.getByLabel("最大尝试次数", {exact: true}).fill("3");
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect(page.locator("#editor")).toBeHidden();
});

test("out-of-range payload numbers cannot silently become null", async ({page, request}) => {
  await login(page);
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("overflow-must-not-be-null");
  await page.getByLabel("目标 URL").fill(callback);
  for (const payload of ['{"amount":1e400}', '[0,{"amount":-1e400}]']) {
    await page.locator("#payload").fill(payload);
    await page.getByRole("button", {name: "创建计划", exact: true}).click();
    await expect(page.locator("#editor-error")).toBeVisible();
    await expect(page.locator("#editor")).toBeVisible();
    await expect(page.locator("#payload")).toHaveValue(payload);
    const response = await request.get("/v1/schedules?q=overflow-must-not-be-null", {headers: auth()});
    expect(await response.json()).toEqual([]);
  }
  await page.locator("#payload").fill('{"amount":"1e400","optional":null,"value":1.5}');
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect(page.locator("#editor")).toBeHidden();
});

test("large integer payloads survive create, display and unrelated edits", async ({page, request}) => {
  const raw = '{"id":18446744073709551615,"nested":[-9223372036854775808,9007199254740993],"text":"9007199254740993","__proto__":{"id":9007199254740995}}';
  await login(page);
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("precise-payload-create");
  await page.getByLabel("目标 URL").fill(callback);
  await page.locator("#payload").fill(raw);
  const sent = page.waitForRequest((r) => r.method() === "POST" && r.url().endsWith("/v1/schedules"));
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  const data = (await sent).postData();
  expect(typeof JSON.parse(data).payload.id).toBe("number");
  expect(JSON.parse(data).payload.nested.every((value) => typeof value === "number")).toBe(true);
  for (const number of ["18446744073709551615", "-9223372036854775808", "9007199254740993", "9007199254740995"]) expect(data).toContain(number);
  await expect(page.locator("#editor")).toBeHidden();
  const response = await request.post("/v1/schedules", {headers: {...auth(), "Content-Type": "application/json"}, data:
    `{"name":"precise-payload-edit","trigger":{"type":"delay","seconds":86400},"target":{"url":${JSON.stringify(callback)}},"payload":${raw}}`});
  expect(response.ok()).toBeTruthy();
  const schedule = await response.json();
  await search(page, schedule.id);
  await page.locator("tbody").getByRole("button", {name: "详情", exact: true}).click();
  await expect(page.locator("#detail-content")).toContainText("18446744073709551615");
  await page.getByRole("button", {name: "编辑计划", exact: true}).click();
  await expect(page.locator("#payload")).toHaveValue(/9007199254740993/);
  await page.getByLabel("计划名称", {exact: true}).fill("precise-payload-renamed");
  await page.getByRole("button", {name: "保存修改"}).click();
  await expect(page.locator("#editor")).toBeHidden();
  const stored = await (await request.get(`/v1/schedules/${schedule.id}`, {headers: auth()})).text();
  for (const number of ["18446744073709551615", "-9223372036854775808", "9007199254740993", "9007199254740995"]) expect(stored).toContain(number);
  expect(typeof JSON.parse(stored).spec.payload.id).toBe("number");
  expect(JSON.parse(stored).spec.payload.nested.every((value) => typeof value === "number")).toBe(true);
  expect(JSON.parse(stored).spec.payload.text).toBe("9007199254740993");
  expect(await page.evaluate(() => Object.prototype.id)).toBeUndefined();
});

test("attempt history pages stay bounded and late pages cannot replace refreshed details", async ({page, request}) => {
  const schedule = await create(request, "browser-paged-attempt-history", {
    trigger: {type: "delay", seconds: 0}, target: {url: callback.replace("/ok", "/fail")},
    retry: {max_attempts: 1, initial_delay_seconds: 1, max_delay_seconds: 1},
  });
  let run;
  await expect.poll(async () => {
    const runs = await (await request.get(`/v1/runs?schedule_id=${schedule.id}`, {headers: auth()})).json();
    run = runs[0]; return run?.status;
  }).toBe("dead");
  const rows = Array.from({length: 121}, (_, index) => ({
    number: index + 1, status: "failed", http_status: 503, started_at: run.created_at, finished_at: run.created_at,
    response_excerpt: "<img src=x onerror=alert(1)>",
  }));
  let release, delayOlder = false;
  await page.route(`**/v1/runs/${run.id}/attempts*`, async (route) => {
    const before = Number(new URL(route.request().url()).searchParams.get("before_number") || Infinity);
    if (delayOlder && before === 72) await new Promise((resolve) => { release = resolve; });
    await route.fulfill({json: rows.filter((row) => row.number < before).slice(-50)});
  });
  await login(page); await search(page, schedule.id);
  await page.locator("tbody").getByRole("button", {name: "详情", exact: true}).click();
  await page.locator("#detail-actions").getByRole("button", {name: "执行记录", exact: true}).click();
  await page.locator("tbody").getByRole("button", {name: "详情", exact: true}).click();
  const cards = page.locator(".attempt-card");
  await expect(cards).toHaveCount(50);
  await expect(cards.first()).toContainText("第 72 次尝试");
  await expect(cards.last()).toContainText("第 121 次尝试");
  await expect(page.locator(".attempt-card img")).toHaveCount(0);
  await expect(page.getByRole("button", {name: "较新记录", exact: true})).toBeDisabled();
  await page.getByRole("button", {name: "更早记录", exact: true}).click();
  await expect(cards).toHaveCount(50);
  await expect(cards.first()).toContainText("第 22 次尝试");
  await page.getByRole("button", {name: "更早记录", exact: true}).click();
  await expect(cards).toHaveCount(21);
  await expect(cards.first()).toContainText("第 1 次尝试");
  await expect(page.getByRole("button", {name: "更早记录", exact: true})).toBeDisabled();
  await page.getByRole("button", {name: "较新记录", exact: true}).click();
  await expect(cards.first()).toContainText("第 22 次尝试");
  await page.getByRole("button", {name: "较新记录", exact: true}).click();
  await expect(cards.first()).toContainText("第 72 次尝试");
  delayOlder = true;
  await page.getByRole("button", {name: "更早记录", exact: true}).click();
  await expect.poll(() => Boolean(release)).toBe(true);
  await page.getByRole("button", {name: "刷新详情", exact: true}).click();
  await expect(cards.first()).toContainText("第 72 次尝试");
  const oldResponse = page.waitForResponse((response) => response.url().includes("attempts?before_number=72"));
  release();
  await oldResponse;
  await expect(cards).toHaveCount(50);
  await expect(cards.first()).toContainText("第 72 次尝试");
});

test("delivery capacity is visible and cleared when switching tenants", async ({page}) => {
  await page.route("**/v1/capacity", (route) => route.fulfill({json: {
    tenant_id: "console", max_concurrent: 2, rate_per_second: 3, burst: 6,
    running: 1, available_tokens: 2.5,
  }}));
  await login(page);
  await expect(page.locator("#delivery-capacity")).toContainText("正在投递 1 / 2");
  await expect(page.locator("#delivery-capacity")).toContainText("每秒 3 次，最多突发 6 次");
  await page.getByRole("button", {name: "退出工作空间"}).click();
  await expect(page.locator("#delivery-capacity")).toHaveText("—");
  await page.unroute("**/v1/capacity");
  await login(page, otherKey);
  await expect(page.locator("#delivery-capacity")).toContainText("正在投递 0 / 不限");
  await expect(page.locator("#delivery-capacity")).toContainText("速率不限");
});

test("business metadata can be created, filtered, edited and cleared", async ({page, request}) => {
  const metadata = {"order.id": "42_%'\\", env: "production", empty: "", ["__proto__"]: "<img src=x onerror=alert(1)>"};
  await create(request, "metadata-decoy-example", {metadata: {...metadata, "order.id": "other"}});
  await login(page);
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("metadata-created-example");
  await page.getByLabel("目标 URL").fill(callback);
  await page.locator("#delay-seconds").fill("86400");
  await page.locator("#metadata").fill('{"wrong":42}');
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect(page.locator("#editor-error")).toContainText("业务元数据必须是值为字符串的 JSON 对象");
  await page.locator("#metadata").fill(JSON.stringify(metadata));
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect(page.locator("#editor")).toBeHidden();
  await page.locator("#metadata-filter summary").click();
  await page.getByLabel("元数据键", {exact: true}).fill("order.id");
  await page.getByLabel("元数据值", {exact: true}).fill(metadata["order.id"]);
  await page.getByRole("button", {name: "筛选", exact: true}).click();
  await expect(page.locator("tbody tr")).toHaveCount(1);
  await expect(page.locator("tbody")).toContainText("metadata-created-example");
  await page.locator("tbody").getByRole("button", {name: "详情", exact: true}).click();
  await expect(page.locator("#detail-content")).toContainText(metadata.__proto__);
  await expect(page.locator("#detail-content img")).toHaveCount(0);
  await page.getByRole("button", {name: "关闭详情"}).click();
  await page.locator("tbody").getByRole("button", {name: "编辑", exact: true}).click();
  expect(JSON.parse(await page.locator("#metadata").inputValue())).toEqual(metadata);
  await page.locator("#metadata").fill('{"order.id":"edited-order","empty":""}');
  await page.getByRole("button", {name: "保存修改"}).click();
  await expect(page.locator("#editor")).toBeHidden();
  await expect(page.locator("#empty-state")).toBeVisible();
  await page.getByLabel("元数据值", {exact: true}).fill("edited-order");
  await page.getByRole("button", {name: "筛选", exact: true}).click();
  await expect(page.locator("tbody tr")).toHaveCount(1);
  await page.getByLabel("元数据键", {exact: true}).fill("empty");
  await page.getByLabel("元数据值", {exact: true}).fill("");
  await page.getByRole("button", {name: "筛选", exact: true}).click();
  await expect(page.locator("tbody tr")).toHaveCount(2);
  await page.getByLabel("元数据键", {exact: true}).fill("");
  await page.getByLabel("元数据值", {exact: true}).fill("needs-key");
  await page.getByRole("button", {name: "筛选", exact: true}).click();
  await expect(page.locator("#list-error")).toContainText("需要同时填写元数据键");
  await page.getByRole("button", {name: "清除", exact: true}).click();
  await expect(page.locator("#metadata-key")).toHaveValue("");
  await expect(page.locator("#metadata-value")).toHaveValue("");
  await expect(page.locator("#list-error")).toBeHidden();
  await page.getByRole("button", {name: "执行记录", exact: true}).click();
  await expect(page.locator("#metadata-filter")).toBeVisible();
  await expect(page.locator("#metadata-key")).toHaveValue("");
  await expect(page.locator("#metadata-value")).toHaveValue("");
});

test("RRULE exception dates require a timezone in preview and save", async ({page}) => {
  await login(page);
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("timezone-exception-example");
  await page.getByLabel("目标 URL").fill(callback);
  await page.getByLabel("触发方式").selectOption("rrule");
  const base = "DTSTART;TZID=Asia/Shanghai:20300101T090000\nRRULE:FREQ=DAILY;COUNT=3";
  await page.getByLabel("RRULE 规则集").fill(`${base}\nEXDATE:20300102T090000`);
  await page.getByRole("button", {name: "预览未来 5 次触发"}).click();
  await expect(page.locator("#preview-result")).toContainText("Floating EXDATE");
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect(page.locator("#editor-error")).toContainText("Floating EXDATE");
  await page.getByLabel("RRULE 规则集").fill(`${base}\nEXDATE;TZID=Asia/Shanghai:20300102T090000`);
  await page.getByRole("button", {name: "预览未来 5 次触发"}).click();
  await expect(page.locator("#preview-result li")).toHaveCount(2);
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect(page.locator("#editor")).toBeHidden();
});

test("pending retry can be deferred and cancelled without changing its schedule", async ({page, request}) => {
  const {schedule, run} = await pendingRetry(request, "pending-controls-defer-cancel");
  const before = await getSchedule(request, schedule.id);
  await openRun(page, run);
  await page.setViewportSize({width: 390, height: 844});
  await expect(page.getByLabel("从现在起延后多少秒入队", {exact: true})).toBeVisible();
  expect(await page.locator("#detail").evaluate((dialog) => dialog.scrollWidth <= dialog.clientWidth)).toBe(true);
  await page.locator(".run-controls").screenshot({path: test.info().outputPath("pending-run-controls-mobile.png")});
  await page.getByLabel("从现在起延后多少秒入队", {exact: true}).fill("120");
  const updated = page.waitForResponse((response) => response.url().endsWith(`/runs/${run.id}/reschedule`) && response.request().method() === "POST");
  await page.getByRole("button", {name: "调整入队时间", exact: true}).click();
  expect((await updated).status()).toBe(200);
  await expect(page.locator("#toast")).toHaveText("实例入队时间已调整");
  const deferred = await (await request.get(`/v1/runs/${run.id}`, {headers: auth()})).json();
  expect(deferred.id).toBe(run.id);
  expect(deferred.scheduled_at).toBe(run.scheduled_at);
  expect(deferred.expires_at).toBe(run.expires_at);
  expect(deferred.attempt_count).toBe(1);
  expect(Date.parse(deferred.available_at)).toBeGreaterThan(Date.now() + 110000);
  await page.getByRole("button", {name: "取消实例", exact: true}).click();
  await page.locator("#confirm").getByRole("button", {name: "取消实例", exact: true}).click();
  await expect(page.locator("#detail-content .badge").first()).toHaveText("已取消");
  await expect(page.getByRole("heading", {name: "管理待执行实例", exact: true})).toHaveCount(0);
  const cancelled = await (await request.get(`/v1/runs/${run.id}`, {headers: auth()})).json();
  expect(cancelled.status).toBe("cancelled");
  expect(cancelled.attempt_count).toBe(1);
  expect(await getSchedule(request, schedule.id)).toEqual(before);
  expect(await (await request.get(`/v1/runs/${run.id}/attempts`, {headers: auth()})).json()).toHaveLength(1);
});

test("immediate enqueue retains the original run and obeys schedule pause", async ({page, request}) => {
  const {schedule, run} = await pendingRetry(request, "pending-controls-enqueue");
  await openRun(page, run);
  await page.getByRole("button", {name: "立即入队", exact: true}).click();
  await expect(page.locator("#confirm-description")).toContainText("暂停状态");
  const updated = page.waitForResponse((response) => response.url().endsWith(`/runs/${run.id}/reschedule`) && response.request().method() === "POST");
  await page.locator("#confirm").getByRole("button", {name: "立即入队", exact: true}).click();
  expect((await updated).status()).toBe(200);
  const pending = await (await request.get(`/v1/runs/${run.id}`, {headers: auth()})).json();
  expect(pending.status).toBe("pending");
  expect(pending.attempt_count).toBe(1);
  expect(Date.parse(pending.available_at)).toBeLessThanOrEqual(Date.now());
  expect(pending.scheduled_at).toBe(run.scheduled_at);
  expect(pending.expires_at).toBe(run.expires_at);
  expect((await request.post(`/v1/schedules/${schedule.id}/resume`, {headers: auth()})).ok()).toBeTruthy();
  await expect.poll(async () => {
    const current = await (await request.get(`/v1/runs/${run.id}`, {headers: auth()})).json();
    return current.status === "pending" && current.attempt_count === 2;
  }).toBe(true);
  expect(await (await request.get(`/v1/runs/${run.id}/attempts`, {headers: auth()})).json()).toHaveLength(2);
  expect((await request.post(`/v1/runs/${run.id}/cancel`, {headers: auth()})).ok()).toBeTruthy();
});

test("stale pending-run details cannot overwrite another availability change", async ({page, request}) => {
  const {run} = await pendingRetry(request, "pending-controls-conflict");
  await openRun(page, run);
  const response = await request.post(`/v1/runs/${run.id}/reschedule`, {headers: auth(), data: {
    expected_available_at: run.available_at, timing: {type: "delay", seconds: 90},
  }});
  expect(response.ok()).toBeTruthy();
  const changed = await response.json();
  await page.getByLabel("从现在起延后多少秒入队", {exact: true}).fill("30");
  await page.getByRole("button", {name: "调整入队时间", exact: true}).click();
  await expect(page.locator("#detail-content .error").first()).toContainText("Run availability changed");
  expect((await (await request.get(`/v1/runs/${run.id}`, {headers: auth()})).json()).available_at).toBe(changed.available_at);
  await page.getByRole("button", {name: "刷新详情", exact: true}).click();
  await expect(page.locator("#detail-content .error")).toHaveCount(0);
  await page.getByLabel("从现在起延后多少秒入队", {exact: true}).fill("120");
  const saved = page.waitForResponse((result) => result.url().endsWith(`/runs/${run.id}/reschedule`) && result.request().method() === "POST");
  await page.getByRole("button", {name: "调整入队时间", exact: true}).click();
  expect((await saved).status()).toBe(200);
  expect((await request.post(`/v1/runs/${run.id}/cancel`, {headers: auth()})).ok()).toBeTruthy();
});

test("extended years supported by the API can be displayed and edited", async ({page, request}) => {
  for (const [index, at] of ["-4713-11-24T00:00:00Z", "+10000-01-01T00:00:00Z"].entries()) {
    const schedule = await create(request, `extended-year-${index}`, {trigger: {type: "once", at}});
    await login(page);
    await search(page, schedule.spec.name);
    await expect(page.locator("#list-error")).toBeHidden();
    const row = page.locator("tbody tr").filter({hasText: schedule.spec.name});
    await expect(row).toBeVisible();
    await row.getByRole("button", {name: "详情", exact: true}).click();
    await expect(page.locator("#detail-title")).toHaveText(schedule.spec.name);
    await expect(page.locator("#detail-content .error")).toHaveCount(0);
    await page.getByRole("button", {name: "编辑计划", exact: true}).click();
    await expect(page.locator("#once-at")).toHaveValue(at);
    await page.getByLabel("计划名称", {exact: true}).fill(`${schedule.spec.name}-edited`);
    await page.getByRole("button", {name: "保存修改"}).click();
    await expect(page.locator("#editor")).toBeHidden();
    const updated = await getSchedule(request, schedule.id);
    expect(updated.revision).toBe(2);
    expect(updated.spec.trigger.at).toBe(at);
  }
});

test("batch import retries a committed request with the same key and preserves integer payloads", async ({page, request}) => {
  await login(page);
  const keys = [], statuses = [];
  await page.route("**/v1/schedules/batch", async (route) => {
    keys.push(route.request().headers()["idempotency-key"]);
    const response = await route.fetch();
    statuses.push(response.status());
    if (keys.length === 1) await route.fulfill({status: 503, contentType: "application/json", body: '{"error":"模拟响应丢失，请重试"}'});
    else await route.fulfill({response});
  });
  await page.getByRole("button", {name: "批量导入", exact: true}).click();
  const input = `{"schedules":[{"name":"batch-browser-first","trigger":{"type":"delay","seconds":86400},"target":{"url":${JSON.stringify(callback)}},"payload":{"order_id":9007199254740993}},{"name":"batch-browser-second","trigger":{"type":"delay","seconds":86400},"target":{"url":${JSON.stringify(callback)}}}]}`;
  await page.getByLabel("批量计划（JSON）").fill(input);
  await page.getByRole("button", {name: "导入计划", exact: true}).click();
  await expect(page.locator("#batch-error")).toContainText("模拟响应丢失");
  await page.getByLabel("批量计划（JSON）").fill(`\n${input}\n`);
  await page.getByRole("button", {name: "导入计划", exact: true}).click();
  await expect(page.locator("#batch-import")).toBeHidden();
  expect(keys).toHaveLength(2);
  expect(keys[0]).toBe(keys[1]);
  expect(statuses).toEqual([201, 200]);
  const response = await request.get("/v1/schedules?q=batch-browser-", {headers: auth()});
  const raw = await response.text();
  expect(JSON.parse(raw)).toHaveLength(2);
  expect(raw).toContain('"order_id":9007199254740993');
  await search(page, "batch-browser-");
  await expect(page.locator("tbody tr")).toHaveCount(2);
});

test("invalid batch import leaves all schedules unchanged and keeps the draft", async ({page, request}) => {
  await login(page);
  await page.getByRole("button", {name: "批量导入", exact: true}).click();
  const body = {schedules: [
    {name: "batch-invalid-first", trigger: {type: "delay", seconds: 86400}, target: {url: callback}},
    {name: "batch-invalid-second", trigger: {type: "delay", seconds: 86400}, target: {url: "file:///tmp/hook"}},
  ]};
  await page.getByLabel("批量计划（JSON）").fill(JSON.stringify(body));
  await page.getByRole("button", {name: "导入计划", exact: true}).click();
  await expect(page.locator("#batch-error")).toBeVisible();
  await expect(page.locator("#batch-import")).toBeVisible();
  await expect(page.getByLabel("批量计划（JSON）")).toHaveValue(JSON.stringify(body));
  const response = await request.get("/v1/schedules?q=batch-invalid-", {headers: auth()});
  expect(await response.json()).toEqual([]);
  await page.getByRole("button", {name: "关闭批量导入"}).click();
});

test("execution history combines original metadata with precise time bounds", async ({page, request}) => {
  const at = "2020-01-01T00:00:00.000001Z";
  const schedule = await create(request, "run-search-browser", {trigger: {type: "once", at}, metadata: {"order.id": "history-search"}});
  await expect.poll(async () => {
    const response = await request.get(`/v1/runs?schedule_id=${schedule.id}`, {headers: auth()});
    return (await response.json()).length;
  }).toBe(1);
  await login(page);
  await page.getByRole("button", {name: "执行记录", exact: true}).click();
  await expect(page.locator("#run-schedule-field")).toBeVisible();
  await page.locator("#metadata-filter summary").click();
  await page.locator("#metadata-key").fill("order.id");
  await page.locator("#metadata-value").fill("history-search");
  await page.locator("#run-time-filter summary").click();
  await page.getByLabel("开始时间（含）").fill(at);
  await page.getByLabel("结束时间（不含）").fill("2020-01-01T00:00:00.000002Z");
  await page.getByRole("button", {name: "筛选", exact: true}).click();
  await expect(page.locator("tbody tr")).toHaveCount(1);
  await expect(page.locator("tbody tr")).toContainText(schedule.spec.name);
  await page.locator("#metadata-value").fill("does-not-match");
  await page.getByRole("button", {name: "筛选", exact: true}).click();
  await expect(page.locator("#empty-state")).toBeVisible();
  await page.locator("#metadata-value").fill("history-search");
  await page.getByLabel("结束时间（不含）").fill(at);
  await page.getByRole("button", {name: "筛选", exact: true}).click();
  await expect(page.locator("#list-error")).toContainText("from must be earlier than until");
});

test("saved-rule preview works while paused without changing the schedule", async ({page, request}) => {
  const schedule = await create(request, "saved-preview-browser", {trigger: {type: "rrule", value: "DTSTART:20300101T090000Z\nRRULE:FREQ=DAILY;COUNT=3\nEXDATE:20300102T090000Z"}});
  expect((await request.post(`/v1/schedules/${schedule.id}/pause`, {headers: auth()})).ok()).toBeTruthy();
  const before = await getSchedule(request, schedule.id);
  await login(page);
  await search(page, schedule.spec.name);
  await page.locator("tbody tr").getByRole("button", {name: "详情", exact: true}).click();
  await page.getByRole("button", {name: "查看未来 5 次触发", exact: true}).click();
  const preview = page.locator("#detail-content section").filter({hasText: "规则未来触发点"});
  await expect(preview.locator("li")).toHaveCount(2);
  await expect(preview.locator("li").first()).toHaveAttribute("title", "2030-01-01T09:00:00Z");
  await expect(preview.locator("li").last()).toHaveAttribute("title", "2030-01-03T09:00:00Z");
  expect(await getSchedule(request, schedule.id)).toEqual(before);
  const runs = await request.get(`/v1/runs?schedule_id=${schedule.id}`, {headers: auth()});
  expect(await runs.json()).toEqual([]);
});
