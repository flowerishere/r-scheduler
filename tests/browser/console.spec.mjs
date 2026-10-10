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
