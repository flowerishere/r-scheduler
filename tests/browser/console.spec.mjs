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
  await expect(page.getByText(privateSchedule.spec.name, {exact: true})).toBeVisible();
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
  await expect(page.getByRole("button", {name: "刷新", exact: true})).toBeInViewport();
  await page.evaluate(() => { document.activeElement.blur(); window.scrollTo(0, 0); });
  await page.screenshot({path: "test-results/console-mobile.png", fullPage: true});
  await page.setViewportSize({width: 1440, height: 1000});
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({path: "test-results/console-desktop.png", fullPage: true});
});
