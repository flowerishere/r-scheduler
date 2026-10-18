import {test, expect} from "@playwright/test";

const headers = {Authorization: "Bearer console-browser-test-key"};
let callback;

test.beforeAll(async ({request}) => {
  const response = await request.get("/v1/schedules?q=browser-test-callback", {headers});
  expect(response.ok()).toBeTruthy();
  callback = (await response.json())[0].spec.target.url;
});

test.beforeEach(async ({page}) => {
  page.draftReplies = [];
  page.draftErrors = [];
  page.on("pageerror", (error) => page.draftErrors.push(error.message));
});
test.afterEach(async ({page}) => {
  for (const reply of page.draftReplies) reply.release();
  await page.unrouteAll({behavior: "wait"});
  expect(page.draftErrors).toEqual([]);
});

async function login(page) {
  await page.goto("/console/");
  await page.getByLabel("API key", {exact: true}).fill("console-browser-test-key");
  await page.getByRole("button", {name: "连接工作空间"}).click();
  await expect(page.locator("#workspace")).toBeVisible();
  await expect(page.locator("#loading")).toBeHidden();
}

async function holdMutations(page, pattern, failFirst = false) {
  const replies = [];
  await page.route(pattern, async (route) => {
    if (!["POST", "PUT"].includes(route.request().method())) return route.continue();
    const failure = failFirst && replies.length === 0;
    const response = failure ? null : await route.fetch();
    let release;
    const waiting = new Promise((resolve) => { release = resolve; });
    const reply = {release, request: route.request(), response};
    replies.push(reply); page.draftReplies.push(reply);
    await waiting;
    if (failure) await route.fulfill({status: 503, json: {error: "temporary draft-save failure"}});
    else await route.fulfill({response});
  });
  return replies;
}

async function newSchedule(page, name) {
  await page.getByRole("button", {name: "新建计划", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill(name);
  await page.getByLabel("目标 URL").fill(callback);
}

function batchBody(name) {
  return JSON.stringify({schedules: [{name, trigger: {type: "delay", seconds: 86400}, target: {url: callback}}]});
}

async function newBatch(page, name) {
  await page.getByRole("button", {name: "批量导入", exact: true}).click();
  await page.getByLabel("批量计划（JSON）").fill(batchBody(name));
}

test("saving an edited schedule freezes its fields, preview and reload until success", async ({page, request}) => {
  const created = await request.post("/v1/schedules", {headers, data: {
    name: "draft-freeze-original", trigger: {type: "delay", seconds: 86400}, target: {url: callback},
  }});
  expect(created.ok()).toBeTruthy();
  const schedule = await created.json();
  await login(page);
  await page.getByPlaceholder("搜索计划名称或完整 ID").fill(schedule.id);
  await page.getByRole("button", {name: "筛选", exact: true}).click();
  await page.locator("tbody").getByRole("button", {name: "编辑", exact: true}).click();
  await page.getByLabel("计划名称", {exact: true}).fill("draft-freeze-saved");
  const replies = await holdMutations(page, `**/v1/schedules/${schedule.id}`);
  await page.getByRole("button", {name: "保存修改", exact: true}).click();
  await expect.poll(() => replies.length).toBe(1);
  for (const id of ["job-name", "trigger-type", "once-at", "target-url", "payload", "preview-trigger", "reload-editor", "save-schedule"]) {
    await expect(page.locator(`#${id}`)).toBeDisabled();
  }
  await expect(page.getByRole("button", {name: "关闭计划编辑"})).toBeEnabled();
  await expect(page.locator("#editor").getByRole("button", {name: "取消", exact: true})).toBeEnabled();
  replies[0].release();
  await expect(page.locator("#editor")).toBeHidden();
  const saved = await (await request.get(`/v1/schedules/${schedule.id}`, {headers})).json();
  expect(saved.spec.name).toBe("draft-freeze-saved");
});

test("batch import freezes the submitted JSON while a successful response is pending", async ({page}) => {
  await login(page);
  await newBatch(page, "draft-freeze-batch");
  const replies = await holdMutations(page, "**/v1/schedules/batch");
  await page.getByRole("button", {name: "导入计划", exact: true}).click();
  await expect.poll(() => replies.length).toBe(1);
  await expect(page.getByLabel("批量计划（JSON）")).toBeDisabled();
  await expect(page.getByRole("button", {name: "导入计划", exact: true})).toBeDisabled();
  await expect(page.getByRole("button", {name: "关闭批量导入"})).toBeEnabled();
  await expect(page.locator("#batch-import").getByRole("button", {name: "取消", exact: true})).toBeEnabled();
  expect(replies[0].request.postDataJSON().schedules[0].name).toBe("draft-freeze-batch");
  replies[0].release();
  await expect(page.locator("#batch-import")).toBeHidden();
});

test("a failed save restores editing and preserves the chosen trigger and draft", async ({page}) => {
  await login(page);
  await newSchedule(page, "draft-recover-single");
  await page.getByLabel("触发方式").selectOption("cron");
  const replies = await holdMutations(page, "**/v1/schedules", true);
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect.poll(() => replies.length).toBe(1);
  await expect(page.getByLabel("Cron 表达式")).toBeDisabled();
  replies[0].release();
  await expect(page.locator("#editor-error")).toContainText("temporary draft-save failure");
  await expect(page.getByLabel("计划名称", {exact: true})).toBeEditable();
  await expect(page.getByLabel("计划名称", {exact: true})).toHaveValue("draft-recover-single");
  await expect(page.getByLabel("触发方式")).toHaveValue("cron");
  await expect(page.getByLabel("Cron 表达式")).toBeEditable();
  await expect(page.locator("#delay-seconds")).toBeDisabled();
  await expect(page.locator("#once-at")).toBeDisabled();
  await expect(page.locator("#preview-trigger")).toBeEnabled();
  await page.getByLabel("计划名称", {exact: true}).fill("draft-recover-single-retried");
  await page.getByRole("button", {name: "创建计划", exact: true}).click();
  await expect.poll(() => replies.length).toBe(2);
  expect(replies[1].request.postDataJSON().name).toBe("draft-recover-single-retried");
  replies[1].release();
  await expect(page.locator("#editor")).toBeHidden();
});

test("a failed batch import restores its JSON draft for correction and retry", async ({page}) => {
  await login(page);
  await newBatch(page, "draft-recover-batch");
  const replies = await holdMutations(page, "**/v1/schedules/batch", true);
  await page.getByRole("button", {name: "导入计划", exact: true}).click();
  await expect.poll(() => replies.length).toBe(1);
  await expect(page.getByLabel("批量计划（JSON）")).toBeDisabled();
  replies[0].release();
  await expect(page.locator("#batch-error")).toContainText("temporary draft-save failure");
  await expect(page.getByLabel("批量计划（JSON）")).toBeEditable();
  await expect(page.getByLabel("批量计划（JSON）")).toHaveValue(batchBody("draft-recover-batch"));
  await page.getByLabel("批量计划（JSON）").fill(batchBody("draft-recover-batch-retried"));
  await page.getByRole("button", {name: "导入计划", exact: true}).click();
  await expect.poll(() => replies.length).toBe(2);
  expect(replies[1].request.postDataJSON().schedules[0].name).toBe("draft-recover-batch-retried");
  replies[1].release();
  await expect(page.locator("#batch-import")).toBeHidden();
});

for (const kind of ["single", "batch"]) {
  test(`closing a pending ${kind} save permits a new draft, and its old response cannot unlock the new save`, async ({page}) => {
    const batch = kind === "batch";
    const open = batch ? newBatch : newSchedule;
    const field = page.locator(batch ? "#batch-json" : "#job-name");
    const save = page.locator(batch ? "#save-batch" : "#save-schedule");
    const dialog = page.locator(batch ? "#batch-import" : "#editor");
    const closeName = batch ? "关闭批量导入" : "关闭计划编辑";
    const endpoint = batch ? "**/v1/schedules/batch" : "**/v1/schedules";
    await login(page);
    const replies = await holdMutations(page, endpoint);
    await open(page, `draft-closed-${kind}-first`);
    await save.click();
    await expect.poll(() => replies.length).toBe(1);
    await expect(field).toBeDisabled();
    await page.getByRole("button", {name: closeName}).click();
    await open(page, `draft-closed-${kind}-second`);
    await expect(field).toBeEditable();
    await save.click();
    await expect.poll(() => replies.length).toBe(2);
    await expect(field).toBeDisabled();
    const oldResponse = page.waitForResponse((response) => response.request() === replies[0].request);
    replies[0].release();
    await oldResponse;
    await expect(page.locator("#loading")).toBeHidden();
    await expect(dialog).toBeVisible();
    await expect(field).toHaveValue(batch ? batchBody(`draft-closed-${kind}-second`) : `draft-closed-${kind}-second`);
    await expect(field).toBeDisabled();
    await expect(save).toBeDisabled();
    replies[1].release();
    await expect(dialog).toBeHidden();
  });
}
