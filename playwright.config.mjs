import { defineConfig } from "@playwright/test";

const port = process.env.SCHEDULER_UI_PORT || "18080";
const baseURL = `http://127.0.0.1:${port}`;
export default defineConfig({
  testDir: "./tests/browser",
  fullyParallel: false,
  workers: 1,
  timeout: 30000,
  retries: 0,
  reporter: [["list"], ["html", {open: "never"}]],
  use: {baseURL, browserName: "chromium", viewport: {width: 1440, height: 1000}, trace: "retain-on-failure", screenshot: "only-on-failure"},
  webServer: {
    command: "node scripts/ui-server.mjs",
    url: `${baseURL}/ready`,
    timeout: 120000,
    reuseExistingServer: false,
    gracefulShutdown: {signal: "SIGTERM", timeout: 15000},
    env: {SCHEDULER_UI_PORT: port, NO_PROXY: "127.0.0.1,localhost", no_proxy: "127.0.0.1,localhost"},
  },
});
