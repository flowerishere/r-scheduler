// Playwright owns this process. All containers and data are disposable; an
// existing DATABASE_URL is deliberately ignored so tests cannot touch user data.
import { resolve } from "node:path";
import { spawn, execFileSync } from "node:child_process";
import { createServer } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

process.chdir(fileURLToPath(new URL("..", import.meta.url)));
const container = `scheduler-ui-tests-${randomUUID().slice(0, 12)}`;
const port = process.env.SCHEDULER_UI_PORT || "18080";
let owned = false, child, receiver, stopping = false;
const docker = (...args) => execFileSync("docker", args, {encoding: "utf8", timeout: 60000, stdio: ["ignore", "pipe", "pipe"]}).trim();

async function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  if (child && child.exitCode === null) {
    child.kill("SIGTERM");
    for (let count = 0; count < 100 && child.exitCode === null; count++) await delay(50);
    if (child.exitCode === null) child.kill("SIGKILL");
  }
  if (receiver) { receiver.closeAllConnections(); receiver.close(); }
  if (owned) { try { docker("rm", "-fv", container); } catch (error) { console.error(error.message); code = 1; } }
  process.exit(code);
}
process.on("SIGTERM", () => void stop());
process.on("SIGINT", () => void stop());
process.on("uncaughtException", (error) => { console.error(error); void stop(1); });
process.on("unhandledRejection", (error) => { console.error(error); void stop(1); });

try {
  execFileSync("cargo", ["build", "--locked", "--bin", "scheduler-service"], {stdio: "inherit", timeout: 100000});
  // Create first, then start: even a start failure leaves a known owned resource.
  docker("create", "--name", container, "-e", "POSTGRES_USER=scheduler", "-e", "POSTGRES_PASSWORD=ui-test-password", "-e", "POSTGRES_DB=scheduler", "-p", "127.0.0.1::5432", "postgres:17-alpine");
  owned = true;
  docker("start", container);
  let ready = false;
  for (let attempt = 0; attempt < 60; attempt++) {
    // The initialization-only PostgreSQL server accepts Unix sockets but does
    // not listen on TCP. Wait for the final server used by the application.
    try { docker("exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "scheduler", "-d", "scheduler"); ready = true; break; } catch { await delay(250); }
  }
  if (!ready) throw new Error("PostgreSQL did not become ready");
  const databasePort = JSON.parse(docker("inspect", container))[0].NetworkSettings.Ports["5432/tcp"][0].HostPort;
  receiver = createServer((request, response) => {
    request.resume();
    response.writeHead(request.url === "/fail" ? 503 : 200, {"Content-Type": "text/plain"});
    response.end(request.url === "/fail" ? "intentional browser-test failure" : "accepted");
  });
  await new Promise((resolve) => receiver.listen(0, "127.0.0.1", resolve));
  // Tests obtain the callback address from the seeded schedule below.
  const callbackPort = receiver.address().port;
  const apiKeys = JSON.stringify({console: "console-browser-test-key", other: "other-browser-test-key"});
  child = spawn(resolve(process.env.CARGO_TARGET_DIR || "target", "debug/scheduler-service"), ["serve", "--role", "all"], {
    env: {...process.env, DATABASE_URL: `postgres://scheduler:ui-test-password@127.0.0.1:${databasePort}/scheduler`,
      SCHEDULER_API_KEYS: apiKeys, SCHEDULER_BIND: `127.0.0.1:${port}`, SCHEDULER_ALLOW_PRIVATE_TARGETS: "true",
      SCHEDULER_WORKERS: "2", SCHEDULER_POLL_MS: "50", RUST_LOG: "scheduler_service=warn"},
    stdio: ["ignore", "inherit", "inherit"],
  });
  child.on("exit", (code) => { if (!stopping) { console.error(`Test service exited: ${code}`); void stop(1); } });
  // Seed a discoverable local-only callback configuration, not a production feature.
  for (let attempt = 0; attempt < 100; attempt++) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/v1/schedules`, {method: "POST", headers: {Authorization: "Bearer console-browser-test-key", "Content-Type": "application/json"}, body: JSON.stringify({
        name: "browser-test-callback", trigger: {type: "delay", seconds: 86400}, target: {url: `http://127.0.0.1:${callbackPort}/ok`},
      })});
      if (response.ok) break;
      throw new Error(`Seed request returned ${response.status}`);
    } catch { if (attempt === 99) throw new Error("Service did not become ready"); await delay(100); }
  }
  console.log("Disposable browser-test service ready");
} catch (error) { console.error(error); await stop(1); }
