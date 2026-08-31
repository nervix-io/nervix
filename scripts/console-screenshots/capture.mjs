#!/usr/bin/env node
// Capture the web console images the book publishes.
//
// The console is driven through the surfaces a reader uses: a real nervix-server
// process serving /console/, seeded with nervix-cli. Nothing here reaches into the
// test harness, so a published image is what the documented binaries actually
// render. Console behaviour itself is covered by tests/features/web-console.
//
//   node capture.mjs --server <path> --cli <path> --output <dir>

import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";

import { chromium } from "playwright";

const HERE = dirname(fileURLToPath(import.meta.url));

// Matches the single node the quickstart starts, so the console the reader sees
// here is the console they get by following it.
const NODE_ID = "node-1";
const USER = "default";
const PASSWORD = "nervix-docs";
const DOMAIN = "quickstart";

// Wide enough that the sidebar plus a zoomed-out execution graph fit without the
// graph being clipped, and the aspect ratio the published pages are laid out for.
const VIEWPORT = { width: 1920, height: 1200 };
// A single-row graph leaves a large empty band in a full-height stage, so the
// graph-only image is framed in a short viewport instead.
const GRAPH_VIEWPORT = { width: 1920, height: 620 };
// Captured at device pixels so the images stay legible when a reader opens one
// full size; without `scale: "device"` Playwright downsamples this away again.
const DEVICE_SCALE_FACTOR = 2;
// Half of the graph's 2.7s pulse period. Every edge shares that duration, so
// freezing here puts every pulse at the midpoint of its own path.
const PULSE_FREEZE_OFFSET_SECONDS = 1.35;

const READY_TIMEOUT_MS = 90_000;
const SETTLE_TIMEOUT_MS = 20_000;
const SETTLE_INTERVAL_MS = 150;
const POLL_INTERVAL_MS = 200;

// `Animations: disabled` freezes CSS animations and transitions; the SVG pulses
// are SMIL and are frozen separately in settle().
const CAPTURE_OPTIONS = {
  type: "png",
  animations: "disabled",
  caret: "hide",
  scale: "device",
};

const CHROME_CANDIDATES = [
  "/usr/bin/google-chrome",
  "/usr/bin/google-chrome-stable",
  "/usr/bin/chromium",
  "/usr/bin/chromium-browser",
];

function parseArguments(argv) {
  const parsed = {};
  for (let index = 0; index < argv.length; index += 2) {
    const key = argv[index];
    const value = argv[index + 1];
    if (!key.startsWith("--") || value === undefined) {
      throw new Error(`expected --key value pairs, got '${argv.slice(index).join(" ")}'`);
    }
    parsed[key.slice(2)] = value;
  }
  for (const required of ["server", "cli", "output"]) {
    if (!parsed[required]) {
      throw new Error(`--${required} is required`);
    }
  }
  return parsed;
}

async function freePort() {
  return new Promise((resolvePort, reject) => {
    const probe = createServer();
    probe.on("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const { port } = probe.address();
      probe.close(() => resolvePort(port));
    });
  });
}

function run(command, args, options = {}) {
  return new Promise((resolveRun, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"], ...options });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => (stdout += chunk));
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolveRun(stdout);
        return;
      }
      reject(new Error(`${command} exited with ${code}\n${stdout}\n${stderr}`));
    });
  });
}

async function startServer(serverBinary, ports, dbPath) {
  const child = spawn(serverBinary, ["--allow-bootstrap"], {
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      ...process.env,
      NERVIX_NODE_ID: NODE_ID,
      NERVIX_ADDR: `127.0.0.1:${ports.grpc}`,
      NERVIX_HTTP_LISTEN_ADDR: `127.0.0.1:${ports.http}`,
      NERVIX_HTTPS_LISTEN_ADDR: `127.0.0.1:${ports.https}`,
      NERVIX_OBSERVABILITY_LISTEN_ADDR: `127.0.0.1:${ports.observability}`,
      NERVIX_WEB_CONSOLE_LISTEN_ADDR: `127.0.0.1:${ports.console}`,
      NERVIX_CLUSTER_API_LISTEN_ADDR: `127.0.0.1:${ports.clusterApi}`,
      NERVIX_CLUSTER_API_ADVERTISE_ADDR: `127.0.0.1:${ports.clusterApi}`,
      NERVIX_DB_PATH: dbPath,
      NERVIX_INIT_DEFAULT_USER_PASSWORD: PASSWORD,
    },
  });

  let log = "";
  child.stdout.on("data", (chunk) => (log += chunk));
  child.stderr.on("data", (chunk) => (log += chunk));
  let exited = null;
  child.on("close", (code) => (exited = code));

  const readyUrl = `http://127.0.0.1:${ports.observability}/readyz`;
  const deadline = Date.now() + READY_TIMEOUT_MS;
  for (;;) {
    if (exited !== null) {
      throw new Error(`nervix-server exited with ${exited} before becoming ready\n${log}`);
    }
    try {
      const response = await fetch(readyUrl);
      if (response.ok) {
        return child;
      }
    } catch {
      // The listener is not up yet; keep polling until the deadline.
    }
    if (Date.now() > deadline) {
      throw new Error(`nervix-server was not ready within ${READY_TIMEOUT_MS}ms\n${log}`);
    }
    await sleep(POLL_INTERVAL_MS);
  }
}

async function stopServer(child) {
  if (child.exitCode !== null) {
    return;
  }
  const closed = new Promise((resolveClose) => child.once("close", resolveClose));
  child.kill("SIGTERM");
  await Promise.race([closed, sleep(10_000).then(() => child.kill("SIGKILL"))]);
}

async function seedGraph(cliBinary, ports) {
  const address = `http://127.0.0.1:${ports.grpc}`;
  const credentials = ["--server", address, "--username", USER, "--password", PASSWORD];

  // /readyz turns green before the bootstrapping node has committed the default
  // user, so an accepted authenticated call is the real readiness signal.
  const deadline = Date.now() + READY_TIMEOUT_MS;
  for (;;) {
    try {
      await run(cliBinary, [...credentials, "--command", "SHOW CLUSTER STATUS;"]);
      break;
    } catch (error) {
      if (Date.now() > deadline) {
        throw new Error(`nervix-cli could not authenticate within ${READY_TIMEOUT_MS}ms: ${error}`);
      }
      await sleep(POLL_INTERVAL_MS);
    }
  }

  await run(cliBinary, [...credentials, "--command", `CREATE UNPACED DOMAIN ${DOMAIN};`]);
  const graph = await readFile(join(HERE, "quickstart.nspl"), "utf8");
  await run(cliBinary, [...credentials, "--domain", DOMAIN, "--command", graph]);
}

async function waitForText(page, selector, text) {
  await page.waitForFunction(
    ([target, needle]) => {
      const element = document.querySelector(target);
      return element !== null && element.innerText.includes(needle);
    },
    [selector, text],
    { timeout: READY_TIMEOUT_MS },
  );
}

// Freezes the graph's SVG timelines, then waits for its layout to stop moving so
// a capture never lands mid-relayout or during the chart's entry animation.
async function settle(page) {
  await page.evaluate((offset) => {
    for (const svg of document.querySelectorAll("svg")) {
      if (typeof svg.pauseAnimations === "function") {
        svg.pauseAnimations();
        svg.setCurrentTime(offset);
      }
    }
  }, PULSE_FREEZE_OFFSET_SECONDS);

  const deadline = Date.now() + SETTLE_TIMEOUT_MS;
  let previous = null;
  for (;;) {
    const signature = await page.evaluate(() => {
      const layer = document.querySelector(".graph-zoom-layer");
      const renders = layer ? layer.getAttribute("data-render-count") : "none";
      const items = Array.from(document.querySelectorAll(".graph-hit-layer > *")).map((item) => {
        const box = item.getBoundingClientRect();
        return `${item.getAttribute("data-label") ?? ""}@${Math.round(box.x)},${Math.round(box.y)}`;
      });
      return `renders=${renders}|${items.join("|")}`;
    });
    if (signature === previous) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error(`graph layout did not settle within ${SETTLE_TIMEOUT_MS}ms: ${signature}`);
    }
    previous = signature;
    await sleep(SETTLE_INTERVAL_MS);
  }
}

async function capture(page, output, name, locator) {
  await settle(page);
  const path = join(output, name);
  const target = locator ? page.locator(locator) : page;
  await target.screenshot({ ...CAPTURE_OPTIONS, path });
  const { size } = await stat(path);
  if (size === 0) {
    throw new Error(`captured ${name} is empty`);
  }
  console.log(`captured ${name} (${Math.round(size / 1024)} KiB)`);
}

async function captureConsole(page, output) {
  // The console shell: sidebar entities, the live execution graph, and the REPL.
  await page.fill(".prompt-row input", "DESCRIBE RELAY orders;");
  await page.press(".prompt-row input", "Enter");
  await waitForText(page, ".terminal", "relay: orders");
  await page.fill(".prompt-row input", "LIST DOMAINS;");
  await page.press(".prompt-row input", "Enter");
  await waitForText(page, ".terminal", `${DOMAIN} pace=UNPACED status=STOPPED`);

  // The whole pipeline is wider than the panel at full zoom, so frame it first.
  await page.click(".zoom-group button[title='Reset zoom']");
  for (let step = 0; step < 3; step += 1) {
    await page.click(".zoom-group button[title='Zoom out']");
  }
  await waitForText(page, ".zoom-group", "70%");
  await capture(page, output, "console-overview.png");

  // The execution graph on its own, framed without the empty band a full-height
  // stage leaves above and below a single-row graph.
  await page.click(".fullscreen-button");
  await page.setViewportSize(GRAPH_VIEWPORT);
  await page.click(".zoom-group button[title='Reset zoom']");
  for (let step = 0; step < 2; step += 1) {
    await page.click(".zoom-group button[title='Zoom out']");
  }
  await waitForText(page, ".graph-panel.fullscreen", "EXECUTION GRAPH");
  await waitForText(page, ".zoom-group", "80%");
  await capture(page, output, "console-graph.png", ".graph-panel");
  await page.click(".fullscreen-button");
  await page.setViewportSize(VIEWPORT);

  // Clicking a graph item opens its action menu. The relay can sit outside the
  // viewport at this zoom, so dispatch the click rather than moving the mouse.
  await page.locator(".relay-hit[data-label='orders']").evaluate((element) => element.click());
  await waitForText(page, ".graph-action-menu", "SUBSCRIBE");
  await capture(page, output, "console-graph-actions.png", ".graph-action-menu");

  // The guided subscription dialog reached from that menu.
  await page.click(".graph-action-list button:has-text('SUBSCRIBE')");
  await waitForText(page, ".subscribe-dialog", "order_record");
  await page.click(".schema-field-button:has-text('amount')");
  await page.fill(".subscribe-dialog input", "input.amount >= 1000");
  await page.click(".sample-options button:has-text('10%')");
  await capture(page, output, "console-subscribe-dialog.png", ".subscribe-dialog");
  await page.click(".subscribe-actions button:has-text('CANCEL')");

  // The REPL with server-driven completion offered for a partial statement.
  await page.fill(".prompt-row input", "DESCRIBE RELAY ");
  await waitForText(page, ".suggestions", "high_value_orders");
  await capture(page, output, "console-repl.png", ".repl-panel");

  // Resource versions uploaded from the browser. The console falls back to the
  // file name when webkitRelativePath is empty, so a nested name lands as a path.
  await page.fill(".prompt-row input", "");
  await page.click(".nav-item.resources:has-text('order_model')");
  await waitForText(page, ".resource-dialog", "order_model");
  await page.locator(".resource-dialog .file-upload-input").setInputFiles([
    {
      name: "scoring.roto",
      mimeType: "text/plain",
      buffer: Buffer.from("filter tier_score(order) { accept }"),
    },
    {
      name: "labels/classes.txt",
      mimeType: "text/plain",
      buffer: Buffer.from("new\npaid\nheld"),
    },
  ]);
  await waitForText(page, ".resource-version-list", "version 1");
  await waitForText(page, ".resource-version-list", "2 files");
  await capture(page, output, "console-resource-dialog.png", ".resource-dialog");
}

async function main() {
  const options = parseArguments(process.argv.slice(2));
  const serverBinary = resolve(options.server);
  const cliBinary = resolve(options.cli);
  for (const binary of [serverBinary, cliBinary]) {
    if (!existsSync(binary)) {
      throw new Error(`${binary} does not exist; build it before capturing`);
    }
  }
  const output = resolve(options.output);
  await rm(output, { recursive: true, force: true });
  await mkdir(output, { recursive: true });

  const ports = {
    grpc: await freePort(),
    http: await freePort(),
    https: await freePort(),
    observability: await freePort(),
    console: await freePort(),
    clusterApi: await freePort(),
  };
  const stateDir = await mkdtemp(join(tmpdir(), "nervix-console-screenshots-"));

  let server = null;
  let browser = null;
  try {
    server = await startServer(serverBinary, ports, join(stateDir, "db"));
    await seedGraph(cliBinary, ports);

    const executablePath = CHROME_CANDIDATES.find((candidate) => existsSync(candidate));
    browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-dev-shm-usage"],
      ...(executablePath ? { executablePath } : {}),
    });
    const context = await browser.newContext({
      viewport: VIEWPORT,
      deviceScaleFactor: DEVICE_SCALE_FACTOR,
    });
    const page = await context.newPage();
    page.setDefaultTimeout(READY_TIMEOUT_MS);

    const auth = Buffer.from(`${USER}:${PASSWORD}`).toString("base64");
    await page.goto(`http://127.0.0.1:${ports.console}/console/?auth=${auth}`);
    await waitForText(page, ".topbar-status .pill.ok", "CONNECTED");
    await waitForText(page, ".graph-hit-layer", "kafka_orders");
    await waitForText(page, ".graph-hit-layer", "route_orders");

    await captureConsole(page, output);
  } finally {
    if (browser !== null) {
      await browser.close();
    }
    if (server !== null) {
      await stopServer(server);
    }
    await rm(stateDir, { recursive: true, force: true });
  }
  console.log(`console screenshots written to ${output}`);
}

await main();
