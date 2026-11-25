const assert = require("assert");
const { spawn } = require("child_process");
const WebSocket = require("ws");

const PORT = 18080;

function waitForMessage(ws, predicate, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Timed out waiting for message")), timeoutMs);
    const handler = (raw) => {
      const msg = JSON.parse(raw.toString("utf8"));
      if (predicate(msg)) {
        clearTimeout(timer);
        ws.off("message", handler);
        resolve(msg);
      }
    };
    ws.on("message", handler);
  });
}

async function run() {
  const proc = spawn(process.execPath, ["server.js"], {
    env: { ...process.env, PORT: String(PORT) },
    stdio: ["ignore", "pipe", "pipe"],
  });

  await new Promise((resolve, reject) => {
    proc.stdout.on("data", (d) => {
      if (d.toString().includes("Listening")) resolve();
    });
    proc.stderr.on("data", (d) => process.stderr.write(d));
    setTimeout(() => reject(new Error("Server failed to start")), 5000);
  });

  const url = `ws://127.0.0.1:${PORT}`;
  const rover = new WebSocket(url);
  await new Promise((r) => rover.once("open", r));
  rover.send(JSON.stringify({ type: "hello", role: "rover", roverId: "PI1", token: "secret123" }));
  const roverWelcome = await waitForMessage(rover, (m) => m.type === "welcome");
  assert.strictEqual(roverWelcome.roverId, "PI1");

  // Bad passcode rejected
  const badCtrl = new WebSocket(url);
  await new Promise((r) => badCtrl.once("open", r));
  badCtrl.send(JSON.stringify({ type: "hello", role: "controller", roverId: "PI1", token: "wrongpass" }));
  const err = await waitForMessage(badCtrl, (m) => m.type === "error");
  assert.ok(err.message.includes("Incorrect"));

  // Good controller connects and exchanges messages
  const ctrl = new WebSocket(url);
  await new Promise((r) => ctrl.once("open", r));
  ctrl.send(JSON.stringify({ type: "hello", role: "controller", roverId: "PI1", token: "secret123" }));
  const ctrlWelcome = await waitForMessage(ctrl, (m) => m.type === "welcome");
  assert.strictEqual(ctrlWelcome.roverOnline, true);

  const cmdPromise = waitForMessage(rover, (m) => m.type === "drive");
  ctrl.send(JSON.stringify({ type: "drive", left: 0.25, right: 0.25 }));
  const cmd = await cmdPromise;
  assert.strictEqual(cmd.left, 0.25);

  const telPromise = waitForMessage(ctrl, (m) => m.type === "telemetry");
  rover.send(JSON.stringify({ type: "telemetry", roverId: "PI1", gps: { lat: 30.6, lon: -96.3 } }));
  const tel = await telPromise;
  assert.strictEqual(tel.gps.lat, 30.6);

  rover.close();
  ctrl.close();
  badCtrl.close();
  proc.kill();
  console.log("Relay integration test passed.");
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
