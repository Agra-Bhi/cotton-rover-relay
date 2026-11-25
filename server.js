/**
 * Cloud WebSocket Relay Server for Cotton Research Rovers.
 *
 * Both the Raspberry Pi on the rover and the iOS app connect OUTBOUND to this server
 * over WSS/WS, allowing worldwide remote control even when the rover is behind a
 * cellular hotspot / CGNAT.
 */

const http = require("http");
const crypto = require("crypto");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = parseInt(process.env.PORT || "8080", 10);
const HEARTBEAT_MS = 15000;
const MAX_PAYLOAD_BYTES = 64 * 1024;

// Optional static map: ROVER_TOKENS="PI1:secret1,PI2:secret2"
// If omitted, any rover may register with its own passcode (min 6 chars) unless
// RELAY_SERVER_KEY is set, in which case rovers must also match RELAY_SERVER_KEY.
const STATIC_TOKENS = parseStaticTokens(process.env.ROVER_TOKENS || "");
const RELAY_SERVER_KEY = process.env.RELAY_SERVER_KEY || "";

/**
 * Active rooms keyed by roverId:
 *   { rover: WebSocket | null, roverToken: string, controllers: Set<WebSocket> }
 */
const rooms = new Map();

function parseStaticTokens(raw) {
  const map = new Map();
  for (const entry of raw.split(",")) {
    const trimmed = entry.trim();
    if (!trimmed) continue;
    const idx = trimmed.indexOf(":");
    if (idx > 0) {
      map.set(trimmed.slice(0, idx).trim(), trimmed.slice(idx + 1).trim());
    }
  }
  return map;
}

function safeEqual(a, b) {
  if (typeof a !== "string" || typeof b !== "string") return false;
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
}

function sendJson(ws, payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function getOrCreateRoom(roverId) {
  let room = rooms.get(roverId);
  if (!room) {
    room = { rover: null, roverToken: null, controllers: new Set() };
    rooms.set(roverId, room);
  }
  return room;
}

function broadcastControllers(room, payload) {
  const data = JSON.stringify(payload);
  for (const ctrl of room.controllers) {
    if (ctrl.readyState === WebSocket.OPEN) {
      ctrl.send(data);
    }
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    const summary = [];
    for (const [roverId, room] of rooms.entries()) {
      summary.push({
        roverId,
        roverOnline: Boolean(room.rover && room.rover.readyState === WebSocket.OPEN),
        controllers: room.controllers.size,
      });
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, service: "cotton-rover-relay", rovers: summary }));
    return;
  }
  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({ server, maxPayload: MAX_PAYLOAD_BYTES });

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.meta = null; // { role: 'rover'|'controller', roverId: string }

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  // Require handshake within 10 seconds
  const authTimer = setTimeout(() => {
    if (!ws.meta) {
      sendJson(ws, { type: "error", message: "Handshake timeout" });
      ws.close(4001, "Handshake timeout");
    }
  }, 10000);

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString("utf8"));
    } catch {
      sendJson(ws, { type: "error", message: "Invalid JSON" });
      return;
    }
    if (!msg || typeof msg !== "object") return;

    // Step 1: Handshake
    if (!ws.meta) {
      if (msg.type !== "hello") {
        sendJson(ws, { type: "error", message: "First message must be hello" });
        ws.close(4002, "Expected hello");
        return;
      }
      const role = msg.role;
      const roverId = typeof msg.roverId === "string" ? msg.roverId.trim() : "";
      const token = typeof msg.token === "string" ? msg.token : "";

      if (!roverId || roverId.length > 32) {
        sendJson(ws, { type: "error", message: "Invalid roverId" });
        ws.close(4003, "Invalid roverId");
        return;
      }

      if (role === "rover") {
        if (RELAY_SERVER_KEY && !safeEqual(msg.serverKey || "", RELAY_SERVER_KEY)) {
          sendJson(ws, { type: "error", message: "Invalid relay server key" });
          ws.close(4004, "Unauthorized");
          return;
        }
        const staticToken = STATIC_TOKENS.get(roverId);
        if (staticToken && !safeEqual(token, staticToken)) {
          sendJson(ws, { type: "error", message: "Invalid rover token" });
          ws.close(4004, "Unauthorized");
          return;
        }
        if (!staticToken && token.length < 6) {
          sendJson(ws, { type: "error", message: "Rover passcode must be at least 6 characters" });
          ws.close(4004, "Weak token");
          return;
        }

        clearTimeout(authTimer);
        const room = getOrCreateRoom(roverId);
        if (room.rover && room.rover !== ws && room.rover.readyState === WebSocket.OPEN) {
          room.rover.close(4009, "Replaced by newer rover connection");
        }
        room.rover = ws;
        room.roverToken = staticToken || token;
        ws.meta = { role: "rover", roverId };

        sendJson(ws, {
          type: "welcome",
          roverId,
          roverOnline: true,
          controllerCount: room.controllers.size,
        });
        broadcastControllers(room, { type: "peer_status", roverId, roverOnline: true });
        console.log(`[Relay] Rover connected: ${roverId} (controllers=${room.controllers.size})`);
        return;
      }

      if (role === "controller") {
        const room = rooms.get(roverId);
        const expected = STATIC_TOKENS.get(roverId) || (room ? room.roverToken : null);
        if (!expected) {
          sendJson(ws, { type: "error", message: `Rover "${roverId}" is not online yet` });
          ws.close(4005, "Rover offline");
          return;
        }
        if (!safeEqual(token, expected)) {
          sendJson(ws, { type: "error", message: "Incorrect passcode for this rover" });
          ws.close(4004, "Unauthorized");
          return;
        }

        clearTimeout(authTimer);
        room.controllers.add(ws);
        ws.meta = { role: "controller", roverId };

        const roverOnline = Boolean(room.rover && room.rover.readyState === WebSocket.OPEN);
        sendJson(ws, { type: "welcome", roverId, roverOnline });
        sendJson(room.rover, {
          type: "peer_status",
          roverId,
          controllerCount: room.controllers.size,
        });
        console.log(`[Relay] Controller connected to ${roverId} (controllers=${room.controllers.size})`);
        return;
      }

      sendJson(ws, { type: "error", message: "Unknown role" });
      ws.close(4003, "Unknown role");
      return;
    }

    // Step 2: Message forwarding
    const room = rooms.get(ws.meta.roverId);
    if (!room) return;

    if (ws.meta.role === "controller") {
      if (msg.type === "ping") {
        sendJson(ws, { type: "pong", ts: msg.ts, relay: true });
      }
      if (room.rover && room.rover.readyState === WebSocket.OPEN) {
        room.rover.send(JSON.stringify(msg));
      } else if (msg.type !== "ping") {
        sendJson(ws, { type: "info", level: "warn", message: "Rover is currently offline" });
      }
    } else if (ws.meta.role === "rover") {
      broadcastControllers(room, msg);
    }
  });

  ws.on("close", () => {
    clearTimeout(authTimer);
    if (!ws.meta) return;
    const { role, roverId } = ws.meta;
    const room = rooms.get(roverId);
    if (!room) return;

    if (role === "rover" && room.rover === ws) {
      room.rover = null;
      broadcastControllers(room, { type: "peer_status", roverId, roverOnline: false });
      console.log(`[Relay] Rover disconnected: ${roverId}`);
    } else if (role === "controller") {
      room.controllers.delete(ws);
      if (room.rover && room.rover.readyState === WebSocket.OPEN) {
        sendJson(room.rover, {
          type: "peer_status",
          roverId,
          controllerCount: room.controllers.size,
        });
      }
      console.log(`[Relay] Controller disconnected from ${roverId} (controllers=${room.controllers.size})`);
    }
  });
});

// Keep-alive sweep so cellular drops are detected quickly
const interval = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, HEARTBEAT_MS);

wss.on("close", () => clearInterval(interval));

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[Relay] Listening on http://0.0.0.0:${PORT} (ws:// and /health)`);
});
