const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const players = new Map(); // name -> { ws, range, pos }
const pending = new Map(); // requestId -> name
let mc = null;             // websocket ของ Minecraft

const server = http.createServer((req, res) => {
  fs.readFile(path.join(__dirname, "public", "index.html"), (err, data) => {
    if (err) { res.writeHead(500); return res.end("error"); }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(data);
  });
});

const wss = new WebSocketServer({ noServer: true });
server.on("upgrade", (req, socket, head) => {
  const p = req.url.split("?")[0];
  if (p === "/mc" || p === "/voice") {
    wss.handleUpgrade(req, socket, head, (ws) => { ws.kind = p; wss.emit("connection", ws); });
  } else socket.destroy();
});

const send = (ws, obj) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); };

wss.on("connection", (ws) => {
  if (ws.kind === "/mc") {
    mc = ws;
    console.log("Minecraft connected");
    ws.on("message", (raw) => { try { onMinecraft(JSON.parse(raw)); } catch (e) {} });
    ws.on("close", () => { if (mc === ws) mc = null; console.log("Minecraft disconnected"); });
    return;
  }
  let myName = null;
  ws.on("message", (raw) => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.type === "join") {
      const name = String(m.name || "").replace(/[<>"]/g, "").trim().slice(0, 32);
      if (!name) return;
      const old = players.get(name);
      if (old && old.ws !== ws) old.ws.close();
      myName = name;
      players.set(name, { ws, range: clampRange(m.range), pos: null });
      send(ws, { type: "peers", peers: [...players.keys()].filter((n) => n !== name) });
      for (const [n, p] of players) if (n !== name) send(p.ws, { type: "peer-joined", name });
    } else if (m.type === "range" && myName) {
      players.get(myName).range = clampRange(m.range);
    } else if (m.type === "signal" && myName) {
      const t = players.get(m.to);
      if (t) send(t.ws, { type: "signal", from: myName, data: m.data });
    }
  });
  ws.on("close", () => {
    if (myName && players.get(myName) && players.get(myName).ws === ws) {
      players.delete(myName);
      for (const p of players.values()) send(p.ws, { type: "peer-left", name: myName });
    }
  });
});

function clampRange(r) { r = Number(r); return Math.min(64, Math.max(1, isNaN(r) ? 16 : r)); }

// รับคำตอบจาก Minecraft (ผลของ querytarget)
function onMinecraft(msg) {
  if (!msg.header || msg.header.messagePurpose !== "commandResponse") return;
  const name = pending.get(msg.header.requestId);
  if (!name) return;
  pending.delete(msg.header.requestId);
  const p = players.get(name);
  if (!p) return;
  const body = msg.body || {};
  if (body.statusCode !== 0) { p.pos = null; return; }
  try {
    let d = body.details;
    if (typeof d === "string") d = JSON.parse(d);
    const t = Array.isArray(d) ? d[0] : d;
    p.pos = { x: t.position.x, y: t.position.y, z: t.position.z, dim: t.dimension };
  } catch (e) { p.pos = null; }
}

// ถามตำแหน่งผู้เล่นทุก 0.5 วินาที
setInterval(() => {
  if (!mc || mc.readyState !== 1) return;
  for (const name of players.keys()) {
    const id = crypto.randomUUID();
    pending.set(id, name);
    send(mc, {
      header: { version: 1, requestId: id, messageType: "commandRequest", messagePurpose: "commandRequest" },
      body: { version: 1, commandLine: `querytarget "${name}"`, origin: { type: "player" } },
    });
  }
  if (pending.size > 500) pending.clear();
}, 500);

// คำนวณความดังของแต่ละคนตามระยะ แล้วส่งให้ทุกคนทุก 0.3 วินาที
setInterval(() => {
  const mcOn = !!mc && mc.readyState === 1;
  for (const [ln, L] of players) {
    const volumes = {};
    for (const [sn, S] of players) {
      if (sn === ln) continue;
      if (!mcOn) { volumes[sn] = 1; continue; } // ยังไม่เชื่อม Minecraft: ได้ยินเต็มเสียง
      if (!L.pos || !S.pos || L.pos.dim !== S.pos.dim) { volumes[sn] = 0; continue; }
      const d = Math.hypot(L.pos.x - S.pos.x, L.pos.y - S.pos.y, L.pos.z - S.pos.z);
      volumes[sn] = d >= S.range ? 0 : Math.pow(1 - d / S.range, 1.5);
    }
    send(L.ws, { type: "state", mc: mcOn, located: !!L.pos, volumes });
  }
}, 300);

server.listen(PORT, () => console.log("Proximity voice on port " + PORT));
