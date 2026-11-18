'use strict';
/**
 * Drone Strike - realtime duel server.
 *
 * Firebase (Auth/Firestore) keeps accounts, IDs and friends. THIS server only
 * carries the match: it pairs players, then relays their state at network
 * speed over a WebSocket (TCP_NODELAY, no batching, no database in the loop).
 *
 * Model: each phone simulates its own drone and reports its position; shots are
 * relayed and the *victim's* phone decides whether a shot hit it (so what you
 * see on your own screen is what counts). The server decides who won.
 *
 * Env:
 *   PORT                      listen port (default 8080)
 *   REQUIRE_AUTH=1            reject clients without a valid Firebase ID token
 *   FIREBASE_SERVICE_ACCOUNT  path to a service-account JSON (for token checks)
 */
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const REQUIRE_AUTH = process.env.REQUIRE_AUTH === '1';
const COUNTDOWN_MS = 3000;
const ROOM_WAIT_MS = Number(process.env.ROOM_WAIT_MS || 120000);

// ---- Firebase token verification --------------------------------------------
// Key can come from (1) FIREBASE_SERVICE_ACCOUNT_JSON: the whole JSON text in an
// environment variable, or (2) FIREBASE_SERVICE_ACCOUNT: path to the JSON file.
let adminAuth = null;
{
  const fs = require('fs');
  const path = require('path');
  const jsonEnv = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  const saPath = process.env.FIREBASE_SERVICE_ACCOUNT;
  let cred = null;
  try {
    if (jsonEnv) {
      cred = JSON.parse(jsonEnv);
      console.log('Firebase key: read from FIREBASE_SERVICE_ACCOUNT_JSON');
    } else if (saPath) {
      const tries = [saPath, path.join('/etc/secrets', path.basename(saPath)),
                     path.join(process.cwd(), path.basename(saPath))];
      const found = tries.find((f) => fs.existsSync(f));
      if (!found) {
        let listing = '';
        try { listing = ' /etc/secrets contains: [' + fs.readdirSync('/etc/secrets').join(', ') + ']'; }
        catch { listing = ' (/etc/secrets does not exist: no Secret File was created)'; }
        throw new Error('key file not found. Looked in: ' + tries.join(', ') + '.' + listing);
      }
      cred = JSON.parse(fs.readFileSync(found, 'utf8'));
      console.log('Firebase key: read from ' + found);
    } else if (!process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      console.log('No Firebase key configured (set FIREBASE_SERVICE_ACCOUNT_JSON or FIREBASE_SERVICE_ACCOUNT).');
    }
    if (cred || process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      const admin = require('firebase-admin');
      admin.initializeApp(cred ? { credential: admin.credential.cert(cred) } : undefined);
      adminAuth = admin.auth();
    }
  } catch (e) {
    console.error('Firebase auth setup FAILED: ' + e.message);
  }
}

if (REQUIRE_AUTH && !adminAuth) {
  console.error('REQUIRE_AUTH=1 but firebase-admin is not configured. Exiting.');
  process.exit(1);
}
console.log(adminAuth ? 'Auth: Firebase ID tokens verified' : 'Auth: DEV MODE (tokens not verified)');

// ---- helpers ----------------------------------------------------------------
const num = (v, lo, hi, d = 0) => (typeof v === 'number' && isFinite(v) ? Math.min(hi, Math.max(lo, v)) : d);
const clean = (s, n) => String(s == null ? '' : s).replace(/[\u0000-\u001f<>]/g, '').slice(0, n);
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };

const ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function roomCode() {
  let c = '';
  for (let i = 0; i < 5; i++) c += ALPHA[Math.floor(Math.random() * ALPHA.length)];
  return c;
}

// ---- state -------------------------------------------------------------------
const queue = [];            // sockets waiting for a random opponent
const rooms = new Map();     // code -> Room

class Room {
  constructor(code, isPrivate) {
    this.code = code;
    this.isPrivate = isPrivate;
    this.players = [];       // sockets, index = slot
    this.state = 'wait';     // wait | count | live | done
    this.timers = [];
    // Only while waiting for the guest. Cleared in begin(), otherwise it would
    // kill a live friend match 2 minutes after the room was created.
    this.waitTimer = isPrivate ? setTimeout(() => { if (this.state === 'wait') this.destroy('expired'); }, ROOM_WAIT_MS) : null;
    if (this.waitTimer) this.timers.push(this.waitTimer);
  }
  add(ws) {
    this.players.push(ws);
    ws.room = this;
    ws.slot = this.players.length - 1;
  }
  begin() {
    if (this.waitTimer) { clearTimeout(this.waitTimer); this.waitTimer = null; }
    this.state = 'count';
    this.players.forEach((p, i) => {
      const o = this.players[1 - i];
      send(p, { t: 'match', slot: i, room: this.code, countdown: COUNTDOWN_MS,
                opp: { name: o.name, pid: o.pid, look: o.look } });
    });
    this.timers.push(setTimeout(() => {
      if (this.state !== 'count') return;
      this.state = 'live';
      this.players.forEach((p) => send(p, { t: 'go' }));
    }, COUNTDOWN_MS));
  }
  other(ws) { return this.players[1 - ws.slot]; }
  end(winner, reason) {
    if (this.state === 'done') return;
    this.state = 'done';
    // `won` is explicit so a phone never has to work out its own result.
    this.players.forEach((p, i) => send(p, { t: 'end', winner, reason, won: i === winner }));
    this.destroy(null);
  }
  destroy(notify) {
    this.timers.forEach(clearTimeout);
    if (notify) this.players.forEach((p) => send(p, { t: 'err', m: notify }));
    this.players.forEach((p) => { if (p.room === this) p.room = null; });
    rooms.delete(this.code);
  }
}

function leaveEverything(ws) {
  const qi = queue.indexOf(ws);
  if (qi >= 0) queue.splice(qi, 1);
  const r = ws.room;
  if (!r) return;
  if (r.state === 'live' || r.state === 'count') {
    r.end(1 - ws.slot, 'left');
  } else {
    r.destroy(ws.slot === 0 && r.players.length > 1 ? 'host left' : null);
  }
}

function tryMatch() {
  while (queue.length >= 2) {
    const a = queue.shift();
    let bi = queue.findIndex((x) => x.uid !== a.uid);
    if (bi < 0) { queue.unshift(a); return; }
    const b = queue.splice(bi, 1)[0];
    let code; do { code = roomCode(); } while (rooms.has(code));
    const r = new Room(code, false);
    rooms.set(code, r);
    r.add(a); r.add(b);
    r.begin();
  }
}

// ---- message handling ------------------------------------------------------------
const bucketOk = (ws, now) => {
  ws.tokens = Math.min(120, (ws.tokens ?? 120) + ((now - (ws.last ?? now)) / 1000) * 90);
  ws.last = now;
  if (ws.tokens < 1) return false;
  ws.tokens -= 1;
  return true;
};

async function onMessage(ws, raw) {
  if (raw.length > 1024) return;
  let m; try { m = JSON.parse(raw); } catch { return; }
  if (!m || typeof m.t !== 'string') return;
  if (!bucketOk(ws, Date.now())) return;

  // ---- hot path: relay ----
  if (ws.room && ws.room.state !== 'wait') {
    const peer = ws.room.other(ws);
    if (m.t === 's') {        // my drone state
      send(peer, { t: 's', x: num(m.x, -200, 200), z: num(m.z, -200, 200),
        yaw: num(m.yaw, -10, 10), vx: num(m.vx, -80, 80), vz: num(m.vz, -80, 80),
        hp: num(m.hp, 0, 9999), mhp: num(m.mhp, 1, 9999, 100), d: m.d ? 1 : 0, n: num(m.n, 0, 1e9) });
      return;
    }
    if (m.t === 'f' && ws.room.state === 'live') {   // a shot I fired
      send(peer, { t: 'f', x: num(m.x, -200, 200), y: num(m.y, 0, 20, 1), z: num(m.z, -200, 200),
        vx: num(m.vx, -150, 150), vy: num(m.vy, -150, 150), vz: num(m.vz, -150, 150),
        dm: num(m.dm, 0, 500), w: num(m.w, 0, 5), c: m.c ? 1 : 0 });
      return;
    }
    if (m.t === 'dead' && ws.room.state === 'live') { ws.room.end(1 - ws.slot, 'kill'); return; }
  }

  switch (m.t) {
    case 'ping': send(ws, { t: 'pong', c: num(m.c, 0, 1e12) }); break;

    case 'hello': {
      let uid = clean(m.uid, 64);
      if (adminAuth) {
        try { uid = (await adminAuth.verifyIdToken(String(m.token || ''))).uid; }
        catch { send(ws, { t: 'err', m: 'auth failed' }); ws.close(); return; }
      } else if (REQUIRE_AUTH) { ws.close(); return; }
      if (!uid) { ws.close(); return; }
      ws.uid = uid;
      ws.name = clean(m.name, 14) || 'PILOT';
      ws.pid = clean(m.pid, 12);
      const lk = m.look && typeof m.look === 'object' ? m.look : {};
      ws.look = { suit: clean(lk.suit, 24), sp: clean(lk.sp, 24), ap: clean(lk.ap, 24) };   // cosmetic only
      send(ws, { t: 'ready' });
      break;
    }

    case 'queue':
      if (!ws.uid || ws.room || queue.includes(ws)) break;
      queue.push(ws); send(ws, { t: 'queued' }); tryMatch();
      break;

    case 'cancel': {
      const i = queue.indexOf(ws); if (i >= 0) queue.splice(i, 1);
      if (ws.room && ws.room.state === 'wait') ws.room.destroy(null);
      break;
    }

    case 'create': {            // friend challenge: host opens a private room
      if (!ws.uid || ws.room) break;
      let code; do { code = roomCode(); } while (rooms.has(code));
      const r = new Room(code, true); rooms.set(code, r); r.add(ws);
      send(ws, { t: 'created', code });
      break;
    }

    case 'join': {
      if (!ws.uid || ws.room) break;
      const r = rooms.get(clean(m.code, 8).toUpperCase());
      if (!r || r.state !== 'wait' || r.players.length !== 1) { send(ws, { t: 'err', m: 'room not available' }); break; }
      if (r.players[0].uid === ws.uid) { send(ws, { t: 'err', m: 'cannot join your own room' }); break; }
      r.add(ws); r.begin();
      break;
    }

    case 'leave': leaveEverything(ws); break;
  }
}

// ---- boot ----------------------------------------------------------------------------
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, queue: queue.length, rooms: rooms.size }));
  } else { res.writeHead(404); res.end(); }
});

const wss = new WebSocketServer({ server, maxPayload: 2048, perMessageDeflate: false });

wss.on('connection', (ws, req) => {
  if (req.socket.setNoDelay) req.socket.setNoDelay(true);   // no Nagle: send small packets immediately
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (d) => { onMessage(ws, d.toString()).catch(() => {}); });
  ws.on('close', () => leaveEverything(ws));
  ws.on('error', () => {});
});

// Drop dead connections (phone lost signal) so the opponent isn't left hanging.
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false; ws.ping();
  });
}, 8000);

server.listen(PORT, () => console.log(`Duel server listening on :${PORT}`));
