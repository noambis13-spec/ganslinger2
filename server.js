/* ==========================================================================
   GUNSLINGER 3D NETWORK - Render server
   --------------------------------------------------------------------------
   * Serves the game (public/index.html)
   * Room-code relay over Socket.IO. The HOST browser still runs the whole
     game simulation (bots, bullets, damage, loot, portal) exactly like the
     PeerJS version did; this server only creates rooms and forwards messages
     between the host and the guests of a room.

   Socket events
     client -> server
       createRoom                 host asks for a new room            -> roomCreated {code}
       joinRoom   {code}          guest joins a room                  -> roomJoined {code} | roomError {reason}
       toHost     <msg>           guest -> host      (msg = {t:'join'|'ready'|'in'|'sh'|'pg', ...})
       toGuest    {sid, m}        host  -> one guest (msg = {t:'welcome'|'lobby'|'world'|'s'|'ev'|'end'|'pn'|'deny'})
       kick       {sid}           host removes a guest from the room
     server -> client
       fromGuest  {sid, m}        (to host)
       fromHost   <msg>           (to guest)
       peerLeft   {sid}           (to host)  a guest disconnected
       hostLeft                   (to guests) the host disconnected, room closed
   ========================================================================== */
'use strict';
const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 10;                       // host + guests

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  transports: ['websocket', 'polling'],
  maxHttpBufferSize: 1e6,
  perMessageDeflate: false,
  pingInterval: 20000,
  pingTimeout: 25000,
});

app.disable('x-powered-by');
app.use(express.static(path.join(__dirname, 'public'), { maxAge: '5m' }));
app.get('/healthz', (req, res) => res.type('text').send('ok'));

/* code -> { host: socketId, guests: Set<socketId> } */
const rooms = new Map();

function newCode() {
  for (let i = 0; i < 60; i++) {
    const c = String(10000 + Math.floor(Math.random() * 90000));   // same 5-digit codes as before
    if (!rooms.has(c)) return c;
  }
  return null;
}
function rate(socket, max) {                  // simple per-socket message limiter
  const b = socket.data.b, now = Date.now();
  if (now - b.t >= 1000) { b.t = now; b.n = 0; }
  return ++b.n <= max;
}
const validMsg = m => m && typeof m === 'object' && typeof m.t === 'string' && m.t.length <= 16;
const sockOf = id => io.sockets.sockets.get(id);

function detach(s) {                          // take a socket out of its room (no events sent)
  const code = s.data.code;
  if (code) s.leave(code);
  s.data.code = null; s.data.role = null;
}

io.on('connection', socket => {
  socket.data = { role: null, code: null, b: { t: Date.now(), n: 0 } };

  socket.on('createRoom', () => {
    if (socket.data.code) return;
    const code = newCode();
    if (!code) return socket.emit('roomError', { reason: 'Server is busy, try again.' });
    rooms.set(code, { host: socket.id, guests: new Set() });
    socket.data.role = 'host'; socket.data.code = code;
    socket.join(code);
    socket.emit('roomCreated', { code });
  });

  socket.on('joinRoom', p => {
    if (socket.data.code) return;
    const code = String((p && p.code) || '').replace(/\D/g, '');
    const room = rooms.get(code);
    if (!room) return socket.emit('roomError', { reason: 'Room not found — check the code.' });
    if (room.guests.size + 1 >= MAX_PLAYERS) return socket.emit('roomError', { reason: 'Room is full (10/10).' });
    room.guests.add(socket.id);
    socket.data.role = 'guest'; socket.data.code = code;
    socket.join(code);
    socket.emit('roomJoined', { code });
  });

  /* guest -> host */
  socket.on('toHost', m => {
    if (socket.data.role !== 'guest' || !rate(socket, 200) || !validMsg(m)) return;
    const room = rooms.get(socket.data.code); if (!room) return;
    const h = sockOf(room.host); if (h) h.emit('fromGuest', { sid: socket.id, m });
  });

  /* host -> one guest */
  socket.on('toGuest', p => {
    if (socket.data.role !== 'host' || !rate(socket, 4000) || !p || !validMsg(p.m)) return;
    const room = rooms.get(socket.data.code);
    if (!room || !room.guests.has(p.sid)) return;
    const g = sockOf(p.sid); if (g) g.emit('fromHost', p.m);
  });

  /* host removes a guest (e.g. after a 'deny') */
  socket.on('kick', p => {
    if (socket.data.role !== 'host' || !p) return;
    const room = rooms.get(socket.data.code);
    if (!room || !room.guests.has(p.sid)) return;
    room.guests.delete(p.sid);
    const g = sockOf(p.sid);
    if (g) { detach(g); setTimeout(() => { try { g.disconnect(true); } catch (e) {} }, 1500); }
  });

  socket.on('disconnect', () => {
    const { role, code } = socket.data;
    const room = code && rooms.get(code);
    if (!room) return;
    if (role === 'host') {
      for (const sid of room.guests) {
        const g = sockOf(sid);
        if (g) { g.emit('hostLeft'); detach(g); }
      }
      rooms.delete(code);
    } else if (role === 'guest') {
      if (room.guests.delete(socket.id)) {
        const h = sockOf(room.host); if (h) h.emit('peerLeft', { sid: socket.id });
      }
    }
  });
});

server.listen(PORT, '0.0.0.0', () => console.log('Gunslinger 3D Network listening on port ' + PORT));

function shutdown() { io.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000).unref(); }
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
