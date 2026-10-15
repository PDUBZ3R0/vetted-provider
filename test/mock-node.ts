// -----------------------------------------------------------------------------
// A fake EVM JSON-RPC node with configurable limits, over HTTP and WebSocket.
// The probes are checked against it: every limit configured here must be the
// limit the probes report. The WebSocket side is a minimal RFC 6455 server
// (text frames, masked client frames, ping/close) — enough for JSON-RPC.
// -----------------------------------------------------------------------------

import http from 'node:http';
import crypto from 'node:crypto';
import type { AddressInfo, Socket } from 'node:net';

export type MockOpts = {
    chainId: number;
    head: number;
    maxRange: number;          // getLogs block range
    maxAddresses: number;      // addresses per filter
    topicOnly: boolean;        // address-less filters allowed
    resultCap: number;         // max logs per reply
    revertData: boolean;       // return revert data from eth_call
    gasCap: number;            // eth_call gas granted (clamped silently)
    stateOverride: boolean;
    stateDepth: number;        // blocks of state history
    oldLogs: boolean;
    batchMax: number;
    rps: number | null;        // null = unlimited
    missing: string[];         // methods answered -32601
    lagBlocks?: number;        // report head - lagBlocks
    headMs?: number;           // WS newHeads interval
    wsDieAfterMs?: number;     // each subscription stops delivering this long after it starts
};

const A = '0x' + 'a'.repeat(40), B = '0x' + 'b'.repeat(40);
const TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const APPROVAL = '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925';
const hex = (n: number) => '0x' + n.toString(16);

export class MockNode {
    readonly o: MockOpts;
    server!: http.Server;
    url = '';
    wsUrl = '';
    private hits: number[] = [];
    requests = 0;
    filterSeq = Math.floor(Math.random() * 1e6);
    filterIds = new Set<string>();
    constructor(o: MockOpts) { this.o = o; }

    private limited(): boolean {
        if (this.o.rps == null) return false;
        const now = Date.now();
        this.hits = this.hits.filter(t => now - t < 1000);
        this.hits.push(now);
        return this.hits.length > this.o.rps;
    }

    private head() { return this.o.head - (this.o.lagBlocks ?? 0); }

    handle(req: { id?: unknown; method: string; params?: any[] }): { result?: unknown; error?: { code: number; message: string; data?: unknown } } {
        const o = this.o, p = req.params ?? [];
        this.requests++;
        if (o.missing.includes(req.method)) return { error: { code: -32601, message: `the method ${req.method} does not exist/is not available` } };
        switch (req.method) {
            case 'eth_chainId': return { result: hex(o.chainId) };
            case 'eth_blockNumber': return { result: hex(this.head()) };
            case 'web3_clientVersion': return { result: 'MockNode/v1' };
            case 'eth_getBalance': {
                const blk = parseInt(p[1], 16);
                if (this.head() - blk > o.stateDepth) return { error: { code: -32000, message: 'missing trie node' } };
                return { result: '0x0' };
            }
            case 'eth_call': {
                const tx = p[0] ?? {};
                if (p[2]) {
                    if (!o.stateOverride) return { error: { code: -32602, message: 'too many arguments, want at most 2' } };
                    return { result: '0x' + '0'.repeat(62) + '2a' };
                }
                if (tx.data?.startsWith('0x63deadbeef')) {
                    return { error: o.revertData ? { code: 3, message: 'execution reverted', data: '0xdeadbeef' } : { code: -32000, message: 'execution reverted' } };
                }
                if (tx.data === '0x5a60005260206000f3') {
                    const g = Math.min(parseInt(tx.gas ?? '0x2faf080', 16), o.gasCap) - 21_100;
                    return { result: '0x' + g.toString(16).padStart(64, '0') };
                }
                return { result: '0x' };
            }
            case 'eth_getLogs': {
                const f = p[0] ?? {};
                const from = parseInt(f.fromBlock ?? hex(this.head()), 16), to = parseInt(f.toBlock ?? hex(this.head()), 16);
                if (to - from + 1 > o.maxRange) return { error: { code: -32005, message: `block range is too large, max ${o.maxRange}` } };
                const addrs: string[] | null = f.address == null ? null : Array.isArray(f.address) ? f.address : [f.address];
                if (addrs && addrs.length > o.maxAddresses) return { error: { code: -32602, message: `too many addresses, max ${o.maxAddresses}` } };
                if (!addrs && f.topics && !o.topicOnly) return { error: { code: -32701, message: 'Please specify an address in your request' } };
                if (from < 100 && !o.oldLogs) return { error: { code: -32000, message: 'requested block range is beyond history' } };
                const t0 = f.topics?.[0];
                const want = (t: string) => t0 == null || (Array.isArray(t0) ? t0.includes(t) : t0 === t);
                const out: unknown[] = [];
                for (let b = from; b <= to; b++) {
                    for (const [addr, topic] of [[A, TRANSFER], [A, APPROVAL], [B, TRANSFER]]) {
                        if (addrs && !addrs.map(x => x.toLowerCase()).includes(addr)) continue;
                        if (!want(topic)) continue;
                        out.push({ address: addr, topics: [topic], blockNumber: hex(b), data: '0x' });
                        if (out.length > o.resultCap) return { error: { code: -32005, message: `query returned more than ${o.resultCap} results` } };
                    }
                }
                return { result: out };
            }
            case 'eth_newFilter': { const id = hex(++this.filterSeq); this.filterIds.add(id); return { result: id }; }
            case 'eth_getFilterChanges':
            case 'eth_uninstallFilter':
                if (!this.filterIds.has(p[0])) return { error: { code: -32000, message: 'filter not found' } };
                if (req.method === 'eth_uninstallFilter') { this.filterIds.delete(p[0]); return { result: true }; }
                return { result: [] };
            case 'eth_sendRawTransaction': return { result: '0x' + '11'.repeat(32) };
            default: return { result: null };
        }
    }

    async start(): Promise<void> {
        this.server = http.createServer((req, res) => {
            let body = '';
            req.on('data', c => body += c);
            req.on('end', () => {
                if (this.limited()) { res.writeHead(429, { 'retry-after': '1' }); res.end('{"jsonrpc":"2.0","id":null,"error":{"code":-32005,"message":"rate limited"}}'); return; }
                let msg: any;
                try { msg = JSON.parse(body); } catch { res.writeHead(400); res.end(); return; }
                const reply = this.dispatch(msg);
                res.writeHead(200, { 'content-type': 'application/json' });
                res.end(JSON.stringify(reply));
            });
        });
        this.server.on('upgrade', (req, socket) => this.upgrade(req, socket as Socket));
        await new Promise<void>(r => this.server.listen(0, '127.0.0.1', r));
        const port = (this.server.address() as AddressInfo).port;
        this.url = `http://127.0.0.1:${port}`;
        this.wsUrl = `ws://127.0.0.1:${port}`;
    }

    private dispatch(msg: any): unknown {
        if (Array.isArray(msg)) {
            if (msg.length > this.o.batchMax) return { jsonrpc: '2.0', id: null, error: { code: -32600, message: `batch too large, max ${this.o.batchMax}` } };
            return msg.map(m => ({ jsonrpc: '2.0', id: m.id, ...this.handle(m) }));
        }
        return { jsonrpc: '2.0', id: msg.id, ...this.handle(msg) };
    }

    // --- WebSocket ---
    private upgrade(req: http.IncomingMessage, socket: Socket): void {
        const key = req.headers['sec-websocket-key'];
        const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
        socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' + `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
        const send = (obj: unknown) => { if (!socket.destroyed) socket.write(frame(Buffer.from(JSON.stringify(obj)))); };
        const timers: ReturnType<typeof setInterval>[] = [];
        let buf = Buffer.alloc(0), subN = 0;
        socket.on('data', chunk => {
            buf = Buffer.concat([buf, chunk]);
            for (;;) {
                const f = parseFrame(buf);
                if (!f) break;
                buf = buf.subarray(f.length);
                if (f.op === 8) { socket.end(frame(Buffer.alloc(0), 8)); return; }
                if (f.op === 9) { socket.write(frame(f.payload, 10)); continue; }
                if (f.op !== 1) continue;
                let msg: any;
                try { msg = JSON.parse(f.payload.toString()); } catch { continue; }
                if (this.limited()) { send({ jsonrpc: '2.0', id: Array.isArray(msg) ? null : msg.id, error: { code: -32005, message: 'rate limited' } }); continue; }
                if (!Array.isArray(msg) && msg.method === 'eth_subscribe') {
                    const id = hex(++subN), kind = msg.params?.[0];
                    send({ jsonrpc: '2.0', id: msg.id, result: id });
                    let n = 0;
                    const subAt = Date.now();
                    timers.push(setInterval(() => {
                        if (this.o.wsDieAfterMs != null && Date.now() - subAt > this.o.wsDieAfterMs) return;
                        n++;
                        const result = kind === 'newHeads' ? { number: hex(this.head() + n) } : { address: A, topics: [TRANSFER] };
                        send({ jsonrpc: '2.0', method: 'eth_subscription', params: { subscription: id, result } });
                    }, this.o.headMs ?? 300));
                    continue;
                }
                if (!Array.isArray(msg) && msg.method === 'eth_unsubscribe') { send({ jsonrpc: '2.0', id: msg.id, result: true }); continue; }
                send(this.dispatch(msg));
            }
        });
        socket.on('close', () => timers.forEach(clearInterval));
        socket.on('error', () => timers.forEach(clearInterval));
    }

    stop(): Promise<void> {
        this.server.closeAllConnections?.();
        return new Promise(r => this.server.close(() => r()));
    }
}

function frame(payload: Buffer, op = 1): Buffer {
    const n = payload.length;
    const head = n < 126 ? Buffer.from([0x80 | op, n]) : n < 65536 ? Buffer.from([0x80 | op, 126, n >> 8, n & 255]) : (() => { const h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); return h; })();
    return Buffer.concat([head, payload]);
}

function parseFrame(b: Buffer): { op: number; payload: Buffer; length: number } | null {
    if (b.length < 2) return null;
    const op = b[0] & 15, masked = (b[1] & 128) !== 0;
    let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return null; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return null; len = Number(b.readBigUInt64BE(2)); off = 10; }
    const maskOff = off; if (masked) off += 4;
    if (b.length < off + len) return null;
    const payload = Buffer.from(b.subarray(off, off + len));
    if (masked) for (let i = 0; i < len; i++) payload[i] ^= b[maskOff + (i & 3)];
    return { op, payload, length: off + len };
}
