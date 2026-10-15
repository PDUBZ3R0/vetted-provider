// -----------------------------------------------------------------------------
// Raw JSON-RPC clients for probing: HTTP (fetch) and WebSocket (the `ws`
// package — Node's global WebSocket hides its socket, so it can't be unref'd). No ethers on purpose: the probes need to see exactly what came
// back — HTTP status, Retry-After, the error object as sent, how long it took —
// and a library's retries, batching and error wrapping would hide all of it.
// -----------------------------------------------------------------------------
import WebSocket from 'ws';
const DEFAULT_TIMEOUT = 15_000;
let nextId = 1;
// --- classification ------------------------------------------------------------
const RATE_RE = /rate.?limit|too many requests|\b429\b|request limit|exceeded.*(?:rate|requests per|quota|capacity)|capacity exceeded|throttl|over.?quota|daily limit/i;
const MISSING_RE = /method .*(?:not found|does not exist|not supported|not available|is not available|unsupported|disabled|not allowed|blocked|whitelist|not enabled)|unsupported method|not implemented|unknown method|no such method|method not found|-32601/i;
export const errText = (r) => r.transport ?? (r.error ? `${r.error.code}: ${r.error.message}${r.error.data != null ? ` ${typeof r.error.data === 'string' ? r.error.data : JSON.stringify(r.error.data)}` : ''}` : '');
export const isRateLimited = (r) => r.httpStatus === 429 || (!!r.error && RATE_RE.test(r.error.message)) || (!!r.transport && RATE_RE.test(r.transport));
export const isMissingMethod = (r) => r.error?.code === -32601 || (!!r.error && MISSING_RE.test(r.error.message)) || r.httpStatus === 405 && !r.error;
export const ok = (r) => r.error == null && r.transport == null;
// --- HTTP ----------------------------------------------------------------------
export class HttpClient {
    url;
    kind = 'http';
    constructor(url) { this.url = url; }
    async post(body, timeoutMs) {
        const t0 = performance.now();
        let res;
        try {
            res = await fetch(this.url, {
                method: 'POST',
                headers: { 'content-type': 'application/json', accept: 'application/json' },
                body: JSON.stringify(body),
                signal: AbortSignal.timeout(timeoutMs),
            });
        }
        catch (e) {
            const err = e;
            const why = err.name === 'TimeoutError' ? `timeout ${timeoutMs}ms` : (err.cause?.code ?? err.cause?.message ?? err.message);
            return { json: null, reply: { transport: why, ms: performance.now() - t0 } };
        }
        const text = await res.text().catch(() => '');
        const ms = performance.now() - t0;
        const base = { httpStatus: res.status, retryAfter: res.headers.get('retry-after') ?? undefined, ms };
        let json = null;
        try {
            json = text ? JSON.parse(text) : null;
        }
        catch { /* not JSON */ }
        if (json == null) {
            return { json: null, reply: { ...base, transport: `HTTP ${res.status}${text ? `: ${text.slice(0, 120).replace(/\s+/g, ' ')}` : ''}` } };
        }
        return { json, reply: base };
    }
    async call(method, params = [], timeoutMs = DEFAULT_TIMEOUT) {
        const { json, reply } = await this.post({ jsonrpc: '2.0', id: nextId++, method, params }, timeoutMs);
        if (json == null)
            return reply;
        const j = (Array.isArray(json) ? json[0] : json);
        if (!j || typeof j !== 'object')
            return { ...reply, transport: `unexpected body ${JSON.stringify(json).slice(0, 80)}` };
        if (j.error)
            return { ...reply, error: normErr(j.error) };
        if (!('result' in j))
            return { ...reply, transport: `no result or error in reply ${JSON.stringify(j).slice(0, 80)}` };
        return { ...reply, result: j.result };
    }
    async batch(calls, timeoutMs = DEFAULT_TIMEOUT) {
        const body = calls.map(c => ({ jsonrpc: '2.0', id: nextId++, method: c.method, params: c.params ?? [] }));
        const { json, reply } = await this.post(body, timeoutMs);
        if (json == null)
            return { replies: null, reply };
        if (!Array.isArray(json)) {
            const j = json;
            return { replies: null, reply: { ...reply, error: j.error ? normErr(j.error) : { code: 0, message: `batch answered with a single object ${JSON.stringify(json).slice(0, 80)}` } } };
        }
        return { replies: json, reply };
    }
    close() { }
}
export class WsClient {
    url;
    kind = 'ws';
    ws = null;
    pending = new Map();
    batches = new Map();
    closedWhy = null;
    /** eth_subscription notifications, by subscription id. */
    subs = new Map();
    /** Time of the last frame of any kind from the server. */
    lastRx = 0;
    constructor(url) { this.url = url; }
    /** Open the socket. Resolves to an error string on failure, null on success. */
    connect(timeoutMs = 10_000) {
        return new Promise(resolve => {
            let settled = false;
            const done = (why) => { if (!settled) {
                settled = true;
                clearTimeout(t);
                resolve(why);
            } };
            const t = setTimeout(() => { done(`connect timeout ${timeoutMs}ms`); try {
                ws.close();
            }
            catch { /* */ } }, timeoutMs);
            let ws;
            try {
                ws = new WebSocket(this.url);
            }
            catch (e) {
                done(e.message);
                return;
            }
            this.ws = ws;
            ws.onopen = () => { this.hold(); done(null); };
            // The browser-style WebSocket hides the HTTP status of a refused upgrade
            // (HyperEVM answers 405); all we get is an error event, then close.
            ws.onerror = (ev) => done(`connect failed${ev.message ? `: ${ev.message}` : ''}`);
            ws.onclose = (ev) => {
                this.closedWhy = `socket closed (code ${ev.code}${ev.reason ? ` ${ev.reason}` : ''})`;
                done(this.closedWhy);
                for (const [, p] of this.pending) {
                    clearTimeout(p.timer);
                    p.resolve({ transport: this.closedWhy });
                }
                for (const [, p] of this.batches) {
                    clearTimeout(p.timer);
                    p.resolve({ transport: this.closedWhy });
                }
                this.pending.clear();
                this.batches.clear();
            };
            ws.onmessage = (ev) => { this.onMessage(typeof ev.data === 'string' ? ev.data : Buffer.from(ev.data).toString()); this.hold(); };
        });
    }
    get open() { return this.ws?.readyState === 1; }
    /**
     * Keep the process alive only while something is waiting on this socket (a
     * request, a batch, a subscription). An idle open socket must not stop a
     * finished script from exiting — the old single-endpoint provider did the
     * same with ethers' socket.
     */
    hold() {
        const sock = this.ws?._socket;
        if (!sock)
            return;
        if (this.pending.size || this.batches.size || this.subs.size)
            sock.ref();
        else
            sock.unref();
    }
    onMessage(text) {
        this.lastRx = Date.now();
        let msg;
        try {
            msg = JSON.parse(text);
        }
        catch {
            return;
        }
        if (Array.isArray(msg)) {
            const key = msg.map(m => m.id).filter(x => typeof x === 'number').sort((a, b) => a - b).join(',');
            const p = this.batches.get(key) ?? [...this.batches.values()][0]; // id:null replies cannot be keyed: take the oldest
            if (p) {
                for (const [k, v] of this.batches)
                    if (v === p)
                        this.batches.delete(k);
                clearTimeout(p.timer);
                p.resolve({ replies: msg, ms: performance.now() - p.t0 });
            }
            return;
        }
        const m = msg;
        if (m.method === 'eth_subscription' && m.params?.subscription) {
            this.subs.get(m.params.subscription)?.(m.params.result);
            return;
        }
        if (typeof m.id === 'number') {
            const p = this.pending.get(m.id);
            if (!p)
                return;
            this.pending.delete(m.id);
            clearTimeout(p.timer);
            p.resolve(m.error ? { error: normErr(m.error), ms: performance.now() - p.t0 } : { result: m.result, ms: performance.now() - p.t0 });
        }
    }
    call(method, params = [], timeoutMs = DEFAULT_TIMEOUT) {
        const t0 = performance.now();
        if (!this.open)
            return Promise.resolve({ transport: this.closedWhy ?? 'socket not open', ms: 0 });
        const id = nextId++;
        return new Promise(resolve => {
            const timer = setTimeout(() => { this.pending.delete(id); this.hold(); resolve({ transport: `timeout ${timeoutMs}ms`, ms: performance.now() - t0 }); }, timeoutMs);
            this.pending.set(id, { resolve: v => resolve(v), timer, t0 });
            this.hold();
            try {
                this.ws.send(JSON.stringify({ jsonrpc: '2.0', id, method, params }));
            }
            catch (e) {
                clearTimeout(timer);
                this.pending.delete(id);
                this.hold();
                resolve({ transport: e.message, ms: performance.now() - t0 });
            }
        });
    }
    batch(calls, timeoutMs = DEFAULT_TIMEOUT) {
        const t0 = performance.now();
        if (!this.open)
            return Promise.resolve({ replies: null, reply: { transport: this.closedWhy ?? 'socket not open', ms: 0 } });
        const body = calls.map(c => ({ jsonrpc: '2.0', id: nextId++, method: c.method, params: c.params ?? [] }));
        const key = body.map(b => b.id).join(',');
        return new Promise(resolve => {
            const timer = setTimeout(() => { this.batches.delete(key); this.hold(); resolve({ replies: null, reply: { transport: `timeout ${timeoutMs}ms`, ms: performance.now() - t0 } }); }, timeoutMs);
            this.batches.set(key, {
                resolve: v => {
                    const x = v;
                    resolve(x.replies ? { replies: x.replies, reply: { ms: x.ms ?? 0 } } : { replies: null, reply: { transport: x.transport ?? 'no reply', ms: performance.now() - t0 } });
                },
                timer, t0,
            });
            this.hold();
            try {
                this.ws.send(JSON.stringify(body));
            }
            catch (e) {
                clearTimeout(timer);
                this.batches.delete(key);
                this.hold();
                resolve({ replies: null, reply: { transport: e.message, ms: 0 } });
            }
        });
    }
    /** eth_subscribe; returns the subscription id or the failed reply. */
    async subscribe(params, onEvent) {
        const r = await this.call('eth_subscribe', params);
        if (!ok(r) || typeof r.result !== 'string')
            return r;
        this.subs.set(r.result, onEvent);
        this.hold();
        return r.result;
    }
    close() {
        try {
            this.ws?.close();
        }
        catch { /* */ }
        for (const [, p] of this.pending)
            clearTimeout(p.timer);
        for (const [, p] of this.batches)
            clearTimeout(p.timer);
    }
}
function normErr(e) {
    const x = e;
    return { code: typeof x?.code === 'number' ? x.code : Number(x?.code ?? 0), message: String(x?.message ?? JSON.stringify(e)), data: x?.data };
}
export const hex = (n) => '0x' + n.toString(16);
export const num = (x) => typeof x === 'string' ? Number(BigInt(x)) : Number(x);
