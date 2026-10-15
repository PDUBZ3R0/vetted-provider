// -----------------------------------------------------------------------------
// Raw JSON-RPC clients for probing: HTTP (fetch) and WebSocket (Node's global
// WebSocket). No ethers on purpose: the probes need to see exactly what came
// back — HTTP status, Retry-After, the error object as sent, how long it took —
// and a library's retries, batching and error wrapping would hide all of it.
// -----------------------------------------------------------------------------

export type RpcError = { code: number; message: string; data?: unknown };

export type Reply = {
    result?: unknown;
    error?: RpcError;
    /** Transport failure: timeout, refused, non-JSON body, socket closed. No JSON-RPC answer at all. */
    transport?: string;
    httpStatus?: number;
    retryAfter?: string;
    ms: number;
};

export type Call = { method: string; params?: unknown[] };

export interface Client {
    readonly url: string;
    readonly kind: 'http' | 'ws';
    call(method: string, params?: unknown[], timeoutMs?: number): Promise<Reply>;
    /** Send several calls as ONE JSON-RPC batch. `replies` is null when the batch itself failed. */
    batch(calls: Call[], timeoutMs?: number): Promise<{ replies: Array<{ id: unknown; result?: unknown; error?: RpcError }> | null; reply: Reply }>;
    close(): void;
}

const DEFAULT_TIMEOUT = 15_000;
let nextId = 1;

// --- classification ------------------------------------------------------------

const RATE_RE = /rate.?limit|too many requests|\b429\b|request limit|exceeded.*(?:rate|requests per|quota|capacity)|capacity exceeded|throttl|over.?quota|daily limit/i;
const MISSING_RE = /method .*(?:not found|does not exist|not supported|not available|is not available|unsupported|disabled|not allowed|blocked|whitelist|not enabled)|unsupported method|not implemented|unknown method|no such method|method not found|-32601/i;

export const errText = (r: Reply): string =>
    r.transport ?? (r.error ? `${r.error.code}: ${r.error.message}${r.error.data != null ? ` ${typeof r.error.data === 'string' ? r.error.data : JSON.stringify(r.error.data)}` : ''}` : '');

export const isRateLimited = (r: Reply): boolean =>
    r.httpStatus === 429 || (!!r.error && RATE_RE.test(r.error.message)) || (!!r.transport && RATE_RE.test(r.transport));

export const isMissingMethod = (r: Reply): boolean =>
    r.error?.code === -32601 || (!!r.error && MISSING_RE.test(r.error.message)) || r.httpStatus === 405 && !r.error;

export const ok = (r: Reply): boolean => r.error == null && r.transport == null;

// --- HTTP ----------------------------------------------------------------------

export class HttpClient implements Client {
    readonly url: string;
    readonly kind = 'http' as const;
    constructor(url: string) { this.url = url; }

    private async post(body: unknown, timeoutMs: number): Promise<{ json: unknown; reply: Reply }> {
        const t0 = performance.now();
        let res: Response;
        try {
            res = await fetch(this.url, {
                method: 'POST',
                headers: { 'content-type': 'application/json', accept: 'application/json' },
                body: JSON.stringify(body),
                signal: AbortSignal.timeout(timeoutMs),
            });
        } catch (e) {
            const err = e as Error & { cause?: { code?: string; message?: string } };
            const why = err.name === 'TimeoutError' ? `timeout ${timeoutMs}ms` : (err.cause?.code ?? err.cause?.message ?? err.message);
            return { json: null, reply: { transport: why, ms: performance.now() - t0 } };
        }
        const text = await res.text().catch(() => '');
        const ms = performance.now() - t0;
        const base = { httpStatus: res.status, retryAfter: res.headers.get('retry-after') ?? undefined, ms };
        let json: unknown = null;
        try { json = text ? JSON.parse(text) : null; } catch { /* not JSON */ }
        if (json == null) {
            return { json: null, reply: { ...base, transport: `HTTP ${res.status}${text ? `: ${text.slice(0, 120).replace(/\s+/g, ' ')}` : ''}` } };
        }
        return { json, reply: base };
    }

    async call(method: string, params: unknown[] = [], timeoutMs = DEFAULT_TIMEOUT): Promise<Reply> {
        const { json, reply } = await this.post({ jsonrpc: '2.0', id: nextId++, method, params }, timeoutMs);
        if (json == null) return reply;
        const j = (Array.isArray(json) ? json[0] : json) as { result?: unknown; error?: RpcError } | undefined;
        if (!j || typeof j !== 'object') return { ...reply, transport: `unexpected body ${JSON.stringify(json).slice(0, 80)}` };
        if (j.error) return { ...reply, error: normErr(j.error) };
        if (!('result' in j)) return { ...reply, transport: `no result or error in reply ${JSON.stringify(j).slice(0, 80)}` };
        return { ...reply, result: j.result };
    }

    async batch(calls: Call[], timeoutMs = DEFAULT_TIMEOUT) {
        const body = calls.map(c => ({ jsonrpc: '2.0', id: nextId++, method: c.method, params: c.params ?? [] }));
        const { json, reply } = await this.post(body, timeoutMs);
        if (json == null) return { replies: null, reply };
        if (!Array.isArray(json)) {
            const j = json as { error?: RpcError };
            return { replies: null, reply: { ...reply, error: j.error ? normErr(j.error) : { code: 0, message: `batch answered with a single object ${JSON.stringify(json).slice(0, 80)}` } } };
        }
        return { replies: json as Array<{ id: unknown; result?: unknown; error?: RpcError }>, reply };
    }

    close(): void { /* stateless */ }
}

// --- WebSocket -------------------------------------------------------------------

type Pending = { resolve: (v: unknown) => void; timer: ReturnType<typeof setTimeout>; t0: number };

export class WsClient implements Client {
    readonly url: string;
    readonly kind = 'ws' as const;
    private ws: WebSocket | null = null;
    private pending = new Map<number, Pending>();
    private batches = new Map<string, Pending>();
    private closedWhy: string | null = null;
    /** eth_subscription notifications, by subscription id. */
    readonly subs = new Map<string, (result: unknown) => void>();
    /** Time of the last frame of any kind from the server. */
    lastRx = 0;

    constructor(url: string) { this.url = url; }

    /** Open the socket. Resolves to an error string on failure, null on success. */
    connect(timeoutMs = 10_000): Promise<string | null> {
        return new Promise(resolve => {
            let settled = false;
            const done = (why: string | null) => { if (!settled) { settled = true; clearTimeout(t); resolve(why); } };
            const t = setTimeout(() => { done(`connect timeout ${timeoutMs}ms`); try { ws.close(); } catch { /* */ } }, timeoutMs);
            let ws: WebSocket;
            try { ws = new WebSocket(this.url); } catch (e) { done((e as Error).message); return; }
            this.ws = ws;
            ws.onopen = () => done(null);
            // The browser-style WebSocket hides the HTTP status of a refused upgrade
            // (HyperEVM answers 405); all we get is an error event, then close.
            ws.onerror = (ev) => done(`connect failed${(ev as unknown as { message?: string }).message ? `: ${(ev as unknown as { message: string }).message}` : ''}`);
            ws.onclose = (ev) => {
                this.closedWhy = `socket closed (code ${ev.code}${ev.reason ? ` ${ev.reason}` : ''})`;
                done(this.closedWhy);
                for (const [, p] of this.pending) { clearTimeout(p.timer); p.resolve({ transport: this.closedWhy }); }
                for (const [, p] of this.batches) { clearTimeout(p.timer); p.resolve({ transport: this.closedWhy }); }
                this.pending.clear(); this.batches.clear();
            };
            ws.onmessage = (ev) => this.onMessage(typeof ev.data === 'string' ? ev.data : Buffer.from(ev.data as ArrayBuffer).toString());
        });
    }

    get open(): boolean { return this.ws?.readyState === 1; }

    private onMessage(text: string): void {
        this.lastRx = Date.now();
        let msg: unknown;
        try { msg = JSON.parse(text); } catch { return; }
        if (Array.isArray(msg)) {
            const key = msg.map(m => (m as { id?: unknown }).id).filter(x => typeof x === 'number').sort((a, b) => (a as number) - (b as number)).join(',');
            const p = this.batches.get(key) ?? [...this.batches.values()][0];   // id:null replies cannot be keyed: take the oldest
            if (p) {
                for (const [k, v] of this.batches) if (v === p) this.batches.delete(k);
                clearTimeout(p.timer);
                p.resolve({ replies: msg, ms: performance.now() - p.t0 });
            }
            return;
        }
        const m = msg as { id?: number; method?: string; params?: { subscription?: string; result?: unknown }; result?: unknown; error?: RpcError };
        if (m.method === 'eth_subscription' && m.params?.subscription) {
            this.subs.get(m.params.subscription)?.(m.params.result);
            return;
        }
        if (typeof m.id === 'number') {
            const p = this.pending.get(m.id);
            if (!p) return;
            this.pending.delete(m.id);
            clearTimeout(p.timer);
            p.resolve(m.error ? { error: normErr(m.error), ms: performance.now() - p.t0 } : { result: m.result, ms: performance.now() - p.t0 });
        }
    }

    call(method: string, params: unknown[] = [], timeoutMs = DEFAULT_TIMEOUT): Promise<Reply> {
        const t0 = performance.now();
        if (!this.open) return Promise.resolve({ transport: this.closedWhy ?? 'socket not open', ms: 0 });
        const id = nextId++;
        return new Promise(resolve => {
            const timer = setTimeout(() => { this.pending.delete(id); resolve({ transport: `timeout ${timeoutMs}ms`, ms: performance.now() - t0 }); }, timeoutMs);
            this.pending.set(id, { resolve: v => resolve(v as Reply), timer, t0 });
            try { this.ws!.send(JSON.stringify({ jsonrpc: '2.0', id, method, params })); }
            catch (e) { clearTimeout(timer); this.pending.delete(id); resolve({ transport: (e as Error).message, ms: performance.now() - t0 }); }
        });
    }

    batch(calls: Call[], timeoutMs = DEFAULT_TIMEOUT) {
        const t0 = performance.now();
        type Out = { replies: Array<{ id: unknown; result?: unknown; error?: RpcError }> | null; reply: Reply };
        if (!this.open) return Promise.resolve<Out>({ replies: null, reply: { transport: this.closedWhy ?? 'socket not open', ms: 0 } });
        const body = calls.map(c => ({ jsonrpc: '2.0', id: nextId++, method: c.method, params: c.params ?? [] }));
        const key = body.map(b => b.id).join(',');
        return new Promise<Out>(resolve => {
            const timer = setTimeout(() => { this.batches.delete(key); resolve({ replies: null, reply: { transport: `timeout ${timeoutMs}ms`, ms: performance.now() - t0 } }); }, timeoutMs);
            this.batches.set(key, {
                resolve: v => {
                    const x = v as { replies?: Out['replies']; ms?: number; transport?: string };
                    resolve(x.replies ? { replies: x.replies, reply: { ms: x.ms ?? 0 } } : { replies: null, reply: { transport: x.transport ?? 'no reply', ms: performance.now() - t0 } });
                },
                timer, t0,
            });
            try { this.ws!.send(JSON.stringify(body)); }
            catch (e) { clearTimeout(timer); this.batches.delete(key); resolve({ replies: null, reply: { transport: (e as Error).message, ms: 0 } }); }
        });
    }

    /** eth_subscribe; returns the subscription id or the failed reply. */
    async subscribe(params: unknown[], onEvent: (r: unknown) => void): Promise<string | Reply> {
        const r = await this.call('eth_subscribe', params);
        if (!ok(r) || typeof r.result !== 'string') return r;
        this.subs.set(r.result, onEvent);
        return r.result;
    }

    close(): void {
        try { this.ws?.close(); } catch { /* */ }
        for (const [, p] of this.pending) clearTimeout(p.timer);
        for (const [, p] of this.batches) clearTimeout(p.timer);
    }
}

function normErr(e: unknown): RpcError {
    const x = e as { code?: unknown; message?: unknown; data?: unknown };
    return { code: typeof x?.code === 'number' ? x.code : Number(x?.code ?? 0), message: String(x?.message ?? JSON.stringify(e)), data: x?.data };
}

export const hex = (n: number | bigint): string => '0x' + n.toString(16);
export const num = (x: unknown): number => typeof x === 'string' ? Number(BigInt(x)) : Number(x);
