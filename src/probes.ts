// -----------------------------------------------------------------------------
// What we test on every endpoint, and how.
//
// Each probe is transport-agnostic (a Client is HTTP or WebSocket) and records
// what happened rather than a verdict; score.ts turns reports into ratings.
// Probes stop at the first failure on an escalating ladder and then bisect
// between the last success and the failure, so a limit is located in a handful
// of requests instead of a sweep. Everything here is a read: no transaction is
// ever sent.
// -----------------------------------------------------------------------------

import { type Client, type Reply, WsClient, errText, isMissingMethod, isRateLimited, ok, hex, num } from './rpc.ts';
import { REVERT_DEADBEEF, REVERT_MARKER, RETURN_GAS, RETURN_42, TRANSFER_TOPIC, APPROVAL_TOPIC, randomAddress } from './evm.ts';

export type Sample = {
    /** The busiest log emitter in recent blocks (e.g. the main stablecoin). */
    address?: string;
    /** Its most common topic0, and a second one for the OR test. */
    topic: string;
    topic2: string;
    /** The block it was seen in. */
    block?: number;
};

export type Ctx = {
    chainId: number;
    sample: Sample;
    /** Upper bound for the rate ramp; 0 skips it. */
    maxRps: number;
    /** Seconds to watch WebSocket subscriptions. */
    wsWatchSec: number;
    log: (s: string) => void;
};

export type LogsReport = {
    /** Largest block range accepted with a filter that matches nothing (a random address): the pure RANGE limit. */
    maxRange: number | null;
    rangeError?: string;
    /** Most addresses accepted in one filter (100-block range). */
    maxAddresses: number | null;
    addressError?: string;
    /** Topic filter with no address at all — publicnode refuses this (-32701). */
    topicOnly: boolean | null;
    topicOnlyError?: string;
    /** Largest range accepted for an address-less Transfer filter: where result-count caps bite. */
    topicOnlyMaxRange: number | null;
    resultCapError?: string;
    /** OR'd topic0s ([[a, b]]). */
    orTopics: boolean | null;
    /** Address list + topic + range at the individual limits, together. */
    combined: boolean | null;
    combinedError?: string;
    /** Logs near genesis — pruned / limited-history endpoints refuse. */
    oldLogs: boolean | null;
    oldLogsError?: string;
};

export type Report = {
    url: string;
    kind: 'http' | 'ws';
    tracking?: string;
    source: string;
    reachable: boolean;
    error?: string;
    chainId?: number;
    chainIdOk?: boolean;
    client?: string;
    head?: number;
    /** Blocks behind the best head seen across all endpoints (filled in by the runner). */
    headLag?: number;
    latency?: { p50: number; p95: number; n: number; failures: number };
    logs?: LogsReport;
    call?: {
        /** A reverting eth_call returns the revert data (needed to simulate anything). */
        revertData: boolean | null;
        revertError?: string;
        /** Gas the node actually grants an eth_call (bisected). */
        gasCap: number | null;
        gasCapError?: string;
        stateOverride: boolean | null;
        stateOverrideError?: string;
    };
    history?: {
        /** Deepest block (in blocks behind head) whose state could be read. */
        stateDepth: number | null;
        archive: boolean | null;
        error?: string;
    };
    batch?: { max: number; error?: string; nullIdErrors: boolean };
    methods?: Record<string, boolean | null>;
    methodErrors?: Record<string, string>;
    rate?: { cleanRps: number; limitedAt: number | null; retryAfter?: string; error?: string; sent: number };
    /** Rate-limit replies met (and waited out) during the ordinary probes, before the ramp. */
    throttledDuringProbes?: number;
    ws?: { connected: boolean; error?: string; heads: number; logs: number | null; logsError?: string; lastHeadAgeSec: number | null; died: boolean };
    ms: number;
};

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
const pct = (xs: number[], p: number) => { if (!xs.length) return 0; const s = [...xs].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.floor(p * s.length))]; };
const short = (s: string | undefined, n = 160) => s == null ? undefined : (s.length > n ? s.slice(0, n) + '…' : s);

/**
 * Largest value in `ladder` (then bisected toward the next rung) for which `attempt` succeeds.
 * Stops at the first failure. Returns null if even the first rung fails.
 */
async function climb(ladder: number[], attempt: (n: number) => Promise<Reply>, bisectSteps = 6): Promise<{ max: number | null; error?: string }> {
    let good: number | null = null, bad: number | null = null, error: string | undefined;
    for (const n of ladder) {
        const r = await attempt(n);
        if (ok(r)) { good = n; continue; }
        if (isRateLimited(r)) { await sleep(2000); const again = await attempt(n); if (ok(again)) { good = n; continue; } }
        bad = n; error = errText(r); break;
    }
    if (good == null || bad == null) return { max: good, error };
    for (let i = 0; i < bisectSteps && bad - good > Math.max(1, good * 0.05); i++) {
        const mid = Math.round(Math.sqrt(good * bad));   // geometric: limits are usually round powers
        const m = mid <= good ? good + 1 : mid >= bad ? bad - 1 : mid;
        const r = await attempt(m);
        if (ok(r)) good = m; else bad = m;
    }
    return { max: good, error };
}

// --- the probes -------------------------------------------------------------------

async function identity(c: Client, ctx: Ctx, rep: Report): Promise<boolean> {
    const id = await c.call('eth_chainId', [], 10_000);
    if (!ok(id)) { rep.error = short(errText(id)); return false; }
    rep.reachable = true;
    rep.chainId = num(id.result);
    rep.chainIdOk = rep.chainId === ctx.chainId;
    const head = await c.call('eth_blockNumber');
    if (ok(head)) rep.head = num(head.result);
    const v = await c.call('web3_clientVersion');
    if (ok(v) && typeof v.result === 'string') rep.client = v.result.slice(0, 80);
    return true;
}

async function latency(c: Client, rep: Report): Promise<void> {
    const ms: number[] = [];
    let failures = 0;
    for (let i = 0; i < 12; i++) {
        const r = i % 3 === 2
            ? await c.call('eth_call', [{ data: RETURN_GAS, gas: hex(100_000) }, 'latest'])
            : await c.call('eth_blockNumber');
        if (ok(r)) ms.push(r.ms); else failures++;
    }
    rep.latency = { p50: Math.round(pct(ms, 0.5)), p95: Math.round(pct(ms, 0.95)), n: ms.length, failures };
}

async function logs(c: Client, ctx: Ctx, rep: Report): Promise<void> {
    const head = rep.head!;
    const L: LogsReport = { maxRange: null, maxAddresses: null, topicOnly: null, topicOnlyMaxRange: null, orTopics: null, combined: null, oldLogs: null };
    rep.logs = L;
    const top = Math.max(0, head - 5);   // a few blocks back: not every node has indexed the head yet
    const range = (n: number) => ({ fromBlock: hex(Math.max(0, top - n + 1)), toBlock: hex(top) });
    const nobody = randomAddress();

    // 1. pure range limit: a filter that matches nothing
    const r1 = await climb([10, 100, 1_000, 5_000, 10_000, 50_000, 100_000, 1_000_000].filter(n => n <= top + 1),
        n => c.call('eth_getLogs', [{ ...range(n), address: nobody }], 30_000));
    L.maxRange = r1.max; L.rangeError = short(r1.error);

    // 2. address count (100 blocks, random addresses — the limit is on the filter, not the data)
    const addrs = Array.from({ length: 1000 }, randomAddress);
    const r2 = await climb([1, 10, 50, 100, 400, 1000],
        n => c.call('eth_getLogs', [{ ...range(Math.min(100, L.maxRange ?? 10)), address: addrs.slice(0, n) }], 30_000), 2);
    L.maxAddresses = r2.max; L.addressError = short(r2.error);

    // 3. topic only, no address
    const t1 = await c.call('eth_getLogs', [{ ...range(1), topics: [ctx.sample.topic] }], 30_000);
    L.topicOnly = ok(t1);
    if (!ok(t1)) L.topicOnlyError = short(errText(t1));
    else {
        // 3b. how wide before a result cap ("more than 10000 results", response size) bites
        const r3 = await climb([10, 100, 1_000, 10_000].filter(n => n <= (L.maxRange ?? 10)),
            n => c.call('eth_getLogs', [{ ...range(n), topics: [ctx.sample.topic] }], 60_000), 2);
        L.topicOnlyMaxRange = r3.max; L.resultCapError = short(r3.error);
    }

    // 4. OR'd topics on one address
    const orFilter = { ...range(Math.min(100, L.maxRange ?? 10)), address: ctx.sample.address ?? nobody, topics: [[ctx.sample.topic, ctx.sample.topic2]] };
    L.orTopics = ok(await c.call('eth_getLogs', [orFilter], 30_000));

    // 5. everything together at the individual limits (capped to stay polite)
    if (L.maxRange && L.maxAddresses) {
        const n = Math.min(L.maxAddresses, 400);
        const r5 = await c.call('eth_getLogs', [{ ...range(Math.min(L.maxRange, 10_000)), address: [ctx.sample.address ?? nobody, ...addrs.slice(0, n - 1)], topics: [ctx.sample.topic] }], 60_000);
        L.combined = ok(r5);
        if (!ok(r5)) L.combinedError = short(errText(r5));
    }

    // 6. logs near genesis
    const r6 = await c.call('eth_getLogs', [{ fromBlock: hex(1), toBlock: hex(Math.min(10, top)), address: nobody }], 30_000);
    L.oldLogs = ok(r6);
    if (!ok(r6)) L.oldLogsError = short(errText(r6));
}

async function calls(c: Client, rep: Report): Promise<void> {
    const C: NonNullable<Report['call']> = { revertData: null, gasCap: null, stateOverride: null };
    rep.call = C;

    // revert data — init code that reverts with 0xdeadbeef
    const rv = await c.call('eth_call', [{ data: REVERT_DEADBEEF, gas: hex(100_000) }, 'latest']);
    if (ok(rv)) { C.revertData = false; C.revertError = 'call did not revert (creation calls not executed?)'; }
    else if (rv.transport) { C.revertData = null; C.revertError = short(rv.transport); }
    else { C.revertData = JSON.stringify(rv.error).toLowerCase().includes(REVERT_MARKER); if (!C.revertData) C.revertError = short(errText(rv)); }

    // gas cap — what GAS reports inside a call we ask a lot of gas for
    let granted = 0, gerr: string | undefined;
    for (const g of [5e6, 25e6, 50e6, 100e6, 300e6, 1e9]) {
        const r = await c.call('eth_call', [{ data: RETURN_GAS, gas: hex(g) }, 'latest']);
        if (!ok(r)) { gerr = errText(r); break; }
        const left = typeof r.result === 'string' && r.result.length > 2 ? Number(BigInt(r.result)) : 0;
        granted = Math.max(granted, left);
        if (left < g * 0.9) break;   // clamped silently: this is the cap
    }
    C.gasCap = granted || null;
    if (gerr) C.gasCapError = short(gerr);

    // state override — run our code at a random address
    const at = randomAddress();
    const so = await c.call('eth_call', [{ to: at, data: '0x' }, 'latest', { [at]: { code: RETURN_42 } }]);
    C.stateOverride = ok(so) && typeof so.result === 'string' && /2a$/.test(so.result);
    if (!C.stateOverride) C.stateOverrideError = short(ok(so) ? `returned ${String(so.result).slice(0, 20)} (override ignored)` : errText(so));
}

async function history(c: Client, rep: Report): Promise<void> {
    const head = rep.head!;
    const H: NonNullable<Report['history']> = { stateDepth: null, archive: null };
    rep.history = H;
    for (const depth of [64, 1_000, 100_000, 1_000_000, head - 1]) {
        if (depth <= 0 || depth >= head) continue;
        const r = await c.call('eth_getBalance', ['0x0000000000000000000000000000000000000000', hex(head - depth)]);
        if (!ok(r)) { H.error = short(errText(r)); break; }
        H.stateDepth = depth;
    }
    H.archive = H.stateDepth != null && H.stateDepth >= head - 1;
}

async function batch(c: Client, rep: Report): Promise<void> {
    let max = 0, error: string | undefined, nullIdErrors = false;
    for (const n of [2, 10, 50, 100, 500, 1000]) {
        const { replies, reply } = await c.batch(Array.from({ length: n }, () => ({ method: 'eth_chainId' })), 30_000);
        if (!replies) { error = short(errText(reply)); break; }
        const bad = replies.filter(r => r.error || !('result' in r));
        if (replies.some(r => r.id == null)) nullIdErrors = true;
        if (replies.length !== n || bad.length) {
            error = short(bad[0]?.error ? `${bad[0].error.code}: ${bad[0].error.message}` : `${replies.length}/${n} replies`);
            break;
        }
        max = n;
    }
    rep.batch = { max, error, nullIdErrors };
}

const METHODS: Array<[string, (to: string) => unknown[]]> = [
    ['eth_feeHistory', () => ['0x4', 'latest', [25, 75]]],
    ['eth_maxPriorityFeePerGas', () => []],
    ['eth_getBlockReceipts', () => ['latest']],
    ['eth_createAccessList', to => [{ to, data: '0x' }, 'latest']],
    ['eth_getProof', to => [to, [], 'latest']],
    ['debug_traceCall', to => [{ to, data: '0x' }, 'latest', {}]],
    ['trace_call', to => [{ to, data: '0x' }, ['trace'], 'latest']],
    ['txpool_status', () => []],
    ['eth_blobBaseFee', () => []],
    ['eth_newFilter', () => [{ fromBlock: 'latest' }]],
];

async function methods(c: Client, rep: Report): Promise<void> {
    rep.methods = {};
    rep.methodErrors = {};
    const to = randomAddress();
    for (const [m, params] of METHODS) {
        const r = await c.call(m, params(to));
        if (ok(r)) rep.methods[m] = true;
        else if (isMissingMethod(r)) { rep.methods[m] = false; rep.methodErrors[m] = short(errText(r), 100)!; }
        else { rep.methods[m] = null; rep.methodErrors[m] = short(errText(r), 100)!; }
    }
}

/**
 * Ramp requests per second until the first rate-limit reply, 2 s per step,
 * never above ctx.maxRps. Short and capped on purpose: these are other people's
 * free endpoints.
 */
async function rate(c: Client, ctx: Ctx, rep: Report): Promise<void> {
    const R: NonNullable<Report['rate']> = { cleanRps: 0, limitedAt: null, sent: 0 };
    rep.rate = R;
    for (const rps of [5, 10, 20, 40, 80, 160].filter(x => x <= ctx.maxRps)) {
        const n = rps * 2;
        const replies: Promise<Reply>[] = [];
        for (let i = 0; i < n; i++) {
            replies.push(c.call('eth_blockNumber', [], 10_000));
            await sleep(1000 / rps);
        }
        const rs = await Promise.all(replies);
        R.sent += n;
        const limited = rs.filter(isRateLimited);
        const failed = rs.filter(r => !ok(r) && !isRateLimited(r));
        if (limited.length) { R.limitedAt = rps; R.retryAfter = limited.find(r => r.retryAfter)?.retryAfter; break; }
        if (failed.length > n / 10) { R.limitedAt = rps; R.error = short(errText(failed[0])); break; }
        R.cleanRps = rps;
        await sleep(1000);
    }
    if (R.limitedAt) await sleep(3000);   // let the bucket refill before anything else runs
}

async function subscriptions(c: WsClient, ctx: Ctx, rep: Report): Promise<void> {
    const W: NonNullable<Report['ws']> = { connected: true, heads: 0, logs: null, lastHeadAgeSec: null, died: false };
    rep.ws = W;
    let lastHead = 0, logCount = 0;
    const h = await c.subscribe(['newHeads'], () => { W.heads++; lastHead = Date.now(); });
    if (typeof h !== 'string') { W.error = short(errText(h)); return; }
    const l = await c.subscribe(['logs', { topics: [ctx.sample.topic] }], () => { logCount++; });
    if (typeof l !== 'string') W.logsError = short(errText(l));
    const t0 = Date.now();
    await sleep(ctx.wsWatchSec * 1000);
    W.logs = typeof l === 'string' ? logCount : null;
    W.lastHeadAgeSec = lastHead ? Math.round((Date.now() - lastHead) / 100) / 10 : null;
    // Silent death: heads came, then stopped for most of the window.
    W.died = W.heads > 0 && lastHead - t0 < ctx.wsWatchSec * 1000 * 0.5 && (Date.now() - lastHead) > ctx.wsWatchSec * 1000 * 0.4 || !c.open;
    await c.call('eth_unsubscribe', [h]);
    if (typeof l === 'string') await c.call('eth_unsubscribe', [l]);
}

// --- runner -----------------------------------------------------------------------

/**
 * Wraps a client so a rate-limit reply is waited out and retried (1s, 2s, 4s)
 * instead of being read as a capability failure — otherwise a 429 in the middle
 * of a range bisection would report a tiny getLogs limit. The rate ramp uses the
 * raw client: there, the 429 IS the measurement.
 */
class Polite implements Client {
    readonly url: string;
    readonly kind: 'http' | 'ws';
    waits = 0;
    private readonly inner: Client;
    constructor(inner: Client) { this.inner = inner; this.url = inner.url; this.kind = inner.kind; }
    async call(method: string, params?: unknown[], timeoutMs?: number): Promise<Reply> {
        for (let i = 0; ; i++) {
            const r = await this.inner.call(method, params, timeoutMs);
            if (!isRateLimited(r) || i >= 3) return r;
            this.waits++;
            await sleep(1000 * 2 ** i);
        }
    }
    async batch(calls: Parameters<Client['batch']>[0], timeoutMs?: number) {
        for (let i = 0; ; i++) {
            const b = await this.inner.batch(calls, timeoutMs);
            const limited = isRateLimited(b.reply) || !!b.replies?.some(r => r.error && isRateLimited({ error: r.error, ms: 0 }));
            if (!limited || i >= 3) return b;
            this.waits++;
            await sleep(1000 * 2 ** i);
        }
    }
    close(): void { this.inner.close(); }
}

export type Only = Partial<Record<'logs' | 'calls' | 'history' | 'batch' | 'methods' | 'rate' | 'ws', boolean>>;

export async function vetEndpoint(c: Client, ctx: Ctx, base: Pick<Report, 'tracking' | 'source'>, skip: Only = {}): Promise<Report> {
    const t0 = Date.now();
    const rep: Report = { url: c.url, kind: c.kind, tracking: base.tracking, source: base.source, reachable: false, ms: 0 };
    const p = new Polite(c);
    try {
        if (c instanceof WsClient) {
            const err = await c.connect();
            if (err) { rep.error = err; rep.ws = { connected: false, error: err, heads: 0, logs: null, lastHeadAgeSec: null, died: false }; return rep; }
        }
        if (!await identity(p, ctx, rep)) return rep;
        if (!rep.chainIdOk) { rep.error = `wrong chain: eth_chainId says ${rep.chainId}`; return rep; }
        if (rep.head == null) { rep.error = 'eth_blockNumber failed'; return rep; }
        const step = async (name: keyof Only, f: () => Promise<void>) => {
            if (skip[name]) return;
            try { await f(); } catch (e) { ctx.log(`    [${name}] ${(e as Error).message}`); }
        };
        await latency(p, rep);
        await step('calls', () => calls(p, rep));
        await step('logs', () => logs(p, ctx, rep));
        await step('history', () => history(p, rep));
        await step('batch', () => batch(p, rep));
        await step('methods', () => methods(p, rep));
        if (c instanceof WsClient) await step('ws', () => subscriptions(c, ctx, rep));
        rep.throttledDuringProbes = p.waits;
        // Last, on the raw client, after a pause so the probes above have not
        // already spent the bucket: here a rate-limit reply is the measurement.
        await sleep(p.waits ? 5000 : 1500);
        await step('rate', () => rate(c, ctx, rep));
    } finally {
        c.close();
        rep.ms = Date.now() - t0;
    }
    return rep;
}

/**
 * Find a busy log emitter and its common topics from the last few blocks, through
 * whichever endpoint answers. The getLogs probes use it so OR / combined filters
 * have real data behind them. Falls back to Transfer/Approval with no address.
 */
export async function sampleLogs(clients: Client[], log: (s: string) => void): Promise<Sample> {
    for (const c of clients) {
        if (c instanceof WsClient && !c.open && await c.connect(5000)) continue;
        const head = await c.call('eth_blockNumber', [], 8000);
        if (!ok(head)) continue;
        const h = num(head.result);
        for (const span of [5, 50]) {
            const r = await c.call('eth_getLogs', [{ fromBlock: hex(Math.max(0, h - span)), toBlock: hex(h) }], 20_000);
            if (!ok(r) || !Array.isArray(r.result) || r.result.length === 0) continue;
            const rows = r.result as Array<{ address: string; topics: string[]; blockNumber: string }>;
            const byTopic = new Map<string, number>(), byAddr = new Map<string, number>();
            for (const x of rows) if (x.topics?.[0]) byTopic.set(x.topics[0], (byTopic.get(x.topics[0]) ?? 0) + 1);
            const topics = [...byTopic].sort((a, b) => b[1] - a[1]).map(t => t[0]);
            const topic = topics.includes(TRANSFER_TOPIC) ? TRANSFER_TOPIC : topics[0];
            for (const x of rows) if (x.topics?.[0] === topic) byAddr.set(x.address, (byAddr.get(x.address) ?? 0) + 1);
            const address = [...byAddr].sort((a, b) => b[1] - a[1])[0]?.[0];
            const topic2 = topics.find(t => t !== topic) ?? APPROVAL_TOPIC;
            log(`  sample: ${rows.length} logs in ${span} blocks via ${c.url} — busiest emitter ${address}`);
            if (c instanceof WsClient) c.close();
            return { address, topic, topic2, block: h };
        }
        if (c instanceof WsClient) c.close();
    }
    log('  sample: no endpoint returned recent logs — using Transfer/Approval with no address');
    return { topic: TRANSFER_TOPIC, topic2: APPROVAL_TOPIC };
}
