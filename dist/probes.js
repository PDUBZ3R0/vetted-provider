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
import { WsClient, errText, isMissingMethod, isRateLimited, ok, hex, num } from "./rpc.js";
import { REVERT_DEADBEEF, REVERT_MARKER, RETURN_GAS, RETURN_42, TRANSFER_TOPIC, APPROVAL_TOPIC, randomAddress } from "./evm.js";
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const pct = (xs, p) => { if (!xs.length)
    return 0; const s = [...xs].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.floor(p * s.length))]; };
const short = (s, n = 160) => s == null ? undefined : (s.length > n ? s.slice(0, n) + '…' : s);
/**
 * Largest value in `ladder` (then bisected toward the next rung) for which `attempt` succeeds.
 * Stops at the first failure. Returns null if even the first rung fails.
 */
async function climb(ladder, attempt, bisectSteps = 6) {
    let good = null, bad = null, error;
    for (const n of ladder) {
        const r = await attempt(n);
        if (ok(r)) {
            good = n;
            continue;
        }
        if (isRateLimited(r)) {
            await sleep(2000);
            const again = await attempt(n);
            if (ok(again)) {
                good = n;
                continue;
            }
        }
        bad = n;
        error = errText(r);
        break;
    }
    if (good == null || bad == null)
        return { max: good, error };
    let lo = good, hi = bad;
    for (let i = 0; i < bisectSteps && hi - lo > Math.max(1, lo * 0.05); i++) {
        const mid = Math.round(Math.sqrt(lo * hi)); // geometric: limits are usually round powers
        const m = mid <= lo ? lo + 1 : mid >= hi ? hi - 1 : mid;
        const r = await attempt(m);
        if (ok(r))
            lo = m;
        else
            hi = m;
    }
    return { max: lo, error };
}
// --- the probes -------------------------------------------------------------------
async function identity(c, ctx, rep) {
    const id = await c.call('eth_chainId', [], 10_000);
    if (!ok(id)) {
        rep.error = short(errText(id));
        return false;
    }
    rep.reachable = true;
    rep.chainId = num(id.result);
    rep.chainIdOk = rep.chainId === ctx.chainId;
    const head = await c.call('eth_blockNumber');
    if (ok(head))
        rep.head = num(head.result);
    const v = await c.call('web3_clientVersion');
    if (ok(v) && typeof v.result === 'string')
        rep.client = v.result.slice(0, 80);
    return true;
}
async function latency(c, rep) {
    const ms = [];
    let failures = 0;
    for (let i = 0; i < 12; i++) {
        const r = i % 3 === 2
            ? await c.call('eth_call', [{ data: RETURN_GAS, gas: hex(100_000) }, 'latest'])
            : await c.call('eth_blockNumber');
        if (ok(r))
            ms.push(r.ms);
        else
            failures++;
    }
    rep.latency = { p50: Math.round(pct(ms, 0.5)), p95: Math.round(pct(ms, 0.95)), n: ms.length, failures };
}
async function logs(c, ctx, rep) {
    const head = rep.head;
    const L = { maxRange: null, maxAddresses: null, topicOnly: null, topicOnlyMaxRange: null, orTopics: null, combined: null, oldLogs: null };
    rep.logs = L;
    const top = Math.max(0, head - 5); // a few blocks back: not every node has indexed the head yet
    const range = (n) => ({ fromBlock: hex(Math.max(0, top - n + 1)), toBlock: hex(top) });
    const nobody = randomAddress();
    // 1. pure range limit: a filter that matches nothing
    const r1 = await climb([10, 100, 1_000, 5_000, 10_000, 50_000, 100_000, 1_000_000].filter(n => n <= top + 1), n => c.call('eth_getLogs', [{ ...range(n), address: nobody }], 30_000));
    L.maxRange = r1.max;
    L.rangeError = short(r1.error);
    // 2. address count (100 blocks, random addresses — the limit is on the filter, not the data)
    const addrs = Array.from({ length: 1000 }, randomAddress);
    const r2 = await climb([1, 10, 50, 100, 400, 1000], n => c.call('eth_getLogs', [{ ...range(Math.min(100, L.maxRange ?? 10)), address: addrs.slice(0, n) }], 30_000), 2);
    L.maxAddresses = r2.max;
    L.addressError = short(r2.error);
    // 3. topic only, no address
    const t1 = await c.call('eth_getLogs', [{ ...range(1), topics: [ctx.sample.topic] }], 30_000);
    L.topicOnly = ok(t1);
    if (!ok(t1))
        L.topicOnlyError = short(errText(t1));
    else {
        // 3b. how wide before a result cap ("more than 10000 results", response size) bites
        const r3 = await climb([10, 100, 1_000, 10_000].filter(n => n <= (L.maxRange ?? 10)), n => c.call('eth_getLogs', [{ ...range(n), topics: [ctx.sample.topic] }], 60_000), 2);
        L.topicOnlyMaxRange = r3.max;
        L.resultCapError = short(r3.error);
    }
    // 4. OR'd topics on one address
    const orFilter = { ...range(Math.min(100, L.maxRange ?? 10)), address: ctx.sample.address ?? nobody, topics: [[ctx.sample.topic, ctx.sample.topic2]] };
    L.orTopics = ok(await c.call('eth_getLogs', [orFilter], 30_000));
    // 5. everything together at the individual limits (capped to stay polite)
    if (L.maxRange && L.maxAddresses) {
        const n = Math.min(L.maxAddresses, 400);
        const r5 = await c.call('eth_getLogs', [{ ...range(Math.min(L.maxRange, 10_000)), address: [ctx.sample.address ?? nobody, ...addrs.slice(0, n - 1)], topics: [ctx.sample.topic] }], 60_000);
        L.combined = ok(r5);
        if (!ok(r5))
            L.combinedError = short(errText(r5));
    }
    // 6. logs near genesis
    const r6 = await c.call('eth_getLogs', [{ fromBlock: hex(1), toBlock: hex(Math.min(10, top)), address: nobody }], 30_000);
    L.oldLogs = ok(r6);
    if (!ok(r6))
        L.oldLogsError = short(errText(r6));
}
async function calls(c, rep) {
    const C = { revertData: null, gasCap: null, stateOverride: null };
    rep.call = C;
    // revert data — init code that reverts with 0xdeadbeef
    const rv = await c.call('eth_call', [{ data: REVERT_DEADBEEF, gas: hex(100_000) }, 'latest']);
    if (ok(rv)) {
        C.revertData = false;
        C.revertError = 'call did not revert (creation calls not executed?)';
    }
    else if (rv.transport) {
        C.revertData = null;
        C.revertError = short(rv.transport);
    }
    else {
        C.revertData = JSON.stringify(rv.error).toLowerCase().includes(REVERT_MARKER);
        if (!C.revertData)
            C.revertError = short(errText(rv));
    }
    // gas cap — what GAS reports inside a call we ask a lot of gas for
    let granted = 0, gerr;
    for (const g of [5e6, 25e6, 50e6, 100e6, 300e6, 1e9]) {
        const r = await c.call('eth_call', [{ data: RETURN_GAS, gas: hex(g) }, 'latest']);
        if (!ok(r)) {
            gerr = errText(r);
            break;
        }
        const left = typeof r.result === 'string' && r.result.length > 2 ? Number(BigInt(r.result)) : 0;
        granted = Math.max(granted, left);
        if (left < g * 0.9)
            break; // clamped silently: this is the cap
    }
    C.gasCap = granted || null;
    if (gerr)
        C.gasCapError = short(gerr);
    // state override — run our code at a random address
    const at = randomAddress();
    const so = await c.call('eth_call', [{ to: at, data: '0x' }, 'latest', { [at]: { code: RETURN_42 } }]);
    C.stateOverride = ok(so) && typeof so.result === 'string' && /2a$/.test(so.result);
    if (!C.stateOverride)
        C.stateOverrideError = short(ok(so) ? `returned ${String(so.result).slice(0, 20)} (override ignored)` : errText(so));
}
async function history(c, rep) {
    const head = rep.head;
    const H = { stateDepth: null, archive: null };
    rep.history = H;
    for (const depth of [64, 1_000, 100_000, 1_000_000, head - 1]) {
        if (depth <= 0 || depth >= head)
            continue;
        const r = await c.call('eth_getBalance', ['0x0000000000000000000000000000000000000000', hex(head - depth)]);
        if (!ok(r)) {
            H.error = short(errText(r));
            break;
        }
        H.stateDepth = depth;
    }
    H.archive = H.stateDepth != null && H.stateDepth >= head - 1;
}
async function batch(c, rep) {
    let max = 0, error, nullIdErrors = false;
    for (const n of [2, 10, 50, 100, 500, 1000]) {
        const { replies, reply } = await c.batch(Array.from({ length: n }, () => ({ method: 'eth_chainId' })), 30_000);
        if (!replies) {
            error = short(errText(reply));
            break;
        }
        const bad = replies.filter(r => r.error || !('result' in r));
        if (replies.some(r => r.id == null))
            nullIdErrors = true;
        if (replies.length !== n || bad.length) {
            error = short(bad[0]?.error ? `${bad[0].error.code}: ${bad[0].error.message}` : `${replies.length}/${n} replies`);
            break;
        }
        max = n;
    }
    rep.batch = { max, error, nullIdErrors };
}
const METHODS = [
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
async function methods(c, rep) {
    rep.methods = {};
    rep.methodErrors = {};
    const to = randomAddress();
    for (const [m, params] of METHODS) {
        const r = await c.call(m, params(to));
        if (ok(r))
            rep.methods[m] = true;
        else if (isMissingMethod(r)) {
            rep.methods[m] = false;
            rep.methodErrors[m] = short(errText(r), 100);
        }
        else {
            rep.methods[m] = null;
            rep.methodErrors[m] = short(errText(r), 100);
        }
    }
}
/**
 * Ramp requests per second until the first rate-limit reply, 2 s per step,
 * never above ctx.maxRps. Short and capped on purpose: these are other people's
 * free endpoints.
 */
async function rate(c, ctx, rep) {
    const R = { cleanRps: 0, limitedAt: null, sent: 0 };
    rep.rate = R;
    for (const rps of [5, 10, 20, 40, 80, 160].filter(x => x <= ctx.maxRps)) {
        const n = rps * 2;
        const replies = [];
        for (let i = 0; i < n; i++) {
            replies.push(c.call('eth_blockNumber', [], 10_000));
            await sleep(1000 / rps);
        }
        const rs = await Promise.all(replies);
        R.sent += n;
        const limited = rs.filter(isRateLimited);
        const failed = rs.filter(r => !ok(r) && !isRateLimited(r));
        if (limited.length) {
            R.limitedAt = rps;
            R.retryAfter = limited.find(r => r.retryAfter)?.retryAfter;
            break;
        }
        if (failed.length > n / 10) {
            R.limitedAt = rps;
            R.error = short(errText(failed[0]));
            break;
        }
        R.cleanRps = rps;
        await sleep(1000);
    }
    if (R.limitedAt)
        await sleep(3000); // let the bucket refill before anything else runs
}
async function subscriptions(c, ctx, rep) {
    const W = { connected: true, heads: 0, logs: null, lastHeadAgeSec: null, died: false };
    rep.ws = W;
    let lastHead = 0, logCount = 0;
    const h = await c.subscribe(['newHeads'], () => { W.heads++; lastHead = Date.now(); });
    if (typeof h !== 'string') {
        W.error = short(errText(h));
        return;
    }
    const l = await c.subscribe(['logs', { topics: [ctx.sample.topic] }], () => { logCount++; });
    if (typeof l !== 'string')
        W.logsError = short(errText(l));
    const t0 = Date.now();
    await sleep(ctx.wsWatchSec * 1000);
    W.logs = typeof l === 'string' ? logCount : null;
    W.lastHeadAgeSec = lastHead ? Math.round((Date.now() - lastHead) / 100) / 10 : null;
    // Silent death: heads came, then stopped for most of the window.
    W.died = W.heads > 0 && lastHead - t0 < ctx.wsWatchSec * 1000 * 0.5 && (Date.now() - lastHead) > ctx.wsWatchSec * 1000 * 0.4 || !c.open;
    await c.call('eth_unsubscribe', [h]);
    if (typeof l === 'string')
        await c.call('eth_unsubscribe', [l]);
}
// --- runner -----------------------------------------------------------------------
/**
 * Wraps a client so a rate-limit reply is waited out and retried (1s, 2s, 4s)
 * instead of being read as a capability failure — otherwise a 429 in the middle
 * of a range bisection would report a tiny getLogs limit. The rate ramp uses the
 * raw client: there, the 429 IS the measurement.
 */
class Polite {
    url;
    kind;
    waits = 0;
    inner;
    constructor(inner) { this.inner = inner; this.url = inner.url; this.kind = inner.kind; }
    async call(method, params, timeoutMs) {
        for (let i = 0;; i++) {
            const r = await this.inner.call(method, params, timeoutMs);
            if (!isRateLimited(r) || i >= 3)
                return r;
            this.waits++;
            await sleep(1000 * 2 ** i);
        }
    }
    async batch(calls, timeoutMs) {
        for (let i = 0;; i++) {
            const b = await this.inner.batch(calls, timeoutMs);
            const limited = isRateLimited(b.reply) || !!b.replies?.some(r => r.error && isRateLimited({ error: r.error, ms: 0 }));
            if (!limited || i >= 3)
                return b;
            this.waits++;
            await sleep(1000 * 2 ** i);
        }
    }
    close() { this.inner.close(); }
}
export async function vetEndpoint(c, ctx, base, skip = {}) {
    const t0 = Date.now();
    const rep = { url: c.url, kind: c.kind, tracking: base.tracking, source: base.source, reachable: false, ms: 0 };
    const p = new Polite(c);
    try {
        if (c instanceof WsClient) {
            const err = await c.connect();
            if (err) {
                rep.error = err;
                rep.ws = { connected: false, error: err, heads: 0, logs: null, lastHeadAgeSec: null, died: false };
                return rep;
            }
        }
        if (!await identity(p, ctx, rep))
            return rep;
        if (!rep.chainIdOk) {
            rep.error = `wrong chain: eth_chainId says ${rep.chainId}`;
            return rep;
        }
        if (rep.head == null) {
            rep.error = 'eth_blockNumber failed';
            return rep;
        }
        const step = async (name, f) => {
            if (skip[name])
                return;
            try {
                await f();
            }
            catch (e) {
                ctx.log(`    [${name}] ${e.message}`);
            }
        };
        await latency(p, rep);
        await step('calls', () => calls(p, rep));
        await step('logs', () => logs(p, ctx, rep));
        await step('history', () => history(p, rep));
        await step('batch', () => batch(p, rep));
        await step('methods', () => methods(p, rep));
        if (c instanceof WsClient)
            await step('ws', () => subscriptions(c, ctx, rep));
        rep.throttledDuringProbes = p.waits;
        // Last, on the raw client, after a pause so the probes above have not
        // already spent the bucket: here a rate-limit reply is the measurement.
        await sleep(p.waits ? 5000 : 1500);
        await step('rate', () => rate(c, ctx, rep));
    }
    finally {
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
export async function sampleLogs(clients, log) {
    for (const c of clients) {
        if (c instanceof WsClient && !c.open && await c.connect(5000))
            continue;
        const head = await c.call('eth_blockNumber', [], 8000);
        if (!ok(head))
            continue;
        const h = num(head.result);
        for (const span of [5, 50]) {
            const r = await c.call('eth_getLogs', [{ fromBlock: hex(Math.max(0, h - span)), toBlock: hex(h) }], 20_000);
            if (!ok(r) || !Array.isArray(r.result) || r.result.length === 0)
                continue;
            const rows = r.result;
            const byTopic = new Map(), byAddr = new Map();
            for (const x of rows)
                if (x.topics?.[0])
                    byTopic.set(x.topics[0], (byTopic.get(x.topics[0]) ?? 0) + 1);
            const topics = [...byTopic].sort((a, b) => b[1] - a[1]).map(t => t[0]);
            const topic = topics.includes(TRANSFER_TOPIC) ? TRANSFER_TOPIC : topics[0];
            for (const x of rows)
                if (x.topics?.[0] === topic)
                    byAddr.set(x.address, (byAddr.get(x.address) ?? 0) + 1);
            const address = [...byAddr].sort((a, b) => b[1] - a[1])[0]?.[0];
            const topic2 = topics.find(t => t !== topic) ?? APPROVAL_TOPIC;
            log(`  sample: ${rows.length} logs in ${span} blocks via ${c.url} — busiest emitter ${address}`);
            if (c instanceof WsClient)
                c.close();
            return { address, topic, topic2, block: h };
        }
        if (c instanceof WsClient)
            c.close();
    }
    log('  sample: no endpoint returned recent logs — using Transfer/Approval with no address');
    return { topic: TRANSFER_TOPIC, topic2: APPROVAL_TOPIC };
}
