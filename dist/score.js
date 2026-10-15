// -----------------------------------------------------------------------------
// Reports -> ratings, per use profile.
//
// "Best RPC" depends on the job, so there is no single score. Each profile has
// GATES (fail one and the endpoint is out, whatever else it does well) and then
// a 0-100 weighted score. Gates are the things that silently break a bot:
// the wrong chain, a stale head, a simulation that loses its revert reason.
//
//   scanner     wide eth_getLogs, address lists, topic-only filters, history
//   trader      fresh head, revert data, low latency, headroom on rate
//   reader      big eth_call gas cap, big batches, latency
//   subscriber  a WebSocket whose subscriptions keep delivering
// -----------------------------------------------------------------------------
export const PROFILES = ['scanner', 'trader', 'reader', 'subscriber'];
const clamp = (x) => Math.max(0, Math.min(1, x));
/** 0 at `lo`, 1 at `hi`, on a log scale — for limits that span orders of magnitude. */
const logScale = (v, lo, hi) => v == null || v <= 0 ? 0 : clamp((Math.log10(v) - Math.log10(lo)) / (Math.log10(hi) - Math.log10(lo)));
/** 1 at or below `good` ms, 0 at or above `bad` ms. */
const fast = (ms, good, bad) => ms == null ? 0 : clamp((bad - ms) / (bad - good));
function common(r, o) {
    if (!r.reachable)
        return r.error ?? 'unreachable';
    if (!r.chainIdOk)
        return `wrong chain (${r.chainId})`;
    if (r.headLag == null)
        return 'head unknown';
    if (r.headLag > o.maxLag)
        return `head ${r.headLag} blocks behind`;
    if (r.latency && r.latency.failures > r.latency.n)
        return 'most requests fail';
    return null;
}
export function rate(r, p, o) {
    const fail = (why) => ({ pass: false, score: 0, why });
    const g = common(r, o);
    if (g)
        return fail(g);
    const rps = logScale(r.rate?.cleanRps, 5, 80);
    const lat = fast(r.latency?.p50, 40, 600);
    const lag = r.headLag === 0 ? 1 : r.headLag === 1 ? 0.5 : 0;
    if (p === 'scanner') {
        const L = r.logs;
        if (!L || !L.maxRange || L.maxRange < 100)
            return fail(`getLogs range ${L?.maxRange ?? 'unusable'}`);
        const s = 35 * logScale(L.maxRange, 100, 100_000)
            + 15 * logScale(L.maxAddresses, 1, 1000)
            + 15 * (L.topicOnly ? 1 : 0)
            + 5 * (L.orTopics ? 1 : 0)
            + 5 * (L.combined ? 1 : 0)
            + 10 * (L.oldLogs ? 1 : 0)
            + 15 * rps;
        return { pass: true, score: Math.round(s) };
    }
    if (p === 'trader') {
        if (r.call?.revertData !== true)
            return fail(`no revert data (${r.call?.revertError ?? 'untested'})`);
        if (r.headLag > 1)
            return fail(`head ${r.headLag} blocks behind`);
        const s = 45 * fast(r.latency?.p50, 30, 500) + 10 * fast(r.latency?.p95, 80, 1500) + 25 * lag + 20 * rps;
        return { pass: true, score: Math.round(s) };
    }
    if (p === 'reader') {
        if (!r.call?.gasCap)
            return fail(`eth_call unusable (${r.call?.gasCapError ?? 'untested'})`);
        const s = 30 * logScale(r.call.gasCap, 5e6, 500e6)
            + 20 * logScale(r.batch?.max, 2, 500)
            + 25 * lat
            + 15 * rps
            + 10 * (r.call.stateOverride ? 1 : 0);
        return { pass: true, score: Math.round(s) };
    }
    // subscriber
    if (r.kind !== 'ws')
        return fail('not a websocket');
    if (!r.ws?.connected)
        return fail(r.ws?.error ?? 'no connection');
    if (!r.ws.heads)
        return fail(`no newHeads (${r.ws.error ?? 'none delivered'})`);
    if (r.ws.died)
        return fail('subscription went silent');
    const s = 40 * lat + 30 * (r.ws.logs ? 1 : 0) + 15 * lag + 15 * rps;
    return { pass: true, score: Math.round(s) };
}
export function rateAll(r, o) {
    return Object.fromEntries(PROFILES.map(p => [p, rate(r, p, o)]));
}
