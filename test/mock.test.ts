// The probes against mock nodes with known limits: each limit set on the mock
// must be the limit reported, over HTTP and over WebSocket. No network needed.
//
//   node --experimental-strip-types test/mock.test.ts

import { MockNode, type MockOpts } from './mock-node.ts';
import { HttpClient, WsClient } from '../src/rpc.ts';
import { vetEndpoint, sampleLogs, type Ctx, type Report } from '../src/probes.ts';
import { rateAll } from '../src/score.ts';

let pass = 0, fail = 0;
const check = (cond: boolean, what: string, got?: unknown) => {
    if (cond) { pass++; console.log(`  ok   ${what}${got !== undefined ? `  (${JSON.stringify(got)})` : ''}`); }
    else { fail++; console.log(`  FAIL ${what}  got ${JSON.stringify(got)}`); }
};
const near = (got: number | null | undefined, want: number, tol = 0.15) => got != null && Math.abs(got - want) <= want * tol;

const base: MockOpts = {
    chainId: 999, head: 2_000_000, maxRange: 2000, maxAddresses: 100, topicOnly: false, resultCap: 10_000,
    revertData: true, gasCap: 50_000_000, stateOverride: true, stateDepth: 1_000, oldLogs: false, batchMax: 50,
    rps: 25, missing: ['debug_traceCall', 'trace_call', 'txpool_status'],
};

const good = new MockNode({ ...base, maxRange: 50_000, maxAddresses: 1000, topicOnly: true, stateDepth: 10_000_000, oldLogs: true, batchMax: 1000, rps: null, gasCap: 550_000_000 });
const strict = new MockNode(base);
const wrong = new MockNode({ ...base, chainId: 998 });
const stale = new MockNode({ ...base, lagBlocks: 40, revertData: false });
const dying = new MockNode({ ...base, wsDieAfterMs: 1200, headMs: 200 });
for (const m of [good, strict, wrong, stale, dying]) await m.start();

const logs: string[] = [];
const sample = await sampleLogs([new HttpClient(good.url)], s => logs.push(s));
console.log('sample');
check(sample.address === '0x' + 'a'.repeat(40), 'busiest emitter found from recent logs', sample.address);
check(sample.topic.startsWith('0xddf252ad'), 'Transfer chosen as the sample topic');
check(sample.topic2.startsWith('0x8c5be1e5'), 'second topic for the OR test is Approval');

const ctx: Ctx = { chainId: 999, sample, maxRps: 40, wsWatchSec: 3, log: s => logs.push(s) };
const run = (c: HttpClient | WsClient) => vetEndpoint(c, ctx, { source: 'test' });
const lagged = (r: Report, lag: number) => { r.headLag = lag; return r; };

console.log('\nstrict endpoint over HTTP (range 2000, 100 addresses, no topic-only, cap 50M, batch 50, 25 rps)');
{
    const r = lagged(await run(new HttpClient(strict.url)), 0);
    check(r.reachable && r.chainIdOk === true, 'reachable, right chain');
    check(r.client === 'MockNode/v1', 'client version read', r.client);
    check(near(r.logs?.maxRange, 2000), 'getLogs range limit located', r.logs?.maxRange);
    check(r.logs!.maxRange! <= 2000, 'never reports more than the real limit', r.logs?.maxRange);
    check(r.logs?.maxAddresses === 100, 'address limit located', r.logs?.maxAddresses);
    check(r.logs?.topicOnly === false && /address/.test(r.logs?.topicOnlyError ?? ''), 'address-less filter refused, reason kept', r.logs?.topicOnlyError);
    check(r.logs?.orTopics === true, 'OR topics work');
    check(r.logs?.combined === true, 'address list + topic + range together');
    check(r.logs?.oldLogs === false, 'no logs near genesis');
    check(r.call?.revertData === true, 'revert data returned');
    check(near(r.call?.gasCap, 50e6, 0.01), 'eth_call gas cap found from a silent clamp', r.call?.gasCap);
    check(r.call?.stateOverride === true, 'state override works');
    check(r.history?.stateDepth === 1000 && r.history?.archive === false, 'state history depth 1000, not archive', r.history);
    check(r.batch?.max === 50, 'batch limit located', r.batch);
    check(r.methods?.debug_traceCall === false && r.methods?.eth_feeHistory === true, 'method support: debug missing, feeHistory present', r.methods);
    check(r.rate?.cleanRps === 20 && r.rate?.limitedAt === 40, 'rate ramp: clean at 20, throttled at 40', r.rate);
    check(r.rate?.retryAfter === '1', 'Retry-After captured', r.rate?.retryAfter);
    const s = rateAll(r, { maxLag: 5 });
    check(s.scanner.pass && s.trader.pass && s.reader.pass, 'passes scanner, trader, reader', s);
    check(!s.subscriber.pass, 'HTTP never passes subscriber', s.subscriber.why);
}

console.log('\ngood endpoint over WebSocket');
{
    const r = lagged(await run(new WsClient(good.wsUrl)), 0);
    check(r.reachable && r.kind === 'ws', 'connected over ws');
    check(r.logs?.topicOnly === true && r.logs.maxRange != null && r.logs.maxRange >= 45_000, 'wide range and topic-only over ws', r.logs?.maxRange);
    check(near(r.call?.gasCap, 550e6, 0.01), 'big gas cap', r.call?.gasCap);
    check(r.history?.archive === true, 'archive state');
    check(r.batch?.max === 1000, 'batches over ws', r.batch);
    check((r.ws?.heads ?? 0) >= 5 && (r.ws?.logs ?? 0) >= 5 && r.ws?.died === false, 'newHeads + logs subscriptions delivered', r.ws);
    check(r.rate?.limitedAt === null && r.rate?.cleanRps === 40, 'never throttled up to --max-rps', r.rate);
    const s = rateAll(r, { maxLag: 5 });
    check(s.subscriber.pass && s.subscriber.score > 50, 'passes subscriber', s.subscriber);
    const strictS = rateAll(lagged(await run(new HttpClient(strict.url)), 0), { maxLag: 5 });
    check(s.scanner.score > strictS.scanner.score, 'scores the wider endpoint higher for scanning', [s.scanner.score, strictS.scanner.score]);
}

console.log('\nfailures are reported, not fatal');
{
    const w = await run(new HttpClient(wrong.url));
    check(w.chainIdOk === false && /wrong chain/.test(w.error ?? ''), 'wrong chain id caught', w.error);
    check(!rateAll(lagged(w, 0), { maxLag: 5 }).reader.pass, 'wrong chain fails every profile');

    const st = lagged(await run(new HttpClient(stale.url)), 40);
    const s = rateAll(st, { maxLag: 5 });
    check(!s.trader.pass && /behind/.test(s.trader.why ?? ''), 'stale head fails the gate', s.trader.why);
    check(st.call?.revertData === false, 'missing revert data detected', st.call?.revertError);
    check(!rateAll(lagged(st, 0), { maxLag: 5 }).trader.pass, 'no revert data fails trader even when fresh');

    const d = await run(new WsClient(dying.wsUrl));
    check(d.ws?.died === true && !rateAll(lagged(d, 0), { maxLag: 5 }).subscriber.pass, 'subscription that goes silent is caught', d.ws);

    const dead = await run(new HttpClient('http://127.0.0.1:1'));
    check(!dead.reachable && !!dead.error, 'refused connection reported', dead.error);
    const deadWs = await run(new WsClient('ws://127.0.0.1:1'));
    check(!deadWs.reachable && deadWs.ws?.connected === false, 'refused websocket reported', deadWs.error);
}

for (const m of [good, strict, wrong, stale, dying]) await m.stop();
console.log(`\n${fail ? `${fail} FAILED` : 'all passed'} (${pass} ok)`);
if (fail) console.log(logs.join('\n'));
process.exit(fail ? 1 : 0);
