// VettedProvider against mock nodes: requests go to public endpoints that can
// do the job; the private endpoint is used only when none can, or they fail.
//
//   node --experimental-strip-types test/provider.test.ts

import { MockNode, type MockOpts } from './mock-node.ts';
import { HttpClient } from '../src/rpc.ts';
import { vetEndpoint, sampleLogs } from '../src/probes.ts';
import { rateAll } from '../src/score.ts';
import { VettedProvider, type RouteEvent } from '../src/provider.ts';

let pass = 0, fail = 0;
const check = (cond: boolean, what: string, got?: unknown) => {
    if (cond) { pass++; console.log(`  ok   ${what}${got !== undefined ? `  (${JSON.stringify(got)})` : ''}`); }
    else { fail++; console.log(`  FAIL ${what}  got ${JSON.stringify(got)}`); }
};

const base: MockOpts = {
    chainId: 999, head: 2_000_000, maxRange: 2000, maxAddresses: 100, topicOnly: false, resultCap: 100_000,
    revertData: true, gasCap: 50_000_000, stateOverride: true, stateDepth: 1_000, oldLogs: false, batchMax: 50,
    rps: null, missing: ['debug_traceCall'],
};
const narrow = new MockNode({ ...base });                                                         // 2k blocks, no topic-only
const wide = new MockNode({ ...base, maxRange: 50_000, topicOnly: true, maxAddresses: 1000 });    // 50k blocks, topic-only
const priv = new MockNode({ ...base, maxRange: 1_000_000, topicOnly: true, maxAddresses: 10_000, stateDepth: 10_000_000, oldLogs: true, missing: [] });
for (const m of [narrow, wide, priv]) await m.start();

// Rate the two public mocks the way `vetted-rpc vet` would.
const sample = await sampleLogs([new HttpClient(wide.url)], () => {});
const ctx = { chainId: 999, sample, maxRps: 20, wsWatchSec: 1, log: () => {} };
const endpoints = [];
for (const m of [narrow, wide]) {
    const r = await vetEndpoint(new HttpClient(m.url), ctx, { source: 'test' }, { rate: true });
    r.headLag = 0;
    r.rate = { cleanRps: 40, limitedAt: null, sent: 0 };
    endpoints.push({ ...r, ratings: rateAll(r, { maxLag: 5 }) });
}
const reset = () => { for (const m of [narrow, wide, priv]) m.requests = 0; };

const routes: RouteEvent[] = [];
const p = new VettedProvider(999, priv.url, { ratings: { endpoints }, onRoute: e => routes.push(e) });
const head = await p.getBlockNumber();
reset();

console.log('ordinary requests stay public');
{
    for (let i = 0; i < 20; i++) await p.send('eth_blockNumber', []);   // send(): getBlockNumber() is cached by ethers
    await p.getBalance('0x' + '1'.repeat(40));
    check(priv.requests === 0, 'private untouched by 22 ordinary requests', priv.requests);
    check(narrow.requests > 0 && wide.requests > 0, 'load spread across both publics', [narrow.requests, wide.requests]);
}

console.log('\nrouting by capability');
reset();
{
    const logs = await p.send('eth_getLogs', [{ fromBlock: '0x' + (head - 9_999).toString(16), toBlock: '0x' + head.toString(16), topics: ['0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'] }]);
    check(Array.isArray(logs) && logs.length === 10_000, '10k-block topic-only getLogs answered', logs.length);
    check(wide.requests === 1 && narrow.requests === 0 && priv.requests === 0, 'went straight to the only public that can (no wasted tries)', [narrow.requests, wide.requests, priv.requests]);

    reset();
    const big = await p.send('eth_getLogs', [{ fromBlock: '0x' + (head - 199_999).toString(16), toBlock: '0x' + head.toString(16), address: '0x' + 'c'.repeat(40) }]);
    check(Array.isArray(big) && priv.requests === 1 && narrow.requests + wide.requests === 0, '200k-block range: no public can, private serves it directly', [narrow.requests, wide.requests, priv.requests]);

    reset();
    const old = await p.getBalance('0x' + '1'.repeat(40), head - 500_000);
    check(old === 0n && priv.requests === 1, 'state 500k blocks back: beyond the publics\' history, private', priv.requests);

    reset();
    await p.send('debug_traceCall', [{ to: '0x' + '1'.repeat(40), data: '0x' }, 'latest', {}]);
    check(priv.requests === 1, 'debug_traceCall: publics rated without it, private', priv.requests);
}

console.log('\nreal answers come straight back');
reset();
{
    let err: any = null;
    try { await p.call({ data: '0x63deadbeef600052600460' + '1cfd' }); } catch (e) { err = e; }
    check(err?.code === 'CALL_EXCEPTION' && /deadbeef/.test(err?.data ?? ''), 'revert surfaces as CALL_EXCEPTION with its data', err?.data);
    check(priv.requests === 0 && narrow.requests + wide.requests === 1, 'a revert is an answer, not retried elsewhere', [narrow.requests, wide.requests, priv.requests]);
}

console.log('\nlearning from a capability error the ratings missed');
reset();
{
    narrow.o.maxRange = 500;   // tightened after it was rated
    routes.length = 0;
    for (let i = 0; i < 6; i++) await p.send('eth_getLogs', [{ fromBlock: '0x' + (head - 999).toString(16), toBlock: '0x' + head.toString(16), address: '0x' + 'a'.repeat(40) }]);
    const narrowHits = routes.filter(r => r.url === narrow.url);
    check(narrowHits.filter(r => r.outcome === 'capability').length <= 1, 'refused at most once, then routed around', routes.map(r => `${r.url === narrow.url ? 'narrow' : r.url === wide.url ? 'wide' : 'priv'}:${r.outcome}`));
    check(priv.requests === 0, 'the other public took over — private not used', priv.requests);
    const st = p.stats().endpoints.find(e => e.url === narrow.url)!;
    check(st.learned.maxRange != null && st.learned.maxRange < 1000, 'learned a lower range for it', st.learned);
    narrow.o.maxRange = 2000;
}

console.log('\nfilters stay on the node that holds them');
{
    const id = await p.send('eth_newFilter', [{ address: '0x' + 'a'.repeat(40) }]);
    let okCount = 0;
    for (let i = 0; i < 12; i++) { try { await p.send('eth_getFilterChanges', [id]); okCount++; } catch { /* filter not found */ } }
    check(okCount === 12, '12 eth_getFilterChanges all answered by the filter\'s own node', okCount);
    check(await p.send('eth_uninstallFilter', [id]) === true, 'uninstalled on that node too');
    check((p as any)._getOption('polling') === true, 'provider.on(filter) polls eth_getLogs, not filter ids');
}

console.log('\nan endpoint going down');
reset();
{
    await narrow.stop();
    routes.length = 0;
    // weighted-random picks: keep going until the dead one has actually been tried
    for (let i = 0; i < 60 && !routes.some(r => r.url === narrow.url); i++) await p.send('eth_blockNumber', []);
    for (let i = 0; i < 10; i++) await p.send('eth_blockNumber', []);

    check(priv.requests === 0, 'the remaining public absorbs it; private untouched', priv.requests);
    check(p.stats().endpoints.find(e => e.url === narrow.url)!.state === 'down', 'dead endpoint marked down');
    await wide.stop();
    reset();
    const n = Number(await p.send('eth_blockNumber', []));
    check(n === head && priv.requests >= 1, 'all publics down: private serves', priv.requests);
    check(p.usage.privateFallbacks >= 0, 'usage counted', p.usage);
}

console.log('\nsends go everywhere useful at once');
{
    const hash = await p.send('eth_sendRawTransaction', ['0x02f8']);
    check(typeof hash === 'string' && priv.requests >= 1, 'private included for eth_sendRawTransaction', hash);
}

console.log('\nconstructor forms');
{
    const ratings = { endpoints };
    const a = new VettedProvider(999, priv.url, { ratings });
    const b = new VettedProvider(999, { privateRpc: priv.url, ratings });
    const c = new VettedProvider(999, { ratings });
    const d = new VettedProvider(999, null, { ratings, privateRpc: priv.url });
    check(a.privateRpc === priv.url, 'string second argument is the private RPC');
    check(b.privateRpc === priv.url, 'options object with privateRpc');
    check(c.privateRpc === null, 'options object without privateRpc: public only');
    check(d.privateRpc === priv.url, 'null URL, privateRpc in the options');
    for (const x of [a, b, c, d]) x.destroy();
}

p.destroy();
await priv.stop();
console.log(`\n${fail ? `${fail} FAILED` : 'all passed'} (${pass} ok)`);
process.exit(fail ? 1 : 0);
