// VettedProvider against mock nodes: requests go to public endpoints that can
// do the job; the private endpoint is used only when none can, or they fail.
//
//   node --experimental-strip-types test/provider.test.ts

import { MockNode, type MockOpts } from './mock-node.ts';
import { HttpClient } from '../src/rpc.ts';
import { vetEndpoint, sampleLogs } from '../src/probes.ts';
import { rateAll } from '../src/score.ts';
import { VettedProvider, type RouteEvent } from '../src/provider.ts';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

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

console.log('\ntransactions go to sendRpc, and only there');
{
    const seq = new MockNode({ ...base }), relay = new MockNode({ ...base });
    await seq.start(); await relay.start();
    const all = [narrow, wide, priv, seq, relay];
    const zero = () => { for (const m of all) m.requests = 0; };
    const counts = () => all.map(m => m.requests);
    const tx = '0x02f8';
    const ratings = { endpoints };
    const warm = async (x: VettedProvider) => { await x.send('eth_blockNumber', []); zero(); };

    let x = new VettedProvider(999, { privateRpc: priv.url, sendRpc: seq.url, ratings });
    await warm(x);
    await x.send('eth_sendRawTransaction', [tx]);
    check(seq.requests === 1 && narrow.requests + wide.requests + priv.requests === 0, 'sendRpc set: the sequencer alone gets it — not private, not public', counts());
    zero();
    for (let i = 0; i < 10; i++) await x.send('eth_blockNumber', []);
    check(seq.requests === 0, 'reads never go to sendRpc', counts());
    check(x.stats().sendTo[0] === seq.url, 'stats() names the send target', x.stats().sendTo);
    x.destroy();

    x = new VettedProvider(999, { sendRpc: [seq.url, relay.url], ratings });
    await warm(x);
    await x.send('eth_sendRawTransaction', [tx]);
    check(seq.requests === 1 && relay.requests === 1 && narrow.requests + wide.requests === 0, 'two send targets: both get it at once, publics none', counts());
    x.destroy();

    x = new VettedProvider(999, priv.url, { ratings });
    await warm(x);
    await x.send('eth_sendRawTransaction', [tx]);
    check(priv.requests === 1 && narrow.requests + wide.requests === 0, 'no sendRpc, private given: private alone (no public mempool leak)', counts());
    x.destroy();

    x = new VettedProvider(999, { ratings });
    await warm(x);
    await x.send('eth_sendRawTransaction', [tx]);
    check(narrow.requests + wide.requests === 1, 'neither: exactly one public endpoint, the best trader', counts());
    x.destroy();

    x = new VettedProvider(999, { sendRpc: 'http://127.0.0.1:1', ratings });
    await warm(x);
    let err: any = null;
    try { await x.send('eth_sendRawTransaction', [tx]); } catch (e) { err = e; }
    check(err != null && narrow.requests + wide.requests + priv.requests === 0, 'sendRpc down: the error comes back, nothing falls back to public', { err: err?.shortMessage ?? err?.message, counts: counts() });
    x.destroy();

    await seq.stop(); await relay.stop();
    zero();
}

console.log('\nprivate websocket, bestUrl, address-less filters nobody serves');
{
    const ratings = { endpoints };
    const pw = new MockNode({ ...base, maxRange: 1_000_000 }); await pw.start();
    const all = [narrow, wide, priv, pw];
    const zero = () => { for (const m of all) m.requests = 0; };
    let x = new VettedProvider(999, { privateRpc: priv.url, privateWs: pw.wsUrl, ratings });
    await x.send('eth_blockNumber', []); zero();
    await x.send('eth_getLogs', [{ fromBlock: '0x' + (head - 199_999).toString(16), toBlock: '0x' + head.toString(16), address: '0x' + 'c'.repeat(40) }]);
    check(pw.requests === 1 && priv.requests === 0, 'private work goes over the private websocket while it is up', all.map(m => m.requests));
    await pw.stop();
    zero();
    await x.send('eth_getLogs', [{ fromBlock: '0x' + (head - 199_999).toString(16), toBlock: '0x' + head.toString(16), address: '0x' + 'c'.repeat(40) }]);
    await x.send('eth_getLogs', [{ fromBlock: '0x' + (head - 199_999).toString(16), toBlock: '0x' + head.toString(16), address: '0x' + 'c'.repeat(40) }]);
    check(priv.requests >= 1, 'private websocket down: private HTTP takes over', all.map(m => m.requests));
    check(await x.bestUrl('scanner', 'http') === wide.url, 'bestUrl(scanner, http) is the wide endpoint', await x.bestUrl('scanner', 'http'));
    check(await x.bestUrl('subscriber', 'ws') === null, 'bestUrl(subscriber, ws) is null when no public websocket passes');
    x.destroy();

    x = new VettedProvider(999, { ratings: { endpoints: endpoints.filter(e => e.url === narrow.url) } });   // only the address-requiring one
    let err: any = null;
    try { await x.send('eth_getLogs', [{ fromBlock: '0x' + (head - 9).toString(16), toBlock: '0x' + head.toString(16), topics: ['0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'] }]); } catch (e) { err = e; }
    check(err?.error?.code === -32701 && /specify an address/i.test(err?.error?.message ?? ''), 'no public accepts topic-only and no private: answered -32701 "specify an address"', err?.error);
    x.destroy();
    zero();
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

console.log('\nlagging and latest-state-only nodes (Sonic)');
{
    // One node keeps only its latest state, one runs 3 blocks behind, one is a good
    // archive node. Reads pinned near the head must all succeed, and the pool must
    // learn to keep them away from the first two.
    const sb: MockOpts = { ...base, maxRange: 10_000, topicOnly: true, sonicState: true, stateDepth: 100_000_000, oldLogs: true };
    const latest = new MockNode({ ...sb, stateDepth: 0, sonicStateNoNumber: true });   // the message as Sonic sends it: no number
    const behind = new MockNode({ ...sb, lagBlocks: 3 });
    const good = new MockNode({ ...sb });
    for (const m of [latest, behind, good]) await m.start();
    const eps = [];
    for (const m of [latest, behind, good]) {
        const r = await vetEndpoint(new HttpClient(m.url), ctx, { source: 'test' }, { rate: true });
        r.rate = { cleanRps: 200, limitedAt: null, sent: 0 };
        r.headLag = 0;            // rated before it fell behind
        if (r.history) r.history.stateDepth = null as any;   // history unknown, like drpc/swiftnodes on Sonic
        eps.push({ ...r, ratings: rateAll(r, { maxLag: 5 }) });
    }
    const q = new VettedProvider(999, { ratings: { endpoints: eps }, maxPublicAttempts: 3 });
    await q.getBlockNumber();
    const h = sb.head;   // the real head (a getBlockNumber may be answered by the node behind)
    const pinned = '0x' + (h - 1).toString(16), atHead = '0x' + h.toString(16);
    let ok = 0, failed: string[] = [];
    for (let i = 0; i < 30; i++) {
        try { await q.send('eth_call', [{ to: '0x' + '1'.repeat(40), data: '0x' }, pinned]); ok++; } catch (e) { failed.push((e as Error).message.slice(0, 60)); }
    }
    check(ok === 30, 'every eth_call pinned 1 block back answered', failed[0] ?? ok);
    const st = q.stats().endpoints;
    const lat = st.find(e => e.url === latest.url)!;
    check(lat.learned.stateDepth === 0, 'learned: latest-state-only node', lat.learned);
    latest.requests = 0;
    for (let i = 0; i < 10; i++) await q.send('eth_getBalance', ['0x' + '1'.repeat(40), pinned]);
    check(latest.requests === 0, 'no more pinned reads sent to it', latest.requests);
    ok = 0; failed = [];
    for (let i = 0; i < 20; i++) {
        try { await q.send('eth_getLogs', [{ address: '0x' + 'a'.repeat(40), fromBlock: '0x' + (h - 5).toString(16), toBlock: atHead }]); ok++; } catch (e) { failed.push((e as Error).message.slice(0, 60)); }
    }
    check(ok === 20, 'every getLogs up to the head answered (a node behind is retried elsewhere)', failed[0] ?? ok);
    const beh = q.stats().endpoints.find(e => e.url === behind.url)!;
    check(beh.head != null && beh.head < h, 'learned: that node is behind', { head: beh.head, best: h });
    q.destroy();
    for (const m of [latest, behind, good]) await m.stop();
}

console.log('\npaid-only history (publicnode: "Archive requests require a personal token")');
{
    // One node serves recent data free and refuses older reads unless you pay;
    // the other serves everything. Old reads must fail over, and the pool must
    // learn to keep them off the paywalled node — while it still gets recent ones.
    const gb: MockOpts = { ...base, maxRange: 100_000, topicOnly: true, maxAddresses: 1000, stateDepth: 100_000_000, oldLogs: true };
    const walled = new MockNode({ ...gb, gatedDepth: 128 });
    const free = new MockNode({ ...gb });
    for (const m of [walled, free]) await m.start();
    const eps = [];
    for (const m of [walled, free]) {
        const r = await vetEndpoint(new HttpClient(m.url), ctx, { source: 'test' }, { rate: true });
        r.rate = { cleanRps: 200, limitedAt: null, sent: 0 };
        r.headLag = 0;
        eps.push({ ...r, ratings: rateAll(r, { maxLag: 5 }) });
    }
    // Rated alike (a vet probes near the head, where the paywall does not show),
    // so routing alone does not keep old reads off the walled node.
    eps[0] = { ...eps[1], url: walled.url };
    const q = new VettedProvider(999, { ratings: { endpoints: eps }, maxPublicAttempts: 3 });
    const h = await q.getBlockNumber();
    let ok = 0; const failed: string[] = [];
    for (let i = 0; i < 30; i++) {
        try { const t = await q.send('eth_getTransactionByHash', ['0x' + i.toString(16).padStart(62, '0') + 'aa']); if (t?.from) ok++; } catch (e) { failed.push((e as Error).message.slice(0, 70)); }
    }
    check(ok === 30, 'every old transaction lookup answered (paywall error retried elsewhere, never returned)', failed[0] ?? ok);
    const w = () => q.stats().endpoints.find(e => e.url === walled.url)!;
    check((w().learned.gated as unknown as string[] ?? []).includes('eth_getTransactionByHash'), 'learned: that node paywalls by-hash lookups', w().learned);
    walled.requests = 0;
    for (let i = 0; i < 10; i++) await q.send('eth_getTransactionByHash', ['0x' + 'f'.repeat(62) + 'aa']);
    check(walled.requests === 0, 'no more by-hash lookups sent to it', walled.requests);

    ok = 0; failed.length = 0;
    for (let i = 0; i < 20; i++) {
        try { await q.send('eth_getLogs', [{ address: '0x' + 'a'.repeat(40), fromBlock: '0x' + (h - 50_000).toString(16), toBlock: '0x' + (h - 49_990).toString(16) }]); ok++; } catch (e) { failed.push((e as Error).message.slice(0, 70)); }
    }
    check(ok === 20, 'every deep getLogs answered', failed[0] ?? ok);
    check(w().learned.logsDepth != null && w().learned.logsDepth! < 50_000, 'learned: how deep its free logs go', w().learned.logsDepth);
    walled.requests = 0;
    for (let i = 0; i < 20; i++) await q.send('eth_getLogs', [{ address: '0x' + 'a'.repeat(40), fromBlock: '0x' + (h - 10).toString(16), toBlock: '0x' + h.toString(16) }]);
    check(walled.requests > 0, 'it still serves recent getLogs', walled.requests);
    q.destroy();
    for (const m of [walled, free]) await m.stop();
}

console.log('\nan open websocket does not keep a finished script alive');
{
    // Scripts that never call destroy(): the socket holds the process only while
    // something is waiting on it — a request, or a live subscription.
    const rpcSrc = fileURLToPath(new URL('../src/rpc.ts', import.meta.url));
    const run = (body: string, limitMs: number) => new Promise<number | string>(resolve => {
        const child = `import { WsClient } from ${JSON.stringify(rpcSrc)};
            const c = new WsClient(${JSON.stringify(priv.wsUrl)});
            if (await c.connect()) process.exit(2);
            ${body}`;
        const cp = spawn(process.execPath, ['--experimental-strip-types', '--disable-warning=ExperimentalWarning', '--input-type=module', '-e', child], { stdio: 'inherit' });
        const kill = setTimeout(() => { cp.kill(); resolve('alive'); }, limitMs);
        cp.on('exit', c => { clearTimeout(kill); resolve(c ?? 'killed'); });
    });
    const call = await run(`const r = await c.call('eth_blockNumber', []); if (!r.result) process.exit(3);`, 5000);
    check(call === 0, 'exits on its own after the last reply', call);
    const subbed = await run(`const id = await c.subscribe(['newHeads'], () => {}); if (typeof id !== 'string') process.exit(3);`, 1500);
    check(subbed === 'alive', 'a live subscription keeps it running', subbed);
    const unsubbed = await run(`const id = await c.subscribe(['newHeads'], () => {}); if (typeof id !== 'string') process.exit(3);
            await c.unsubscribe(id);`, 5000);
    check(unsubbed === 0, 'exits once the subscription is dropped', unsubbed);
}

p.destroy();
await priv.stop();
console.log(`\n${fail ? `${fail} FAILED` : 'all passed'} (${pass} ok)`);
process.exit(fail ? 1 : 0);
