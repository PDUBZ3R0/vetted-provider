// Subscriptions through VettedProvider against a real node: one anvil, two
// websocket proxies in front of it that can be silenced or cut, so failover,
// backfill, ordering, de-duplication and the polling fallback are exercised on
// real eth_subscribe traffic.
//
//   node --experimental-strip-types test/feed.test.ts      (needs anvil on PATH; skipped otherwise)

import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { WebSocketServer, WebSocket } from 'ws';
import { JsonRpcProvider, Wallet, type Log } from 'ethers';
import { VettedProvider } from '../src/provider.ts';
import type { FeedEvent } from '../src/feed.ts';

if (spawnSync('anvil', ['--version']).status !== 0) {
    console.log('anvil not found — skipping the subscription tests (install foundry to run them)');
    process.exit(0);
}

let pass = 0, fail = 0;
const check = (cond: boolean, what: string, got?: unknown) => {
    if (cond) { pass++; console.log(`  ok   ${what}${got !== undefined ? `  (${JSON.stringify(got)})` : ''}`); }
    else { fail++; console.log(`  FAIL ${what}  got ${JSON.stringify(got)}`); }
};
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
async function until(cond: () => boolean, ms: number): Promise<boolean> {
    const end = Date.now() + ms;
    while (Date.now() < end) { if (cond()) return true; await sleep(50); }
    return cond();
}

// --- anvil -------------------------------------------------------------------------
const PORT = 18_545 + Math.floor(Math.random() * 1000);
const anvil: ChildProcess = spawn('anvil', ['--port', String(PORT), '--block-time', '1', '--silent'], { stdio: 'ignore' });
const httpUrl = `http://127.0.0.1:${PORT}`, wsUpstream = `ws://127.0.0.1:${PORT}`;
const direct = new JsonRpcProvider(httpUrl, 31337, { staticNetwork: true });
for (let i = 0; i < 50; i++) { try { await direct.getBlockNumber(); break; } catch { await sleep(100); } }

// A contract that logs its calldata word as topic1: LOG1(0, 0, calldataload(0)).
const wallet = new Wallet('0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80', direct);
const deploy = await wallet.sendTransaction({ data: '0x6860003560006000a10060005260096017f3' });
const EMITTER = (await deploy.wait())!.contractAddress!.toLowerCase();
let nonce = await wallet.getNonce();
let emitted = 0;
/** One log per call; its only topic carries a running number so order and gaps are checkable. */
async function emit(): Promise<void> {
    emitted++;
    await wallet.sendTransaction({ to: EMITTER, data: '0x' + emitted.toString(16).padStart(64, '0'), nonce: nonce++, gasLimit: 50_000 });
}

// --- proxies -------------------------------------------------------------------------
class Proxy {
    server: WebSocketServer;
    url = '';
    /** Drop eth_subscription frames (the socket stays up and answers calls). */
    silent = false;
    /** Refuse and cut every connection. */
    down = false;
    sockets = new Set<WebSocket>();
    constructor() {
        this.server = new WebSocketServer({ port: 0 });
        this.url = `ws://127.0.0.1:${(this.server.address() as { port: number }).port}`;
        this.server.on('connection', client => {
            if (this.down) { client.terminate(); return; }
            this.sockets.add(client);
            const up = new WebSocket(wsUpstream);
            const queue: string[] = [];
            up.on('open', () => { for (const m of queue) up.send(m); queue.length = 0; });
            client.on('message', d => { const m = d.toString(); if (up.readyState === 1) up.send(m); else queue.push(m); });
            up.on('message', d => {
                const m = d.toString();
                if (this.silent && m.includes('"eth_subscription"')) return;
                if (client.readyState === 1) client.send(m);
            });
            const end = () => { this.sockets.delete(client); try { up.terminate(); } catch { /* */ } try { client.terminate(); } catch { /* */ } };
            client.on('close', end); up.on('close', end); client.on('error', end); up.on('error', end);
        });
    }
    cut(): void { this.down = true; for (const s of this.sockets) s.terminate(); }
    restore(): void { this.down = false; this.silent = false; }
    close(): Promise<void> { this.cut(); return new Promise(r => this.server.close(() => r())); }
}
const P1 = new Proxy(), P2 = new Proxy();

// Ratings as `vet` would write them: P1 the better subscriber, the direct HTTP
// endpoint the scanner (it serves the backfill getLogs).
const rated = (url: string, kind: 'http' | 'ws', subScore: number) => ({
    url, kind, source: 'test', reachable: true, chainIdOk: true,
    latency: { p50: kind === 'ws' ? 5 : 10 },
    logs: { maxRange: 100_000, maxAddresses: 1000, topicOnly: true },
    rate: { cleanRps: 500, limitedAt: null, sent: 0 },
    ratings: {
        scanner: { pass: kind === 'http', score: 80, why: [] },
        trader: { pass: true, score: 80, why: [] },
        reader: { pass: true, score: 80, why: [] },
        subscriber: { pass: kind === 'ws', score: subScore, why: [] },
    },
});
const ratings = { endpoints: [rated(httpUrl, 'http', 0), rated(P1.url, 'ws', 90), rated(P2.url, 'ws', 50)] as any };

const events: FeedEvent[] = [];
const p = new VettedProvider(31337, { ratings, useWs: true, stallMs: 2500, feedRetryMs: 1500, onFeed: e => events.push(e) });
p.pollingInterval = 500;
const lastOf = (t: FeedEvent['type']) => [...events].reverse().find(e => e.type === t) as any;

// --- the subscriber under test ----------------------------------------------------------
const got: number[] = [];
const blocks: number[] = [];
const onLog = (l: Log) => { got.push(parseInt(l.topics[0], 16)); };
const onBlock = (n: number) => { blocks.push(n); };
await p.on({ address: EMITTER }, onLog);
await p.on('block', onBlock);

console.log('push on the best subscriber');
{
    check(await until(() => !!lastOf('connected'), 5000), 'connected', lastOf('connected')?.url === P1.url ? 'P1' : lastOf('connected'));
    for (let i = 0; i < 3; i++) await emit();
    check(await until(() => got.length >= 3, 6000), 'logs pushed', got);
    check(await until(() => blocks.length >= 2, 4000), 'block events', blocks.slice(-3));
    const st = p.stats().feed;
    check(st.mode === 'push' && st.url === P1.url, 'feed state reports push on P1', st.mode);
}

console.log('\na silent socket: failover + backfill');
{
    P1.silent = true;
    const before = got.length;
    for (let i = 0; i < 3; i++) await emit();          // emitted while nothing is delivered
    await sleep(1500);
    check(got.length === before, 'nothing delivered while silent', got.length - before);
    check(await until(() => events.some(e => e.type === 'failover' && e.url === P1.url), 6000), 'silence detected, failed over', lastOf('failover')?.reason);
    check(await until(() => lastOf('connected')?.url === P2.url, 5000), 'now on P2');
    check(await until(() => got.length >= before + 3, 6000), 'the gap was backfilled', lastOf('backfill'));
    for (let i = 0; i < 2; i++) await emit();
    check(await until(() => got.length >= emitted, 6000), 'live logs keep coming after the switch', got.length);
}

console.log('\na cut socket, the other one cooling: polling, then back to push');
{
    P2.cut();
    const before = got.length;
    check(await until(() => !!events.find(e => e.type === 'failover' && e.url === P2.url), 4000), 'close detected, failed over');
    // P1 is still silent (and cooling): the only options are polling or a fresh P1 attempt.
    for (let i = 0; i < 2; i++) await emit();
    check(await until(() => got.length >= before + 2, 8000), 'logs still delivered (polling or retried push)', p.stats().feed.mode);
    P1.restore(); P2.restore();
    check(await until(() => p.stats().feed.mode === 'push', 12_000), 'back to push once an endpoint works', p.stats().feed.url === P1.url ? 'P1' : 'P2');
    for (let i = 0; i < 2; i++) await emit();
    check(await until(() => got.length >= emitted, 6000), 'and delivering', got.length);
}

console.log('\nexactly once, in order');
{
    await sleep(1500);
    const expect = Array.from({ length: emitted }, (_, i) => i + 1);
    check(JSON.stringify(got) === JSON.stringify(expect), `every log once (${emitted})`, got.length === emitted ? 'ok' : got);
    const ordered = got.every((v, i) => i === 0 || v > got[i - 1]);
    check(ordered, 'never an older log after a newer one');
    const uniqueBlocks = new Set(blocks).size === blocks.length;
    check(uniqueBlocks && blocks.every((b, i) => i === 0 || b === blocks[i - 1] + 1), 'a block event for every block, in order, none repeated', `${blocks[0]}..${blocks[blocks.length - 1]} (${blocks.length})`);
    const chainHead = await direct.getBlockNumber();
    check(blocks[blocks.length - 1] >= chainHead - 1, 'block events kept up with the chain', { lastEvent: blocks[blocks.length - 1], chainHead });
    const st = p.stats().feed;
    check(st.duplicatesDropped > 0, 'backfill overlap was dropped, not re-delivered', st.duplicatesDropped);
}

console.log('\nlisteners gone: the socket closes');
{
    await p.off({ address: EMITTER }, onLog);
    await p.off('block', onBlock);
    check(p.stats().feed.mode === 'idle', 'feed idle', p.stats().feed.mode);
}

console.log('\na script that subscribes and unsubscribes exits on its own');
{
    const src = fileURLToPath(new URL('../src/provider.ts', import.meta.url));
    const body = `import { VettedProvider } from ${JSON.stringify(src)};
        const p = new VettedProvider(31337, { ratings: ${JSON.stringify(ratings)} });
        let n = 0;
        const h = () => { n++; };
        await p.on('block', h);
        while (n < 2) await new Promise(r => setTimeout(r, 100));
        await p.off('block', h);`;
    const run = (code: string, ms: number) => new Promise<number | string>(resolve => {
        const cp = spawn(process.execPath, ['--experimental-strip-types', '--disable-warning=ExperimentalWarning', '--input-type=module', '-e', code], { stdio: 'inherit' });
        const t = setTimeout(() => { cp.kill(); resolve('alive'); }, ms);
        cp.on('exit', c => { clearTimeout(t); resolve(c ?? 'killed'); });
    });
    check(await run(body, 10_000) === 0, 'exited after off()');
    check(await run(body.replace("await p.off('block', h);", ''), 6000) === 'alive', 'stays alive while subscribed');
}

p.destroy();
direct.destroy();
await P1.close(); await P2.close();
anvil.kill();
console.log(`\n${fail ? `${fail} FAILED` : 'all passed'} (${pass} ok)`);
process.exit(fail ? 1 : 0);
