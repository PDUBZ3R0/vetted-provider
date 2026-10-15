#!/usr/bin/env -S node
// -----------------------------------------------------------------------------
// vetted-provider — rate every public RPC for a chain, pick the best for a job.
//
//   vetrpc vet <chainId> [options]     test every chainlist endpoint, save ratings
//   vetrpc best <chainId> [options]    print the best endpoint(s) from saved ratings
//
// vet options:
//   --rpc <url>          also test this endpoint (repeatable) — your paid node, say
//   --only http|ws       test one transport only
//   --concurrency <n>    endpoints tested at once (default 4)
//   --max-rps <n>        ceiling for the rate-limit ramp (default 40; 0 skips it)
//   --ws-watch <sec>     how long to watch WebSocket subscriptions (default 15;
//                        300+ to catch sockets that die silently after minutes)
//   --max-lag <blocks>   head lag allowed before an endpoint fails every profile (default 5)
//   --skip <list>        skip probes: logs,calls,history,batch,methods,rate,ws
//   --limit <n>          test only the first n endpoints (quick look)
//   --from <label>       where this run happens (default: hostname) — ratings are
//                        location-dependent, so it is recorded with them
//   --out <dir>          results directory (default ./results)
//
// best options:
//   --profile scanner|trader|reader|subscriber   (default trader)
//   --kind http|ws       (default http; subscriber implies ws)
//   --n <count>          how many, best first (default 1)
//   --out <dir>          where the ratings live (default ./results)
// -----------------------------------------------------------------------------
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { discover } from "./chainlist.js";
import { HttpClient, WsClient, ok, num } from "./rpc.js";
import { vetEndpoint, sampleLogs } from "./probes.js";
import { rateAll, PROFILES } from "./score.js";
const VERSION = '0.1.0';
const args = process.argv.slice(2);
const cmd = args[0];
const opt = (f) => { const i = args.indexOf(f); return i >= 0 ? args[i + 1] : undefined; };
const opts = (f) => args.flatMap((a, i) => a === f && args[i + 1] ? [args[i + 1]] : []);
const numOpt = (f, d) => { const v = opt(f); if (v == null)
    return d; const n = Number(v); if (!Number.isFinite(n))
    die(`${f} expects a number`); return n; };
function die(msg) { console.error(msg); process.exit(1); }
const log = (s) => console.error(s);
const usage = () => die('usage:\n' +
    '  vetted-rpc vet <chainId> [--rpc <url>]... [--only http|ws] [--concurrency n] [--max-rps n] [--ws-watch sec]\n' +
    '                           [--max-lag blocks] [--skip logs,calls,...] [--limit n] [--from label] [--out dir]\n' +
    '  vetted-rpc best <chainId> [--profile scanner|trader|reader|subscriber] [--kind http|ws] [--n count] [--out dir]');
const chainId = Number(args[1]);
if (!cmd || !Number.isInteger(chainId) || chainId <= 0)
    usage();
const outDir = path.resolve(opt('--out') ?? 'results');
const resultsFile = path.join(outDir, `${chainId}.json`);
// --- vet -------------------------------------------------------------------------
async function vet() {
    const only = opt('--only');
    const concurrency = Math.max(1, numOpt('--concurrency', 4));
    const maxRps = numOpt('--max-rps', 40);
    const wsWatchSec = numOpt('--ws-watch', 15);
    const maxLag = numOpt('--max-lag', 5);
    const limit = numOpt('--limit', 0);
    const from = opt('--from') ?? os.hostname();
    const skip = Object.fromEntries((opt('--skip') ?? '').split(',').filter(Boolean).map(s => [s.trim(), true]));
    if (maxRps <= 0)
        skip.rate = true;
    log(`vetted-rpc ${VERSION} — chain ${chainId}, from ${from}`);
    const info = await discover(chainId, { extra: opts('--rpc') });
    let eps = info.endpoints.filter(e => !only || e.kind === only);
    if (limit > 0)
        eps = eps.slice(0, limit);
    log(`${info.name}: ${eps.filter(e => e.kind === 'http').length} http, ${eps.filter(e => e.kind === 'ws').length} ws endpoint(s) to test`);
    if (!eps.length)
        die('nothing to test');
    const sample = await sampleLogs(info.endpoints.filter(e => e.kind === 'http').slice(0, 6).map(e => new HttpClient(e.url)), log);
    const ctx = { chainId, sample, maxRps, wsWatchSec, log };
    const reports = [];
    let next = 0, done = 0;
    const worker = async () => {
        while (next < eps.length) {
            const e = eps[next++];
            const c = e.kind === 'ws' ? new WsClient(e.url) : new HttpClient(e.url);
            const r = await vetEndpoint(c, ctx, { tracking: e.tracking, source: e.source }, skip);
            reports.push(r);
            done++;
            log(`  [${done}/${eps.length}] ${r.reachable && r.chainIdOk ? 'ok ' : 'ERR'} ${e.url}  ${r.reachable && r.chainIdOk ? `p50 ${r.latency?.p50}ms` : r.error ?? ''}  (${(r.ms / 1000).toFixed(0)}s)`);
        }
    };
    await Promise.all(Array.from({ length: Math.min(concurrency, eps.length) }, worker));
    // Head lag from ONE near-simultaneous round, not from heads read minutes apart.
    await headLag(reports);
    const endpoints = reports.map(r => ({ ...r, ratings: rateAll(r, { maxLag }) }));
    endpoints.sort((a, b) => bestScore(b.ratings) - bestScore(a.ratings));
    const results = {
        tool: `vetted-rpc ${VERSION}`, chainId, name: info.name, testedAt: new Date().toISOString(), from,
        options: { maxRps, wsWatchSec, maxLag, only: only ?? 'all', skip: Object.keys(skip), sample },
        endpoints,
    };
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(resultsFile, JSON.stringify(results, null, 2));
    printTable(results);
    printRecommendation(results);
    log(`\nsaved ${path.relative(process.cwd(), resultsFile) || resultsFile}`);
}
async function headLag(reports) {
    const live = reports.filter(r => r.reachable && r.chainIdOk);
    const read = async (r) => {
        for (let attempt = 0; attempt < 2; attempt++) {
            let c;
            if (r.kind === 'ws') {
                const w = new WsClient(r.url);
                if (await w.connect(8000)) {
                    w.close();
                    return null;
                }
                c = w;
            }
            else
                c = new HttpClient(r.url);
            const h = await c.call('eth_blockNumber', [], 8000);
            c.close();
            if (ok(h))
                return num(h.result);
            await new Promise(res => setTimeout(res, 2000));
        }
        return null;
    };
    const heads = await Promise.all(live.map(read));
    const max = Math.max(...heads.filter((h) => h != null));
    live.forEach((r, i) => { if (heads[i] != null) {
        r.head = heads[i];
        r.headLag = Math.max(0, max - heads[i]);
    } });
}
const bestScore = (rt) => Math.max(...PROFILES.map(p => rt[p].pass ? rt[p].score : -1));
// --- output -----------------------------------------------------------------------
const fmtN = (n) => n == null ? '—' : n >= 1e9 ? `${+(n / 1e9).toFixed(1)}G` : n >= 1e6 ? `${+(n / 1e6).toFixed(1)}M` : n >= 1e3 ? `${+(n / 1e3).toFixed(1)}k` : String(n);
const yn = (b) => b == null ? '?' : b ? 'y' : 'n';
const pad = (s, n) => s.length > n ? s.slice(0, n - 1) + '…' : s.padEnd(n);
const lpad = (s, n) => s.padStart(n);
function printTable(res) {
    console.log(`\n${res.name} (${res.chainId}) — ${res.endpoints.length} endpoints, tested from ${res.from}`);
    const head = `${pad('endpoint', 46)} ${lpad('lag', 4)} ${lpad('p50', 6)} ${lpad('range', 6)} ${lpad('addr', 5)} ${'topic'} ${'rev'} ${lpad('gas', 6)} ${lpad('batch', 5)} ${lpad('rps', 4)}  ${PROFILES.map(p => lpad(p.slice(0, 5), 5)).join(' ')}`;
    console.log(head);
    console.log('-'.repeat(head.length));
    for (const e of res.endpoints) {
        const up = e.reachable && e.chainIdOk;
        const scores = PROFILES.map(p => lpad(e.ratings[p].pass ? String(e.ratings[p].score) : '·', 5)).join(' ');
        if (!up) {
            console.log(`${pad(e.url, 46)} ${pad(e.error ?? 'unreachable', 60)}`);
            continue;
        }
        console.log(`${pad(e.url, 46)} ${lpad(String(e.headLag ?? '?'), 4)} ${lpad(e.latency ? `${e.latency.p50}ms` : '—', 6)} ` +
            `${lpad(fmtN(e.logs?.maxRange), 6)} ${lpad(fmtN(e.logs?.maxAddresses), 5)} ${lpad(yn(e.logs?.topicOnly), 5)} ${lpad(yn(e.call?.revertData), 3)} ` +
            `${lpad(fmtN(e.call?.gasCap), 6)} ${lpad(fmtN(e.batch?.max), 5)} ${lpad(e.rate ? `${e.rate.cleanRps}${e.rate.limitedAt ? '' : '+'}` : '—', 4)}  ${scores}`);
    }
    console.log('(range = max getLogs blocks; addr = max addresses per filter; topic = address-less filters; rev = revert data;');
    console.log(' gas = eth_call gas granted; rps = clean rate before throttling, + = never throttled up to --max-rps; · = fails that profile)');
}
function printRecommendation(res) {
    console.log('\nbest per profile:');
    for (const p of PROFILES) {
        for (const kind of p === 'subscriber' ? ['ws'] : ['http', 'ws']) {
            const pool = res.endpoints.filter(e => e.kind === kind);
            if (!pool.length)
                continue;
            const passing = pool.filter(e => e.ratings[p].pass).sort((a, b) => b.ratings[p].score - a.ratings[p].score);
            if (passing.length) {
                console.log(`  ${pad(p, 10)} ${pad(kind, 4)} ${passing[0].url}  (${passing[0].ratings[p].score}${passing.length > 1 ? `; next ${passing[1].url} ${passing[1].ratings[p].score}` : ''})`);
            }
            else {
                const whys = new Map();
                for (const e of pool) {
                    const w = (e.ratings[p].why ?? '').replace(/\(.*\)/, '').trim();
                    whys.set(w, (whys.get(w) ?? 0) + 1);
                }
                const top = [...whys].sort((a, b) => b[1] - a[1])[0]?.[0];
                console.log(`  ${pad(p, 10)} ${pad(kind, 4)} NONE of ${pool.length} pass (most: ${top}) — a private node is worth it for this job`);
            }
        }
    }
}
// --- best ---------------------------------------------------------------------------
function best() {
    if (!fs.existsSync(resultsFile))
        die(`no ratings for chain ${chainId} at ${resultsFile} — run: vetted-rpc vet ${chainId}`);
    const res = JSON.parse(fs.readFileSync(resultsFile, 'utf8'));
    const profile = (opt('--profile') ?? 'trader');
    if (!PROFILES.includes(profile))
        die(`--profile must be one of ${PROFILES.join(', ')}`);
    const kind = profile === 'subscriber' ? 'ws' : (opt('--kind') ?? 'http');
    const n = numOpt('--n', 1);
    const ageH = (Date.now() - Date.parse(res.testedAt)) / 3.6e6;
    if (ageH > 24)
        log(`[!] ratings are ${ageH.toFixed(0)}h old (from ${res.from}) — re-run vet if this matters`);
    const picks = res.endpoints.filter(e => e.kind === kind && e.ratings[profile].pass)
        .sort((a, b) => b.ratings[profile].score - a.ratings[profile].score).slice(0, n);
    if (!picks.length)
        die(`no ${kind} endpoint passes ${profile} for chain ${chainId}`);
    for (const e of picks)
        console.log(e.url);
}
// --- dispatch (last: everything above must be initialised first) ------------------
if (cmd === 'vet')
    await vet();
else if (cmd === 'best')
    best();
else
    usage();
