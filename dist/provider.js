// -----------------------------------------------------------------------------
// VettedProvider — an ethers v6 JsonRpcProvider backed by a pool of rated public
// RPCs, with an optional private endpoint used only when it is worth paying for.
//
//   const p = new VettedProvider(999);                                    // public only
//   const p = new VettedProvider(999, process.env.MY_PAID_RPC);           // + private
//   const p = new VettedProvider(999, { privateRpc: url, noTracking: true }); // options form
//
// It IS a JsonRpcProvider (it overrides only _send), so it drops into any code
// that takes one. Every request is routed on its own:
//
//   1. What does it NEED? An eth_getLogs over 40k blocks and 300 addresses needs
//      an endpoint rated for that; an eth_call needs one that returns revert
//      data, and enough gas cap; a read at block N-500k needs that much state
//      history; debug_/trace_ need endpoints that have them; a call pinned to
//      block N needs an endpoint whose head has reached N.
//   2. Which PUBLIC endpoints can do it, are healthy, and have rate budget left?
//      Pick among the best few, weighted by rating and live latency (spreads
//      load instead of hammering the top one). Out of budget everywhere: wait a
//      moment for a refill rather than spend private requests.
//   3. The PRIVATE endpoint is used only when no public one can do the job, as
//      the fallback when public attempts fail, or for methods listed in
//      `privateFor` (sent to private AND the best publics at once).
//
// TRANSACTIONS are not routed like reads. eth_sendRawTransaction (and the other
// send methods) go to `sendRpc` — the sequencer of an FCFS chain, a private
// relay on a mempool chain — and ONLY there: on a mempool chain, also handing
// the transaction to a public endpoint would put it in the public mempool and
// defeat the relay. Without `sendRpc`, sends go to the private endpoint; without
// either, to the best-rated `trader` public endpoint (falling back to the next).
// `sendRpc` is never used for reads — sequencer and relay endpoints usually
// accept nothing but sends.
//
// Answers are classified: a revert or bad params is a real answer and goes
// straight back; "range too large", "method not found", "missing trie node",
// rate limits and transport failures are the endpoint's problem — the pool
// learns from it (lowers that endpoint's known limit, cools it down, opens its
// circuit) and the request moves to the next candidate.
//
// Ratings come from `vetted-rpc vet <chainId>` (results/<chainId>.json). Without
// them the pool still works on chainlist's unrated list, learning as it goes.
// -----------------------------------------------------------------------------
import fs from 'node:fs';
import path from 'node:path';
import { JsonRpcProvider, Network, keccak256 } from 'ethers';
import { HttpClient, WsClient, isRateLimited, isMissingMethod, num } from "./rpc.js";
import { discover } from "./chainlist.js";
const CAPABILITY_RE = /block range|range (?:is )?too (?:large|wide)|exceed(?:s|ed)? .*(?:range|blocks)|too many (?:blocks|addresses|results|logs)|more than \d+ (?:results|logs)|response size|limit exceeded|query returned more|missing trie node|header not found|unknown block|state .*not available|historical state|pruned|beyond history|not (?:yet )?(?:synced|available)|please specify an address|address.*required|gas .*(?:cap|limit).*exceed|exceeds (?:block )?gas limit|execution timeout|-32701/i;
const ALREADY_KNOWN = /already known|known transaction|already imported|nonce too low|replacement transaction underpriced/i;
const HISTORY_DEPTH_GRACE = 64;
/** Methods that submit a transaction: routed to sendRpc, never load-balanced. */
const SEND_METHODS = new Set([
    'eth_sendRawTransaction', 'eth_sendRawTransactionSync', 'eth_sendRawTransactionConditional',
    'eth_sendPrivateTransaction', 'eth_sendPrivateRawTransaction', 'eth_sendBundle',
]);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const parseBlock = (t) => typeof t === 'string' && /^0x[0-9a-f]+$/i.test(t) ? Number(BigInt(t)) : typeof t === 'number' ? t : undefined;
export class VettedProvider extends JsonRpcProvider {
    chainId;
    privateRpc;
    privateWs;
    opts;
    members = [];
    /** Explicit transaction targets (sendRpc). Not part of the read pool. */
    senders = [];
    /** Resolves once the pool is built. (Not `ready`: that is ethers' own getter.) */
    booted;
    bestHead = null;
    /** Filter id -> the member that created it: eth_getFilterChanges / eth_uninstallFilter must go back there. */
    filters = new Map();
    headsAt = 0;
    headsBusy = null;
    /** Requests answered by the private endpoint vs public ones. */
    usage = { public: 0, private: 0, privateFallbacks: 0, waitsForBudget: 0 };
    /**
     * @param chainId  the chain to serve
     * @param privateOrOptions  your private RPC URL, or the options object (which may carry `privateRpc`)
     * @param options  options, when the second argument is the URL
     */
    constructor(chainId, privateOrOptions, options = {}) {
        const o = typeof privateOrOptions === 'string' || privateOrOptions == null
            ? { ...options, privateRpc: privateOrOptions ?? options.privateRpc }
            : { ...privateOrOptions };
        const privateRpc = o.privateRpc || null;
        const network = Network.from(chainId);
        // The URL handed to JsonRpcProvider is never used: _send is ours. batchMaxCount 1
        // so every request reaches _send on its own and is routed on its own.
        // polling: provider.on(filter) polls stateless eth_getLogs instead of eth_newFilter +
        // eth_getFilterChanges — a filter id lives on ONE node, and a pool answers each
        // request from wherever is best, so filter-id subscriptions would break.
        super(privateRpc ?? 'http://vetted-rpc.invalid', network, { staticNetwork: network, batchMaxCount: 1, polling: true });
        this.chainId = chainId;
        this.privateRpc = privateRpc;
        this.privateWs = o.privateWs || null;
        const { privateRpc: _omit, privateWs: _omitWs, ...rest } = o;
        this.opts = {
            privateFor: [], useWs: true, noTracking: false, timeoutMs: 15_000, maxPublicAttempts: 3, budgetWaitMs: 2000,
            ...rest,
        };
        this.booted = this.init();
        this.booted.catch(() => { });
    }
    // --- setup --------------------------------------------------------------------
    async init() {
        const rated = this.loadRatings();
        if (rated?.length) {
            for (const r of rated) {
                if (r.url === this.privateRpc || r.url === this.privateWs)
                    continue;
                if (!r.reachable || !r.chainIdOk)
                    continue;
                if (this.opts.noTracking && r.tracking === 'yes')
                    continue;
                if (r.kind === 'ws' && !this.opts.useWs)
                    continue;
                if (!Object.values(r.ratings).some(x => x.pass))
                    continue; // failed every gate
                this.add(r.url, r.kind, false, r);
            }
        }
        else {
            const info = await discover(this.chainId).catch(() => null);
            for (const e of info?.endpoints ?? []) {
                if (e.url === this.privateRpc || e.url === this.privateWs || (e.kind === 'ws' && !this.opts.useWs))
                    continue;
                if (this.opts.noTracking && e.tracking === 'yes')
                    continue;
                this.add(e.url, e.kind, false);
            }
        }
        if (this.privateRpc)
            this.add(this.privateRpc, /^wss?:/i.test(this.privateRpc) ? 'ws' : 'http', true);
        if (this.privateWs && this.privateWs !== this.privateRpc)
            this.add(this.privateWs, 'ws', true);
        const send = this.opts.sendRpc == null ? [] : Array.isArray(this.opts.sendRpc) ? this.opts.sendRpc : [this.opts.sendRpc];
        for (const url of [...new Set(send.filter(Boolean))]) {
            const pv = this.privateMember();
            if (pv && pv.url === url) {
                this.senders.push(pv);
                continue;
            }
            this.add(url, /^wss?:/i.test(url) ? 'ws' : 'http', true);
            this.senders.push(this.members.pop()); // a send target, not a read endpoint
        }
        if (!this.members.length)
            throw new Error(`VettedProvider: no usable endpoint for chain ${this.chainId} (no ratings, chainlist unreachable, no private RPC)`);
        await this.refreshHeads();
    }
    /**
     * Read eth_blockNumber from the best few public endpoints at once, so routing
     * knows each one's head (a call pinned to block N must not go to an endpoint
     * still at N-3) and the chain head (how deep a historical read is). Repeated
     * in the background every 30 s while requests flow. Never touches the private
     * endpoint unless it is the only one.
     */
    refreshHeads() {
        this.headsBusy ??= (async () => {
            const pub = this.members.filter(x => !x.isPrivate && Date.now() >= x.openUntil);
            const pick = (pub.length ? pub : this.members).sort((a, b) => a.ewmaMs - b.ewmaMs).slice(0, 6);
            await Promise.all(pick.map(async (x) => {
                const r = await this.exchange(x, { jsonrpc: '2.0', id: 0, method: 'eth_blockNumber', params: [] });
                this.judge(x, { method: 'eth_blockNumber', profile: 'trader' }, r);
            }));
            this.headsAt = Date.now();
        })().finally(() => { this.headsBusy = null; });
        return this.headsBusy;
    }
    loadRatings() {
        const src = this.opts.ratings ?? 'results';
        if (typeof src === 'object')
            return src.endpoints;
        const file = fs.existsSync(src) && fs.statSync(src).isDirectory() ? path.join(src, `${this.chainId}.json`) : src;
        if (!fs.existsSync(file))
            return null;
        try {
            const res = JSON.parse(fs.readFileSync(file, 'utf8'));
            return Number(res.chainId) === this.chainId ? res.endpoints : null;
        }
        catch {
            return null;
        }
    }
    add(url, kind, isPrivate, rep) {
        const rps = isPrivate ? (this.opts.privateRps ?? Infinity)
            : rep?.rate ? Math.max(2, Math.floor((rep.rate.cleanRps || 5) * 0.8))
                : 5; // unrated public: be gentle until it proves otherwise
        this.members.push({
            url, kind, isPrivate,
            client: kind === 'ws' ? new WsClient(url) : new HttpClient(url),
            rep, ratings: rep?.ratings,
            learned: { missing: new Set() },
            ewmaMs: rep?.latency?.p50 || 300, head: null, headAt: 0, fails: 0, openUntil: 0, coolUntil: 0,
            rps, tokens: Math.min(rps, 10), refillAt: Date.now(),
            served: 0, errors: 0,
        });
    }
    // --- request needs ---------------------------------------------------------------
    needOf(p) {
        const m = p.method, params = (p.params ?? []);
        const need = { method: m, profile: 'trader' };
        if (m === 'eth_getLogs') {
            const f = params[0] ?? {};
            need.profile = 'scanner';
            if (f.blockHash)
                return need;
            const head = this.bestHead;
            const from = f.fromBlock === 'earliest' ? 0 : parseBlock(f.fromBlock) ?? head ?? undefined;
            const to = parseBlock(f.toBlock) ?? head ?? undefined;
            if (from != null && to != null)
                need.logsRange = Math.max(1, to - from + 1);
            need.fromBlock = from;
            need.block = to;
            const a = f.address;
            need.logsAddrs = a == null ? 0 : Array.isArray(a) ? a.length : 1;
            need.topicOnly = a == null && Array.isArray(f.topics) && f.topics.length > 0;
        }
        else if (m === 'eth_call' || m === 'eth_estimateGas' || m === 'eth_createAccessList') {
            need.profile = 'reader';
            need.revertData = true;
            const tx = params[0] ?? {};
            if (tx.gas)
                need.gas = Number(BigInt(tx.gas));
            need.block = parseBlock(params[1]);
            if (m === 'eth_call' && params[2] && typeof params[2] === 'object')
                need.stateOverride = true;
        }
        else if (/^eth_get(Balance|Code|StorageAt|TransactionCount|Proof)$/.test(m)) {
            need.block = parseBlock(params[params.length - 1]);
        }
        else if (m === 'eth_getBlockByNumber' || m === 'eth_getBlockReceipts' || m === 'eth_getBlockTransactionCountByNumber') {
            need.block = parseBlock(params[0]);
        }
        return need;
    }
    /** Can member `x` do `need`, from its ratings and what it has taught us? undefined = unknown (allowed, ranked lower). */
    can(x, n) {
        if (x.learned.missing.has(n.method))
            return false;
        const L = x.rep?.logs, C = x.rep?.call, H = x.rep?.history;
        if (n.method in (x.rep?.methods ?? {}) && x.rep.methods[n.method] === false)
            return false;
        if (n.block != null && x.head != null && Date.now() - x.headAt < 10_000 && x.head < n.block)
            return false; // not caught up to the block asked for
        if (n.logsRange != null) {
            const max = x.learned.maxRange ?? L?.maxRange;
            if (max != null && n.logsRange > max)
                return false;
            const addrs = x.learned.maxAddresses ?? L?.maxAddresses;
            if (n.logsAddrs && addrs != null && n.logsAddrs > addrs)
                return false;
            if (n.topicOnly && (x.learned.topicOnly ?? L?.topicOnly) === false)
                return false;
            if (n.fromBlock != null && this.bestHead != null && this.bestHead - n.fromBlock > 1_000_000 && L?.oldLogs === false)
                return false;
        }
        if (n.gas != null) {
            const cap = x.learned.gasCap ?? C?.gasCap;
            if (cap != null && n.gas > cap)
                return false;
        }
        if (n.stateOverride && C?.stateOverride === false)
            return false;
        if (n.revertData && (x.learned.revertData ?? C?.revertData) === false)
            return false;
        if (n.block != null && this.bestHead != null) {
            const depth = this.bestHead - n.block;
            const max = x.learned.stateDepth ?? H?.stateDepth;
            if (depth > HISTORY_DEPTH_GRACE && max != null && !H?.archive && depth > max)
                return false;
        }
        return true;
    }
    available(x, now) {
        return now >= x.openUntil && now >= x.coolUntil;
    }
    refill(x, now) {
        if (!Number.isFinite(x.rps)) {
            x.tokens = Infinity;
            return;
        }
        x.tokens = Math.min(x.rps, x.tokens + (now - x.refillAt) / 1000 * x.rps);
        x.refillAt = now;
    }
    weight(x, n) {
        const score = x.ratings?.[n.profile]?.pass ? x.ratings[n.profile].score : x.ratings ? 10 : 30;
        return Math.max(1, score) * (300 / (300 + x.ewmaMs)) / (1 + x.fails);
    }
    /** Next public member for `need`, excluding `tried`; waits for rate budget (up to budgetWaitMs) rather than going private. */
    async pickPublic(n, tried) {
        const deadline = Date.now() + this.opts.budgetWaitMs;
        for (;;) {
            const now = Date.now();
            const pool = this.members.filter(x => !x.isPrivate && !tried.has(x) && this.available(x, now) && this.can(x, n));
            if (!pool.length)
                return null;
            for (const x of pool)
                this.refill(x, now);
            const ready = pool.filter(x => x.tokens >= 1);
            if (ready.length) {
                const top = ready.sort((a, b) => this.weight(b, n) - this.weight(a, n)).slice(0, 3);
                const total = top.reduce((s, x) => s + this.weight(x, n), 0);
                let r = Math.random() * total;
                for (const x of top) {
                    r -= this.weight(x, n);
                    if (r <= 0)
                        return x;
                }
                return top[0];
            }
            // Everyone capable is out of budget: waiting for the soonest refill is
            // cheaper than a paid request — unless it is further off than we will wait.
            const soonest = Math.min(...pool.map(x => (1 - x.tokens) / x.rps * 1000));
            if (now + soonest > deadline)
                return null;
            this.usage.waitsForBudget++;
            await sleep(Math.max(5, Math.ceil(soonest)));
        }
    }
    /** The private transport to use now: the websocket while it is up, else HTTP. */
    privateMember() {
        const now = Date.now();
        const mine = this.members.filter(x => x.isPrivate);
        return mine.find(x => x.kind === 'ws' && this.available(x, now))
            ?? mine.find(x => this.available(x, now))
            ?? mine[0] ?? null;
    }
    /**
     * The best-rated PUBLIC endpoint for a job, e.g. a websocket to subscribe on:
     * bestUrl('subscriber', 'ws'). Null when no rated public endpoint passes —
     * the caller then decides whether its private endpoint is worth it.
     */
    async bestUrl(profile, kind) {
        await this.booted;
        const now = Date.now();
        const n = { method: 'eth_blockNumber', profile };
        const best = this.members
            .filter(x => !x.isPrivate && (!kind || x.kind === kind) && x.ratings?.[profile]?.pass && now >= x.openUntil)
            .sort((a, b) => this.weight(b, n) - this.weight(a, n))[0];
        return best?.url ?? null;
    }
    // --- sending ------------------------------------------------------------------------
    async exchange(x, p) {
        if (x.kind === 'ws') {
            const ws = x.client;
            if (!ws.open) {
                x.wsReady ??= ws.connect(8000).finally(() => { x.wsReady = undefined; });
                const err = await x.wsReady;
                if (err)
                    return { transport: err, ms: 0 };
            }
        }
        // Calls that cannot choose their endpoint (filters, head refresh, fan-out) still
        // spend budget; the debt is capped at one second's worth so it is repaid quickly.
        if (x.tokens !== Infinity)
            x.tokens = Math.max(-x.rps, x.tokens - 1);
        return x.client.call(p.method, (p.params ?? []), this.opts.timeoutMs);
    }
    /** Record the outcome on the member; returns what the router should do next. */
    judge(x, n, r) {
        const now = Date.now();
        if (r.transport && !isRateLimited(r)) {
            x.fails++;
            x.errors++;
            x.openUntil = now + Math.min(300_000, 15_000 * 2 ** Math.min(5, x.fails - 1));
            return 'transport';
        }
        if (isRateLimited(r)) {
            x.errors++;
            const ra = Number(r.retryAfter);
            x.coolUntil = now + (Number.isFinite(ra) && ra > 0 ? ra * 1000 : 3000);
            if (Number.isFinite(x.rps))
                x.rps = Math.max(1, x.rps * 0.7); // learn a gentler pace
            x.tokens = 0;
            return 'rate-limited';
        }
        x.fails = 0;
        x.ewmaMs = x.ewmaMs * 0.8 + r.ms * 0.2;
        if (!r.error) {
            x.served++;
            if (n.method === 'eth_blockNumber' && typeof r.result === 'string') {
                x.head = num(r.result);
                x.headAt = now;
                if (this.bestHead == null || x.head > this.bestHead)
                    this.bestHead = x.head;
            }
            return 'ok';
        }
        if (isMissingMethod(r)) {
            x.learned.missing.add(n.method);
            return 'capability';
        }
        const msg = `${r.error.message} ${typeof r.error.data === 'string' ? r.error.data : ''}`;
        if (CAPABILITY_RE.test(msg) || r.error.code === -32005) {
            x.errors++;
            if (n.logsRange != null) {
                if (/address/i.test(msg)) {
                    if (n.topicOnly)
                        x.learned.topicOnly = false;
                    else if (n.logsAddrs)
                        x.learned.maxAddresses = Math.min(x.learned.maxAddresses ?? Infinity, n.logsAddrs - 1);
                }
                else
                    x.learned.maxRange = Math.min(x.learned.maxRange ?? Infinity, Math.max(1, Math.floor(n.logsRange / 2)));
            }
            if (/trie node|header not found|unknown block|historical|pruned|not available/i.test(msg) && n.block != null && this.bestHead != null) {
                x.learned.stateDepth = Math.min(x.learned.stateDepth ?? Infinity, Math.max(0, this.bestHead - n.block - 1));
            }
            if (/gas/i.test(msg) && n.gas != null)
                x.learned.gasCap = Math.min(x.learned.gasCap ?? Infinity, n.gas - 1);
            return 'capability';
        }
        // A revert without data, from an endpoint that should have given it: it cannot simulate.
        if (n.revertData && /revert/i.test(msg) && r.error.data == null && x.rep?.call?.revertData !== true)
            x.learned.revertData = false;
        return 'answer-error';
    }
    toResult(p, r) {
        if (r.error)
            return { id: p.id, error: { code: r.error.code, message: r.error.message, data: r.error.data } };
        if (r.transport)
            return { id: p.id, error: { code: -32603, message: `VettedProvider: ${r.transport}` } };
        return { id: p.id, result: r.result };
    }
    async route(p) {
        await this.booted;
        if (Date.now() - this.headsAt > 30_000)
            void this.refreshHeads();
        const n = this.needOf(p);
        if (SEND_METHODS.has(n.method))
            return this.submit(p, n);
        if (this.opts.privateFor.includes(n.method))
            return this.fanOut(p, n);
        if (/^eth_(getFilterChanges|getFilterLogs|uninstallFilter)$/.test(n.method))
            return this.sticky(p, n);
        const tried = new Set();
        let last = null;
        for (let i = 0; i < this.opts.maxPublicAttempts; i++) {
            const x = await this.pickPublic(n, tried);
            if (!x)
                break;
            tried.add(x);
            const r = await this.exchange(x, p);
            const outcome = this.judge(x, n, r);
            this.opts.onRoute?.({ method: n.method, url: x.url, private: false, outcome, ms: Math.round(r.ms), detail: r.error?.message ?? r.transport });
            if (outcome === 'ok' || outcome === 'answer-error') {
                this.usage.public++;
                if (outcome === 'ok' && /^eth_new(Block|PendingTransaction)?Filter$/.test(n.method) && typeof r.result === 'string')
                    this.filters.set(r.result, x);
                return this.toResult(p, r);
            }
            last = r;
        }
        // Private transports, preferred first (websocket while it is up, then HTTP): the
        // next one gets a try when one refuses or fails.
        const first = this.privateMember();
        const privates = first ? [first, ...this.members.filter(x => x.isPrivate && x !== first)] : [];
        if (privates.length && tried.size)
            this.usage.privateFallbacks++;
        for (const pv of privates) {
            if (!this.available(pv, Date.now()))
                continue;
            const r = await this.exchange(pv, p);
            const outcome = this.judge(pv, n, r);
            this.opts.onRoute?.({ method: n.method, url: pv.url, private: true, outcome, ms: Math.round(r.ms), detail: r.error?.message ?? r.transport });
            this.usage.private++;
            if (outcome === 'ok' || outcome === 'answer-error') {
                if (outcome === 'ok' && /^eth_new(Block|PendingTransaction)?Filter$/.test(n.method) && typeof r.result === 'string')
                    this.filters.set(r.result, pv);
                return this.toResult(p, r);
            }
            last = r;
        }
        if (last)
            return this.toResult(p, last);
        if (n.topicOnly) {
            // The same code and words a refusing endpoint uses, so callers that already
            // handle that refusal (fall back to address-chunked filters) handle this too.
            return { id: p.id, error: { code: -32701, message: `Please specify an address in your request — no endpoint for chain ${this.chainId} accepts address-less eth_getLogs` } };
        }
        return { id: p.id, error: { code: -32603, message: `VettedProvider: no endpoint can serve ${n.method}${n.logsRange ? ` over ${n.logsRange} blocks` : ''} on chain ${this.chainId}` } };
    }
    /** Filter calls go back to the node holding the filter; if it is gone, the filter is gone with it. */
    async sticky(p, n) {
        const id = String((p.params ?? [])[0]);
        const x = this.filters.get(id);
        if (!x)
            return { id: p.id, error: { code: -32000, message: 'filter not found' } };
        const r = await this.exchange(x, p);
        const outcome = this.judge(x, n, r);
        this.opts.onRoute?.({ method: n.method, url: x.url, private: x.isPrivate, outcome, ms: Math.round(r.ms), detail: r.error?.message ?? r.transport });
        if (x.isPrivate)
            this.usage.private++;
        else
            this.usage.public++;
        if (n.method === 'eth_uninstallFilter' || r.error)
            this.filters.delete(id);
        return this.toResult(p, r);
    }
    /**
     * A transaction. Explicit sendRpc targets (or the private endpoint) get it in
     * parallel, and nothing else does — a failure there is returned, never retried
     * on a public endpoint. With neither, the best `trader` publics are tried in
     * turn until one accepts it.
     */
    async submit(p, n) {
        const pv = this.privateMember();
        const fixed = this.senders.length ? this.senders : pv ? [pv] : [];
        const fire = async (x) => {
            const r = await this.exchange(x, p);
            const outcome = this.judge(x, n, r);
            this.opts.onRoute?.({ method: n.method, url: x.url, private: x.isPrivate, outcome, ms: Math.round(r.ms), detail: r.error?.message ?? r.transport });
            if (x.isPrivate)
                this.usage.private++;
            else
                this.usage.public++;
            return r;
        };
        if (fixed.length) {
            const replies = await Promise.all(fixed.map(fire));
            return this.firstAccepted(p, n, replies);
        }
        const tried = new Set();
        const replies = [];
        for (let i = 0; i < this.opts.maxPublicAttempts; i++) {
            const now = Date.now();
            const x = this.members.filter(m => !m.isPrivate && !tried.has(m) && this.available(m, now) && this.can(m, n))
                .sort((a, b) => this.weight(b, n) - this.weight(a, n))[0];
            if (!x)
                break;
            tried.add(x);
            const r = await fire(x);
            replies.push(r);
            // Accepted, or refused for a reason another endpoint would repeat (bad nonce,
            // underpriced, invalid): stop. Only transport / rate-limit failures move on.
            if (!r.transport && !isRateLimited(r))
                return this.firstAccepted(p, n, replies);
        }
        if (!replies.length)
            return { id: p.id, error: { code: -32603, message: `VettedProvider: no endpoint to send ${n.method} to` } };
        return this.firstAccepted(p, n, replies);
    }
    /** First success; "already known" counts as success and resolves to the transaction hash. */
    firstAccepted(p, n, replies) {
        const good = replies.find(r => !r.error && !r.transport);
        if (good)
            return this.toResult(p, good);
        if (n.method === 'eth_sendRawTransaction' && replies.some(r => r.error && ALREADY_KNOWN.test(r.error.message))) {
            return { id: p.id, result: keccak256(p.params[0]) };
        }
        return this.toResult(p, replies.find(r => r.error) ?? replies[0]);
    }
    /**
     * Same request to the private endpoint and the best publics at once (methods in
     * privateFor). The first success wins.
     */
    async fanOut(p, n) {
        await this.booted;
        const now = Date.now();
        const targets = [
            ...this.members.filter(x => x.isPrivate),
            ...this.members.filter(x => !x.isPrivate && this.available(x, now) && this.can(x, n)).sort((a, b) => this.weight(b, n) - this.weight(a, n)).slice(0, 3),
        ];
        if (!targets.length)
            return { id: p.id, error: { code: -32603, message: `VettedProvider: no endpoint for ${n.method}` } };
        const replies = await Promise.all(targets.map(async (x) => {
            const r = await this.exchange(x, p);
            const outcome = this.judge(x, n, r);
            this.opts.onRoute?.({ method: n.method, url: x.url, private: x.isPrivate, outcome, ms: Math.round(r.ms), detail: r.error?.message ?? r.transport });
            if (x.isPrivate)
                this.usage.private++;
            else
                this.usage.public++;
            return r;
        }));
        return this.firstAccepted(p, n, replies);
    }
    // ethers types the reply as JsonRpcResult[], but its own HTTP _send returns error
    // objects in the same array (and getRpcError reads them); the cast says so.
    async _send(payload) {
        const list = Array.isArray(payload) ? payload : [payload];
        return Promise.all(list.map(p => this.route(p)));
    }
    // --- introspection ----------------------------------------------------------------
    /** Per-endpoint view: served, errors, latency, health, learned limits. */
    stats() {
        const now = Date.now();
        return {
            usage: { ...this.usage },
            bestHead: this.bestHead,
            sendTo: (this.senders.length ? this.senders : this.members.filter(x => x.isPrivate)).map(x => x.url),
            endpoints: [...this.members, ...this.senders.filter(x => !this.members.includes(x))].map(x => ({
                url: x.url, kind: x.kind, private: x.isPrivate, served: x.served, errors: x.errors,
                ewmaMs: Math.round(x.ewmaMs), head: x.head,
                state: now < x.openUntil ? 'down' : now < x.coolUntil ? 'cooling' : 'ok',
                rps: Number.isFinite(x.rps) ? +x.rps.toFixed(1) : null,
                learned: { ...x.learned, missing: [...x.learned.missing] },
            })),
        };
    }
    destroy() {
        for (const x of new Set([...this.members, ...this.senders]))
            x.client.close();
        super.destroy();
    }
}
