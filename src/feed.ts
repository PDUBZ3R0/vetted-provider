// -----------------------------------------------------------------------------
// PushFeed — real eth_subscribe for a pool, with failover that loses nothing.
//
// A pool answers each request from wherever is best, which is right for reads
// and wrong for subscriptions: a subscription lives on ONE socket. So the feed
// keeps one dedicated websocket, on the best endpoint rated `subscriber` (your
// private websocket after the publics, or first with `subscribePrivate`), and
// carries every subscription on it:
//
//   heartbeat   a newHeads subscription is always open while anything is
//               subscribed. It drives provider.on('block') and it is the
//               liveness signal: public sockets are known to go silent with
//               no error at all, so silence for `stallMs` counts as death.
//   logs        one eth_subscribe('logs', filter) per provider.on(filter).
//
// FAILOVER. A closed socket, a silent one, or a refused subscription moves the
// feed to the next endpoint (the failed one cools down, doubling each time).
// Everything is resubscribed there, and the gap is BACKFILLED through the pool's
// eth_getLogs from a few blocks before the last head seen — a bare reconnect
// would silently skip whatever happened while the socket was down. Logs pushed
// by the new socket during the backfill are held back and released after it,
// so a consumer never sees an older log after a newer one. Overlap is removed by
// (blockHash, logIndex), so nothing is delivered twice.
//
// FALLBACK. When no websocket works, the feed polls (eth_blockNumber +
// eth_getLogs through the pool) at the provider's pollingInterval, and keeps
// trying to get back to push every `retryMs`.
//
// The socket keeps the process alive only while something is subscribed: once
// the last listener is removed, the feed closes and a finished script exits.
// -----------------------------------------------------------------------------

import { WsClient } from './rpc.ts';

export type FeedEvent =
    | { type: 'connected'; url: string; private: boolean }
    | { type: 'failover'; url: string; reason: string }
    | { type: 'polling'; reason: string }
    | { type: 'backfill'; from: number; to: number; logs: number }
    | { type: 'backfill-failed'; from: number; to: number; error: string }
    | { type: 'error'; url?: string; error: string };

export type LogFilter = { address?: string | string[]; topics?: Array<string | string[] | null> };
export type RawLog = { blockNumber: string; blockHash: string; logIndex: string; transactionHash?: string; removed?: boolean; [k: string]: unknown };

/** What the feed needs from its provider. */
export type FeedHost = {
    ready(): Promise<void>;
    /** Websocket endpoints to subscribe on, best first. */
    candidates(): Array<{ url: string; private: boolean }>;
    /** A pooled request (routed like any other). */
    request(method: string, params: unknown[]): Promise<unknown>;
    emitBlock(n: number): void;
    emitLog(filter: LogFilter, log: RawLog): void;
    pollMs(): number;
    notify(e: FeedEvent): void;
};

export type FeedOptions = {
    /** No frame (head or log) for this long = the socket is dead. Default 30000. */
    stallMs: number;
    /** Base cooldown for an endpoint that failed, doubling per failure (cap 10 min); also how often polling retries push. Default 30000. */
    retryMs: number;
    /** Blocks re-read before the last head seen when backfilling. Default 2. */
    overlap: number;
    /** Max blocks per backfill getLogs. Default 2000 (the pool narrows it if an endpoint refuses). */
    span: number;
};

type Sub = { kind: 'block' } | { kind: 'logs'; filter: LogFilter };
type Mode = 'idle' | 'switching' | 'push' | 'poll';

const hex = (n: number) => '0x' + n.toString(16);
const SEEN_MAX = 20_000;

export class PushFeed {
    private subs = new Map<object, Sub>();
    private remote = new Map<object, string>();
    private client: WsClient | null = null;
    /** The socket being set up during a switch: its frames are accepted (into the buffer). */
    private candidate: WsClient | null = null;
    private url: string | null = null;
    private urlPrivate = false;
    private connectedAt = 0;
    private mode: Mode = 'idle';
    /** Bumped on shutdown: any async step from an older generation stands down. */
    private gen = 0;
    private lastHead = 0;
    private lastActivity = 0;
    private pollFrom = 0;
    private cooldown = new Map<string, { until: number; n: number }>();
    private seen = new Map<string, true>();
    /** Non-null while switching: deliveries wait here until the backfill is out. */
    private buffer: Array<() => void> | null = null;
    private switching: Promise<void> | null = null;
    private watchdog: ReturnType<typeof setInterval> | null = null;
    private pollTimer: ReturnType<typeof setTimeout> | null = null;
    private retryTimer: ReturnType<typeof setInterval> | null = null;
    readonly stats = { connects: 0, failovers: 0, backfilledLogs: 0, duplicatesDropped: 0, pushedLogs: 0, heads: 0 };

    private readonly host: FeedHost;
    private readonly o: FeedOptions;

    constructor(host: FeedHost, o: FeedOptions) { this.host = host; this.o = o; }

    /** Where the feed stands, for stats(). */
    state() {
        return { mode: this.mode, url: this.url, private: this.urlPrivate, subscriptions: this.subs.size, lastHead: this.lastHead, ...this.stats };
    }

    // --- subscriber lifecycle (called by the ethers Subscriber shims) ----------------

    add(key: object, sub: Sub): void {
        if (this.subs.has(key)) return;
        this.subs.set(key, sub);
        if (this.mode === 'idle') { void this.begin(); return; }
        if (this.mode === 'push' && sub.kind === 'logs') void this.subscribeOne(key, sub);
        // switching: the switch subscribes everything in `subs`; poll: the next tick covers it.
    }

    remove(key: object): void {
        if (!this.subs.delete(key)) return;
        const id = this.remote.get(key);
        this.remote.delete(key);
        if (this.subs.size === 0) { this.shutdown(); return; }
        if (id && this.client) void this.client.unsubscribe(id);
    }

    shutdown(): void {
        this.gen++;
        this.subs.clear();
        this.remote.clear();
        this.stopTimers();
        for (const c of [this.client, this.candidate]) if (c) { c.onClose = undefined; c.close(); }
        this.client = this.candidate = null;
        this.url = null;
        this.buffer = null;
        this.mode = 'idle';
    }

    // --- setup and switching --------------------------------------------------------

    private async begin(): Promise<void> {
        const g = this.gen;
        this.mode = 'switching';
        try {
            await this.host.ready();
            if (g !== this.gen) return;
            this.lastHead = await this.head();
        } catch (e) {
            if (g !== this.gen) return;
            this.host.notify({ type: 'error', error: `feed start: ${(e as Error).message}` });
        }
        if (g !== this.gen) return;
        await this.move('start', false);
    }

    /** Fail over (or start): next endpoint, resubscribe, backfill, release what was held. */
    private move(reason: string, backfill = true): Promise<void> {
        this.switching ??= (async () => {
            const g = this.gen;
            const old = this.client, oldUrl = this.url;
            this.client = null;
            this.url = null;
            this.mode = 'switching';
            this.stopTimers();
            this.remote.clear();
            if (old) { old.onClose = undefined; old.close(); }
            if (oldUrl) {
                this.cool(oldUrl);
                this.stats.failovers++;
                this.host.notify({ type: 'failover', url: oldUrl, reason });
            }
            const gapFrom = Math.max(0, this.lastHead - this.o.overlap);
            this.buffer = [];
            const pushed = await this.connectNext();
            if (g !== this.gen) return;
            if (!pushed) {
                this.buffer = null;
                this.enterPolling(reason, gapFrom);
                return;
            }
            if (backfill && this.lastHead > 0) await this.backfill(gapFrom);
            if (g !== this.gen) return;
            const held = this.buffer ?? [];
            this.buffer = null;
            for (const f of held) f();
        })().finally(() => { this.switching = null; });
        return this.switching;
    }

    /** Try candidates in order until one connects AND accepts every subscription. */
    private async connectNext(): Promise<boolean> {
        const g = this.gen;
        for (const c of this.host.candidates()) {
            if (g !== this.gen) return false;
            const cd = this.cooldown.get(c.url);
            if (cd && Date.now() < cd.until) continue;
            const client = new WsClient(c.url);
            this.candidate = client;
            const err = await client.connect(8000);
            if (err) { this.reject(client, c.url, `connect: ${err}`); continue; }
            const hb = await client.subscribe(['newHeads'], r => this.onHead(client, r));
            if (typeof hb !== 'string') { this.reject(client, c.url, `newHeads: ${describe(hb)}`); continue; }
            const ids = new Map<object, string>();
            let refused: string | null = null;
            for (const [key, sub] of this.subs) {
                if (sub.kind !== 'logs') continue;
                const id = await client.subscribe(['logs', sub.filter], r => this.onLog(client, sub, r));
                if (typeof id !== 'string') { refused = `logs: ${describe(id)}`; break; }
                ids.set(key, id);
            }
            if (refused) { this.reject(client, c.url, refused); continue; }
            if (g !== this.gen) { client.close(); return false; }
            this.candidate = null;
            this.client = client;
            this.remote = ids;
            this.url = c.url;
            this.urlPrivate = c.private;
            this.connectedAt = Date.now();
            this.lastActivity = Date.now();
            this.mode = 'push';
            this.stats.connects++;
            client.onClose = why => { if (client === this.client) void this.move(why); };
            this.watchdog = setInterval(() => {
                if (this.mode === 'push' && Date.now() - this.lastActivity > this.o.stallMs) {
                    void this.move(`silent for ${Math.round((Date.now() - this.lastActivity) / 1000)}s`);
                }
            }, Math.max(250, Math.floor(this.o.stallMs / 4)));
            this.watchdog.unref?.();
            this.host.notify({ type: 'connected', url: c.url, private: c.private });
            return true;
        }
        this.candidate = null;
        return false;
    }

    private reject(client: WsClient, url: string, why: string): void {
        client.close();
        this.cool(url);
        this.host.notify({ type: 'error', url, error: why });
    }

    private cool(url: string): void {
        const prev = this.cooldown.get(url);
        // A socket that served well for a while before failing starts its backoff over.
        const n = prev && !(url === this.url && Date.now() - this.connectedAt > 300_000) ? prev.n + 1 : 1;
        this.cooldown.set(url, { n, until: Date.now() + Math.min(600_000, this.o.retryMs * 2 ** (n - 1)) });
    }

    /** A filter added while pushing: subscribe it on the live socket; a refusal moves the feed. */
    private async subscribeOne(key: object, sub: Sub & { kind: 'logs' }): Promise<void> {
        const client = this.client;
        if (!client) return;
        const id = await client.subscribe(['logs', sub.filter], r => this.onLog(client, sub, r));
        if (!this.subs.has(key)) { if (typeof id === 'string') void client.unsubscribe(id); return; }
        if (typeof id === 'string') { if (client === this.client) this.remote.set(key, id); return; }
        if (client === this.client) void this.move(`refused a logs subscription: ${describe(id)}`);
    }

    // --- incoming ---------------------------------------------------------------------

    private deliver(client: WsClient, f: () => void): void {
        if (client !== this.client && client !== this.candidate) return;
        this.lastActivity = Date.now();
        if (this.buffer) this.buffer.push(f); else f();
    }

    private onHead(client: WsClient, r: unknown): void {
        const n = parseInt(String((r as { number?: string })?.number ?? ''), 16);
        if (!Number.isFinite(n)) { this.deliver(client, () => {}); return; }
        this.deliver(client, () => {
            this.stats.heads++;
            this.advance(n);
        });
    }

    /** New head: emit block events for each block not yet announced (a short gap is filled; a long one jumps). */
    private advance(n: number): void {
        if (n <= this.lastHead) return;
        const from = n - this.lastHead <= 16 && this.lastHead > 0 ? this.lastHead + 1 : n;
        this.lastHead = n;
        if (![...this.subs.values()].some(s => s.kind === 'block')) return;
        for (let b = from; b <= n; b++) this.host.emitBlock(b);
    }

    private onLog(client: WsClient, sub: Sub & { kind: 'logs' }, r: unknown): void {
        this.deliver(client, () => {
            this.stats.pushedLogs++;
            this.emitOnce(sub.filter, r as RawLog);
        });
    }

    private emitOnce(filter: LogFilter, log: RawLog): boolean {
        const key = `${log.blockHash}:${log.logIndex}:${log.removed ? 1 : 0}:${filterKey(filter)}`;
        if (this.seen.has(key)) { this.stats.duplicatesDropped++; return false; }
        this.seen.set(key, true);
        if (this.seen.size > SEEN_MAX) {
            let drop = SEEN_MAX / 4;
            for (const k of this.seen.keys()) { this.seen.delete(k); if (--drop <= 0) break; }
        }
        this.host.emitLog(filter, log);
        return true;
    }

    // --- backfill and polling --------------------------------------------------------------

    private async head(): Promise<number> {
        return parseInt(String(await this.host.request('eth_blockNumber', [])), 16);
    }

    /** Logs in [from, to] for one filter, in block order, through the pool. */
    private async logsIn(filter: LogFilter, from: number, to: number): Promise<RawLog[]> {
        const out: RawLog[] = [];
        let span = this.o.span;
        for (let a = from; a <= to;) {
            const b = Math.min(to, a + span - 1);
            try {
                const part = await this.host.request('eth_getLogs', [{ ...filter, fromBlock: hex(a), toBlock: hex(b) }]) as RawLog[];
                out.push(...part);
                a = b + 1;
            } catch (e) {
                if (span > 50 && /range|too (?:many|large)|limit|exceed/i.test((e as Error).message)) { span = Math.floor(span / 4); continue; }
                throw e;
            }
        }
        return out.sort((x, y) => parseInt(x.blockNumber, 16) - parseInt(y.blockNumber, 16) || parseInt(x.logIndex, 16) - parseInt(y.logIndex, 16));
    }

    /** Re-read [from, head] for every log subscription, emit what was missed, then announce the blocks. */
    private async backfill(from: number): Promise<void> {
        const g = this.gen;
        let to: number;
        try { to = await this.head(); } catch (e) {
            this.host.notify({ type: 'backfill-failed', from, to: from, error: (e as Error).message });
            return;
        }
        if (g !== this.gen || to < from) return;
        let n = 0;
        for (const sub of [...this.subs.values()]) {
            if (sub.kind !== 'logs') continue;
            try {
                for (const log of await this.logsIn(sub.filter, from, to)) {
                    if (g !== this.gen) return;
                    if (this.emitOnce(sub.filter, log)) n++;
                }
            } catch (e) {
                this.host.notify({ type: 'backfill-failed', from, to, error: (e as Error).message });
            }
        }
        if (g !== this.gen) return;
        this.stats.backfilledLogs += n;
        this.host.notify({ type: 'backfill', from, to, logs: n });
        this.advance(to);
    }

    private enterPolling(reason: string, from: number): void {
        this.mode = 'poll';
        this.pollFrom = from;
        this.host.notify({ type: 'polling', reason: this.host.candidates().length ? `no websocket would take the subscriptions (${reason})` : 'no websocket endpoint' });
        const g = this.gen;
        const tick = async () => {
            this.pollTimer = null;
            if (g !== this.gen || this.mode !== 'poll') return;
            if (!this.switching) await this.pollOnce();
            if (g === this.gen && this.mode === 'poll') this.pollTimer = setTimeout(tick, this.host.pollMs());
        };
        this.pollTimer = setTimeout(tick, 0);
        if (this.host.candidates().length) {
            this.retryTimer = setInterval(() => { if (this.mode === 'poll' && !this.switching) void this.backToPush(); }, this.o.retryMs);
            this.retryTimer.unref?.();
        }
    }

    private async pollOnce(): Promise<void> {
        const g = this.gen;
        try {
            const head = await this.head();
            if (g !== this.gen || head < this.pollFrom) return;
            for (const sub of [...this.subs.values()]) {
                if (sub.kind !== 'logs') continue;
                for (const log of await this.logsIn(sub.filter, this.pollFrom, head)) {
                    if (g !== this.gen) return;
                    this.emitOnce(sub.filter, log);
                }
            }
            if (g !== this.gen) return;
            this.pollFrom = head + 1;
            this.advance(head);
        } catch (e) {
            this.host.notify({ type: 'error', error: `poll: ${(e as Error).message}` });
        }
    }

    private backToPush(): Promise<void> {
        this.switching ??= (async () => {
            const g = this.gen;
            const gapFrom = Math.max(0, Math.min(this.pollFrom, this.lastHead + 1) - this.o.overlap);
            this.buffer = [];
            if (!(await this.connectNext())) { this.buffer = null; return; }
            if (g !== this.gen) return;
            if (this.pollTimer) { clearTimeout(this.pollTimer); this.pollTimer = null; }
            if (this.retryTimer) { clearInterval(this.retryTimer); this.retryTimer = null; }
            await this.backfill(gapFrom);
            if (g !== this.gen) return;
            const held = this.buffer ?? [];
            this.buffer = null;
            for (const f of held) f();
        })().finally(() => { this.switching = null; });
        return this.switching;
    }

    private stopTimers(): void {
        if (this.watchdog) { clearInterval(this.watchdog); this.watchdog = null; }
        if (this.pollTimer) { clearTimeout(this.pollTimer); this.pollTimer = null; }
        if (this.retryTimer) { clearInterval(this.retryTimer); this.retryTimer = null; }
    }
}

const describe = (r: unknown): string => {
    const x = r as { error?: { message?: string }; transport?: string };
    return x?.error?.message ?? x?.transport ?? 'refused';
};
const filterKey = (f: LogFilter): string => JSON.stringify([f.address ?? null, f.topics ?? null]);
