export type FeedEvent = {
    type: 'connected';
    url: string;
    private: boolean;
} | {
    type: 'failover';
    url: string;
    reason: string;
} | {
    type: 'polling';
    reason: string;
} | {
    type: 'backfill';
    from: number;
    to: number;
    logs: number;
} | {
    type: 'backfill-failed';
    from: number;
    to: number;
    error: string;
} | {
    type: 'error';
    url?: string;
    error: string;
};
export type LogFilter = {
    address?: string | string[];
    topics?: Array<string | string[] | null>;
};
export type RawLog = {
    blockNumber: string;
    blockHash: string;
    logIndex: string;
    transactionHash?: string;
    removed?: boolean;
    [k: string]: unknown;
};
/** What the feed needs from its provider. */
export type FeedHost = {
    ready(): Promise<void>;
    /** Websocket endpoints to subscribe on, best first. */
    candidates(): Array<{
        url: string;
        private: boolean;
    }>;
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
type Sub = {
    kind: 'block';
} | {
    kind: 'logs';
    filter: LogFilter;
};
type Mode = 'idle' | 'switching' | 'push' | 'poll';
export declare class PushFeed {
    private subs;
    private remote;
    private client;
    /** The socket being set up during a switch: its frames are accepted (into the buffer). */
    private candidate;
    private url;
    private urlPrivate;
    private connectedAt;
    private mode;
    /** Bumped on shutdown: any async step from an older generation stands down. */
    private gen;
    private lastHead;
    private lastActivity;
    private pollFrom;
    private cooldown;
    private seen;
    /** Non-null while switching: deliveries wait here until the backfill is out. */
    private buffer;
    private switching;
    private watchdog;
    private pollTimer;
    private retryTimer;
    readonly stats: {
        connects: number;
        failovers: number;
        backfilledLogs: number;
        duplicatesDropped: number;
        pushedLogs: number;
        heads: number;
    };
    private readonly host;
    private readonly o;
    constructor(host: FeedHost, o: FeedOptions);
    /** Where the feed stands, for stats(). */
    state(): {
        connects: number;
        failovers: number;
        backfilledLogs: number;
        duplicatesDropped: number;
        pushedLogs: number;
        heads: number;
        mode: Mode;
        url: string | null;
        private: boolean;
        subscriptions: number;
        lastHead: number;
    };
    add(key: object, sub: Sub): void;
    remove(key: object): void;
    shutdown(): void;
    private begin;
    /** Fail over (or start): next endpoint, resubscribe, backfill, release what was held. */
    private move;
    /** Try candidates in order until one connects AND accepts every subscription. */
    private connectNext;
    private reject;
    private cool;
    /** A filter added while pushing: subscribe it on the live socket; a refusal moves the feed. */
    private subscribeOne;
    private deliver;
    private onHead;
    /** New head: emit block events for each block not yet announced (a short gap is filled; a long one jumps). */
    private advance;
    private onLog;
    private emitOnce;
    private head;
    /** Logs in [from, to] for one filter, in block order, through the pool. */
    private logsIn;
    /** Re-read [from, head] for every log subscription, emit what was missed, then announce the blocks. */
    private backfill;
    private enterPolling;
    private pollOnce;
    private backToPush;
    private stopTimers;
}
export {};
