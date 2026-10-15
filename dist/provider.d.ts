import { JsonRpcProvider, type JsonRpcPayload, type JsonRpcResult, type Subscriber, type Subscription } from 'ethers';
import { type FeedEvent } from './feed.ts';
import type { Report } from './probes.ts';
import type { Profile, Rating } from './score.ts';
export type VettedOptions = {
    /** Your own (usually paid) endpoint, http(s) or ws(s). Used only when public ones cannot do the job. */
    privateRpc?: string | null;
    /** Your own websocket endpoint. Joins privateRpc as a private transport, preferred while it is up (no per-request HTTP/TLS). */
    privateWs?: string | null;
    /** Ratings: a results file, a directory holding <chainId>.json, or the parsed object. Default ./results. */
    ratings?: string | {
        endpoints: Array<Report & {
            ratings: Record<Profile, Rating>;
        }>;
    };
    /**
     * Where transactions are sent: an FCFS chain's sequencer, a private relay on a
     * mempool chain, a builder. Several are sent to at once (first success wins).
     * Sends go ONLY here — never also to public endpoints, never retried on them.
     * Default: privateRpc if given, else the best-rated `trader` public endpoint.
     */
    sendRpc?: string | string[];
    /** Read methods always sent to the private endpoint too, in parallel with the best publics. Default none. */
    privateFor?: string[];
    /** Requests per second allowed on the private endpoint (default unlimited). */
    privateRps?: number;
    /** Use websocket endpoints as request transports too (default true). */
    useWs?: boolean;
    /** Leave out endpoints chainlist marks as tracking users ('yes'). Default false. */
    noTracking?: boolean;
    /** Per-request timeout, ms (default 15000). */
    timeoutMs?: number;
    /** Public endpoints tried before falling back to private (default 3). */
    maxPublicAttempts?: number;
    /**
     * How long a request may wait for a capable public endpoint's rate budget to
     * refill before the private endpoint is used instead (default 2000 ms).
     * 0 = never wait: out of public budget goes straight to private.
     */
    budgetWaitMs?: number;
    /** Called for routing decisions — for logs while tuning. */
    onRoute?: (e: RouteEvent) => void;
    /**
     * provider.on('block' | filter): 'push' (default) subscribes over a websocket
     * with failover and backfill; 'poll' always polls eth_getLogs over the pool.
     */
    subscriptions?: 'push' | 'poll';
    /** Subscribe on your private websocket first, publics as fallback (default: publics first). */
    subscribePrivate?: boolean;
    /** A subscription socket that sends nothing (not even a head) for this long is treated as dead. Default 30000 ms. */
    stallMs?: number;
    /** Cooldown base for a failed subscription endpoint, and how often polling retries push. Default 30000 ms. */
    feedRetryMs?: number;
    /** Subscription feed events: connected, failover, backfill, polling. */
    onFeed?: (e: FeedEvent) => void;
};
export type RouteEvent = {
    method: string;
    url: string;
    private: boolean;
    outcome: 'ok' | 'answer-error' | 'capability' | 'rate-limited' | 'transport';
    ms: number;
    detail?: string;
};
export declare class VettedProvider extends JsonRpcProvider {
    readonly chainId: number;
    readonly privateRpc: string | null;
    readonly privateWs: string | null;
    private readonly opts;
    private members;
    /** Explicit transaction targets (sendRpc). Not part of the read pool. */
    private senders;
    /** Resolves once the pool is built. (Not `ready`: that is ethers' own getter.) */
    private booted;
    private bestHead;
    /** Filter id -> the member that created it: eth_getFilterChanges / eth_uninstallFilter must go back there. */
    private filters;
    private headsAt;
    private headsBusy;
    /** Requests answered by the private endpoint vs public ones. */
    readonly usage: {
        public: number;
        private: number;
        privateFallbacks: number;
        waitsForBudget: number;
    };
    private readonly feed;
    private feedListeners;
    /**
     * @param chainId  the chain to serve
     * @param privateOrOptions  your private RPC URL, or the options object (which may carry `privateRpc`)
     * @param options  options, when the second argument is the URL
     */
    constructor(chainId: number, privateOrOptions?: string | null | VettedOptions, options?: VettedOptions);
    private init;
    /**
     * Read eth_blockNumber from the best few public endpoints at once, so routing
     * knows each one's head (a call pinned to block N must not go to an endpoint
     * still at N-3) and the chain head (how deep a historical read is). Repeated
     * in the background every 30 s while requests flow. Never touches the private
     * endpoint unless it is the only one.
     */
    private refreshHeads;
    private loadRatings;
    private add;
    private needOf;
    /** Can member `x` do `need`, from its ratings and what it has taught us? undefined = unknown (allowed, ranked lower). */
    private can;
    private available;
    private refill;
    private weight;
    /** Next public member for `need`, excluding `tried`; waits for rate budget (up to budgetWaitMs) rather than going private. */
    private pickPublic;
    /** The private transport to use now: the websocket while it is up, else HTTP. */
    private privateMember;
    /**
     * The best-rated PUBLIC endpoint for a job, e.g. a websocket to subscribe on:
     * bestUrl('subscriber', 'ws'). Null when no rated public endpoint passes —
     * the caller then decides whether its private endpoint is worth it.
     */
    bestUrl(profile: Profile, kind?: 'http' | 'ws'): Promise<string | null>;
    /**
     * Websockets to subscribe on, best first: public ones rated `subscriber`
     * (unrated ones too when there are no ratings), then yours — or yours first
     * with `subscribePrivate`. A rated websocket that failed `subscriber` is left
     * out: it was seen not to deliver.
     */
    private feedCandidates;
    /** Listen to subscription feed events (connected, failover, backfill, polling). Returns the remover. */
    onFeed(listener: (e: FeedEvent) => void): () => void;
    _getSubscriber(sub: Subscription): Subscriber;
    private exchange;
    /** Record the outcome on the member; returns what the router should do next. */
    private judge;
    private toResult;
    private route;
    /** Filter calls go back to the node holding the filter; if it is gone, the filter is gone with it. */
    private sticky;
    /**
     * A transaction. Explicit sendRpc targets (or the private endpoint) get it in
     * parallel, and nothing else does — a failure there is returned, never retried
     * on a public endpoint. With neither, the best `trader` publics are tried in
     * turn until one accepts it.
     */
    private submit;
    /** First success; "already known" counts as success and resolves to the transaction hash. */
    private firstAccepted;
    /**
     * Same request to the private endpoint and the best publics at once (methods in
     * privateFor). The first success wins.
     */
    private fanOut;
    _send(payload: JsonRpcPayload | JsonRpcPayload[]): Promise<JsonRpcResult[]>;
    /** Per-endpoint view: served, errors, latency, health, learned limits. */
    stats(): {
        usage: {
            public: number;
            private: number;
            privateFallbacks: number;
            waitsForBudget: number;
        };
        bestHead: number | null;
        feed: {
            connects: number;
            failovers: number;
            backfilledLogs: number;
            duplicatesDropped: number;
            pushedLogs: number;
            heads: number;
            mode: "push" | "idle" | "switching" | "poll";
            url: string | null;
            private: boolean;
            subscriptions: number;
            lastHead: number;
        };
        sendTo: string[];
        endpoints: {
            url: string;
            kind: "http" | "ws";
            private: boolean;
            served: number;
            errors: number;
            ewmaMs: number;
            head: number | null;
            state: string;
            rps: number | null;
            learned: {
                missing: string[];
                maxRange?: number;
                maxAddresses?: number;
                topicOnly?: boolean;
                gasCap?: number;
                stateDepth?: number;
                revertData?: boolean;
            };
        }[];
    };
    destroy(): void;
}
