import { type Client } from './rpc.ts';
export type Sample = {
    /** The busiest log emitter in recent blocks (e.g. the main stablecoin). */
    address?: string;
    /** Its most common topic0, and a second one for the OR test. */
    topic: string;
    topic2: string;
    /** The block it was seen in. */
    block?: number;
};
export type Ctx = {
    chainId: number;
    sample: Sample;
    /** Upper bound for the rate ramp; 0 skips it. */
    maxRps: number;
    /** Seconds to watch WebSocket subscriptions. */
    wsWatchSec: number;
    log: (s: string) => void;
};
export type LogsReport = {
    /** Largest block range accepted with a filter that matches nothing (a random address): the pure RANGE limit. */
    maxRange: number | null;
    rangeError?: string;
    /** Most addresses accepted in one filter (100-block range). */
    maxAddresses: number | null;
    addressError?: string;
    /** Topic filter with no address at all — publicnode refuses this (-32701). */
    topicOnly: boolean | null;
    topicOnlyError?: string;
    /** Largest range accepted for an address-less Transfer filter: where result-count caps bite. */
    topicOnlyMaxRange: number | null;
    resultCapError?: string;
    /** OR'd topic0s ([[a, b]]). */
    orTopics: boolean | null;
    /** Address list + topic + range at the individual limits, together. */
    combined: boolean | null;
    combinedError?: string;
    /** Logs near genesis — pruned / limited-history endpoints refuse. */
    oldLogs: boolean | null;
    oldLogsError?: string;
};
export type Report = {
    url: string;
    kind: 'http' | 'ws';
    tracking?: string;
    source: string;
    reachable: boolean;
    error?: string;
    chainId?: number;
    chainIdOk?: boolean;
    client?: string;
    head?: number;
    /** Blocks behind the best head seen across all endpoints (filled in by the runner). */
    headLag?: number;
    latency?: {
        p50: number;
        p95: number;
        n: number;
        failures: number;
    };
    logs?: LogsReport;
    call?: {
        /** A reverting eth_call returns the revert data (needed to simulate anything). */
        revertData: boolean | null;
        revertError?: string;
        /** Gas the node actually grants an eth_call (bisected). */
        gasCap: number | null;
        gasCapError?: string;
        stateOverride: boolean | null;
        stateOverrideError?: string;
    };
    history?: {
        /** Deepest block (in blocks behind head) whose state could be read. */
        stateDepth: number | null;
        archive: boolean | null;
        error?: string;
    };
    batch?: {
        max: number;
        error?: string;
        nullIdErrors: boolean;
    };
    methods?: Record<string, boolean | null>;
    methodErrors?: Record<string, string>;
    rate?: {
        cleanRps: number;
        limitedAt: number | null;
        retryAfter?: string;
        error?: string;
        sent: number;
    };
    /** Rate-limit replies met (and waited out) during the ordinary probes, before the ramp. */
    throttledDuringProbes?: number;
    ws?: {
        connected: boolean;
        error?: string;
        heads: number;
        logs: number | null;
        logsError?: string;
        lastHeadAgeSec: number | null;
        died: boolean;
    };
    ms: number;
};
export type Only = Partial<Record<'logs' | 'calls' | 'history' | 'batch' | 'methods' | 'rate' | 'ws', boolean>>;
export declare function vetEndpoint(c: Client, ctx: Ctx, base: Pick<Report, 'tracking' | 'source'>, skip?: Only): Promise<Report>;
/**
 * Find a busy log emitter and its common topics from the last few blocks, through
 * whichever endpoint answers. The getLogs probes use it so OR / combined filters
 * have real data behind them. Falls back to Transfer/Approval with no address.
 */
export declare function sampleLogs(clients: Client[], log: (s: string) => void): Promise<Sample>;
