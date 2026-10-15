export type RpcError = {
    code: number;
    message: string;
    data?: unknown;
};
export type Reply = {
    result?: unknown;
    error?: RpcError;
    /** Transport failure: timeout, refused, non-JSON body, socket closed. No JSON-RPC answer at all. */
    transport?: string;
    httpStatus?: number;
    retryAfter?: string;
    ms: number;
};
export type Call = {
    method: string;
    params?: unknown[];
};
export interface Client {
    readonly url: string;
    readonly kind: 'http' | 'ws';
    call(method: string, params?: unknown[], timeoutMs?: number): Promise<Reply>;
    /** Send several calls as ONE JSON-RPC batch. `replies` is null when the batch itself failed. */
    batch(calls: Call[], timeoutMs?: number): Promise<{
        replies: Array<{
            id: unknown;
            result?: unknown;
            error?: RpcError;
        }> | null;
        reply: Reply;
    }>;
    close(): void;
}
export declare const errText: (r: Reply) => string;
export declare const isRateLimited: (r: Reply) => boolean;
export declare const isMissingMethod: (r: Reply) => boolean;
export declare const ok: (r: Reply) => boolean;
export declare class HttpClient implements Client {
    readonly url: string;
    readonly kind: "http";
    constructor(url: string);
    private post;
    call(method: string, params?: unknown[], timeoutMs?: number): Promise<Reply>;
    batch(calls: Call[], timeoutMs?: number): Promise<{
        replies: null;
        reply: Reply;
    } | {
        replies: Array<{
            id: unknown;
            result?: unknown;
            error?: RpcError;
        }>;
        reply: Reply;
    }>;
    close(): void;
}
export declare class WsClient implements Client {
    readonly url: string;
    readonly kind: "ws";
    private ws;
    private pending;
    private batches;
    private closedWhy;
    /** eth_subscription notifications, by subscription id. */
    readonly subs: Map<string, (result: unknown) => void>;
    /** Time of the last frame of any kind from the server. */
    lastRx: number;
    /** Called once when an open socket closes (not when connect() itself fails). */
    onClose?: (why: string) => void;
    constructor(url: string);
    /** Open the socket. Resolves to an error string on failure, null on success. */
    connect(timeoutMs?: number): Promise<string | null>;
    get open(): boolean;
    /**
     * Keep the process alive only while something is waiting on this socket (a
     * request, a batch, a subscription). An idle open socket must not stop a
     * finished script from exiting — the old single-endpoint provider did the
     * same with ethers' socket.
     */
    private hold;
    private onMessage;
    call(method: string, params?: unknown[], timeoutMs?: number): Promise<Reply>;
    batch(calls: Call[], timeoutMs?: number): Promise<{
        replies: Array<{
            id: unknown;
            result?: unknown;
            error?: RpcError;
        }> | null;
        reply: Reply;
    }>;
    /** eth_subscribe; returns the subscription id or the failed reply. */
    subscribe(params: unknown[], onEvent: (r: unknown) => void): Promise<string | Reply>;
    /** eth_unsubscribe. Drops the handler first, so a node that never answers can't keep the process alive. */
    unsubscribe(id: string): Promise<Reply>;
    close(): void;
}
export declare const hex: (n: number | bigint) => string;
export declare const num: (x: unknown) => number;
