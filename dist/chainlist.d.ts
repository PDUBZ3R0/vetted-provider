export type Endpoint = {
    url: string;
    kind: 'http' | 'ws';
    /** chainlist's privacy label: does the provider log IPs / requests? */
    tracking?: string;
    /** Where it came from: chainlist, chainid.network, or the user (--rpc). */
    source: string;
};
export type ChainInfo = {
    chainId: number;
    name: string;
    endpoints: Endpoint[];
};
export declare function discover(chainId: number, opts?: {
    extra?: string[];
    timeoutMs?: number;
}): Promise<ChainInfo>;
