// -----------------------------------------------------------------------------
// Endpoint discovery: every public RPC chainlist.org knows for a chain id.
//
// chainlist.org/rpcs.json is an array of chains, each with
//   rpc: [{ url, tracking?: 'none' | 'limited' | 'yes' }]
// chainid.network/chains.json (the ethereum-lists registry chainlist builds on)
// is the fallback; its rpc entries are plain strings and carry no tracking.
//
// URLs with a placeholder (${INFURA_API_KEY}, …) are dropped: they need a key.
// -----------------------------------------------------------------------------
const SOURCES = [
    { name: 'chainlist', url: 'https://chainlist.org/rpcs.json' },
    { name: 'chainid.network', url: 'https://chainid.network/chains.json' },
];
export async function discover(chainId, opts = {}) {
    let chain;
    let source = '';
    const errors = [];
    for (const s of SOURCES) {
        try {
            const res = await fetch(s.url, { signal: AbortSignal.timeout(opts.timeoutMs ?? 30_000) });
            if (!res.ok) {
                errors.push(`${s.name}: HTTP ${res.status}`);
                continue;
            }
            const list = await res.json();
            chain = list.find(c => Number(c.chainId) === chainId);
            if (chain) {
                source = s.name;
                break;
            }
            errors.push(`${s.name}: no chain ${chainId}`);
        }
        catch (e) {
            errors.push(`${s.name}: ${e.message}`);
        }
    }
    const endpoints = [];
    const seen = new Set();
    const add = (url, src, tracking) => {
        const u = url.trim().replace(/\/+$/, '');
        if (!/^(https?|wss?):\/\//i.test(u) || /\$\{|API_KEY|<.*>/i.test(u))
            return;
        if (seen.has(u.toLowerCase()))
            return;
        seen.add(u.toLowerCase());
        endpoints.push({ url: u, kind: /^wss?:/i.test(u) ? 'ws' : 'http', tracking, source: src });
    };
    for (const u of opts.extra ?? [])
        add(u, 'user');
    for (const r of chain?.rpc ?? []) {
        if (typeof r === 'string')
            add(r, source);
        else if (r?.url)
            add(r.url, source, r.tracking);
    }
    if (!chain && !(opts.extra?.length))
        throw new Error(`chain ${chainId} not found (${errors.join('; ')})`);
    return { chainId, name: chain?.name ?? `chain ${chainId}`, endpoints };
}
