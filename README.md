# vetted-rpc

Public RPCs for EVM chains are free and wildly uneven. One caps `eth_getLogs` at 500 blocks; another refuses filters without an address; a third drops the revert reason from every failed `eth_call`, so nothing can be simulated against it; a fourth is fast but 8 blocks behind. None of that is on chainlist.

**vetted-rpc** tests every public endpoint chainlist knows for a chain, rates each one per job, and gives you an ethers provider that routes each request to an endpoint that can actually serve it.

```sh
npx vetted-rpc vet 999                       # rate every public RPC for HyperEVM
npx vetted-rpc best 999 --profile scanner    # print the best one for log scanning
```

```ts
import { VettedProvider } from 'vetted-rpc';

const provider = new VettedProvider(999);                                   // public endpoints only
const provider = new VettedProvider(999, process.env.PAID_RPC);             // + your private node, used sparingly
const provider = new VettedProvider(999, { privateRpc: process.env.PAID_RPC, noTracking: true });   // or as an option
```

Requires Node ≥ 22.6, which runs the TypeScript directly. No build step and no dependencies, except `ethers` v6 for the provider.

## What gets tested

| | |
|---|---|
| **identity** | `eth_chainId` matches (chainlist has stale entries), client version, head |
| **head lag** | blocks behind the best head across all endpoints, read in one simultaneous round |
| **latency** | p50 / p95 over `eth_blockNumber` and a small `eth_call` |
| **eth_getLogs** | the largest block range (bisected, with a filter that matches nothing, so result caps don't interfere); the most addresses in one filter; whether address-less topic filters are allowed; the widest range before result caps bite; OR'd topics; all of these combined; logs near genesis |
| **eth_call** | revert data returned (a hard requirement for simulation); the gas actually granted, found by running `GAS` inside the call; state overrides |
| **history** | how deep state can be read (`eth_getBalance` at increasing depths), archive or not |
| **batch** | the largest JSON-RPC batch accepted, and whether errors come back with `id: null` |
| **methods** | `eth_feeHistory`, `eth_getBlockReceipts`, `eth_getProof`, `debug_traceCall`, `trace_call`, `txpool_status`, … |
| **WebSocket** | the upgrade works; `newHeads` and `logs` subscriptions deliver; a subscription that goes silent is caught (`--ws-watch 300` for sockets that die after minutes) |
| **rate limit** | requests per second ramped 5 → 10 → 20 → 40 until the first 429, two seconds per step, capped by `--max-rps` |

Every probe is a read; no transaction is ever sent. The `eth_call` probes run tiny init-code programs, so no contract needs to exist on the chain. Rate-limit replies during the ordinary probes are waited out and retried, so a 429 in the middle of a range bisection is never mistaken for a small range limit.

The rate ramp is deliberately short. These are other people's free endpoints, so please don't raise `--max-rps` much or run `vet` in a loop.

## Ratings are per job

There's no single "best RPC". Each profile has **gates** (fail one and the endpoint is out) and a 0–100 score:

| profile | gates | scored on |
|---|---|---|
| `scanner` | getLogs range ≥ 100 | range, addresses per filter, topic-only filters, OR topics, old logs, rate |
| `trader` | revert data, head lag ≤ 1 | latency (p50, p95), head freshness, rate |
| `reader` | `eth_call` works | gas cap, batch size, latency, rate, state overrides |
| `subscriber` | WebSocket, `newHeads` delivered, didn't go silent | latency, `logs` subscription, freshness, rate |

All profiles also gate on the right chain, reachability and `--max-lag`. When no public endpoint passes a profile, `vet` says so: *"NONE of 9 pass (most: no revert data) — a private node is worth it for this job."*

Ratings depend on **where you test from**. A public RPC can be 30 ms from Virginia and 250 ms from Frankfurt. The results file records `--from` (default: hostname), so rate from the machine that will use them.

## VettedProvider

A drop-in `JsonRpcProvider`: it overrides only `_send`, so anything that accepts a `JsonRpcProvider` accepts it. Each request is routed on its own:

1. **What does it need?** A 40k-block `eth_getLogs` needs an endpoint rated for that range, and an address-less one needs topic-only support. An `eth_call` needs revert data and enough gas cap; a state override needs override support. A read 500k blocks back needs that much history. `debug_` / `trace_` methods need endpoints that have them. A call pinned to block N skips endpoints whose head hasn't reached N.
2. **Which public endpoints can do it, are healthy, and have rate budget?** It picks among the best few, weighted by rating and live latency, so load spreads instead of piling onto one. If every capable endpoint is out of budget, it waits briefly for a refill rather than spend a private request.
3. **The private endpoint** is used only when no public endpoint can do the job, as the fallback when public attempts fail, or for methods in `privateFor`. That defaults to `eth_sendRawTransaction`, which goes to private *and* the best publics at once, because landing a transaction is worth one extra request.

The provider learns as it runs:

- **Real answers** (a revert, bad params) return immediately.
- **Capability errors** ("range too large", "method not found", "missing trie node") lower that endpoint's known limit, and the request moves to the next candidate.
- **Rate limits** cool the endpoint down for its `Retry-After` and slow its pace.
- **Transport failures** open its circuit with backoff.

```ts
const p = new VettedProvider(999, {
  privateRpc: process.env.PAID_RPC,   // or pass the URL as the second argument and options third
  ratings: 'results',                 // results dir, a results file, or the parsed object (default ./results)
  privateFor: ['eth_sendRawTransaction'],
  privateRps: 25,                     // budget for the paid endpoint (default unlimited)
  budgetWaitMs: 2000,                 // wait this long for a public rate budget before using private (0 = don't wait)
  noTracking: true,                   // skip endpoints chainlist marks as tracking users
  onRoute: e => console.log(e.method, e.private ? 'PRIVATE' : 'public', e.url, e.outcome),
});
p.usage;    // { public, private, privateFallbacks, waitsForBudget }: what the private node cost you
p.stats();  // per endpoint: served, errors, latency, state (ok / cooling / down), learned limits
```

Without a ratings file it still works on chainlist's unrated list: it's gentle with every endpoint (5 rps) and learns limits from errors.

**Subscriptions poll.** `provider.on(filter, …)` polls stateless `eth_getLogs` rather than `eth_newFilter` + `eth_getFilterChanges`. A filter id lives on one backend node, so filter-id polling fails across a pool, and often on a single load-balanced public endpoint too. If you call the filter methods yourself, they're pinned to the endpoint that created the filter. Push subscriptions over the pooled WebSockets are not supported yet.

## Tests

```sh
npm test
```

Runs against mock JSON-RPC nodes (HTTP and WebSocket) with known limits. Every limit configured on a mock must be the limit the probes report, and the provider must route around them, keeping the private endpoint idle unless it's needed. No network required.

## License

MIT
# vetted-provider
