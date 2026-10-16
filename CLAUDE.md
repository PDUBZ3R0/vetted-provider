# vetted-provider

An open-source TypeScript library: npm package `vetted-provider`, repo `PDUBZ3R0/vetted-provider`, local folder `~/Development/provider`. It has two parts:

1. **`vetrpc` CLI** (`src/cli.ts`): `vet <chainId>` tests every public RPC that chainlist knows for a chain and rates it per job. `best <chainId> --profile <p>` prints the winner. The output is a ratings file, `<chainId>.json`.
2. **`VettedProvider`** (`src/provider.ts`): a drop-in ethers v6 `JsonRpcProvider`. It routes every request to a public endpoint that can actually serve it, and falls back to a private endpoint only when none can.

Used by `~/Development/arbitrade` and `~/Development/sniper`. Both keep ratings in `conf/provider/<chainId>.json`.

## Layout

- `src/chainlist.ts`: endpoint discovery from chainlist.org, falling back to chainid.network.
- `src/probes.ts`: `vetEndpoint` covers identity, head lag, latency, getLogs limits (range, addresses, topic-only, OR topics, old logs), `eth_call` (revert data, gas cap, state overrides), history depth, batch, methods, WebSocket and rate limit. Also `sampleLogs`.
- `src/score.ts`: `rateAll`, profiles `scanner` / `trader` / `reader` / `subscriber`, each with gates plus a 0–100 score.
- `src/provider.ts`: `VettedProvider` handles routing (`can()`, `weight()`, `pickPublic()`), classifies replies (`judge()`), learns limits at runtime (`learned.*`), keeps stats, and sends transactions through `sendRpc`.
- `src/feed.ts`: `PushFeed` delivers `provider.on('block' | filter)` over websockets with heartbeat-based silence detection, failover with backoff, getLogs gap backfill, and a polling fallback.
- `src/rpc.ts`: `HttpClient` and `WsClient` (the `ws` package, with ref/unref so an idle socket never keeps a script alive), plus `isRateLimited` and `isMissingMethod`.
- `test/mock-node.ts`: a fake JSON-RPC node over HTTP and WebSocket with configurable limits. It can mimic Sonic-style lagging or latest-state-only nodes (`sonicState`) and publicnode-style paywalls (`gatedDepth`).
- `test/*.test.ts`: `mock.test.ts` (probes against the mock), `provider.test.ts` (routing and learning), `feed.test.ts` (subscriptions).

## Commands

```
yarn test        # all three suites; must stay green
yarn build       # tsc -> dist/ (the published package ships dist/)
yarn vet <id>    # = vetrpc vet, from source
npm publish      # Paul publishes; bump "version" in package.json first
```

The source runs under `node --experimental-strip-types` (Node ≥ 22.6), so **no constructor parameter properties, enums or namespaces**. The `dist/` build is for consumers. After changing `src/`, run `yarn build` before publishing. Consumers only pick up the change after the publish and `yarn upgrade vetted-provider@<ver>`.

## How routing decides

- `judge()` classifies each reply. It is the core:
  - `transport`: the endpoint is down, so it gets backoff.
  - `rate-limited`: it cools down, and its rps is lowered.
  - `capability`: this endpoint can't serve this request, so try another and learn from it.
  - `answer-error`: a real answer, such as a revert, is returned to the caller as-is.
- Capability errors are matched by regex. Each sets a learned limit, which only ever goes down:
  - `CAPABILITY_RE` covers ranges, result caps, missing address filters, gas caps and pruned state. These set `maxRange`, `maxAddresses`, `topicOnly`, `gasCap` or `stateDepth`.
  - `LAGGING_RE` catches a node behind the requested block. The node's head is recorded so pinned reads avoid it.
  - `STATE_RE` catches "Unknown state. First available state is N" (Sonic nodes that keep only the latest state). This sets `stateDepth`.
  - `GATED_RE` catches paid-only history, such as publicnode's "Archive requests require a personal token". This sets `learned.logsDepth` for getLogs, `stateDepth` for pinned reads, or `learned.gated` (a set of methods) for by-hash lookups. Anything containing "revert" is never treated as a paywall.
- **Never let a capability failure reach the caller as an answer.** That bug crashed the sniper's Polygon survey (0.1.3 fix).
- The private endpoint is used only when no public one can serve the request, or after `maxPublicAttempts`. Sends (`eth_sendRawTransaction` and friends) go only to `sendRpc` and are never load-balanced.

## Testing new behaviour

Add the behaviour to `MockNode` as an option and add a section to `provider.test.ts`. Rate the mocks with `vetEndpoint` + `rateAll`, the way `vet` does. Routing is weighted-random among the top 3 endpoints, so to make a test deterministic, give the nodes identical ratings (copy one report and change the `url`).

## Working with Paul

- Paul commits, builds and publishes himself. **Don't commit, push or publish.** Remind him when a change needs a version bump and a publish to reach arbitrade and the sniper.
- The README is user-facing documentation for an open-source package. Keep it accurate when behaviour changes.
- Ratings files are committed and shared, so a private URL or key must never end up in one. The CLI masks `--rpc` endpoints and doesn't write them.
