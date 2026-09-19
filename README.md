<p align="center">
  <a href="docs/assets/hero-static.png"><img src="docs/assets/hero.gif" width="100%" alt="GHOST — the pink block wordmark, the tagline 'fund-flow pipeline · Robinhood Chain · read-only', a pink stroke growing toward the pixel-art laughing-coin mascot, which floats up and down and blinks once per loop"></a>
</p>

<p align="center">
  <img alt="tests: 43 passing" src="https://img.shields.io/badge/tests-43%20passing-00E676?style=flat-square&labelColor=080609">
  <img alt="python 3.13" src="https://img.shields.io/badge/python-3.13-A3A3A3?style=flat-square&labelColor=080609">
  <img alt="chain 4663 (Robinhood Chain)" src="https://img.shields.io/badge/chain-4663-A3A3A3?style=flat-square&labelColor=080609">
  <img alt="transfers 1,432,014,401 — counted in ClickHouse for the latency table below; transfers, addr_stats and token_stats agree on it" src="https://img.shields.io/badge/transfers-1%2C432%2C014%2C401-EB25D5?style=flat-square&labelColor=080609">
</p>

## Official CA

```
0x1b617f3cee8eb9cabe03a32b1f0a1797302dbfde
```

[Token page on Blockscout](https://robinhoodchain.blockscout.com/token/0x1b617f3cee8eb9cabe03a32b1f0a1797302dbfde) · [Trace it on ghostscan.app](https://ghostscan.app/flow.html?root=0x1b617f3cee8eb9cabe03a32b1f0a1797302dbfde) · [Shown on the site](https://ghostscan.app/#access)

This is the only address. Any other token, site or link that claims to be this project is not ours — verify against this file and the header of ghostscan.app.

**Where did the money go?** — for every address on Robinhood Chain.

`rhflow` is the data pipeline behind a fund-flow explorer for Robinhood Chain (Arbitrum Orbit L2,
chain id 4663): the interactive graph of token movements between addresses, the way Etherscan Flow shows it
for Ethereum. This repository is the read-only part that can be reproduced by anyone with a free HyperSync
token: the full-chain backfill of ERC-20 `Transfer` events, the exact counts over what it produced, and the
measurements that decided how it is built. The web explorer itself is not part of this repository.

[Quickstart](#quickstart) · [Watch demo](#watch-demo) · [API](#api) · [What the backfill produced](#what-the-backfill-produced) · [How it works](#how-it-works) · [Data & limits](#data--limits)

## Watch demo

<a href="docs/assets/flow-poster.png"><img src="docs/assets/flow.gif" width="100%" alt="7-second loop of the GHOST explorer at 30 fps: the flow map with wallet and contract cards, packets travelling along the wires while 'Continuous trace' replays the route, token candles, block trace and tx decode panels below, the trace stream scrolling on the right"></a>

This is the explorer front-end (a separate repo) replaying its built-in **synthetic demo scenario**, recorded from a
browser: the cards, amounts and addresses on it are illustrative, not chain data. What this repo provides is the API
behind such a screen; the facts it returns for a real address are in the next section. The GIF is 11.8 MB because the
trace stream changes every frame; [`scripts/record_flow.js`](scripts/record_flow.js) captures it and
[`scripts/readme_media.py`](scripts/readme_media.py) `--flow-frames DIR` re-encodes it (ffmpeg only).

## From an address to a receipt

One real address, as the API reports it on the full chain (dataset head 65,859,890):

| Step | Observed fact | Where to check |
|---|---|---|
| Address | `0x1e832ba8e59f1182fdd36218f6889887eec641fe`, EOA, no label | [address on Blockscout](https://robinhoodchain.blockscout.com/address/0x1e832ba8e59f1182fdd36218f6889887eec641fe) · `GET /address/0x1e832ba8e59f1182fdd36218f6889887eec641fe` |
| Activity | 1,383 transfers in, 1,249 out, 1,411 distinct transactions, 20 tokens; first 2026-07-09 18:54 UTC, last 2026-09-16 09:59 UTC | `addr_stats` folded over 64 block ranges; `n_tx` is a `uniqMerge`, exact |
| Latest transfer | 1.000000 of token `0x9a79…1697` (18-decimal assumption; the token is outside the enriched top-5,000, so no symbol) from the token contract itself, block 64,419,574, 2026-09-16 09:59 UTC | [tx 0x8a61…b87f](https://robinhoodchain.blockscout.com/tx/0x8a6115a666d9d24c7b99c880dfeb4da56c6b6207c13bbb11b0dab38f7bb6b87f) · `GET /address/0x1e832ba8e59f1182fdd36218f6889887eec641fe/transfers?limit=1` |
| Graph | 61 nodes and 240 pairs at depth 2 in 1,042 ms; 16 of the counterparties are hubs (≥ 5M transfers), shown from `hub_top`, not expanded | `GET /graph?address=0x1e832ba8e59f1182fdd36218f6889887eec641fe&depth=2` → `truncated: true` |

Every row is one API call; the numbers are the response fields, not estimates.

## API

Eight read-only `GET` routes, JSON, every 200 wrapped as `{"data": …, "meta": {"dataset_head", "took_ms"}}`.
Addresses are lowercase `0x` hex, amounts are decimal strings of the raw integer plus `decimals` (never
floats), times are unix seconds, every list is capped and paginated with an opaque `cursor`.

| The question | Route | What comes back |
|---|---|---|
| who is this address, how busy is it? | `/address/{addr}` | `type` (`EOA`/`CTR`/`ERC`/`WAL`/`BRG`/`DEX`), `label`, first/last time, `n_in`/`n_out`/`n_tx`, top 20 tokens, top 10 counterparties each way |
| show me its transfers | `/address/{addr}/transfers?token=&dir=&from_ts=&to_ts=&limit≤200&cursor=` | rows in block/log order with `tx_hash`, `token`, `symbol`, `amount`, `kind`; `next_cursor` |
| where did the money go from here? | `/graph?address=&depth=1\|2&token=&from_ts=&to_ts=&hide_tokens=&per_node≤25` | `nodes` + per-token `edges`, `truncated` when the per-node top-k or the 400 / 1,500 cap cut something |
| is A connected to B? | `/path?from=&to=&token=&max_hops≤4` | `hops` (aggregated edges) or `found: false` with `explored_nodes` |
| what happened in this transaction? | `/tx/{hash}/flow` | every transfer of the tx in log order, no aggregation |
| what is this token? | `/token/{addr}` | `symbol`, `decimals`, `name`, `kind`, `n_transfers`, `holders_seen` |
| find by label, symbol or hex | `/search?q=` | ≤ 20 hits: 40-hex → address, 64-hex → tx, else label / symbol prefix |
| is the data fresh? | `/health` | `dataset_head` (block), `clickhouse: "ok"` |

400 for malformed input, 404 for an address or tx that no `Transfer` touches, 503 when ClickHouse is
unreachable or a statement passes the 10 s budget. CORS is open for `GET`. A running `rhflow serve` exposes
the interactive OpenAPI UI at `/docs` and the schema at `/openapi.json`; the field-level contract is in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), and [`docs/EXAMPLES.md`](docs/EXAMPLES.md) walks through nine
real calls (a bridge, a hub, an airdrop of 2,000 transfers in one tx, three `/path` outcomes, the 18
tokens that all call themselves `USDG`, the 400/404 cases) with their actual responses.

## Quickstart

Requirements: Git, [uv](https://docs.astral.sh/uv/) (it installs Python 3.13 itself), a free
[Envio HyperSync token](https://envio.dev/app/api-tokens).

```sh
git clone https://github.com/0xYato/rhflow && cd rhflow
uv sync
cp .env.example .env && chmod 600 .env          # put ENVIO_TOKEN there; it is never printed
uv run rhflow backfill --out data/transfers     # every ERC-20 Transfer from block 0 to head-1000
uv run rhflow stats --out data/transfers        # exact shards / logs / transactions over what is on disk
```

Phase 2 (ClickHouse + API; see [`deploy/README.md`](deploy/README.md) for the container and units):

```sh
uv run rhflow store init                        # database + rhflow/sql/schema.sql (idempotent)
uv run rhflow store load --shards data/transfers   # resumable INSERT ... SELECT FROM file(), exact per batch
uv run rhflow store hubs                        # top-50 counterparties of the 42 hub addresses (rerun daily)
uv run rhflow enrich tokens && uv run rhflow enrich addresses   # symbol/decimals/kind, eth_getCode, labels
uv run rhflow tail                              # follows the head 1,000 blocks behind, 20k blocks per step
uv run rhflow serve                             # FastAPI on 127.0.0.1:8791 (OpenAPI at /docs)
```

The backfill is restartable: kill it at any point and run the same command again — finished shards are
skipped, the unfinished one is redone. Try a slice first: `--from-block 60000000 --to-block 60100000`
(10 shards, ~2 minutes).

No HyperSync token yet? Those same 10 shards are attached to the
[v0.1.0 release](https://github.com/0xYato/rhflow/releases/tag/v0.1.0) as
`rhflow-slice-60000000-60100000.tar` (127,774,720 bytes, sha256 `fab6aef6…7596`; blocks 60,000,000 – 60,099,999,
2,810,248 `Transfer` events in 573,892 transactions, as `rhflow stats` counts them):

```sh
curl -L -o slice.tar https://github.com/0xYato/rhflow/releases/download/v0.1.0/rhflow-slice-60000000-60100000.tar
mkdir -p data/transfers && tar xf slice.tar -C data/transfers && uv run rhflow stats --out data/transfers
# shards=10 logs=2,810,248 txs=573,892 logs/tx=4.90 size=0.1 GB
```

`rhflow store load --shards data/transfers` takes the slice the same way it takes the full backfill.

## What the backfill produced

The dataset on disk, counted by `rhflow stats` (every shard is read, transactions are distinct hashes;
nothing is extrapolated):

| | |
|---|---|
| Block range | 0 → 63,385,706 (head minus 1,000 at start) |
| Shards | 6,339 × 10,000 blocks, 0 left as `.tmp` |
| `Transfer` events | **1,418,335,621** |
| Transactions with at least one `Transfer` | **317,032,777** (4.47 events per tx) |
| On disk (gzip level 4) | 64.4 GB (64,419,561,416 bytes) |
| Written | 2026-09-15 03:42 → 2026-09-16 01:33 UTC |

The run that wrote 5,950 of those shards (the first 389 came from an earlier attempt with a per-worker
backoff, which is what led to the shared gate), from its log, on a 15 GB / 8-core box with `--conc 8`:

| | |
|---|---|
| Events | 1,411,781,224 |
| Wall time | 1,244.4 min (20.7 h) |
| Sustained rate | 18,908 events/s |
| Rate-limit responses (HTTP 429) handled | 51,029 |
| Transport retries | 37 |
| Failed shards | 0 |

Chain facts that shaped the pipeline (measured on 2026-09-14, method and raw tables in
[`docs/RECON.md`](docs/RECON.md)): block time 0.100 s since mainnet launch on 2026-06-30 (8.4 s before it),
~860,000 blocks per day.

## How it works

```
Envio HyperSync ──▶ rhflow backfill          POST /query, topic0 = Transfer, join_mode = Default
     │                                        (block timestamps ride along, no second request)
     │  10,000-block shards, 8 in flight, one shared brake on HTTP 429
     ▼
data/transfers/NNN/NNNNNNNNN.jsonl.gz         one line per event: block, tx hash, log index,
     │                                        token, topics, data, block timestamp
     ▼
rhflow stats                                   exact counts: a tx never spans two shards,
                                               so per-shard distinct hashes add up to the total
```

**Why HyperSync and not the RPC.** The public RPC serves ~50-70 requests/s per IP and caps `eth_getLogs`
ranges; a full-chain pull through it is weeks. HyperSync returns ~20-25 MB pages of already-decoded logs
with block timestamps joined in ([`docs/OPTIONS.md`](docs/OPTIONS.md), section 7).

**Why one brake for all workers.** HyperSync's rate limit is per account, not per connection. A per-worker
backoff made things worse: while one worker slept, the other seven kept burning the quota and one shard
timed out after 900 s. With a shared gate, `--conc 8` runs at ~19K events/s with 429s as the pacing signal.

**Why streaming.** One response is 20-25 MB; a whole 10,000-block shard can be gigabytes. Pages go straight
into the gzip stream, so memory per worker is one page.

**What it does not do.** It does not attribute addresses to people, does not decode token amounts into
prices, does not send anything to the chain. Native ETH value transfers and internal calls are not in this
dataset (HyperSync exposes top-level ETH value only; see limits below).

## Data & limits

- Universe: every log with `topic0 = Transfer(address,address,uint256)` on chain 4663. That covers ERC-20 and
  ERC-721 (same signature; NFTs carry the id in `topic3` instead of `data`). ERC-1155, native ETH and internal
  transfers are outside.
- Reorg margin: the head is taken minus 1,000 blocks (~100 s); `rhflow tail` keeps the same margin.
- Timestamps are exact block timestamps from HyperSync's block join, never interpolated.
- Shard files are complete or absent: a shard is written to `.tmp` and renamed on success.
- Rate limit: per HyperSync account. `--conc 8` stays under it; higher values only produce more 429s.
- Measured source behaviour (RPC batch limits, Etherscan V2 on 4663, HyperSync tables, Blockscout API):
  [`docs/OPTIONS.md`](docs/OPTIONS.md).

## Development

```sh
uv sync && uv run pytest -q      # 43 tests, no network: fake HyperSync, fake ClickHouse, fake RPC
uv sync --extra media && python scripts/readme_media.py   # README hero GIF from docs/assets/brand; --flow-frames DIR re-encodes the demo (scripts/record_flow.js)
```

The Phase-1 measurement scripts live in `rhflow/research/` (`uv run python -m rhflow.research.recon`, needs
`uv sync --extra research` for the two that hash selectors); they write to `data/sample/` and produced
[`docs/RECON.md`](docs/RECON.md).

### Measured on the full chain (1,432,014,401 transfers, 8 cores, ClickHouse 26.3)

| endpoint | typical address | busiest hub (270M transfers, 1.73M counterparties) |
|---|---|---|
| `/address/{a}` | 70 ms | 320 ms |
| `/address/{a}/transfers` | 45–205 ms | 140–720 ms |
| `/graph depth=1` / `depth=2` | 52 ms / 826 ms | 590 ms / 680 ms |
| `/graph` with `from_ts`/`to_ts` | 80 ms | 1.4 s (one day) |
| `/path` (bidirectional BFS) | 180–380 ms | — |
| `/tx/{h}/flow`, `/token/{a}`, `/search` | 36 / 99 / 7–20 ms | — |

How the numbers were reached, all exact (no sampling): literal `IN (unhex(..))` lists (an `arrayMap` over a
parameter skipped the primary index), one mirror table per access path, unpartitioned aggregates with the
block range as the last key column, `hub_top` precomputed for addresses above 5M transfers (queries with a
token, window or hidden-token filter still aggregate live: 5–10 s per hub), per-pair `LIMIT` inside the edge
query, `token_stats` for token pages.

Known limits: hubs are shown but never expanded past the root (their top-10 by count would swallow any
graph); `/path` explores at most 5,000 nodes over the per-node top-k, so it proves a path, not its absence;
filtered hub graphs run live and can hit the 10 s statement cap (HTTP 503); `hub_top` is as fresh as its
last `rhflow store hubs` run.

Roadmap (planned, not shipped): (1) a public URL for the API behind TLS: today it listens on the
loopback only and needs a reverse proxy in front; (2) the explorer front-end, in its own repository, wired to
these routes; (3) labels and `eth_getCode` for the long tail of addresses (enrichment currently covers the
busiest ones); (4) native ETH value transfers and ERC-1155, which this dataset does not carry.

Contributing: open an issue with the block range or transaction hashes you looked at. License: not chosen
yet — until a LICENSE file lands, the code is source-available for reading and running, not for
redistribution.
