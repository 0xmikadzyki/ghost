# Phase 2 — storage and API

Status: specification for the build. Every number here is either measured (docs/RECON.md, docs/OPTIONS.md,
README) or marked *target*.

## Facts the design rests on

- Dataset: 1,418,335,621 `Transfer` logs, 317,032,777 txs, blocks 0–63,385,706, 6,339 gzip JSONL shards
  (64.4 GB) at `/root/rhflow/data/transfers/NNN/NNNNNNNNN.jsonl.gz`. One line per log:
  `{"log_index","transaction_hash","block_number","address","topic0","topic1","topic2","topic3","data","ts"}`
  (`topic3` absent or null for ERC-20, present for ERC-721; `ts` = unix seconds, int).
- Box: 8 cores, 15 GB RAM, 371 GB free, Debian 13, Docker installed. No other services.
- Chain: 0.1 s blocks, ~860k blocks/day, head keeps moving (~1.5 days ahead of the dataset already).
- 21.9 % of transfers touch the zero address (mint/burn, mostly WETH wrap/unwrap); WETH alone is 36.7 % of
  events. The graph must be able to hide both by default.
- Token symbol/decimals are not in the logs. `eth_call` on the public RPC works (used by
  rhflow/research/fingerprint.py); ~1.88 M tokens exist, only a few thousand matter.

## Storage: ClickHouse (Docker, same box)

One store. Justification: per-address aggregation over 1.4 B rows with ad-hoc filters (token, time window)
needs a columnar engine; Postgres/SQLite would need pre-aggregating everything and still miss the window
filters. Labels and token metadata are small tables and live in the same ClickHouse — no second database.

Container: `clickhouse/clickhouse-server` pinned to an exact tag, `docker compose` file at
`deploy/docker-compose.yml`; data dir `/root/rhflow/ch`, shards dir mounted read-only at
`/var/lib/clickhouse/user_files/transfers` so `file()` can read them. Memory: `max_server_memory_usage`
around 10 GB, `max_memory_usage` per query 6 GB, `max_threads` 8. Listens on 127.0.0.1 only.

Schema (`rhflow/sql/schema.sql`, idempotent `CREATE ... IF NOT EXISTS`):

```
transfers        block UInt32, ts DateTime, tx_hash FixedString(32), log_index UInt32,
                 token FixedString(20), from_addr FixedString(20), to_addr FixedString(20),
                 amount UInt256,                      -- ERC-20 value, or ERC-721 token id
                 kind Enum8('erc20'=1,'erc721'=2)
                 MergeTree ORDER BY (from_addr, ts, tx_hash, log_index)
                 + a way to read by to_addr and by tx_hash fast (projection or mirror MV — the builder
                   measures both on a 100-shard sample and picks; state the choice and the sizes)

edges            from_addr, to_addr, token, n UInt64, amount_sum UInt256, first_ts, last_ts
                 AggregatingMergeTree ORDER BY (from_addr, to_addr, token) + reverse access by to_addr
addr_token       addr, token, dir Enum8('in','out'), n UInt64, amount_sum UInt256, first_ts, last_ts
                 ORDER BY (addr, token, dir)
addr_stats       addr, first_ts, last_ts, n_in, n_out, n_tx (uniq state), n_tokens (uniq state)
tokens           token, symbol String, decimals UInt8, name String, kind, n_transfers, fetched_at
                 ReplacingMergeTree ORDER BY token
addresses        addr, is_contract UInt8, label String, type Enum8('WAL','CTR','ERC','EOA','BRG','DEX'),
                 source String, fetched_at   ReplacingMergeTree ORDER BY addr
ingest_state     key String, value String     (last loaded block for the tail worker)
```

Loading: `rhflow load` runs `INSERT INTO transfers SELECT ... FROM file('transfers/**/*.jsonl.gz',
JSONEachRow, '<schema>')` in block-range batches (say 500 shards per statement so a failure is cheap to
redo), with the hex→binary conversions in SQL (`unhex(substring(topic1, 27))`,
`reinterpretAsUInt256(reverse(unhex(substring(data, 3))))`, kind from `topic3`). Aggregates are
materialized views filled by the same inserts (or built once with `INSERT ... SELECT` after the load if
that is measured to be faster — state which). Load must be resumable per batch and must not exceed RAM.

Targets: full load in a few hours; `transfers` on disk ≤ 80 GB including the secondary access path.

## API: FastAPI + uvicorn, ClickHouse over HTTP via httpx

`rhflow serve --host 127.0.0.1 --port 8791`. Read-only, JSON, every response carries
`{"data": ..., "meta": {"dataset_head": <block>, "took_ms": <int>}}`. Addresses in and out are lowercase
`0x` hex; amounts are decimal strings of the raw integer plus `decimals` where known (never floats).
Every list is capped and paginated with an opaque `cursor`. All time filters are unix seconds.

Node type for the UI (`FlowNode.type`): `ERC` if the address appears as a token contract; else from
`addresses.type` if labelled; else `CTR` if `is_contract`; else `EOA`. `WAL` is reserved for labelled
wallets (exchanges etc.), `BRG`/`DEX` from labels only.

```
GET /health
   → {ok, dataset_head, head_ts, clickhouse: "ok"}

GET /address/{addr}
   → {addr, type, label, is_contract, first_ts, last_ts, n_in, n_out, n_tx,
      tokens: [{token, symbol, decimals, n_in, n_out, amount_in, amount_out}] (top 20 by n),
      top_in:  [{addr, type, label, n, amount_sum, token}] (top 10 counterparties),
      top_out: [...]}

GET /address/{addr}/transfers?token=&dir=in|out|both&from_ts=&to_ts=&limit≤200&cursor=
   → {items: [{ts, block, tx_hash, log_index, token, symbol, decimals, from, to, amount, kind}], next_cursor}

GET /graph?address=&depth=1|2&token=&from_ts=&to_ts=&hide_zero=1&hide_tokens=weth,...&per_node≤25
   → {nodes: [{id: addr, type, label, note, n_in, n_out}],
      edges: [{from, to, token, symbol, decimals, n, amount_sum, first_ts, last_ts}],
      truncated: bool}
   Fan-out protection: per node keep the top `per_node` counterparties by n (then by amount_sum), report
   `truncated=true` when anything was cut. depth=2 expands only the nodes kept at depth 1. Hard cap 400
   nodes / 1,500 edges. Zero address and hidden tokens are filtered in SQL, not after.

GET /path?from=&to=&token=&max_hops≤4&per_node≤25
   → {found: bool, hops: [{from, to, token, n, amount_sum, first_ts, last_ts}], explored_nodes: int}
   Bidirectional BFS over `edges` with the same per-node cap; stops at max_hops or 5,000 explored nodes.

GET /tx/{hash}/flow
   → {tx_hash, block, ts, nodes: [...as /graph], edges: [{from, to, token, symbol, decimals, amount,
      log_index, kind}]}   — every transfer of that tx in log order, no aggregation.

GET /token/{addr}
   → {token, symbol, decimals, name, kind, n_transfers, first_ts, last_ts, holders_seen: uniq(to_addr)}

GET /search?q=       — 0x-prefixed 40-hex → address, 64-hex → tx, else label/symbol prefix (≤ 20 hits)
```

Errors: 400 for malformed input (checksum not required), 404 for unknown address/tx (an address with no
transfers is 404), 503 if ClickHouse is unreachable. Query timeouts 10 s. No secrets in responses or logs.

## Tail worker

`rhflow tail`: every 10 s reads `ingest_state.last_block`, asks HyperSync for `Transfer` logs from
`last_block+1` to `height - 1000` (same reorg margin as the backfill), inserts them straight into
`transfers` (which feeds the views), advances `last_block` only after a successful insert. Batches of at
most 20,000 blocks per request loop; same 429 handling as the backfill. Idempotency: the insert is not
deduplicated by ClickHouse, so the state must be updated in the same order as inserts and a crash between
insert and state update may duplicate one batch — `transfers` gets `ReplacingMergeTree` keyed on
(tx_hash, log_index) only if measured cheap; otherwise document the duplicate window.

## Metadata

`rhflow enrich-tokens --top N`: for the N most transferred tokens without a row in `tokens`, call
`symbol()`, `decimals()`, `name()` via batched `eth_call` on `RPC_URL`; store failures too (empty symbol,
`decimals=0`, source='eth_call:err') so they are not retried every run.
`rhflow enrich-addresses`: `eth_getCode` for addresses seen in `edges` with n ≥ threshold → `is_contract`.
`data/labels.json` in the repo: hand-checked labels (WETH, routers, bridge, zero address) with a `source`
field each; `rhflow load-labels` upserts them.

## Out of scope for this phase

Native ETH transfers (top-level or internal), prices/USD, the web front-end.
