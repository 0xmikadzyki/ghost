-- rhflow schema. Idempotent; applied by `rhflow store init` (statements split on ';').
-- Transfer tables are PARTITION BY intDiv(block, 1000000): one shard dir = one partition, so an interrupted
-- load batch is retracted with DROP PARTITION (rhflow/store.py). Aggregate tables carry the same range as the
-- `bpart` column (last key column, retracted with DELETE WHERE bpart) but are NOT partitioned: a point lookup
-- on a partitioned aggregate read one granule in every part (~255 on the full chain) — measured 73 ms per
-- key, 1.7 s for a 15-node graph; unpartitioned it is a handful of parts.
-- Keys are raw bytes: FixedString(20) addresses, FixedString(32) tx hashes; amounts UInt256.

CREATE TABLE IF NOT EXISTS transfers (
    block      UInt32 CODEC(Delta, ZSTD(1)),
    ts         DateTime CODEC(Delta, ZSTD(1)),
    tx_hash    FixedString(32),
    log_index  UInt32 CODEC(ZSTD(1)),
    token      FixedString(20) CODEC(ZSTD(1)),
    from_addr  FixedString(20) CODEC(ZSTD(1)),
    to_addr    FixedString(20) CODEC(ZSTD(1)),
    amount     UInt256 CODEC(ZSTD(1)),                       -- ERC-20 value, or ERC-721 token id
    kind       Enum8('erc20' = 1, 'erc721' = 2)
) ENGINE = MergeTree
PARTITION BY intDiv(block, 1000000)
ORDER BY (from_addr, ts, tx_hash, log_index);

-- secondary access paths: full mirror copies fed by MVs (measured smaller and faster than projections;
-- a bloom_filter index on tx_hash was 40x smaller but scans every partition). Query these tables directly.
CREATE TABLE IF NOT EXISTS transfers_by_to AS transfers
ENGINE = MergeTree
PARTITION BY intDiv(block, 1000000)
ORDER BY (to_addr, ts, tx_hash, log_index);

CREATE TABLE IF NOT EXISTS transfers_by_tx AS transfers
ENGINE = MergeTree
PARTITION BY intDiv(block, 1000000)
ORDER BY (tx_hash, log_index);

-- Aggregates. Rows are split by bpart (and by in/out MV), so readers always GROUP BY the key and fold:
-- SimpleAggregateFunction columns with plain sum()/min()/max(); n_tx/n_tokens are uniq states → uniqMerge().
-- from → to per token.
CREATE TABLE IF NOT EXISTS edges (
    bpart      UInt16,
    from_addr  FixedString(20),
    to_addr    FixedString(20),
    token      FixedString(20),
    n          SimpleAggregateFunction(sum, UInt64),
    amount_sum SimpleAggregateFunction(sum, UInt256),
    first_ts   SimpleAggregateFunction(min, DateTime),
    last_ts    SimpleAggregateFunction(max, DateTime)
) ENGINE = AggregatingMergeTree
ORDER BY (from_addr, to_addr, token, bpart);

-- same rows keyed by to_addr (reverse access; a projection is not allowed on AggregatingMergeTree)
CREATE TABLE IF NOT EXISTS edges_rev AS edges
ENGINE = AggregatingMergeTree
ORDER BY (to_addr, from_addr, token, bpart);

CREATE TABLE IF NOT EXISTS addr_token (
    bpart      UInt16,
    addr       FixedString(20),
    token      FixedString(20),
    dir        Enum8('in' = 1, 'out' = 2),
    n          SimpleAggregateFunction(sum, UInt64),
    amount_sum SimpleAggregateFunction(sum, UInt256),
    first_ts   SimpleAggregateFunction(min, DateTime),
    last_ts    SimpleAggregateFunction(max, DateTime)
) ENGINE = AggregatingMergeTree
ORDER BY (addr, token, dir, bpart);

CREATE TABLE IF NOT EXISTS addr_stats (
    bpart     UInt16,
    addr      FixedString(20),
    first_ts  SimpleAggregateFunction(min, DateTime),
    last_ts   SimpleAggregateFunction(max, DateTime),
    n_in      SimpleAggregateFunction(sum, UInt64),
    n_out     SimpleAggregateFunction(sum, UInt64),
    n_tx      AggregateFunction(uniq, FixedString(32)),
    n_tokens  AggregateFunction(uniq, FixedString(20))
) ENGINE = AggregatingMergeTree
ORDER BY (addr, bpart);

CREATE TABLE IF NOT EXISTS token_stats (
    bpart     UInt16,
    token     FixedString(20),
    first_ts  SimpleAggregateFunction(min, DateTime),
    last_ts   SimpleAggregateFunction(max, DateTime),
    n         SimpleAggregateFunction(sum, UInt64),
    holders   AggregateFunction(uniq, FixedString(20))
) ENGINE = AggregatingMergeTree
ORDER BY (token, bpart);

CREATE TABLE IF NOT EXISTS tokens (
    token       FixedString(20),
    symbol      String,
    decimals    UInt8,
    name        String,
    kind        Enum8('erc20' = 1, 'erc721' = 2),
    n_transfers UInt64,
    source      String,
    fetched_at  DateTime
) ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY token;

-- type: one of WAL, CTR, ERC, EOA, BRG, DEX, or '' = not labelled (API falls back to is_contract → CTR / EOA)
CREATE TABLE IF NOT EXISTS addresses (
    addr        FixedString(20),
    is_contract UInt8,
    label       String,
    type        LowCardinality(String),
    source      String,
    fetched_at  DateTime
) ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY addr;

-- top counterparties of hubs (n_in + n_out >= api.queries.HUB_MAX), precomputed by `rhflow store hubs`: a live
-- top-k over a hub aggregates ~13M (counterparty, token) groups (5-10 s). Read only by unfiltered requests.
CREATE TABLE IF NOT EXISTS hub_top (
    addr        FixedString(20),
    dir         Enum8('out' = 1, 'in' = 2),
    other       FixedString(20),
    n_sum       UInt64,
    amt_sum     UInt256,
    token       FixedString(20),
    n           UInt64,
    amount_sum  UInt256,
    first_ts    DateTime,
    last_ts     DateTime,
    computed_at DateTime
) ENGINE = MergeTree
ORDER BY (addr, dir);

-- key → value, latest wins (read with FINAL). Keys: last_block, batch:NNN-NNN
CREATE TABLE IF NOT EXISTS ingest_state (
    key     String,
    value   String,
    updated DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated)
ORDER BY key;

-- materialized views: fed by every INSERT INTO transfers (loader and tail worker alike)

CREATE MATERIALIZED VIEW IF NOT EXISTS transfers_by_to_mv TO transfers_by_to AS SELECT * FROM transfers;
CREATE MATERIALIZED VIEW IF NOT EXISTS transfers_by_tx_mv TO transfers_by_tx AS SELECT * FROM transfers;

CREATE MATERIALIZED VIEW IF NOT EXISTS edges_mv TO edges AS
SELECT intDiv(block, 1000000) AS bpart, from_addr, to_addr, token,
       count() AS n, sum(amount) AS amount_sum, min(ts) AS first_ts, max(ts) AS last_ts
FROM transfers GROUP BY bpart, from_addr, to_addr, token;

CREATE MATERIALIZED VIEW IF NOT EXISTS edges_rev_mv TO edges_rev AS
SELECT intDiv(block, 1000000) AS bpart, from_addr, to_addr, token,
       count() AS n, sum(amount) AS amount_sum, min(ts) AS first_ts, max(ts) AS last_ts
FROM transfers GROUP BY bpart, from_addr, to_addr, token;

CREATE MATERIALIZED VIEW IF NOT EXISTS addr_token_in_mv TO addr_token AS
SELECT intDiv(block, 1000000) AS bpart, to_addr AS addr, token, 'in' AS dir,
       count() AS n, sum(amount) AS amount_sum, min(ts) AS first_ts, max(ts) AS last_ts
FROM transfers GROUP BY bpart, addr, token;

CREATE MATERIALIZED VIEW IF NOT EXISTS addr_token_out_mv TO addr_token AS
SELECT intDiv(block, 1000000) AS bpart, from_addr AS addr, token, 'out' AS dir,
       count() AS n, sum(amount) AS amount_sum, min(ts) AS first_ts, max(ts) AS last_ts
FROM transfers GROUP BY bpart, addr, token;

CREATE MATERIALIZED VIEW IF NOT EXISTS addr_stats_in_mv TO addr_stats AS
SELECT intDiv(block, 1000000) AS bpart, to_addr AS addr,
       min(ts) AS first_ts, max(ts) AS last_ts, count() AS n_in, 0 AS n_out,
       uniqState(tx_hash) AS n_tx, uniqState(token) AS n_tokens
FROM transfers GROUP BY bpart, addr;

CREATE MATERIALIZED VIEW IF NOT EXISTS addr_stats_out_mv TO addr_stats AS
SELECT intDiv(block, 1000000) AS bpart, from_addr AS addr,
       min(ts) AS first_ts, max(ts) AS last_ts, 0 AS n_in, count() AS n_out,
       uniqState(tx_hash) AS n_tx, uniqState(token) AS n_tokens
FROM transfers GROUP BY bpart, addr;

CREATE MATERIALIZED VIEW IF NOT EXISTS token_stats_mv TO token_stats AS
SELECT intDiv(block, 1000000) AS bpart, token, min(ts) AS first_ts, max(ts) AS last_ts, count() AS n,
       uniqState(to_addr) AS holders
FROM transfers GROUP BY bpart, token;
