"""ClickHouse storage: schema, resumable shard load, stats.

Shards are read straight from the read-only mount with `INSERT ... SELECT FROM file()`; every
conversion (hex → bytes, hex → UInt256, ts) is SQL. A batch is a run of whole shard dirs
(1M blocks each = one partition on every table), so an interrupted batch is retracted with
DROP PARTITION before it is redone and the materialized aggregates stay exact. Progress lives in
`ingest_state`: `batch:NNN-NNN = done`, `last_block` for the tail worker.
"""
import argparse, os, sys, time
from pathlib import Path

SHARD = 10_000
PART_BLOCKS = 1_000_000                       # one shard dir NNN = blocks [NNN*1M, (NNN+1)*1M)
SCHEMA = Path(__file__).with_name("sql") / "schema.sql"
PARTITIONED = ["transfers", "transfers_by_to", "transfers_by_tx"]
AGGREGATES = ["edges", "edges_rev", "addr_token", "addr_stats", "token_stats"]        # unpartitioned; retract by bpart

# ts arrives as a hex string ("0x6aa38dda"); topic3 is absent for ERC-20, the NFT id for ERC-721 (data = "0x").
INSERT = """INSERT INTO transfers
SELECT block_number AS block,
       toDateTime(reinterpretAsUInt64(reverse(unhex(substring(ts, 3))))) AS ts,
       unhex(substring(transaction_hash, 3)) AS tx_hash,
       log_index,
       unhex(substring(address, 3)) AS token,
       unhex(substring(topic1, 27)) AS from_addr,
       unhex(substring(topic2, 27)) AS to_addr,
       reinterpretAsUInt256(reverse(unhex(substring(if(topic3 != '', topic3, data), 3)))) AS amount,
       if(topic3 != '', 'erc721', 'erc20') AS kind
FROM file('transfers/{{{dirs}}}/*.jsonl.gz', JSONEachRow,
     'log_index UInt32, transaction_hash String, block_number UInt32, address String,
      topic1 String, topic2 String, topic3 String, data String, ts String')
SETTINGS input_format_skip_unknown_fields = 1, input_format_null_as_default = 1"""


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def init(ch):
    """Apply schema.sql statement by statement (all IF NOT EXISTS)."""
    sql = "\n".join(l for l in SCHEMA.read_text().splitlines() if not l.lstrip().startswith("--"))
    for st in sql.split(";"):
        if st.strip():
            ch.execute(st)


def plan_batches(dirs, batch_shards=500, from_block=0):
    """Group shard dirs ('060', ...) into aligned batches of batch_shards/100 dirs → [[60, 61, ...], ...]."""
    if batch_shards * SHARD % PART_BLOCKS:
        raise ValueError("batch_shards must be a multiple of 100 (whole shard dirs)")
    per = batch_shards * SHARD // PART_BLOCKS
    out = {}
    for i in sorted(int(d) for d in dirs if d.isdigit()):
        if (i + 1) * PART_BLOCKS > from_block:
            out.setdefault(i // per, []).append(i)
    return [v for _, v in sorted(out.items())]


def batch_key(ids):
    return f"batch:{ids[0]:03d}-{ids[-1]:03d}"


def done_batches(ch):
    return {r["key"] for r in ch.query("SELECT key FROM ingest_state FINAL WHERE value = 'done'")}


def load(ch, shards_dir, batch_shards=500, from_block=0):
    """Load every unfinished batch. Rerun after a crash: finished batches are skipped, the broken one redone."""
    batches = plan_batches(os.listdir(shards_dir), batch_shards, from_block)
    done = done_batches(ch)
    todo = [b for b in batches if batch_key(b) not in done]
    log(f"[init] batches={len(batches)} done={len(batches) - len(todo)} todo={len(todo)}")
    t_all, rows_all = time.time(), 0
    for n, ids in enumerate(todo, 1):
        k, t0 = batch_key(ids), time.time()
        for t in PARTITIONED:                       # retract whatever a crashed attempt left behind
            for i in ids:
                ch.execute(f"ALTER TABLE {t} DROP PARTITION {i}")
        for t in AGGREGATES:
            ch.execute(f"ALTER TABLE {t} DELETE WHERE bpart IN ({','.join(map(str, ids))}) SETTINGS mutations_sync = 2")
        ch.execute(INSERT.format(dirs=",".join(f"{i:03d}" for i in ids)))
        rows = ch.query("SELECT sum(rows) AS n FROM system.parts WHERE database = currentDatabase() "
                        "AND table = 'transfers' AND active AND toUInt32(partition) IN {ids:Array(UInt32)}",
                        {"ids": f"[{",".join(map(str, ids))}]"})[0]["n"]
        last = ch.query("SELECT max(block) AS b FROM transfers")[0]["b"]
        ch.insert_rows("ingest_state", [{"key": k, "value": "done"}, {"key": "last_block", "value": str(last)}])
        el = time.time() - t0; rows_all += rows
        log(f"[{n}/{len(todo)}] {k} rows={rows:,} {rows / el:,.0f} rows/s {el:.0f}s last_block={last}")
    log(f"[done] {rows_all:,} rows in {(time.time() - t_all) / 60:.1f} min")
    return rows_all


def hubs(ch, min_n, k):
    """Precompute hub_top (top-k counterparties per direction) for every address with n_in + n_out >= min_n.
    Rerun to refresh: the API serves a hub live (slow but exact) while its rows are being replaced."""
    from .api import queries as q
    rows = ch.query("SELECT concat('0x', lower(hex(addr))) AS addr FROM addr_stats GROUP BY addr "
                    "HAVING sum(n_in) + sum(n_out) >= {m:UInt64} ORDER BY addr", {"m": min_n})
    for i, r in enumerate(rows, 1):
        t0 = time.time()
        for me, d in (("from_addr", "out"), ("to_addr", "in")):
            ch.execute(f"ALTER TABLE hub_top DELETE WHERE addr = unhex('{q.strip0x(r['addr'])}') AND dir = '{d}' "
                       f"SETTINGS mutations_sync = 2")
            ch.execute(q.hub_insert(me, r["addr"], k))
        log(f"[hubs] {i}/{len(rows)} {r['addr']} {time.time() - t0:.1f}s")
    return len(rows)


def stats(ch):
    for r in ch.query("SELECT table, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS disk "
                      "FROM system.parts WHERE database = currentDatabase() AND active GROUP BY table ORDER BY table"):
        print(f"{r['table']:<14} rows={r['rows']:>15,}  disk={r['disk']}")
    for r in ch.query("SELECT key, value FROM ingest_state FINAL ORDER BY key"):
        print(f"{r['key']:<18} {r['value']}")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="rhflow store", description="ClickHouse storage for rhflow.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init", help="create database and apply rhflow/sql/schema.sql (idempotent)")
    p = sub.add_parser("load", help="load shards into transfers in resumable batches")
    p.add_argument("--shards", default="/root/rhflow/data/transfers", help="shard dir (must be the one mounted "
                   "at user_files/transfers in the container)")
    p.add_argument("--batch-shards", type=int, default=500, help="shards per INSERT; multiple of 100")
    p.add_argument("--from-block", type=int, default=0)
    sub.add_parser("stats", help="rows and disk per table, ingest state")
    p = sub.add_parser("hubs", help="precompute hub_top for hub addresses (rerun to refresh, e.g. daily from cron)")
    p.add_argument("--min-n", type=int, default=None, help="hub = n_in + n_out >= this (default: api.queries.HUB_MAX)")
    p.add_argument("--k", type=int, default=None, help="counterparties per direction (default: HUB_TOP_K)")
    a = ap.parse_args(argv)
    from .ch import CH
    ch = CH(timeout=None)
    if a.cmd == "init":
        CH(db="default").execute(f"CREATE DATABASE IF NOT EXISTS {ch.db}")
        init(ch)
    elif a.cmd == "load":
        load(ch, a.shards, a.batch_shards, a.from_block)
    elif a.cmd == "stats":
        stats(ch)
    elif a.cmd == "hubs":
        from .api import queries as q
        log(f"[hubs] {hubs(ch, a.min_n or q.HUB_MAX, a.k or q.HUB_TOP_K)} hubs done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
