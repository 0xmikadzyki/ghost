"""Tail worker: keep `transfers` within `head_lag` blocks of the HyperSync head.

Every `interval` s: read ingest_state.last_block, pull Transfer logs for (last_block, head - head_lag)
in windows of at most `max_blocks`, insert each HyperSync page straight into `transfers`, then advance
last_block to what was actually fetched. State moves only after a successful insert, so a crash between
the two duplicates at most one page (no dedup in ClickHouse — see docs/ARCHITECTURE.md "Tail worker").

The hex→binary conversion happens server-side (`INSERT ... SELECT unhex(...) FROM input(...)`), the same
expressions as the bulk load, so JSON never has to carry raw bytes.
"""
import asyncio, sys, time

from .backfill import FIELDS, TRANSFER, Gate, fetch
from .env import env, redact

STATE_KEY = "last_block"

# what a HyperSync log row looks like on the way in, and how it becomes a `transfers` row
INPUT = ("block UInt32, ts UInt32, tx_hash String, log_index UInt32, token String, "
         "topic1 String, topic2 String, topic3 Nullable(String), data String")
SELECT = ("block, toDateTime(ts), unhex(substring(tx_hash, 3)), log_index, unhex(substring(token, 3)), "
          "unhex(substring(topic1, 27)), unhex(substring(topic2, 27)), "
          "reinterpretAsUInt256(reverse(unhex(substring(ifNull(topic3, data), 3, 64)))), "
          "if(topic3 IS NULL, 'erc20', 'erc721')")
TABLE = f"transfers SELECT {SELECT} FROM input('{INPUT}')"


def to_row(l, ts):
    """HyperSync log → input() row. None for a Transfer without indexed from/to (can't build an edge)."""
    if not l.get("topic1") or not l.get("topic2") or ts is None:
        return None
    ts = int(ts, 16) if isinstance(ts, str) else ts            # HyperSync sends block.timestamp as "0x6aa8cdcd"
    return {"block": l["block_number"], "ts": ts, "tx_hash": l["transaction_hash"],
            "log_index": l["log_index"], "token": l["address"], "topic1": l["topic1"],
            "topic2": l["topic2"], "topic3": l.get("topic3"), "data": l.get("data") or "0x"}


def get_state(ch):
    r = ch.query("SELECT max(toUInt64(value)) AS v FROM ingest_state WHERE key = {k:String}",
                 {"k": STATE_KEY})
    return int(r[0]["v"]) if r and r[0]["v"] is not None else None


def set_state(ch, block):
    ch.insert_rows("ingest_state", [{"key": STATE_KEY, "value": str(block)}])


async def step(ch, client, gate, hs, head_lag, max_blocks, start=None):
    """One pass: fetch and insert up to `max_blocks` blocks. Returns (blocks, rows, skipped)."""
    last = get_state(ch)
    if last is None:
        if start is None:
            raise SystemExit("ingest_state.last_block is missing; run the load first or pass --from-block")
        last = start - 1
    h = await client.get(f"{hs}/height")
    head = h.json()["height"] - head_lag           # exclusive end, same margin as the backfill
    lo = last + 1
    if lo >= head:
        return 0, 0, 0
    hi = min(lo + max_blocks, head); cur = lo; rows_n = skipped = 0
    while cur < hi:
        d = await fetch(client, gate, f"{hs}/query", {
            "from_block": cur, "to_block": hi,
            "logs": [{"topics": [[TRANSFER]]}],
            "field_selection": FIELDS, "join_mode": "Default",
        })
        rows = []
        for batch in d.get("data", []):
            ts = {b["number"]: b["timestamp"] for b in (batch.get("blocks") or [])}
            for l in (batch.get("logs") or []):
                r = to_row(l, ts.get(l["block_number"]))
                if r is None: skipped += 1
                else: rows.append(r)
        nxt = d.get("next_block")
        if not nxt or nxt <= cur:
            break
        if rows:
            ch.insert_rows(TABLE, rows)
        set_state(ch, nxt - 1)                       # only after the insert succeeded
        rows_n += len(rows); cur = nxt
    return cur - lo, rows_n, skipped


async def run(ch, client=None, interval=10, head_lag=1000, max_blocks=20_000, start=None, once=False):
    """Loop forever (or until caught up when `once`). `client` is injected by tests."""
    hs = env("HYPERSYNC_URL", "https://robinhood.hypersync.xyz")
    if client is None:
        tok = env("ENVIO_TOKEN")
        if not tok:
            raise SystemExit("ENVIO_TOKEN is not set (put it in .env)")
        import httpx
        client = httpx.AsyncClient(timeout=300, http2=False, headers={"Authorization": f"Bearer {tok}"})
    gate = Gate(); delay = interval
    async with client:
        while True:
            t0 = time.time()
            try:
                blocks, rows, skipped = await step(ch, client, gate, hs, head_lag, max_blocks, start)
                delay = interval
                if blocks:
                    print(f"[tail] blocks={blocks} rows={rows} skipped={skipped} "
                          f"{(time.time()-t0)*1000:.0f} ms", file=sys.stderr, flush=True)
                if blocks >= max_blocks:
                    continue                          # still behind: no sleep between windows
                if once:
                    return
            except SystemExit:
                raise
            except Exception as e:
                # ponytail: one backoff for every failure (CH down, HyperSync 5xx, network); state untouched
                delay = min(delay * 2, 300)
                print(f"[tail] error {type(e).__name__}: {redact(str(e))[:160]}; retry in {delay:.0f}s",
                      file=sys.stderr, flush=True)
                if once:
                    raise
            await asyncio.sleep(delay)


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(prog="rhflow tail", description="follow the chain head into ClickHouse")
    ap.add_argument("--interval", type=float, default=10)
    ap.add_argument("--head-lag", type=int, default=1000)
    ap.add_argument("--max-blocks", type=int, default=20_000)
    ap.add_argument("--from-block", type=int, default=None, help="seed last_block when ingest_state is empty")
    ap.add_argument("--once", action="store_true", help="catch up and exit")
    a = ap.parse_args(argv)
    from .ch import CH
    asyncio.run(run(CH(), interval=a.interval, head_lag=a.head_lag, max_blocks=a.max_blocks,
                    start=a.from_block, once=a.once))
    return 0


if __name__ == "__main__":
    sys.exit(main())
