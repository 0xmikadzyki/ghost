"""Backfill of ERC-20 Transfer events on Robinhood Chain (4663) via Envio HyperSync.

Fixed-width block shards. A shard is written to `.tmp` and renamed, so an
unfinished shard never exists on disk and a restart just skips finished files.
Idempotent by construction.

Pages are streamed into gzip as they arrive: one HyperSync response is ~20-25 MB,
a whole shard in memory is gigabytes per worker, and the box swaps at high CONC.
"""
import asyncio, gzip, json, os, sys, time

from .env import env, redact

TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
FIELDS = {
    "log": ["block_number", "transaction_hash", "log_index", "address",
            "topic0", "topic1", "topic2", "topic3", "data"],
    "block": ["number", "timestamp"],
}
STATS = {"shards": 0, "logs": 0, "http": 0, "retry": 0, "bytes": 0, "rl": 0, "fail": 0}


class Gate:
    """One brake for all workers. The HyperSync limit is per account, so a local
    backoff is useless: while one worker sleeps the others burn the quota."""
    def __init__(self):
        self.ev = asyncio.Event(); self.ev.set(); self.streak = 0
    async def wait(self):
        await self.ev.wait()
    async def hit_429(self, retry_after=None):
        STATS["rl"] += 1
        if not self.ev.is_set():      # someone is already braking — just wait for them
            await self.ev.wait(); return
        self.streak += 1
        self.ev.clear()
        await asyncio.sleep(retry_after or min(2.0 * 1.6 ** min(self.streak, 8), 45))
        self.ev.set()
    def ok(self):
        self.streak = max(0, self.streak - 1)


def shard_path(out, lo):
    return os.path.join(out, f"{lo // 1_000_000:03d}", f"{lo:09d}.jsonl.gz")


async def fetch(client, gate, url, body, tries=6):
    delay, last, left = 1.0, "?", tries
    while left > 0:
        await gate.wait()
        try:
            r = await client.post(url, json=body)
            STATS["http"] += 1
            if r.status_code == 200:
                STATS["bytes"] += len(r.content); gate.ok(); return r.json()
            if r.status_code in (401, 403):
                raise SystemExit(f"auth failed: HTTP {r.status_code}")
            if r.status_code == 429:
                ra = r.headers.get("retry-after")
                await gate.hit_429(float(ra) if ra else None); continue
            last = f"HTTP {r.status_code}: {r.text[:200]}"
        except (SystemExit, RuntimeError):
            raise
        except Exception as e:
            last = f"{type(e).__name__}: {str(e)[:200]}"
        left -= 1; STATS["retry"] += 1
        await asyncio.sleep(delay); delay = min(delay * 2, 30)
    raise RuntimeError(redact(
        f"gave up on blocks {body['from_block']}..{body.get('to_block')}; last error: {last}"))


async def do_shard(client, gate, sem, url, out, lo, hi):
    """Pull [lo, hi) with pagination and write one shard. hi is exclusive."""
    path = shard_path(out, lo)
    if os.path.exists(path): return 0
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"; n, cur = 0, lo
    async with sem:
        with gzip.open(tmp, "wt", compresslevel=4) as f:
            while cur < hi:
                d = await fetch(client, gate, url, {
                    "from_block": cur, "to_block": hi,
                    "logs": [{"topics": [[TRANSFER]]}],
                    "field_selection": FIELDS, "join_mode": "Default",
                })
                for batch in d.get("data", []):
                    ts = {b["number"]: b["timestamp"] for b in (batch.get("blocks") or [])}
                    for l in (batch.get("logs") or []):
                        l["ts"] = ts.get(l["block_number"])
                        f.write(json.dumps(l, separators=(",", ":")) + "\n"); n += 1
                nxt = d.get("next_block"); del d
                if not nxt or nxt <= cur: break
                cur = nxt
    os.replace(tmp, path)
    STATS["shards"] += 1; STATS["logs"] += n
    return n


async def run(out, conc=8, shard=10_000, from_block=0, to_block=None, head_lag=1000, client=None):
    """Backfill [from_block, to_block) into `out`. `client` is injected by tests."""
    hs = env("HYPERSYNC_URL", "https://robinhood.hypersync.xyz")
    tok = env("ENVIO_TOKEN")
    if client is None:
        if not tok:
            raise SystemExit("ENVIO_TOKEN is not set (put it in .env)")
        import httpx
        client = httpx.AsyncClient(timeout=300, http2=False,
                                   headers={"Authorization": f"Bearer {tok}"},
                                   limits=httpx.Limits(max_connections=conc + 8))
    async with client:
        h = await client.get(f"{hs}/height")
        head = h.json()["height"] - head_lag
        end = to_block if to_block is not None else head
        todo = [(lo, min(lo + shard, end)) for lo in range(from_block, end, shard)
                if not os.path.exists(shard_path(out, lo))]
        total = (end - from_block + shard - 1) // shard
        print(f"[init] head={head} range={from_block}..{end} shards={total} "
              f"todo={len(todo)} conc={conc}", file=sys.stderr, flush=True)
        sem = asyncio.Semaphore(conc); t0 = time.time(); done = 0
        gate = Gate(); url = f"{hs}/query"
        tasks = [asyncio.create_task(do_shard(client, gate, sem, url, out, lo, hi)) for lo, hi in todo]
        for fut in asyncio.as_completed(tasks):
            try:
                await fut
            except SystemExit:
                raise
            except Exception as e:
                STATS["fail"] += 1
                print(f"[fail] {type(e).__name__}: {redact(str(e))[:160]}", file=sys.stderr, flush=True)
            done += 1
            if done % 25 == 0 or done == len(todo):
                el = time.time() - t0; pct = done / max(len(todo), 1) * 100
                eta = el / done * (len(todo) - done)
                print(f"[{pct:5.1f}%] shards {done}/{len(todo)} logs={STATS['logs']:,} "
                      f"{STATS['logs']/el:,.0f} ev/s {STATS['bytes']/el/1e6:.1f} MB/s "
                      f"retry={STATS['retry']} 429={STATS['rl']} fail={STATS['fail']} ETA {eta/60:.1f} min",
                      file=sys.stderr, flush=True)
        print(f"[done] {STATS['logs']:,} logs in {(time.time()-t0)/60:.1f} min, "
              f"failed shards={STATS['fail']}", file=sys.stderr, flush=True)
    return STATS
