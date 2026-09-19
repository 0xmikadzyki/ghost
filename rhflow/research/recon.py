#!/usr/bin/env python3
"""Robinhood Chain recon (Phase 1). Stages cache to data/sample/, restart-safe."""
import asyncio, json, os, random, statistics, sys, time
from collections import Counter, defaultdict
import httpx

RPC = os.environ.get("RPC_URL", "https://rpc.mainnet.chain.robinhood.com")
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "data", "sample")
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
CONC = int(os.environ.get("RECON_CONC", "4"))

os.makedirs(OUT, exist_ok=True)
STATS = Counter()

def log(*a): print(*a, file=sys.stderr, flush=True)
def hx(x): return int(x, 16) if isinstance(x, str) else x

class RPCError(Exception): pass

async def batch(client, sem, calls, tries=7, allow_error=False):
    payload = [{"jsonrpc": "2.0", "id": i, "method": m, "params": p} for i, (m, p) in enumerate(calls)]
    delay, err = 1.0, "?"
    for _ in range(tries):
        try:
            async with sem:
                r = await client.post(RPC, json=payload)
            if r.status_code == 200:
                STATS["http"] += 1; STATS["calls"] += len(calls)
                data = json.loads(r.text, strict=False)
                if isinstance(data, dict): data = [data]
                res = {d["id"]: d for d in data}
                if not allow_error:
                    for d in data:
                        if "error" in d: raise RPCError(str(d["error"])[:300])
                return res
            if r.status_code not in (429, 500, 502, 503, 504):
                raise RPCError(f"HTTP {r.status_code} {r.text[:200]}")
            err = f"HTTP {r.status_code}"; STATS[f"retry_{r.status_code}"] += 1
        except (httpx.TimeoutException, httpx.TransportError) as e:
            err = f"{type(e).__name__}: {e}"; STATS["retry_net"] += 1
        await asyncio.sleep(delay + random.random() * 0.4)
        delay = min(delay * 2, 30)
    raise RPCError(f"gave up: {err}")

def cached(name):
    p = os.path.join(OUT, name + ".json")
    if os.path.exists(p):
        log(f"[cache] {name}")
        return json.load(open(p))
    return None

def save(name, obj):
    json.dump(obj, open(os.path.join(OUT, name + ".json"), "w"))
    return obj

# ---------------- stages ----------------

async def stage_basics(client, sem):
    c = cached("basics")
    if c: return c
    head = hx((await batch(client, sem, [("eth_blockNumber", [])]))[0]["result"])
    probes = [1, head // 100, head // 10, head // 4, head // 2, head * 3 // 4, head - 1, head]
    calls = [("eth_chainId", [])] + [("eth_getBlockByNumber", [hex(b), False]) for b in probes]
    r = await batch(client, sem, calls)
    out = {"chain_id": r[0]["result"], "head": head, "probes": {}}
    for i, b in enumerate(probes, start=1):
        blk = r[i]["result"]
        out["probes"][str(b)] = {"ts": hx(blk["timestamp"]), "ntx": len(blk["transactions"]),
                                 "gasUsed": hx(blk["gasUsed"]), "hash": blk["hash"]}
    return save("basics", out)

async def stage_blocks(client, sem, head, n=600):
    c = cached("blocks")
    if c: return c
    nums = sorted(set(round(1 + i * (head - 1) / (n - 1)) for i in range(n)))
    log(f"[blocks] sampling {len(nums)} blocks, step={(head-1)//(n-1)}")
    res = []
    chunks = [nums[i:i + 40] for i in range(0, len(nums), 40)]
    async def one(ch):
        r = await batch(client, sem, [("eth_getBlockByNumber", [hex(b), True]) for b in ch])
        out = []
        for i, b in enumerate(ch):
            blk = r[i]["result"]
            txs = [{"to": t.get("to"), "from": t.get("from"), "type": t.get("type"),
                    "input": len(t.get("input", "0x")) > 2, "value": t.get("value")}
                   for t in blk["transactions"]]
            out.append({"n": b, "ts": hx(blk["timestamp"]), "gas": hx(blk["gasUsed"]), "txs": txs})
        return out
    done = 0
    for group in [chunks[i:i + CONC] for i in range(0, len(chunks), CONC)]:
        for part in await asyncio.gather(*[one(ch) for ch in group]):
            res.extend(part)
        done += sum(len(ch) for ch in group)
        log(f"[blocks] {done}/{len(nums)}")
    return save("blocks", res)

async def stage_code(client, sem, addrs):
    c = cached("code")
    if c: return c
    addrs = sorted(addrs)
    log(f"[code] {len(addrs)} unique addresses")
    out = {}
    chunks = [addrs[i:i + 100] for i in range(0, len(addrs), 100)]
    async def one(ch):
        r = await batch(client, sem, [("eth_getCode", [a, "latest"]) for a in ch])
        return {a: len(r[i]["result"]) > 2 for i, a in enumerate(ch)}
    for group in [chunks[i:i + CONC] for i in range(0, len(chunks), CONC)]:
        for part in await asyncio.gather(*[one(ch) for ch in group]):
            out.update(part)
        log(f"[code] {len(out)}/{len(addrs)}")
    return save("code", out)

async def stage_limits(client, sem, head):
    c = cached("limits")
    if c: return c
    out = {"getlogs": {}, "trace": {}}
    for span in (1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000):
        frm = max(1, head - 2000 - span)
        t0 = time.time()
        try:
            r = await batch(client, sem, [("eth_getLogs", [{"fromBlock": hex(frm), "toBlock": hex(frm + span - 1),
                                                            "topics": [TRANSFER]}])], tries=2, allow_error=True)
            d = r[0]
            if "error" in d:
                out["getlogs"][span] = {"ok": False, "error": str(d["error"])[:200]}
            else:
                out["getlogs"][span] = {"ok": True, "logs": len(d["result"]), "sec": round(time.time() - t0, 2)}
        except Exception as e:
            out["getlogs"][span] = {"ok": False, "error": str(e)[:200]}
        log(f"[limits] getLogs span={span}: {out['getlogs'][span]}")
        if not out["getlogs"][span].get("ok"): break
    for m, p in (("debug_traceBlockByNumber", [hex(head - 10), {"tracer": "callTracer"}]),
                 ("trace_block", [hex(head - 10)]),
                 ("debug_traceTransaction", ["0x" + "00" * 32, {"tracer": "callTracer"}]),
                 ("eth_getBalance", ["0x0000000000000000000000000000000000000064", "0x1"])):
        try:
            r = await batch(client, sem, [(m, p)], tries=2, allow_error=True)
            d = r[0]
            out["trace"][m] = {"error": str(d["error"])[:200]} if "error" in d else {"ok": True, "sample": str(d["result"])[:200]}
        except Exception as e:
            out["trace"][m] = {"error": str(e)[:200]}
        log(f"[limits] {m}: {str(out['trace'][m])[:160]}")
    return save("limits", out)

async def stage_logs(client, sem, head, span, windows=60, cap=600_000):
    c = cached("logs_meta")
    if c: return c
    starts = sorted(set(round(1 + i * (head - span - 1) / (windows - 1)) for i in range(windows)))
    path = os.path.join(OUT, "logs.jsonl")
    donep = os.path.join(OUT, "logs_done.json")
    done = set(json.load(open(donep))) if os.path.exists(donep) else set()
    f = open(path, "a")
    total = sum(1 for _ in open(path)) if os.path.exists(path) else 0
    log(f"[logs] {len(starts)} windows x {span} blocks; resume from {len(done)} done, {total} logs")
    async def one(s):
        r = await batch(client, sem, [("eth_getLogs", [{"fromBlock": hex(s), "toBlock": hex(s + span - 1),
                                                        "topics": [TRANSFER]}])], allow_error=True)
        d = r[0]
        if "error" in d: return s, None, str(d["error"])[:200]
        return s, d["result"], None
    pending = [s for s in starts if str(s) not in done]
    for group in [pending[i:i + CONC] for i in range(0, len(pending), CONC)]:
        for s, logs, err in await asyncio.gather(*[one(s) for s in group]):
            if err:
                log(f"[logs] window {s} error: {err}"); continue
            for lg in logs:
                tp = lg["topics"]
                f.write(json.dumps([hx(lg["blockNumber"]), lg["address"], tp[1] if len(tp) > 1 else None,
                                    tp[2] if len(tp) > 2 else None, lg["data"], len(tp), lg["transactionHash"]]) + "\n")
            total += len(logs)
            done.add(str(s))
        f.flush(); json.dump(sorted(done), open(donep, "w"))
        log(f"[logs] windows {len(done)}/{len(starts)} logs={total}")
        if total > cap:
            log("[logs] cap reached"); break
    f.close()
    return save("logs_meta", {"windows": sorted(int(x) for x in done), "span": span, "total_logs": total,
                              "sampled_blocks": len(done) * span})

async def stage_ratelimit(client, sem, head):
    c = cached("ratelimit")
    if c: return c
    out = {}
    for rps in (5, 10, 20, 40, 80, 160):
        sem2 = asyncio.Semaphore(rps)
        t0 = time.time()
        async def one(i):
            try:
                async with sem2:
                    r = await client.post(RPC, json=[{"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}])
                return r.status_code
            except Exception as e:
                return type(e).__name__
        codes = Counter(await asyncio.gather(*[one(i) for i in range(rps * 3)]))
        dt = time.time() - t0
        out[rps] = {"codes": {str(k): v for k, v in codes.items()}, "sec": round(dt, 2),
                    "achieved_rps": round(rps * 3 / dt, 1)}
        log(f"[ratelimit] target={rps} -> {out[rps]}")
        if any(str(k) != "200" for k in codes): break
        await asyncio.sleep(2)
    return save("ratelimit", out)

async def main():
    limits = httpx.Limits(max_connections=200)
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0), limits=limits,
                                 headers={"content-type": "application/json"}) as client:
        sem = asyncio.Semaphore(CONC)
        b = await stage_basics(client, sem)
        head = b["head"]
        log(f"[basics] chain={b['chain_id']} head={head}")
        lim = await stage_limits(client, sem, head)
        blocks = await stage_blocks(client, sem, head)
        tos = {t["to"] for blk in blocks for t in blk["txs"] if t["to"]}
        await stage_code(client, sem, list(tos)[:5000])
        span = max([int(k) for k, v in lim["getlogs"].items() if v.get("ok")] or [1000])
        span = min(span, 100_000)
        await stage_logs(client, sem, head, span)
        await stage_ratelimit(client, sem, head)
    log(f"[done] {dict(STATS)}")
    save("rpc_stats", dict(STATS))

if __name__ == "__main__":
    asyncio.run(main())
