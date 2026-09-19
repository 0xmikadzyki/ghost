"""Read-only HTTP API. `rhflow serve --host 127.0.0.1 --port 8791`.

Every 200 is `{"data": ..., "meta": {"dataset_head", "took_ms"}}`; 400 malformed input, 404 unknown
address/tx, 503 ClickHouse unreachable or over the 10 s budget. Handlers stay thin: SQL and assembly live in
queries.py. `create_app(ch=...)` takes any object with `async query(sql, params) -> rows` (tests inject one).
"""
import asyncio, re, sys, time

import httpx
from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from . import queries as q

ADDR = r"^0x[0-9a-fA-F]{40}$"
HASH = r"^0x[0-9a-fA-F]{64}$"
HEAD_TTL = 5.0


class _Timed:
    """Logs the tag and elapsed ms of any statement slower than SLOW_MS (params are never logged)."""
    SLOW_MS = 500

    def __init__(self, ch):
        self.ch = ch

    async def query(self, sql, params=None):
        t0 = time.perf_counter()
        try:
            return await self.ch.query(sql, params)
        finally:
            ms = (time.perf_counter() - t0) * 1000
            if ms > self.SLOW_MS:
                print(f"[api] slow {sql[:24].split('*/')[0].strip('/* ')} {ms:.0f} ms", file=sys.stderr, flush=True)


def create_app(ch=None):
    if ch is None:
        from ..ch import CHAsync
        ch = _Timed(CHAsync(timeout=q.TIMEOUT_S + 5))   # let ClickHouse time out first (clean 503)
    app = FastAPI(title="rhflow", version="0.1", description="Read-only fund-flow API for Robinhood Chain (chain id 4663). Every 200 is {data, meta:{dataset_head, took_ms}}.")
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])
    head = {"at": 0.0, "block": None, "ts": None}

    @app.exception_handler(RequestValidationError)
    async def _bad(_, e):
        return JSONResponse({"detail": e.errors()[0].get("msg", "bad input"), "loc": e.errors()[0].get("loc")}, 400)

    @app.exception_handler(httpx.TransportError)
    async def _down(_, e):
        return JSONResponse({"detail": f"clickhouse unreachable: {type(e).__name__}"}, 503)

    @app.exception_handler(RuntimeError)
    async def _cherr(_, e):
        print(f"[api] {str(e)[:300]}", file=sys.stderr, flush=True)
        return JSONResponse({"detail": "clickhouse error"}, 503 if "TIMEOUT_EXCEEDED" in str(e) else 500)

    async def dataset_head():
        if time.monotonic() - head["at"] > HEAD_TTL:
            kv = {r["key"]: r["value"] for r in await ch.query(q.Q_HEAD)}
            head.update(at=time.monotonic(), block=int(kv["last_block"]) if kv.get("last_block") else None,
                        ts=int(kv["last_ts"]) if kv.get("last_ts") else None)
        return head["block"], head["ts"]

    async def reply(t0, data):
        block, _ = await dataset_head()
        return {"data": data, "meta": {"dataset_head": block, "took_ms": int((time.perf_counter() - t0) * 1000)}}

    def window(from_ts, to_ts):
        if from_ts is None and to_ts is None:
            return None
        return (from_ts or 0, to_ts if to_ts is not None else 2**32 - 1)

    async def hidden(hide_tokens):
        """`hide_tokens` = comma list of token addresses and/or symbols; symbols resolve via `tokens`."""
        items = [x.strip() for x in (hide_tokens or "").split(",") if x.strip()]
        addrs = [x.lower() for x in items if re.match(ADDR, x)]
        syms = [x.lower() for x in items if not re.match(ADDR, x)]
        if syms:
            addrs += [r["token"] for r in await ch.query(q.Q_HIDDEN, {"syms": q.arr_str(syms)})]
        return addrs

    async def stats_of(addr):
        rows = await ch.query(q.Q_STATS(q.arr([addr])))
        if not rows:
            raise HTTPException(404, "unknown address")
        return rows[0]

    def with_token(rows, tokens, key="token"):
        for r in rows:
            t = tokens.get(r[key]) or {}
            r["symbol"], r["decimals"] = t.get("symbol") or None, t.get("decimals")
        return rows

    # ---- routes ---------------------------------------------------------------------------------------

    @app.get("/health")
    async def health():
        t0 = time.perf_counter()
        block, ts = await dataset_head()
        return await reply(t0, {"ok": True, "dataset_head": block, "head_ts": ts, "clickhouse": "ok"})

    @app.get("/address/{addr}")
    async def address(addr: str = Path(pattern=ADDR)):
        t0, addr = time.perf_counter(), addr.lower()
        s = await stats_of(addr)
        by_tok = {}
        for r in await ch.query(q.Q_ADDR_TOKENS, {"a": q.strip0x(addr)}):
            t = by_tok.setdefault(r["token"], {"token": r["token"], "n_in": 0, "n_out": 0, "amount_in": "0", "amount_out": "0"})
            t["n_" + r["dir"]], t["amount_" + r["dir"]] = int(r["n_sum"]), str(r["amt_sum"])
        toks = sorted(by_tok.values(), key=lambda t: -(t["n_in"] + t["n_out"]))[:20]
        top_out, top_in = [r.get(addr, []) for r in await asyncio.gather(
            q.top(ch, "from_addr", [addr], 10, {}), q.top(ch, "to_addr", [addr], 10, {}))]
        cps = [r["other"] for r in top_in + top_out]
        tokens, addrs, _ = await q.meta(ch, [addr] + cps, [t["token"] for t in toks])
        cp = lambda r: {"addr": r["other"], "type": q.node_type(r["other"], tokens, addrs),
                        "label": (addrs.get(r["other"]) or {}).get("label") or None, "n": int(r["n_sum"]),
                        "amount_sum": str(r["top"][2]), "token": r["top"][0]}
        m = addrs.get(addr) or {}
        return await reply(t0, {
            "addr": addr, "type": q.node_type(addr, tokens, addrs), "label": m.get("label") or None,
            "is_contract": bool(m.get("is_contract", 0)) or addr in tokens,
            "first_ts": s["first_ts"], "last_ts": s["last_ts"], "n_in": int(s["n_in"]), "n_out": int(s["n_out"]),
            "n_tx": int(s["n_tx"]), "tokens": with_token(toks, tokens),
            "top_in": [cp(r) for r in top_in], "top_out": [cp(r) for r in top_out]})

    @app.get("/address/{addr}/transfers")
    async def transfers(addr: str = Path(pattern=ADDR), token: str | None = Query(None, pattern=ADDR),
                        dir: str = Query("both", pattern="^(in|out|both)$"), from_ts: int | None = Query(None, ge=0),
                        to_ts: int | None = Query(None, ge=0), limit: int = Query(50, ge=1, le=200),
                        cursor: str | None = None):
        t0, addr = time.perf_counter(), addr.lower()
        p = {"a": q.strip0x(addr), "lim": limit + 1}
        if token:
            p["token"] = q.strip0x(token)
        w = window(from_ts, to_ts)
        if w:
            p["from_ts"], p["to_ts"] = w
        if cursor:
            try:
                p["c_ts"], p["c_tx"], p["c_li"] = q.dec_cursor(cursor)
            except ValueError:
                raise HTTPException(400, "bad cursor")
        dirs = {"in": ["to_addr"], "out": ["from_addr"], "both": ["from_addr", "to_addr"]}[dir]
        rows = await ch.query(q.q_transfers(dirs, bool(token), bool(w), bool(cursor)), p)
        if not rows:
            await stats_of(addr)                                  # 404 if the address is unknown
        items, more = rows[:limit], len(rows) > limit
        tokens, _, _ = await q.meta(ch, [], {r["token"] for r in items})
        for r in items:
            r["amount"] = str(r["amount"])
        last = items[-1] if more else None
        return await reply(t0, {"items": with_token(items, tokens),
                                "next_cursor": q.enc_cursor(last["ts"], last["tx_hash"], last["log_index"]) if last else None})

    @app.get("/graph")
    async def graph(address: str = Query(pattern=ADDR), depth: int = Query(1, ge=1, le=2),
                    token: str | None = Query(None, pattern=ADDR), from_ts: int | None = Query(None, ge=0),
                    to_ts: int | None = Query(None, ge=0), hide_zero: int = Query(1, ge=0, le=1),
                    hide_tokens: str | None = None, per_node: int = Query(10, ge=1, le=25)):
        t0, address = time.perf_counter(), address.lower()
        f = {"token": token, "window": window(from_ts, to_ts), "hide_zero": bool(hide_zero),
             "hidden": await hidden(hide_tokens)}
        g = await q.graph(ch, address, depth, per_node, f)
        if len(g["nodes"]) == 1 and g["nodes"][0]["n_in"] == 0 and g["nodes"][0]["n_out"] == 0:
            await stats_of(address)
        return await reply(t0, g)

    @app.get("/path")
    async def path(from_: str = Query(alias="from", pattern=ADDR), to: str = Query(pattern=ADDR),
                   token: str | None = Query(None, pattern=ADDR), max_hops: int = Query(4, ge=1, le=4),
                   per_node: int = Query(10, ge=1, le=25)):
        t0, from_, to = time.perf_counter(), from_.lower(), to.lower()
        if from_ == to:
            raise HTTPException(400, "from == to")
        return await reply(t0, await q.path(ch, from_, to, max_hops, per_node, {"token": token, "hide_zero": True}))

    @app.get("/tx/{hash}/flow")
    async def tx_flow(hash: str = Path(pattern=HASH)):
        t0, hash = time.perf_counter(), hash.lower()
        rows = await ch.query(q.Q_TX, {"h": q.strip0x(hash)})
        if not rows:
            raise HTTPException(404, "unknown tx")
        ids = {r["from"] for r in rows} | {r["to"] for r in rows}
        tokens, addrs, stats = await q.meta(ch, ids, {r["token"] for r in rows})
        edges = [{"from": r["from"], "to": r["to"], "token": r["token"], "amount": str(r["amount"]),
                  "log_index": r["log_index"], "kind": r["kind"]} for r in rows]
        return await reply(t0, {"tx_hash": hash, "block": rows[0]["block"], "ts": rows[0]["ts"],
                                "nodes": [q.node(a, tokens, addrs, stats) for a in sorted(ids)],
                                "edges": with_token(edges, tokens)})

    @app.get("/token/{addr}")
    async def token_info(addr: str = Path(pattern=ADDR)):
        t0, addr = time.perf_counter(), addr.lower()
        rows = await ch.query(q.Q_TOKENS(q.arr([addr])))
        if not rows:
            raise HTTPException(404, "unknown token")
        t, span = rows[0], (await ch.query(q.Q_TOKEN_SPAN, {"a": q.strip0x(addr)}) or [{}])[0]
        return await reply(t0, {"token": addr, "symbol": t.get("symbol") or None, "decimals": t.get("decimals"),
                                "name": t.get("name") or None, "kind": t.get("kind"), "n_transfers": int(span.get("n_transfers") or 0),
                                "first_ts": span.get("first_ts"), "last_ts": span.get("last_ts"),
                                "holders_seen": int(span.get("holders_seen") or 0)})

    @app.get("/search")
    async def search(q_: str = Query(alias="q", min_length=1, max_length=80)):
        t0, s = time.perf_counter(), q_.strip()
        if re.match(ADDR, s):
            s = s.lower()
            hits = []
            if await ch.query(q.Q_STATS(q.arr([s]))):
                tokens, addrs, _ = await q.meta(ch, [s])
                hits = [{"kind": "address", "id": s, "label": (addrs.get(s) or {}).get("label") or None,
                         "type": q.node_type(s, tokens, addrs)}]
        elif re.match(HASH, s):
            s = s.lower()
            hits = [{"kind": "tx", "id": s, "label": None, "type": None}] if await ch.query(q.Q_TX_EXISTS, {"h": q.strip0x(s)}) else []
        else:
            hits = [{"kind": "address", "id": r["id"], "label": r["label"], "type": r["type"]}
                    for r in await ch.query(q.Q_SEARCH_LABELS, {"q": s})]
            hits += [{"kind": "token", "id": r["id"], "label": r["symbol"], "type": "ERC"}
                     for r in await ch.query(q.Q_SEARCH_TOKENS, {"q": s})]
        return await reply(t0, {"hits": hits[:20]})

    return app


def serve(host="127.0.0.1", port=8791):
    import uvicorn
    uvicorn.run(create_app(), host=host, port=port, log_level="info")
