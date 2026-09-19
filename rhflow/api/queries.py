"""SQL and the pure-Python graph assembly behind rhflow/api/app.py.

Every statement starts with a `/* q:tag */` comment (tests dispatch on it) and takes values only through
server-side `{name:Type}` parameters. Addresses cross the wire as 40-hex strings without `0x`; ClickHouse
`unhex`es them on the way in and returns `0x`-prefixed lowercase hex on the way out, so Python never
converts. Amounts (UInt256) come back as strings and stay strings.
"""
import asyncio, base64, json

ZERO = "0x" + "00" * 20
MAX_NODES, MAX_EDGES, MAX_EXPLORED = 400, 1500, 5000
HUB_MAX = 5_000_000          # n_in + n_out above this = hub: never expanded past the root (measured 5-15 s each)
HUB_TOP_K = 50               # counterparties per direction kept in hub_top (per_node <= 25, /address asks 10)
TIMEOUT_S = 10

# Access paths (rhflow/sql/schema.sql): `transfers` (ORDER BY from_addr) is mirrored as `transfers_by_to` and
# `transfers_by_tx`; `edges` as `edges_rev` (ORDER BY to_addr). Aggregate tables are SimpleAggregateFunction
# columns partitioned by block range, so every read merges with plain sum/min/max.
T_EDGES = {"from_addr": "edges", "to_addr": "edges_rev"}
T_TRANSFERS = {"from_addr": "transfers", "to_addr": "transfers_by_to", "tx_hash": "transfers_by_tx"}


def sql(tag, body, settings=""):
    """prefer_column_name_to_alias: output aliases (`... AS token`) must not shadow the raw columns in WHERE."""
    return f"/* q:{tag} */ {body.strip()} SETTINGS max_execution_time = {TIMEOUT_S}, prefer_column_name_to_alias = 1{settings}"


def strip0x(a):
    return a[2:].lower()


def arr(hexes):
    """Literal IN-list `(unhex('..'), ...)` spliced into the SQL: inputs are regex-validated hex, nothing to
    quote. Not a `{xs:Array(String)}` parameter on purpose — `IN arrayMap(...)` over a parameter skips the
    primary index (measured: 8584/8584 granules vs 255/8584 for the literal on the full dataset)."""
    return "(" + ",".join(f"unhex('{strip0x(h)}')" for h in hexes) + ")"


def arr_str(xs):
    """Same, for free text (symbols): backslash-escaped like ClickHouse's own literals."""
    return "[" + ",".join("'" + x.replace("\\", "\\\\").replace("'", "\\'") + "'" for x in xs) + "]"


HX = "concat('0x', lower(hex({})))"          # FixedString column -> 0x hex


# ---- cursors ---------------------------------------------------------------------------------------------

def enc_cursor(ts, tx_hash, log_index):
    return base64.urlsafe_b64encode(json.dumps([ts, tx_hash, log_index]).encode()).decode().rstrip("=")


def dec_cursor(c):
    """-> (ts, tx_hash_hex_no_0x, log_index) or ValueError."""
    try:
        ts, tx, li = json.loads(base64.urlsafe_b64decode(c + "=" * (-len(c) % 4)))
        if not (isinstance(ts, int) and isinstance(li, int) and isinstance(tx, str) and len(tx) == 66):
            raise ValueError
        return ts, strip0x(tx), li
    except Exception:
        raise ValueError("bad cursor")


# ---- node typing -----------------------------------------------------------------------------------------

def node_type(addr, tokens, addrs):
    """ERC if the address is a token contract; else the label's type; else CTR if contract; else EOA."""
    if addr in tokens:
        return "ERC"
    m = addrs.get(addr)
    if m and m.get("label") and m.get("type") not in (None, "", "UNK"):
        return m["type"]
    return "CTR" if m and m.get("is_contract") else "EOA"


def node(addr, tokens, addrs, stats):
    m, s = addrs.get(addr) or {}, stats.get(addr) or {}
    return {"id": addr, "type": node_type(addr, tokens, addrs), "label": m.get("label") or None,
            "note": None, "n_in": int(s.get("n_in", 0)), "n_out": int(s.get("n_out", 0))}


# ---- metadata lookups ------------------------------------------------------------------------------------

def Q_TOKENS(xs):
    return sql("tokens", f"""
SELECT {HX.format('token')} AS token, symbol, decimals, kind, name, n_transfers
FROM tokens FINAL WHERE token IN {xs}""")


def Q_ADDRS(xs):
    return sql("addrs", f"""
SELECT {HX.format('addr')} AS addr, is_contract, label, type
FROM addresses FINAL WHERE addr IN {xs}""")


def Q_STATS(xs):
    return sql("stats", f"""
SELECT {HX.format('addr')} AS addr, toUnixTimestamp(min(first_ts)) first_ts, toUnixTimestamp(max(last_ts)) last_ts,
       sum(n_in) n_in, sum(n_out) n_out, uniqMerge(n_tx) n_tx, uniqMerge(n_tokens) n_tokens
FROM addr_stats WHERE addr IN {xs} GROUP BY addr""")


def Q_COUNTS(xs):
    """n_in / n_out only: skips the uniq-state columns of addr_stats (graphs read 250 nodes at a time)."""
    return sql("counts", f"""
SELECT {HX.format('addr')} AS addr, sum(n_in) n_in, sum(n_out) n_out FROM addr_stats WHERE addr IN {xs} GROUP BY addr""")


async def meta(ch, node_ids, token_ids=()):
    """-> (tokens: {addr: row}, addrs: {addr: row}, stats: {addr: row}) for the graph assembly."""
    async def batched(q, ids, key):
        out = {}
        for i in range(0, len(ids), 250):
            out.update((r[key], r) for r in await ch.query(q(arr(ids[i:i + 250]))))
        return out
    nodes, both = sorted(set(node_ids)), sorted(set(node_ids) | set(token_ids))
    return (await batched(Q_TOKENS, both, "token"), await batched(Q_ADDRS, nodes, "addr"),
            await batched(Q_COUNTS, nodes, "addr"))


# ---- edge source -----------------------------------------------------------------------------------------

def _src(me, xs, window, token, hide_zero, hidden, ys=None):
    """Subquery yielding (from_addr, to_addr, token, n, amount_sum, first_ts, last_ts) for `me IN xs`
    (and `other IN ys` when given); xs/ys/hidden are `arr()` literals. Without a time window it merges the
    `edges` aggregates; with one it aggregates `transfers` directly (edge-level first/last_ts cannot express
    a window)."""
    other = "to_addr" if me == "from_addr" else "from_addr"
    where = [f"{me} IN {xs}"] + ([f"{other} IN {ys}"] if ys else [])
    if hide_zero:
        where.append(f"{other} != toFixedString(unhex('{'00' * 20}'), 20)")
    if token:
        where.append("token = toFixedString(unhex({token:String}), 20)")
    if hidden:
        where.append(f"token NOT IN {hidden}")
    if window:
        # ponytail: a window on a hub re-aggregates all its transfers in the window (measured 2-11 s on the
        # 100-shard sample, will exceed the 10 s cap on the full chain); upgrade path = a daily edges table.
        where.append("ts >= toDateTime({from_ts:UInt32}) AND ts <= toDateTime({to_ts:UInt32})")
        return (f"(SELECT from_addr, to_addr, token, count() n, sum(amount) amount_sum, min(ts) first_ts, max(ts) last_ts "
                f"FROM {T_TRANSFERS[me]} WHERE {' AND '.join(where)} GROUP BY from_addr, to_addr, token)")
    return (f"(SELECT from_addr, to_addr, token, sum(n) n, sum(amount_sum) amount_sum, "
            f"min(first_ts) first_ts, max(last_ts) last_ts "
            f"FROM {T_EDGES[me]} WHERE {' AND '.join(where)} GROUP BY from_addr, to_addr, token)")


def _top_raw(me, xs, k, **f):
    """Top `k` counterparties per `me` node (by n, then amount_sum) with the dominant token edge, raw
    FixedStrings: (me, other, n_sum, amt_sum, top(token, n, amount_sum, first_ts, last_ts))."""
    other = "to_addr" if me == "from_addr" else "from_addr"
    return (f"SELECT {me} me, {other} other, sum(n) n_sum, sum(amount_sum) amt_sum, "
            f"argMax((token, n, amount_sum, first_ts, last_ts), (n, amount_sum)) top "
            f"FROM {_src(me, xs, **f)} GROUP BY me, other ORDER BY me, n_sum DESC, amt_sum DESC LIMIT {k} BY me")


def q_top(me, xs, **f):
    """`_top_raw` hexed on the way out; in-order aggregation follows the edges sort key (from_addr, to_addr,
    token) — with the raw argMax 8.4 s → 5.3 s on the busiest hub."""
    return sql(f"top_{me[:-5]}", f"""
SELECT {HX.format('me')} me, {HX.format('other')} other, n_sum, amt_sum,
       ({HX.format('top.1')}, top.2, top.3, toUnixTimestamp(top.4), toUnixTimestamp(top.5)) top
FROM ({_top_raw(me, xs, "{k:UInt32}", **f)})
ORDER BY me, n_sum DESC, amt_sum DESC""", ", optimize_aggregation_in_order = 1")


def q_hub_top(me, xs):
    """Precomputed `hub_top` rows (rhflow store hubs) for the hubs in xs, same shape as q_top."""
    return sql("hub_top", f"""
SELECT {HX.format('addr')} me, {HX.format('other')} other, n_sum, amt_sum,
       ({HX.format('token')}, n, amount_sum, toUnixTimestamp(first_ts), toUnixTimestamp(last_ts)) top
FROM hub_top WHERE addr IN {xs} AND dir = '{"out" if me == "from_addr" else "in"}'
ORDER BY me, n_sum DESC, amt_sum DESC LIMIT {{k:UInt32}} BY me""")


def hub_insert(me, hub, k=HUB_TOP_K):
    """INSERT that (re)computes one hub's `hub_top` rows for one direction (delete the old ones first)."""
    kw, _ = _filters({})
    return (f"INSERT INTO hub_top SELECT me, '{'out' if me == 'from_addr' else 'in'}', other, n_sum, amt_sum, "
            f"top.1, top.2, top.3, top.4, top.5, now() FROM ({_top_raw(me, arr([hub]), k, **kw)}) "
            f"SETTINGS optimize_aggregation_in_order = 1, max_execution_time = 300")


def q_edges(me, xs, ys, **f):
    """Per-token edges from `me IN xs` to `other IN ys` (kept pairs are filtered in Python). A pair can put at
    most MAX_EDGES rows into the final top-MAX_EDGES, so the per-pair cap is exact and keeps a hub × its
    counterparties (measured 535k rows, 3 s) at ≤ 1500 rows per pair."""
    return sql(f"edges_{me[:-5]}", f"""
SELECT {HX.format('from_addr')} `from`, {HX.format('to_addr')} `to`, {HX.format('token')} token,
       n, amount_sum, toUnixTimestamp(first_ts) first_ts, toUnixTimestamp(last_ts) last_ts
FROM {_src(me, xs, ys=ys, **f)} ORDER BY n DESC, amount_sum DESC LIMIT {MAX_EDGES} BY from_addr, to_addr""")


def _filters(f):
    """Filter dict for the API → (`_src` kwargs, query params)."""
    window = f.get("window")
    kw = {"window": bool(window), "token": f.get("token"), "hide_zero": f.get("hide_zero", True),
          "hidden": arr(f["hidden"]) if f.get("hidden") else None}
    p = {}
    if f.get("token"):
        p["token"] = strip0x(f["token"])
    if window:
        p["from_ts"], p["to_ts"] = window
    return kw, p


async def top(ch, me, xs, k, f):
    """-> {me: [{other, n_sum, amt_sum, top: (token, n, amount_sum, first_ts, last_ts)}, ...]} sorted, ≤ k each.
    Unfiltered requests take hubs from the precomputed `hub_top` and aggregate live only the rest."""
    kw, p = _filters(f)
    out, live = {}, list(xs)
    if k <= HUB_TOP_K and kw == _filters({})[0]:
        for r in await ch.query(q_hub_top(me, arr(xs)), {"k": k}):
            out.setdefault(r["me"], []).append(r)
        live = [x for x in xs if x not in out]
    if live:
        for r in await ch.query(q_top(me, arr(live), **kw), {**p, "k": k}):
            out.setdefault(r["me"], []).append(r)
    return out


async def expandable(ch, addrs, keep=()):
    """Frontier minus hubs (n_in + n_out >= HUB_MAX), which stay as nodes but are never expanded.
    -> (kept, dropped_any)."""
    big = set()
    for i in range(0, len(addrs), 250):
        big |= {r["addr"] for r in await ch.query(Q_COUNTS(arr(addrs[i:i + 250])))
                if int(r["n_in"]) + int(r["n_out"]) >= HUB_MAX}
    big -= set(keep)
    return [a for a in addrs if a not in big], bool(big)


async def edges_between(ch, me, xs, ys, f):
    if not xs or not ys:
        return []
    kw, p = _filters(f)
    return await ch.query(q_edges(me, arr(xs), arr(ys), **kw), p)


# ---- /graph ----------------------------------------------------------------------------------------------

async def graph(ch, root, depth, per_node, f):
    """Fan-out-capped neighbourhood. Per node and direction keep the top `per_node` counterparties; depth 2
    expands only the nodes kept at depth 1, hubs never. Hard caps MAX_NODES / MAX_EDGES."""
    nodes, pairs, truncated = {root}, {"from_addr": set(), "to_addr": set()}, False
    frontier = [root]
    for d in range(depth):
        if d:
            frontier, cut = await expandable(ch, frontier)
            truncated |= cut
        nxt = set()
        both = await asyncio.gather(*(top(ch, me, frontier, per_node + 1, f) for me in ("from_addr", "to_addr")))
        for me, kept in zip(("from_addr", "to_addr"), both):
            for a, rows in kept.items():
                if len(rows) > per_node:
                    truncated, rows = True, rows[:per_node]
                for r in rows:
                    if r["other"] not in nodes and len(nodes) >= MAX_NODES:
                        truncated = True
                        continue
                    nodes.add(r["other"]); nxt.add(r["other"]); pairs[me].add((a, r["other"]))
        frontier = sorted(nxt - set(frontier))
        if not frontier:
            break
    edges = []
    for me in ("from_addr", "to_addr"):
        ps = pairs[me]
        rows = await edges_between(ch, me, sorted({a for a, _ in ps}), sorted({b for _, b in ps}), f)
        key = (lambda r: (r["from"], r["to"])) if me == "from_addr" else (lambda r: (r["to"], r["from"]))
        edges += [r for r in rows if key(r) in ps]
    seen = set()
    edges = [e for e in edges if not ((e["from"], e["to"], e["token"]) in seen or seen.add((e["from"], e["to"], e["token"])))]
    edges.sort(key=lambda e: (-int(e["n"]), -int(e["amount_sum"])))
    if len(edges) > MAX_EDGES:
        truncated, edges = True, edges[:MAX_EDGES]
    tokens, addrs, stats = await meta(ch, nodes, {e["token"] for e in edges})
    for e in edges:
        t = tokens.get(e["token"]) or {}
        e.update(symbol=t.get("symbol") or None, decimals=t.get("decimals"), n=int(e["n"]), amount_sum=str(e["amount_sum"]))
    return {"nodes": [node(a, tokens, addrs, stats) for a in sorted(nodes, key=lambda a: a != root)],
            "edges": edges, "truncated": truncated}


# ---- /path -----------------------------------------------------------------------------------------------

def _hop(me, a, r):
    tok, n, amt, f, l = r["top"]
    src, dst = (a, r["other"]) if me == "from_addr" else (r["other"], a)
    return {"from": src, "to": dst, "token": tok, "n": int(n), "amount_sum": str(amt), "first_ts": f, "last_ts": l}


async def path(ch, src, dst, max_hops, per_node, f):
    """Bidirectional BFS over the capped edge lists; stops at max_hops or MAX_EXPLORED nodes. Hubs other
    than the endpoints are reached but never expanded (both sides can still meet on one)."""
    par = {"from_addr": {src: None}, "to_addr": {dst: None}}   # visited + parent hop, per side
    front = {"from_addr": [src], "to_addr": [dst]}
    hops_used, meet, last = 0, None, None
    while hops_used < max_hops and meet is None and (front["from_addr"] or front["to_addr"]):
        live = [m for m in front if front[m]]
        me = min(live, key=lambda m: (len(front[m]), m == last))  # smaller frontier; alternate on ties
        last = me
        other = "to_addr" if me == "from_addr" else "from_addr"
        front[me], _ = await expandable(ch, front[me], keep=(src, dst))
        kept = await top(ch, me, front[me], per_node, f)
        nxt = []
        for a, rows in kept.items():
            for r in rows:
                b = r["other"]
                if b in par[me]:
                    continue
                par[me][b] = (a, _hop(me, a, r)); nxt.append(b)
                if b in par[other]:
                    meet = b; break
            if meet:
                break
        front[me] = nxt; hops_used += 1
        if len(par["from_addr"]) + len(par["to_addr"]) >= MAX_EXPLORED:
            break
    explored = len(par["from_addr"]) + len(par["to_addr"]) - 2
    if meet is None:
        return {"found": False, "hops": [], "explored_nodes": explored}
    hops, x = [], meet
    while par["from_addr"][x]:
        x, h = par["from_addr"][x]; hops.append(h)
    hops.reverse(); x = meet
    while par["to_addr"][x]:
        x, h = par["to_addr"][x]; hops.append(h)
    return {"found": True, "hops": hops, "explored_nodes": explored}


# ---- other endpoints -------------------------------------------------------------------------------------

Q_HEAD = sql("head", "SELECT key, value FROM ingest_state FINAL WHERE key IN ('last_block', 'last_ts')")

Q_ADDR_TOKENS = sql("addr_tokens", f"""
SELECT {HX.format('token')} token, dir, sum(n) n_sum, sum(amount_sum) amt_sum
FROM addr_token WHERE addr = toFixedString(unhex({{a:String}}), 20) GROUP BY token, dir
ORDER BY n_sum DESC LIMIT 40""")


def q_transfers(dirs, token, window, cursor):
    """Newest first over (ts, tx_hash, log_index); one SELECT per direction, unioned when dir=both."""
    where = []
    if token:
        where.append("token = toFixedString(unhex({token:String}), 20)")
    if window:
        where.append("ts >= toDateTime({from_ts:UInt32}) AND ts <= toDateTime({to_ts:UInt32})")
    if cursor:
        where.append("(ts, tx_hash, log_index) < (toDateTime({c_ts:UInt32}), toFixedString(unhex({c_tx:String}), 32), {c_li:UInt32})")
    parts = []
    for me in dirs:
        w = " AND ".join([f"{me} = toFixedString(unhex({{a:String}}), 20)"] + where)
        parts.append(f"""SELECT toUnixTimestamp(ts) ts, block, {HX.format('tx_hash')} tx_hash, log_index, {HX.format('token')} token,
       {HX.format('from_addr')} `from`, {HX.format('to_addr')} `to`, amount, kind
FROM {T_TRANSFERS[me]} WHERE {w} ORDER BY ts DESC, tx_hash DESC, log_index DESC LIMIT {{lim:UInt32}}""")
    body = " UNION ALL ".join(f"({p})" for p in parts) if len(parts) > 1 else parts[0]
    return sql("transfers", f"SELECT * FROM ({body}) ORDER BY ts DESC, tx_hash DESC, log_index DESC LIMIT {{lim:UInt32}}")


Q_TX = sql("tx", f"""
SELECT toUnixTimestamp(ts) ts, block, log_index, {HX.format('token')} token, {HX.format('from_addr')} `from`,
       {HX.format('to_addr')} `to`, amount, kind
FROM {T_TRANSFERS['tx_hash']} WHERE tx_hash = toFixedString(unhex({{h:String}}), 32) ORDER BY log_index""")

Q_TOKEN_SPAN = sql("token_span", """
SELECT toUnixTimestamp(min(first_ts)) first_ts, toUnixTimestamp(max(last_ts)) last_ts, sum(n) n_transfers,
       uniqMerge(holders) holders_seen
FROM token_stats WHERE token = toFixedString(unhex({a:String}), 20)""")

Q_TX_EXISTS = sql("tx_exists", f"SELECT 1 FROM {T_TRANSFERS['tx_hash']} WHERE tx_hash = toFixedString(unhex({{h:String}}), 32) LIMIT 1")

Q_SEARCH_LABELS = sql("search_labels", f"""
SELECT {HX.format('addr')} id, label, type FROM addresses FINAL
WHERE label != '' AND startsWith(lower(label), lower({{q:String}})) ORDER BY label LIMIT 20""")

Q_SEARCH_TOKENS = sql("search_tokens", f"""
SELECT {HX.format('token')} id, symbol, name FROM tokens FINAL
WHERE symbol != '' AND startsWith(lower(symbol), lower({{q:String}})) ORDER BY n_transfers DESC LIMIT 20""")

Q_HIDDEN = sql("hidden", "SELECT concat('0x', lower(hex(token))) token FROM tokens FINAL WHERE lower(symbol) IN {syms:Array(String)}")
