"""Metadata: token symbol/decimals/name via eth_call, is_contract via eth_getCode, hand-checked labels.

Public RPC limits (docs/RECON.md §4, measured): ~50 req/s safe, batches of ≤50 `eth_call` (100 → 429 on
the whole batch), no Retry-After. Batches here run sequentially, one in flight — far under the ceiling.
Failures are stored too (source='eth_call:err') so a dead token is not retried on every run.
"""
import json, re, sys, time
from pathlib import Path

from .env import env

# keccak256(sig)[:4]; computed once with pycryptodome (not a dependency of the main package)
SYMBOL = "0x95d89b41"      # symbol()
DECIMALS = "0x313ce567"    # decimals()
NAME = "0x06fdde03"        # name()
BATCH = 50                 # measured batch ceiling for heavy methods
LABELS = Path(__file__).with_name("labels.json")


class RPC:
    """One JSON-RPC batch per call; 429/5xx → sleep and retry."""
    def __init__(self, url=None, timeout=120):
        import httpx
        self.url = url or env("RPC_URL", "https://rpc.mainnet.chain.robinhood.com")
        self.c = httpx.Client(timeout=timeout, headers={"content-type": "application/json"})

    def call(self, calls, tries=6):
        """calls: [(method, params)] → [result dict per call, in order]; errors as {"error": ...}."""
        body = [{"jsonrpc": "2.0", "id": i, "method": m, "params": p} for i, (m, p) in enumerate(calls)]
        delay, last = 1.0, "?"
        for _ in range(tries):
            try:
                r = self.c.post(self.url, json=body)
                if r.status_code == 200:
                    d = json.loads(r.text, strict=False)
                    by = {x["id"]: x for x in (d if isinstance(d, list) else [d])}
                    return [by.get(i, {"error": "missing"}) for i in range(len(calls))]
                last = f"HTTP {r.status_code}"
            except Exception as e:
                last = f"{type(e).__name__}: {str(e)[:100]}"
            time.sleep(delay); delay = min(delay * 2, 30)
        raise RuntimeError(f"rpc gave up: {last}")


# ---- ABI decoding ----

def _clean(b):
    return b.decode("utf-8", "replace").replace("\x00", "").strip()


def decode_string(hexres):
    """ABI `string` (offset, len, bytes) or `bytes32` (old tokens). None if unusable."""
    if not hexres or not hexres.startswith("0x") or len(hexres) < 66:
        return None
    b = bytes.fromhex(hexres[2:])
    if len(b) == 32:                                           # bytes32 symbol (MKR style)
        return _clean(b.rstrip(b"\x00"))
    off = int.from_bytes(b[:32], "big")
    if off + 32 > len(b):
        return None
    n = int.from_bytes(b[off:off + 32], "big")
    if n > len(b) - off - 32:
        return None
    return _clean(b[off + 32:off + 32 + n])


def decode_uint8(hexres):
    if not hexres or not hexres.startswith("0x") or len(hexres) != 66:
        return None
    v = int(hexres[2:], 16)
    return v if v <= 255 else None


def _res(x):
    return None if "error" in x else x.get("result")


def token_row(addr, sym, dec, name):
    """Three eth_call results → tokens row. Any missing field ⇒ error row, still stored."""
    s, d, n = decode_string(_res(sym)), decode_uint8(_res(dec)), decode_string(_res(name))
    ok = s is not None and d is not None
    return {"token": addr, "symbol": s or "", "decimals": d or 0, "name": n or "",
            "source": "eth_call" if ok else "eth_call:err", "fetched_at": int(time.time())}


# INSERT targets: the address column is unhexed server-side (FixedString(20)); the rest passes through
TOKENS_T = ("tokens (token, symbol, decimals, name, kind, n_transfers, source, fetched_at) "
            "SELECT unhex(substring(token, 3)), symbol, decimals, name, kind, n_transfers, source, fetched_at "
            "FROM input('token String, symbol String, decimals UInt8, name String, kind String, n_transfers UInt64, "
            "source String, fetched_at UInt32')")
ADDRS_T = ("addresses (addr, is_contract, label, type, source, fetched_at) "
           "SELECT unhex(substring(addr, 3)), is_contract, label, type, source, fetched_at "
           "FROM input('addr String, is_contract UInt8, label String, type String, source String, fetched_at UInt32')")


def enrich_tokens(ch, rpc, top=1000):
    """symbol/decimals/name for the `top` most-transferred tokens with no row in `tokens`; the row also carries
    the exact transfer count (from `edges`) and kind (erc20/erc721 as the backfill decoded it — one pass over
    `transfers`, measured 20 s on the full chain)."""
    todo = {r["token"]: int(r["n"]) for r in ch.query(
        "SELECT '0x' || lower(hex(token)) AS token, sum(n) AS n FROM edges "
        "WHERE token NOT IN (SELECT token FROM tokens) GROUP BY token ORDER BY n DESC LIMIT {top:UInt64}",
        {"top": top})}
    kind = {r["token"]: r["kind"] for r in ch.query(
        "SELECT '0x' || lower(hex(token)) AS token, any(kind) AS kind FROM transfers WHERE token IN ("
        + ",".join(f"unhex('{t[2:]}')" for t in todo) + ") GROUP BY token")} if todo else {}
    todo = list(todo.items())
    per = BATCH // 3; done = err = 0
    for i in range(0, len(todo), per):
        chunk = todo[i:i + per]
        calls = [("eth_call", [{"to": a, "data": s}, "latest"]) for a, _ in chunk for s in (SYMBOL, DECIMALS, NAME)]
        r = rpc.call(calls)
        rows = [{**token_row(a, *r[j * 3:j * 3 + 3]), "kind": kind.get(a, "erc20"), "n_transfers": n}
                for j, (a, n) in enumerate(chunk)]
        ch.insert_rows(TOKENS_T, rows)
        done += len(rows); err += sum(x["source"].endswith("err") for x in rows)
        print(f"[tokens] {done}/{len(todo)} err={err}", file=sys.stderr, flush=True)
    return done, err


def enrich_addresses(ch, rpc, min_n=100):
    """eth_getCode → is_contract for every address with n_in + n_out ≥ min_n and no `addresses` row."""
    todo = [r["addr"] for r in ch.query(
        "SELECT '0x' || lower(hex(addr)) AS addr FROM addr_stats WHERE addr NOT IN (SELECT addr FROM addresses) "
        "GROUP BY addr HAVING sum(n_in) + sum(n_out) >= {m:UInt64}", {"m": min_n})]
    done = 0
    for i in range(0, len(todo), BATCH):
        chunk = todo[i:i + BATCH]
        r = rpc.call([("eth_getCode", [a, "latest"]) for a in chunk])
        now = int(time.time())
        rows = [{"addr": a, "is_contract": int(len(_res(r[j]) or "0x") > 2), "label": "", "type": "",
                 "source": "eth_getCode" if "error" not in r[j] else "eth_getCode:err", "fetched_at": now}
                for j, a in enumerate(chunk)]
        ch.insert_rows(ADDRS_T, rows)
        done += len(rows)
        print(f"[addresses] {done}/{len(todo)}", file=sys.stderr, flush=True)
    return done


def load_labels(ch, path=LABELS):
    """Upsert labels; keeps a known is_contract (ReplacingMergeTree replaces the whole row)."""
    labels = json.loads(Path(path).read_text())
    addrs = [l["addr"].lower() for l in labels]
    if any(not re.fullmatch(r"0x[0-9a-f]{40}", a) for a in addrs):
        raise SystemExit(f"{path}: malformed address")
    known = {r["addr"]: r["is_contract"] for r in ch.query(
        "SELECT '0x' || lower(hex(addr)) AS addr, is_contract FROM addresses "
        "WHERE addr IN arrayMap(x -> unhex(substring(x, 3)), {a:Array(String)})",
        {"a": "[" + ",".join(f"'{x}'" for x in addrs) + "]"})}
    now = int(time.time())
    rows = [{"addr": a, "is_contract": int(l.get("is_contract", known.get(a, 0))), "label": l["label"],
             "type": l["type"], "source": l["source"], "fetched_at": now} for a, l in zip(addrs, labels)]
    ch.insert_rows(ADDRS_T, rows)
    return len(rows)


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(prog="rhflow enrich")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("tokens"); p.add_argument("--top", type=int, default=1000)
    p = sub.add_parser("addresses"); p.add_argument("--min-n", type=int, default=100)
    p = sub.add_parser("labels"); p.add_argument("--path", default=str(LABELS))
    a = ap.parse_args(argv)
    from .ch import CH
    ch = CH()
    if a.cmd == "tokens":
        enrich_tokens(ch, RPC(), a.top)
    elif a.cmd == "addresses":
        enrich_addresses(ch, RPC(), a.min_n)
    else:
        print(f"[labels] {load_labels(ch, a.path)} rows", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
