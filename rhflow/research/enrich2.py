#!/usr/bin/env python3
import asyncio, json, os
import httpx
from .recon import RPC, OUT, batch, TRANSFER, hx, log
SIG = {"symbol": "0x95d89b41", "name": "0x06fdde03", "decimals": "0x313ce567"}
def dec_str(h):
    if not h or h == "0x": return None
    b = bytes.fromhex(h[2:])
    if len(b) >= 64:
        try:
            off = int.from_bytes(b[:32], "big"); ln = int.from_bytes(b[off:off+32], "big")
            if 0 < ln <= 128: return b[off+32:off+32+ln].decode("utf-8", "replace")
        except Exception: pass
    return b.rstrip(b"\x00").decode("utf-8", "replace")
A = json.load(open(os.path.join(OUT, "analysis.json"))); head = A["head"]
E = {"getlogs_limit": {"2000": "exceeds limit of 10000", "3000": "exceeds limit of 10000",
  "5000": "exceeds limit of 10000", "10000": "exceeds limit of 10000",
  "addr_10000": "exceeds limit of 10000", "addr_100000": "exceeds limit of 10000",
  "addr_1000000": "exceeds limit of 10000"},
 "state_archive": {"0": "ok/flaky", "100": "ok/flaky", "1000": "metadata is not found",
  "10000": "metadata is not found", "100000": "metadata is not found",
  "1000000": "metadata is not found", "10000000": "metadata is not found"}}

async def main():
    async with httpx.AsyncClient(timeout=180.0, headers={"content-type": "application/json"}) as c:
        sem = asyncio.Semaphore(1)
        # refine span limit between 1000 and 2000
        for span in (1000, 1200, 1500, 1800, 2000):
            r = await batch(c, sem, [("eth_getLogs", [{"fromBlock": hex(head-5000-span), "toBlock": hex(head-5001),
                                                       "topics": [TRANSFER]}])], tries=3, allow_error=True)
            d = r[0]
            E["getlogs_limit"][f"refine_{span}"] = {"error": str(d["error"])[:120]} if "error" in d else {"logs": len(d["result"])}
            log("refine", span, E["getlogs_limit"][f"refine_{span}"])
            await asyncio.sleep(1)
        addrs = [t["addr"] for t in A["top20_tokens"]] + [a for a, _ in A["top_senders"]] + \
                [a for a, _ in A["top_receivers"]] + [a for a, _ in A["top_tx_to"]]
        addrs = [a for a in dict.fromkeys(addrs) if a != "0x0000000000000000000000000000000000000000"]
        meta = {}
        for i in range(0, len(addrs), 6):
            ch = addrs[i:i+6]
            calls = [("eth_call", [{"to": a, "data": SIG[k]}, "latest"]) for a in ch for k in ("symbol", "name", "decimals")]
            r = await batch(c, sem, calls, allow_error=True)
            for j, a in enumerate(ch):
                s, nm, dc = (r[3*j+k] for k in range(3))
                meta[a] = {"symbol": dec_str(s.get("result")) if "error" not in s else None,
                           "name": dec_str(nm.get("result")) if "error" not in nm else None,
                           "decimals": hx(dc["result"]) if "error" not in dc and dc.get("result", "0x") not in (None, "0x") else None}
            log("meta", len(meta), "/", len(addrs))
            await asyncio.sleep(1.5)
        E["token_meta"] = meta
        bs = {}
        for a in addrs:
            try:
                rr = await c.get(f"https://robinhoodchain.blockscout.com/api/v2/addresses/{a}")
                if rr.status_code == 200:
                    j = rr.json()
                    bs[a] = {"name": j.get("name"), "is_contract": j.get("is_contract"),
                             "is_verified": j.get("is_verified"),
                             "impl": [x.get("name") for x in (j.get("implementations") or [])],
                             "tags": [t.get("display_name") for t in (j.get("public_tags") or [])]}
                else: bs[a] = {"http": rr.status_code}
            except Exception as e: bs[a] = {"err": str(e)[:60]}
            await asyncio.sleep(0.2)
        E["blockscout"] = bs
    json.dump(E, open(os.path.join(OUT, "enrich.json"), "w"), indent=1)
    for a in [t["addr"] for t in A["top20_tokens"]]:
        print("TOK", a, E["token_meta"].get(a), E["blockscout"].get(a))
    print("---hubs---")
    for a, n in A["top_senders"] + A["top_tx_to"]:
        if a == "0x0000000000000000000000000000000000000000": continue
        print("HUB", a, n, E["token_meta"].get(a), E["blockscout"].get(a))

asyncio.run(main())
