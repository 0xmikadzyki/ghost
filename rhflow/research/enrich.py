#!/usr/bin/env python3
import asyncio, json, os, time
import httpx
from .recon import RPC, OUT, batch, TRANSFER, hx, log

A = json.load(open(os.path.join(OUT, "analysis.json")))
head = A["head"]
SIG = {"symbol": "0x95d89b41", "name": "0x06fdde03", "decimals": "0x313ce567", "totalSupply": "0x18160ddd"}

def dec_str(h):
    if not h or h == "0x": return None
    b = bytes.fromhex(h[2:])
    if len(b) >= 64:
        try:
            off = int.from_bytes(b[:32], "big"); ln = int.from_bytes(b[off:off+32], "big")
            return b[off+32:off+32+ln].decode("utf-8", "replace")
        except Exception: pass
    return b.rstrip(b"\x00").decode("utf-8", "replace")

async def main():
    out = {}
    async with httpx.AsyncClient(timeout=120.0, headers={"content-type": "application/json"}) as c:
        sem = asyncio.Semaphore(3)
        # 1. getLogs limit: result-count vs block-span
        gl = {}
        for span in (2000, 3000, 5000, 10000):
            r = await batch(c, sem, [("eth_getLogs", [{"fromBlock": hex(head-3000-span), "toBlock": hex(head-3001),
                                                       "topics": [TRANSFER]}])], tries=2, allow_error=True)
            d = r[0]
            gl[span] = {"error": str(d["error"])[:150]} if "error" in d else {"logs": len(d["result"])}
            log("span", span, gl[span])
        # single-token filter over a huge span (address filter reduces result count)
        top = A["top20_tokens"][0]["addr"]
        for span in (10000, 100000, 1000000):
            r = await batch(c, sem, [("eth_getLogs", [{"fromBlock": hex(head-3000-span), "toBlock": hex(head-3001),
                                                       "address": top, "topics": [TRANSFER]}])], tries=2, allow_error=True)
            d = r[0]
            gl[f"addr_{span}"] = {"error": str(d["error"])[:150]} if "error" in d else {"logs": len(d["result"])}
            log("addr span", span, gl[f"addr_{span}"])
        out["getlogs_limit"] = gl
        # 2. archive depth for state
        arch = {}
        for back in (0, 100, 1000, 10000, 100000, 1000000, 10000000):
            r = await batch(c, sem, [("eth_getBalance", ["0x0000000000000000000000000000000000000064", hex(head-back)])],
                            tries=2, allow_error=True)
            d = r[0]
            arch[back] = "ok" if "error" not in d else str(d["error"])[:80]
            log("state depth -", back, arch[back])
        out["state_archive"] = arch
        # 3. identify top tokens + top hubs
        addrs = [t["addr"] for t in A["top20_tokens"]] + [a for a, _ in A["top_senders"]] + \
                [a for a, _ in A["top_receivers"]] + [a for a, _ in A["top_tx_to"]]
        addrs = [a for a in dict.fromkeys(addrs) if a != "0x0000000000000000000000000000000000000000"]
        calls = []
        for a in addrs:
            for k in ("symbol", "name", "decimals"):
                calls.append(("eth_call", [{"to": a, "data": SIG[k]}, "latest"]))
        r = await batch(c, sem, calls, allow_error=True)
        meta = {}
        for i, a in enumerate(addrs):
            s, nm, dc = (r[3*i+j] for j in range(3))
            meta[a] = {"symbol": dec_str(s.get("result")) if "error" not in s else None,
                       "name": dec_str(nm.get("result")) if "error" not in nm else None,
                       "decimals": hx(dc["result"]) if "error" not in dc and dc.get("result", "0x") != "0x" else None}
        out["token_meta"] = meta
        # 4. blockscout labels
        bs = {}
        for a in addrs[:40]:
            try:
                rr = await c.get(f"https://robinhoodchain.blockscout.com/api/v2/addresses/{a}")
                if rr.status_code == 200:
                    j = rr.json()
                    bs[a] = {k: j.get(k) for k in ("name", "is_contract", "is_verified", "implementations", "public_tags")}
            except Exception as e:
                bs[a] = {"err": str(e)[:80]}
            await asyncio.sleep(0.15)
        out["blockscout"] = bs
    json.dump(out, open(os.path.join(OUT, "enrich.json"), "w"), indent=1)
    print(json.dumps(out, indent=1)[:6000])

asyncio.run(main())
