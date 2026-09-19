#!/usr/bin/env python3
"""On-chain fingerprint of top hub addresses (no explorer needed)."""
import asyncio, json, os, httpx
from Crypto.Hash import keccak
from .recon import OUT, batch, hx, log

def sel(sig):
    k = keccak.new(digest_bits=256); k.update(sig.encode()); return "0x" + k.hexdigest()[:8]

PROBES = ["token0()", "token1()", "factory()", "getReserves()", "fee()", "slot0()",
          "WETH()", "owner()", "getOwners()", "l1Address()", "l2Gateway()",
          "counterpartGateway()", "router()", "symbol()", "totalSupply()",
          "entryPoint()", "asset()", "poolManager()", "allPairsLength()", "bridge()"]
IMPL_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
A = json.load(open(os.path.join(OUT, "analysis.json")))
E = json.load(open(os.path.join(OUT, "enrich.json")))
CAND = [a for a, _ in A["top_senders"] + A["top_receivers"] + A["top_tx_to"] + A["top_tx_from"]]
CAND += [p["from"] for p in A["top_pairs"]] + [p["to"] for p in A["top_pairs"]]
CAND = [a for a in dict.fromkeys(CAND) if a != "0x" + "0" * 40]
DONE = E.get("fingerprint", {})
CAND = [a for a in CAND if a not in DONE]

async def main():
    res = dict(DONE)
    async with httpx.AsyncClient(timeout=120.0, headers={"content-type": "application/json"}) as c:
        sem = asyncio.Semaphore(1)
        for i in range(0, len(CAND), 2):
            ch = CAND[i:i+2]
            calls = []
            for a in ch:
                calls.append(("eth_getCode", [a, "latest"]))
                calls.append(("eth_getStorageAt", [a, IMPL_SLOT, "latest"]))
                calls += [("eth_call", [{"to": a, "data": sel(s)}, "latest"]) for s in PROBES]
            r = await batch(c, sem, calls, allow_error=True)
            per = 2 + len(PROBES)
            for j, a in enumerate(ch):
                base = j * per
                codelen = (len(r[base].get("result", "0x")) - 2) // 2
                impl = r[base+1].get("result", "0x" + "0"*64)
                ok = [PROBES[k] for k in range(len(PROBES))
                      if "error" not in r[base+2+k] and r[base+2+k].get("result", "0x") != "0x"]
                res[a] = {"code_bytes": codelen, "proxy_impl": ("0x" + impl[-40:]) if impl and int(impl, 16) else None,
                          "responds": ok}
                log(a, codelen, res[a]["proxy_impl"], ",".join(ok)[:120])
            E["fingerprint"] = res
            json.dump(E, open(os.path.join(OUT, "enrich.json"), "w"), indent=1)
            await asyncio.sleep(0.8)
    E["fingerprint"] = res
    json.dump(E, open(os.path.join(OUT, "enrich.json"), "w"), indent=1)

asyncio.run(main())
