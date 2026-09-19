#!/usr/bin/env python3
import asyncio, json, os, httpx
from collections import Counter, defaultdict
from Crypto.Hash import keccak
from .recon import OUT, batch, hx, log
def sel(s):
    k=keccak.new(digest_bits=256); k.update(s.encode()); return "0x"+k.hexdigest()[:8]
A=json.load(open(f"{OUT}/analysis.json")); E=json.load(open(f"{OUT}/enrich.json"))

# --- log-side extras: address persistence across far-apart windows ---
span=A["log_span"]; wins=sorted(json.load(open(f"{OUT}/logs_meta.json"))["windows"])
def win_of(bn):
    import bisect; i=bisect.bisect_right(wins,bn)-1
    return wins[i] if i>=0 else None
seen=defaultdict(set); cnt=Counter(); tokwin=defaultdict(set)
for line in open(f"{OUT}/logs.jsonl"):
    bn,addr,t1,t2,data,nt,txh=json.loads(line)
    w=win_of(bn); tokwin[addr].add(w)
    for t in (t1,t2):
        if t: a="0x"+t[-40:]; seen[a].add(w); cnt[a]+=1
seen.pop("0x"+"0"*40, None)
multi_win=sum(1 for a,w in seen.items() if len(w)>1)
tot=sum(cnt.values())
top100=sum(c for _,c in cnt.most_common(100))
X={"addr_in_multiple_windows":multi_win,"addr_total":len(seen),
   "addr_multiwindow_share":multi_win/len(seen),
   "top100_addr_share":top100/tot,
   "tokens_in_multiple_windows":sum(1 for a,w in tokwin.items() if len(w)>1),
   "tokens_total":len(tokwin),
   "transfers_per_addr_mean":tot/len(seen)}
print(json.dumps(X,indent=1))

# --- bridge probe on tokens ---
toks=[t["addr"] for t in A["top20_tokens"]]
async def main():
    out={}
    async with httpx.AsyncClient(timeout=120.0, headers={"content-type":"application/json"}) as c:
        sem=asyncio.Semaphore(1)
        P=["l1Address()","l2Gateway()","bridge()","gateway()","minter()","owner()"]
        for i in range(0,len(toks),2):
            ch=toks[i:i+2]
            calls=[("eth_call",[{"to":a,"data":sel(s)},"latest"]) for a in ch for s in P]
            r=await batch(c,sem,calls,allow_error=True)
            for j,a in enumerate(ch):
                d={}
                for k,s in enumerate(P):
                    v=r[j*len(P)+k]
                    if "error" not in v and v.get("result","0x")!="0x":
                        d[s]="0x"+v["result"][-40:] if len(v["result"])==66 else v["result"][:66]
                out[a]=d; log(a,d)
            await asyncio.sleep(1)
        # arbitrum system contracts activity
        sysr=await batch(c,sem,[("eth_getCode",[a,"latest"]) for a in
            ["0x0000000000000000000000000000000000000064","0x000000000000000000000000000000000000006E",
             "0x0000000000000000000000000000000000000066","0x00000000000000000000000000000000000a4b05"]],allow_error=True)
        out["_sys"]={a:len(sysr[i].get("result","0x")) for i,a in enumerate(
            ["ArbSys_0x64","ArbRetryableTx_0x6e","ArbGasInfo_0x66","ArbOS_0xa4b05"])}
        log(out["_sys"])
    E["token_bridge_probe"]=out; E["log_extras"]=X
    json.dump(E,open(f"{OUT}/enrich.json","w"),indent=1)
asyncio.run(main())
