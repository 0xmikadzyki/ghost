import json, os, time, httpx
OUT="data/sample"; A=json.load(open(f"{OUT}/analysis.json")); E=json.load(open(f"{OUT}/enrich.json"))
UA={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36","Accept":"application/json"}
B="https://robinhoodchain.blockscout.com/api/v2"
c=httpx.Client(headers=UA, timeout=30, follow_redirects=True)
out={}
for ep in ("stats","stats/charts/transactions"):
    r=c.get(f"{B}/{ep}"); out[ep]=r.json() if r.status_code==200 else {"http":r.status_code,"body":r.text[:200]}
    print(ep, json.dumps(out[ep])[:600])
addrs=[t["addr"] for t in A["top20_tokens"]]+[a for a,_ in A["top_senders"]]+[a for a,_ in A["top_receivers"]]+[a for a,_ in A["top_tx_to"]]
addrs=[a for a in dict.fromkeys(addrs) if a!="0x"+"0"*40]
lab={}
for a in addrs:
    r=c.get(f"{B}/addresses/{a}")
    if r.status_code==200:
        j=r.json()
        lab[a]={"name":j.get("name"),"is_contract":j.get("is_contract"),"is_verified":j.get("is_verified"),
                "impl":[x.get("name") for x in (j.get("implementations") or [])],
                "tags":[t.get("display_name") for t in (j.get("public_tags") or [])],
                "tx_count":j.get("transactions_count") or j.get("transaction_count")}
    else: lab[a]={"http":r.status_code}
    time.sleep(0.25)
E["blockscout"]=lab; E["bs_stats"]=out
json.dump(E,open(f"{OUT}/enrich.json","w"),indent=1)
print("--- labels ---")
for a,v in lab.items(): print(a, json.dumps(v, ensure_ascii=False))
