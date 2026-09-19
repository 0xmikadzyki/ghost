#!/usr/bin/env python3
"""Aggregate recon samples into data/sample/analysis.json."""
import json, os, statistics
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__)); OUT = os.path.join(HERE, "data", "sample")
L = lambda n: json.load(open(os.path.join(OUT, n + ".json")))
basics, blocks, code, limits, lmeta = L("basics"), L("blocks"), L("code"), L("limits"), L("logs_meta")
head = basics["head"]; A = {}

# --- block time ---
pr = sorted((int(k), v) for k, v in basics["probes"].items())
A["chain_id"] = basics["chain_id"]; A["head"] = head
A["block1_ts"] = pr[0][1]["ts"]; A["head_ts"] = pr[-1][1]["ts"]
A["span_sec"] = pr[-1][1]["ts"] - pr[0][1]["ts"]
A["avg_block_sec"] = A["span_sec"] / (pr[-1][0] - pr[0][0])
A["blocks_per_day"] = 86400 / A["avg_block_sec"]
A["era_block_sec"] = [{"from": pr[i][0], "to": pr[i+1][0],
                       "sec": round((pr[i+1][1]["ts"] - pr[i][1]["ts"]) / (pr[i+1][0] - pr[i][0]), 4)}
                      for i in range(len(pr) - 1)]

# --- density ---
n = [len(b["txs"]) for b in blocks]
n.sort()
q = lambda p: n[min(len(n) - 1, int(len(n) * p))]
A["blocks_sampled"] = len(blocks)
A["empty_block_share"] = sum(1 for x in n if x == 0) / len(n)
A["tx_per_block"] = {"min": n[0], "median": statistics.median(n), "mean": statistics.fmean(n),
                     "p95": q(0.95), "p99": q(0.99), "max": n[-1]}
A["est_total_tx"] = statistics.fmean(n) * head
alltx = [t for b in blocks for t in b["txs"]]
A["tx_total_sampled"] = len(alltx)
A["tx_types"] = dict(Counter(t["type"] for t in alltx))
to_contract = sum(1 for t in alltx if t["to"] and code.get(t["to"]))
to_eoa = sum(1 for t in alltx if t["to"] and not code.get(t["to"]))
A["tx_to_contract"] = to_contract; A["tx_to_eoa"] = to_eoa
A["tx_create"] = sum(1 for t in alltx if not t["to"])
A["tx_to_contract_share"] = to_contract / max(1, len(alltx))
A["tx_with_input"] = sum(1 for t in alltx if t["input"]) / max(1, len(alltx))
A["blocks_with_contract_tx"] = sum(1 for b in blocks if any(t["to"] and code.get(t["to"]) for t in b["txs"])) / len(blocks)
A["top_tx_to"] = Counter(t["to"] for t in alltx if t["to"]).most_common(15)
A["top_tx_from"] = Counter(t["from"] for t in alltx).most_common(15)

# --- transfer logs ---
tok = Counter(); src = Counter(); dst = Counter(); pairs = Counter()
indeg = defaultdict(set); outdeg = defaultdict(set)
ntopics = Counter(); mint = Counter(); burn = Counter(); total = 0
ZERO = "0x" + "00" * 32
tok_txs = defaultdict(set)
for line in open(os.path.join(OUT, "logs.jsonl")):
    bn, addr, t1, t2, data, nt, txh = json.loads(line)
    total += 1; ntopics[nt] += 1; tok[addr] += 1; tok_txs[addr].add(txh)
    if t1 is None or t2 is None: continue
    f = "0x" + t1[-40:]; t = "0x" + t2[-40:]
    src[f] += 1; dst[t] += 1; pairs[(f, t)] += 1
    outdeg[f].add(t); indeg[t].add(f)
    if t1 == ZERO: mint[addr] += 1
    if t2 == ZERO: burn[addr] += 1
A["log_total"] = total
A["log_blocks_sampled"] = lmeta["sampled_blocks"]
A["log_windows"] = len(lmeta["windows"]); A["log_span"] = lmeta["span"]
A["transfers_per_block"] = total / lmeta["sampled_blocks"]
A["est_total_transfers"] = total / lmeta["sampled_blocks"] * head
A["topics_count_dist"] = dict(ntopics)
A["unique_token_contracts"] = len(tok)
A["top20_tokens"] = [{"addr": a, "events": c, "txs": len(tok_txs[a])} for a, c in tok.most_common(20)]
A["unique_senders"] = len(src); A["unique_receivers"] = len(dst)
A["unique_addresses"] = len(set(src) | set(dst))
tot_dir = sum(src.values())
A["top5_sender_share"] = sum(c for _, c in src.most_common(5)) / tot_dir
A["top5_receiver_share"] = sum(c for _, c in dst.most_common(5)) / tot_dir
A["top20_sender_share"] = sum(c for _, c in src.most_common(20)) / tot_dir
touched = set(src) | set(dst)
A["top5_participant_share"] = sum(c for _, c in Counter({a: src[a] + dst[a] for a in touched}).most_common(5)) / (2 * tot_dir)
A["top_senders"] = src.most_common(10); A["top_receivers"] = dst.most_common(10)
A["mint_events"] = sum(mint.values()); A["burn_events"] = sum(burn.values())
A["top_minters"] = mint.most_common(5); A["top_burners"] = burn.most_common(5)
multi = [a for a in touched if len(indeg[a]) > 1 and len(outdeg[a]) > 1]
A["multi_in_out_nodes"] = len(multi)
A["multi_in_out_share"] = len(multi) / max(1, len(touched))
A["nodes_deg1"] = sum(1 for a in touched if len(indeg[a]) + len(outdeg[a]) <= 1) / max(1, len(touched))
A["unique_pairs"] = len(pairs)
A["top_pairs"] = [{"from": f, "to": t, "n": c} for (f, t), c in pairs.most_common(10)]
A["limits"] = limits; A["ratelimit"] = L("ratelimit"); A["rpc_stats"] = L("rpc_stats")
json.dump(A, open(os.path.join(OUT, "analysis.json"), "w"), indent=1)
for k, v in A.items():
    if k in ("limits", "ratelimit"): continue
    print(k, "=", json.dumps(v)[:400])
