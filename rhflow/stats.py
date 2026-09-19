"""Exact counts over a backfilled shard directory: shards, logs, transactions, bytes.

A transaction lives in one block and a block lives in one shard, so the sum of
per-shard distinct tx hashes is the exact transaction count.
"""
import gzip, os, sys
from multiprocessing import Pool


def count_shard(path):
    n, tx = 0, set()
    with gzip.open(path, "rt") as f:
        for line in f:
            n += 1
            tx.add(line.split('"transaction_hash":"', 1)[1][:66])
    return n, len(tx), os.path.getsize(path)


def shards(out):
    for root, _, files in os.walk(out):
        for fn in files:
            if fn.endswith(".jsonl.gz"):
                yield os.path.join(root, fn)


def run(out, jobs=None):
    paths = sorted(shards(out))
    with Pool(jobs) as p:
        rows = p.map(count_shard, paths, chunksize=4)
    logs, txs, size = (sum(c) for c in zip(*rows)) if rows else (0, 0, 0)
    r = {"shards": len(paths), "logs": logs, "txs": txs, "bytes": size}
    print(f"shards={r['shards']:,} logs={logs:,} txs={txs:,} logs/tx={logs/max(txs,1):.2f} "
          f"size={size/1e9:.1f} GB", file=sys.stderr, flush=True)
    return r
