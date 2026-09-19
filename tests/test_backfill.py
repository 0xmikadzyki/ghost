"""Backfill: pagination via next_block, block timestamps joined onto logs, atomic shard files, restart skips
finished shards, a 429 pauses every worker through the shared gate. A fake client stands in for HyperSync."""
import asyncio, gzip, json

import pytest

from rhflow import backfill, stats

TX = "0x" + "ab" * 32
TX2 = "0x" + "cd" * 32


def log(block, tx, idx):
    return {"block_number": block, "transaction_hash": tx, "log_index": idx, "address": "0x" + "11" * 20,
            "topic0": backfill.TRANSFER, "topic1": None, "topic2": None, "topic3": None, "data": "0x"}


class Resp:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body, headers or {}
        self.content = json.dumps(body).encode() if body is not None else b""
        self.text = self.content.decode()
    def json(self):
        return self._body


class FakeHS:
    """Two pages per shard [lo, lo+10000): first page stops at lo+5000 and says next_block."""
    def __init__(self, head=25_000, rate_limit_once=False):
        self.head, self.rl, self.calls = head, rate_limit_once, []
    async def __aenter__(self): return self
    async def __aexit__(self, *a): pass
    async def get(self, url):
        return Resp(200, {"height": self.head})
    async def post(self, url, json):
        self.calls.append(json["from_block"])
        if self.rl:
            self.rl = False
            return Resp(429, headers={"retry-after": "0.01"})
        lo, hi = json["from_block"], json["to_block"]
        mid = lo + 5000
        if lo < mid < hi:
            return Resp(200, {"next_block": mid, "data": [{"blocks": [{"number": lo, "timestamp": 1000 + lo}],
                                                          "logs": [log(lo, TX, 0), log(lo, TX, 1)]}]})
        return Resp(200, {"next_block": hi, "data": [{"blocks": [{"number": lo, "timestamp": 1000 + lo}],
                                                     "logs": [log(lo, TX2, 0)]}]})


@pytest.fixture(autouse=True)
def _reset_stats():
    for k in backfill.STATS: backfill.STATS[k] = 0


def test_shards_pages_and_timestamps(tmp_path):
    out = str(tmp_path)
    r = asyncio.run(backfill.run(out, conc=2, shard=10_000, head_lag=1000, client=FakeHS()))
    # head 25000 - lag 1000 = 24000 -> shards 0, 10000, 20000 (last one is 20000..24000, single page)
    assert r["shards"] == 3 and r["fail"] == 0
    p = backfill.shard_path(out, 10_000)
    rows = [json.loads(l) for l in gzip.open(p, "rt")]
    assert [x["transaction_hash"] for x in rows] == [TX, TX, TX2]
    assert rows[0]["ts"] == 1000 + 10_000 and rows[2]["ts"] == 1000 + 15_000
    assert not list(tmp_path.rglob("*.tmp"))


def test_restart_skips_finished_shards(tmp_path):
    out = str(tmp_path)
    asyncio.run(backfill.run(out, conc=2, head_lag=1000, client=FakeHS()))
    hs = FakeHS()
    asyncio.run(backfill.run(out, conc=2, head_lag=1000, client=hs))
    assert hs.calls == []


def test_429_goes_through_gate_and_retries(tmp_path):
    hs = FakeHS(head=11_000, rate_limit_once=True)
    r = asyncio.run(backfill.run(str(tmp_path), conc=1, head_lag=1000, client=hs))
    assert backfill.STATS["rl"] == 1 and r["fail"] == 0 and r["logs"] == 3
    assert hs.calls[0] == hs.calls[1] == 0          # the rate-limited page was re-requested, not skipped


def test_stats_counts_exact_txs(tmp_path):
    out = str(tmp_path)
    asyncio.run(backfill.run(out, conc=2, head_lag=1000, client=FakeHS()))
    r = stats.run(out, jobs=2)
    assert r == {"shards": 3, "logs": 3 + 3 + 1, "txs": 2 + 2 + 1,
                 "bytes": sum(p.stat().st_size for p in tmp_path.rglob("*.jsonl.gz"))}
