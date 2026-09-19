"""Tail: state advances only after a successful insert, windows of max_blocks, pages inserted one by one,
a 429 goes through the shared gate, ClickHouse down leaves the state alone. Fake HyperSync and fake CH."""
import asyncio, json

import pytest

from rhflow import backfill, tail

TX = "0x" + "ab" * 32
T1 = "0x" + "00" * 12 + "11" * 20
T2 = "0x" + "00" * 12 + "22" * 20


def log(block, idx, topic3=None, topic1=T1):
    return {"block_number": block, "transaction_hash": TX, "log_index": idx, "address": "0x" + "33" * 20,
            "topic0": backfill.TRANSFER, "topic1": topic1, "topic2": T2, "topic3": topic3, "data": "0x" + "00" * 31 + "05"}


class Resp:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body, headers or {}
        self.content = json.dumps(body).encode() if body is not None else b""
        self.text = self.content.decode()
    def json(self): return self._body


class FakeHS:
    """Pages of 5000 blocks; each page carries one log at its first block."""
    def __init__(self, head=25_000, rate_limit_once=False, page=5000):
        self.head, self.rl, self.page, self.calls = head, rate_limit_once, page, []
    async def __aenter__(self): return self
    async def __aexit__(self, *a): pass
    async def get(self, url): return Resp(200, {"height": self.head})
    async def post(self, url, json):
        lo, hi = json["from_block"], json["to_block"]; self.calls.append((lo, hi))
        if self.rl:
            self.rl = False
            return Resp(429, headers={"retry-after": "0.01"})
        nxt = min(lo + self.page, hi)
        return Resp(200, {"next_block": nxt, "data": [{"blocks": [{"number": lo, "timestamp": hex(1000 + lo)}],
                                                       "logs": [log(lo, 0), log(lo, 1, topic1=None)]}]})


class FakeCH:
    def __init__(self, last=None, fail_inserts=0):
        self.state, self.inserts, self.fail = last, [], fail_inserts
    def query(self, sql, params=None):
        assert "ingest_state" in sql and params["k"] == "last_block"
        return [{"v": self.state}]
    def insert_rows(self, table, rows):
        if self.fail:
            self.fail -= 1; raise RuntimeError("clickhouse HTTP 503: down")
        if table == "ingest_state":
            self.state = int(rows[0]["value"]); return
        assert table.startswith("transfers SELECT") and "unhex" in table
        self.inserts.append((self.state, rows))


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    async def fast(_): pass
    monkeypatch.setattr(tail.asyncio, "sleep", fast)


def test_state_advances_per_page_only_after_insert():
    ch, hs = FakeCH(last=9_999), FakeHS(head=25_000)
    asyncio.run(tail.run(ch, client=hs, max_blocks=20_000, once=True))
    # head 25000 - 1000 = 24000 exclusive: blocks 10000..23999 in one window, three pages
    assert hs.calls == [(10_000, 24_000), (15_000, 24_000), (20_000, 24_000)]
    assert [s for s, _ in ch.inserts] == [9_999, 14_999, 19_999]   # state seen at insert time = previous page
    assert ch.state == 23_999
    rows = ch.inserts[0][1]
    assert len(rows) == 1 and rows[0]["topic1"] == T1 and rows[0]["ts"] == 11_000  # topic1=None skipped


def test_window_splitting():
    ch, hs = FakeCH(last=-1), FakeHS(head=31_000, page=100_000)
    asyncio.run(tail.run(ch, client=hs, max_blocks=10_000, once=True))
    assert hs.calls == [(0, 10_000), (10_000, 20_000), (20_000, 30_000)]
    assert ch.state == 29_999


def test_nothing_new_and_missing_state():
    ch, hs = FakeCH(last=23_999), FakeHS(head=25_000)
    asyncio.run(tail.run(ch, client=hs, once=True))
    assert hs.calls == [] and ch.inserts == [] and ch.state == 23_999
    with pytest.raises(SystemExit):
        asyncio.run(tail.run(FakeCH(last=None), client=FakeHS(), once=True))
    ch = FakeCH(last=None)
    asyncio.run(tail.run(ch, client=FakeHS(head=6_000), start=1_000, once=True))
    assert ch.state == 4_999


def test_429_goes_through_gate():
    for k in backfill.STATS: backfill.STATS[k] = 0
    ch, hs = FakeCH(last=-1), FakeHS(head=6_000, rate_limit_once=True)
    asyncio.run(tail.run(ch, client=hs, once=True))
    assert backfill.STATS["rl"] == 1 and hs.calls[0] == hs.calls[1] == (0, 5_000) and ch.state == 4_999


def test_ch_down_does_not_advance():
    ch, hs = FakeCH(last=-1, fail_inserts=1), FakeHS(head=6_000)
    with pytest.raises(RuntimeError):
        asyncio.run(tail.run(ch, client=hs, once=True))
    assert ch.state == -1 and ch.inserts == []
    # the loop (not --once) retries and then succeeds; the page is re-fetched, not skipped
    ch, hs = FakeCH(last=-1, fail_inserts=1), FakeHS(head=6_000)
    class Stop(Exception): pass
    n = {"n": 0}
    async def sleep(_):
        n["n"] += 1
        if n["n"] > 3: raise Stop
    tail.asyncio.sleep = sleep
    with pytest.raises(Stop):
        asyncio.run(tail.run(ch, client=hs))
    assert ch.state == 4_999 and hs.calls[:2] == [(0, 5_000), (0, 5_000)]
