"""Store: batch planning is dir-aligned, a rerun skips finished batches and retracts the unfinished one
before redoing it, init applies every statement. A fake CH records SQL — no network."""
import pytest

from rhflow import store


class FakeCH:
    def __init__(self, done=()):
        self.sql, self.rows, self.done = [], [], set(done)

    def execute(self, sql, params=None):
        self.sql.append(sql)

    def query(self, sql, params=None):
        if "addr_stats" in sql:
            return [{"addr": "0x" + "ab" * 20}]
        if "ingest_state" in sql:
            return [{"key": k} for k in self.done]
        if "system.parts" in sql:
            return [{"n": 5}]
        return [{"b": 61_999_999}]

    def insert_rows(self, table, rows):
        self.rows += rows
        self.done |= {r["key"] for r in rows if r["value"] == "done"}


def test_plan_batches_aligned():
    dirs = ["000", "001", "002", "003", "004", "005", "063", "junk"]
    assert store.plan_batches(dirs, 500) == [[0, 1, 2, 3, 4], [5], [63]]
    assert store.plan_batches(dirs, 200, from_block=4_500_000) == [[4, 5], [63]]
    with pytest.raises(ValueError):
        store.plan_batches(dirs, 150)


def test_load_resumes(tmp_path):
    for d in ("060", "061", "062"):
        (tmp_path / d).mkdir()
    ch = FakeCH(done={"batch:060-060"})
    assert store.load(ch, tmp_path, batch_shards=100) == 10
    drops = [s for s in ch.sql if "DROP PARTITION" in s]
    inserts = [s for s in ch.sql if s.startswith("INSERT")]
    assert len(drops) == 2 * len(store.PARTITIONED) and "DROP PARTITION 61" in drops[0]
    assert len(inserts) == 2 and "transfers/{061}/*.jsonl.gz" in inserts[0]
    assert ch.sql.index(drops[0]) < ch.sql.index(inserts[0])
    assert ch.done == {"batch:060-060", "batch:061-061", "batch:062-062"}
    assert ch.rows[-1] == {"key": "last_block", "value": "61999999"}
    # second run: nothing left
    ch2 = FakeCH(done=ch.done)
    assert store.load(ch2, tmp_path, batch_shards=100) == 0 and ch2.sql == []


def test_init_applies_schema():
    ch = FakeCH()
    store.init(ch)
    assert sum("CREATE TABLE IF NOT EXISTS" in s for s in ch.sql) == 12
    assert sum("CREATE MATERIALIZED VIEW IF NOT EXISTS" in s for s in ch.sql) == 9


def test_hubs_replaces_rows_per_direction():
    ch = FakeCH()
    assert store.hubs(ch, 5_000_000, 50) == 1
    assert [x.split()[0] for x in ch.sql] == ["ALTER", "INSERT", "ALTER", "INSERT"]
    assert "dir = 'out'" in ch.sql[0] and "'out'" in ch.sql[1] and "dir = 'in'" in ch.sql[2] and "'in'" in ch.sql[3]
    assert "unhex('" + "ab" * 20 + "')" in ch.sql[1]
