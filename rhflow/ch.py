"""ClickHouse over its HTTP interface. No client library: httpx is already here and the interface is
one POST per query. Values go through server-side parameters (`{name:Type}` in SQL), never string-formatted.
"""
import httpx

from .env import env


def _params(params):
    return {f"param_{k}": v for k, v in (params or {}).items()}


class CH:
    """Sync client for the loader / tail / enrich; `CHAsync` for the API."""
    def __init__(self, url=None, db=None, timeout=600):
        self.url = (url or env("CLICKHOUSE_URL", "http://127.0.0.1:8123")).rstrip("/")
        self.db = db or env("CLICKHOUSE_DB", "rhflow")
        self.c = httpx.Client(timeout=timeout)

    def _q(self, sql, params, fmt):
        q = {"database": self.db, **_params(params)}
        if fmt:
            sql = f"{sql.rstrip().rstrip(';')} FORMAT {fmt}"
        r = self.c.post(self.url, params=q, content=sql.encode())
        if r.status_code != 200:
            raise RuntimeError(f"clickhouse HTTP {r.status_code}: {r.text[:500]}")
        return r.text

    def query(self, sql, params=None):
        """SELECT → list of dicts."""
        import json
        return [json.loads(l) for l in self._q(sql, params, "JSONEachRow").splitlines() if l]

    def execute(self, sql, params=None):
        """DDL / INSERT ... SELECT; returns the raw response text."""
        return self._q(sql, params, None)

    def insert_rows(self, table, rows):
        """INSERT rows (list of dicts) as JSONEachRow."""
        import json
        body = "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows)
        r = self.c.post(self.url, params={"database": self.db, "query": f"INSERT INTO {table} FORMAT JSONEachRow"},
                        content=body.encode())
        if r.status_code != 200:
            raise RuntimeError(f"clickhouse HTTP {r.status_code}: {r.text[:500]}")


class CHAsync:
    def __init__(self, url=None, db=None, timeout=10):
        self.url = (url or env("CLICKHOUSE_URL", "http://127.0.0.1:8123")).rstrip("/")
        self.db = db or env("CLICKHOUSE_DB", "rhflow")
        self.c = httpx.AsyncClient(timeout=timeout)

    async def query(self, sql, params=None):
        import json
        q = {"database": self.db, **_params(params)}
        r = await self.c.post(self.url, params=q, content=f"{sql.rstrip().rstrip(';')} FORMAT JSONEachRow".encode())
        if r.status_code != 200:
            raise RuntimeError(f"clickhouse HTTP {r.status_code}: {r.text[:500]}")
        return [json.loads(l) for l in r.text.splitlines() if l]

    async def aclose(self):
        await self.c.aclose()
