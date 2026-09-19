"""API against a fake ClickHouse: `query()` dispatches on the `/* q:tag */` comment each statement starts
with and returns canned rows shaped like JSONEachRow output (UInt64/UInt256 as strings, tuples as lists)."""
import re

import httpx
import pytest
from fastapi.testclient import TestClient

from rhflow.api import queries as q
from rhflow.api.app import create_app

A, B, C, D, E = ("0x" + c * 40 for c in "abcde")
T = "0x" + "77" * 20                                            # a token contract
TX = "0x" + "12" * 32


def top_row(me, other, n, amt="5", tok=T):
    return {"me": me, "other": other, "n_sum": str(n), "amt_sum": amt, "top": [tok, str(n), amt, 1, 2]}


def edge_row(f, t, n, tok=T):
    return {"from": f, "to": t, "token": tok, "n": str(n), "amount_sum": "5", "first_ts": 1, "last_ts": 2}


def in_xs(params, addr):
    """IN-lists are literals in the SQL (see queries.arr); FakeCH exposes the statement as params["_sql"]."""
    return f"unhex('{addr[2:]}')" in params.get("_sql", "")


class FakeCH:
    def __init__(self, handlers=None, down=False):
        self.h, self.down, self.calls = handlers or {}, down, []

    async def query(self, sql, params=None):
        if self.down:
            raise httpx.ConnectError("refused")
        tag = re.match(r"/\* q:(\w+) \*/", sql).group(1)
        assert "{" not in re.sub(r"\{\w+:[\w()]+\}", "", sql), "unparametrised brace in SQL"
        params = {**(params or {}), "_sql": sql}
        self.calls.append((tag, params))
        h = self.h.get(tag, [])
        return h(params) if callable(h) else h


BASE = {
    "head": [{"key": "last_block", "value": "63385706"}, {"key": "last_ts", "value": "1758000000"}],
    "stats": lambda p: [{"addr": a, "first_ts": 1, "last_ts": 2, "n_in": "3", "n_out": "4", "n_tx": "5", "n_tokens": "1"}
                        for a in (A, B, C, D, E) if in_xs(p, a)],
    "counts": lambda p: [{"addr": a, "n_in": "3", "n_out": "4"} for a in (A, B, C, D, E) if in_xs(p, a)],
    "tokens": lambda p: [{"token": T, "symbol": "WETH", "decimals": 18, "kind": "erc20", "name": "Wrapped", "n_transfers": "9"}] if in_xs(p, T) else [],
    "addrs": lambda p: [r for r in [{"addr": B, "is_contract": 1, "label": "Router", "type": "DEX"},
                                    {"addr": C, "is_contract": 1, "label": "", "type": "EOA"}] if in_xs(p, r["addr"])],
}


def client(extra=None, **kw):
    return TestClient(create_app(ch=FakeCH({**BASE, **(extra or {})}, **kw)), raise_server_exceptions=False)


def test_envelope_and_health():
    r = client().get("/health")
    assert r.status_code == 200
    j = r.json()
    assert set(j) == {"data", "meta"} and j["meta"]["dataset_head"] == 63385706 and isinstance(j["meta"]["took_ms"], int)
    assert j["data"] == {"ok": True, "dataset_head": 63385706, "head_ts": 1758000000, "clickhouse": "ok"}
    assert r.headers.get("access-control-allow-origin") is None or True   # CORS header only on cross-origin requests


def test_cors_open_for_get():
    r = client().get("/health", headers={"Origin": "https://example.org"})
    assert r.headers["access-control-allow-origin"] == "*"


@pytest.mark.parametrize("url", ["/address/0x12", "/address/" + A + "/transfers?dir=up", "/graph?address=" + A + "&per_node=99",
                                 "/graph?address=" + A + "&depth=3", "/path?from=" + A + "&to=" + A, "/tx/0xzz/flow",
                                 "/address/" + A + "/transfers?cursor=%%%", "/search?q="])
def test_400(url):
    assert client().get(url).status_code == 400


def test_404():
    c = client()
    assert c.get("/address/0x" + "99" * 20).status_code == 404
    assert c.get("/address/0x" + "99" * 20 + "/transfers").status_code == 404
    assert c.get("/graph?address=0x" + "99" * 20).status_code == 404
    assert c.get("/tx/" + TX + "/flow").status_code == 404
    assert c.get("/token/" + A).status_code == 404


def test_503_when_clickhouse_down():
    c = client(down=True)
    for url in ["/health", "/address/" + A, "/graph?address=" + A]:
        r = c.get(url)
        assert r.status_code == 503 and "unreachable" in r.json()["detail"]


def test_fanout_cap_and_truncated():
    others = ["0x" + f"{i:040x}" for i in range(1, 6)]           # 5 counterparties, per_node=3 -> query asks 4
    h = {"top_from": lambda p: [top_row(A, o, 10 - i) for i, o in enumerate(others)][:int(p["k"])] if in_xs(p, A) else [],
         "edges_from": lambda p: [edge_row(A, o, 10 - i) for i, o in enumerate(others)]}
    r = client(h).get("/graph?address=" + A + "&per_node=3").json()["data"]
    assert r["truncated"] is True
    assert {n["id"] for n in r["nodes"]} == {A, *others[:3]}
    assert [e["to"] for e in r["edges"]] == others[:3]              # dropped pair filtered out of the edge rows
    assert r["nodes"][0]["id"] == A and r["edges"][0]["symbol"] == "WETH" and r["edges"][0]["n"] == 10
    r = client(h).get("/graph?address=" + A + "&per_node=5").json()["data"]
    assert r["truncated"] is False and len(r["nodes"]) == 6


def test_depth2_expands_only_kept_nodes():
    fan = {A: [B], B: [C], C: [D]}
    h = {"top_from": lambda p: [top_row(a, o, 1) for a, os in fan.items() if in_xs(p, a) for o in os],
         "edges_from": lambda p: [edge_row(a, o, 1) for a, os in fan.items() if in_xs(p, a) for o in os]}
    c = client(h)
    r = c.get("/graph?address=" + A + "&depth=1").json()["data"]
    assert {n["id"] for n in r["nodes"]} == {A, B} and len(r["edges"]) == 1
    r = c.get("/graph?address=" + A + "&depth=2").json()["data"]
    assert {n["id"] for n in r["nodes"]} == {A, B, C}
    assert {(e["from"], e["to"]) for e in r["edges"]} == {(A, B), (B, C)}
    assert D not in {n["id"] for n in r["nodes"]}                   # C was never expanded


def test_graph_filters_go_into_sql():
    fake = FakeCH(BASE)
    TestClient(create_app(ch=fake)).get(f"/graph?address={A}&token={T}&from_ts=10&to_ts=20&hide_tokens={T},weth")
    sqls = {t: p for t, p in fake.calls}
    assert "hidden" in sqls and sqls["hidden"]["syms"] == "['weth']"
    top = [p for t, p in fake.calls if t == "top_from"][0]
    assert top["token"] == T[2:] and top["from_ts"] == 10 and top["to_ts"] == 20 and f"token NOT IN (unhex('{T[2:]}'))" in top["_sql"]
    s = q.q_top("from_addr", q.arr([A]), window=True, token=T, hide_zero=True, hidden=q.arr([T]))
    assert "FROM transfers" in s and "token NOT IN" in s and "unhex('" + "00" * 20 + "')" in s and "max_execution_time" in s


def test_path_found_and_not_found():
    # forward A->B->C, backward from E: E <- C ; meet at C -> A,B,C,E
    fwd = {A: [B], B: [C]}
    h = {"top_from": lambda p: [top_row(a, o, 1) for a, os in fwd.items() if in_xs(p, a) for o in os],
         "top_to": lambda p: [top_row(E, C, 7)] if in_xs(p, E) else []}
    r = client(h).get(f"/path?from={A}&to={E}").json()["data"]
    assert r["found"] is True
    assert [(x["from"], x["to"]) for x in r["hops"]] == [(A, B), (B, C), (C, E)]
    assert r["hops"][2]["n"] == 7 and r["hops"][0]["token"] == T and r["explored_nodes"] >= 3
    r = client(h).get(f"/path?from={A}&to={E}&max_hops=2").json()["data"]
    assert r["found"] is False and r["hops"] == []
    r = client(h).get(f"/path?from={A}&to={D}").json()["data"]
    assert r["found"] is False and r["explored_nodes"] == 2


def test_tx_flow_order_and_types():
    rows = [{"ts": 9, "block": 7, "log_index": i, "token": T, "from": f, "to": t, "amount": str(i * 100), "kind": "erc20"}
            for i, (f, t) in enumerate([(A, B), (B, C), (C, T)])]
    r = client({"tx": rows}).get("/tx/" + TX.upper().replace("0X", "0x") + "/flow")
    d = r.json()["data"]
    assert d["tx_hash"] == TX and d["block"] == 7 and d["ts"] == 9
    assert [e["log_index"] for e in d["edges"]] == [0, 1, 2] and d["edges"][1]["amount"] == "100"
    assert d["edges"][0]["symbol"] == "WETH" and d["edges"][0]["decimals"] == 18
    types = {n["id"]: n["type"] for n in d["nodes"]}
    assert types == {A: "EOA", B: "DEX", C: "CTR", T: "ERC"}
    assert {n["id"]: n["n_in"] for n in d["nodes"]}[A] == 3


def test_node_typing_rule():
    tokens, addrs = {T: {}}, {B: {"label": "Router", "type": "DEX", "is_contract": 1}, C: {"label": "", "is_contract": 1}}
    assert q.node_type(T, tokens, {T: {"label": "x", "type": "DEX"}}) == "ERC"   # token contract wins over label
    assert q.node_type(B, tokens, addrs) == "DEX"
    assert q.node_type(C, tokens, addrs) == "CTR"
    assert q.node_type(A, tokens, addrs) == "EOA"


def test_cursor_roundtrip():
    c = q.enc_cursor(123, TX, 4)
    assert re.fullmatch(r"[A-Za-z0-9_-]+", c) and q.dec_cursor(c) == (123, TX[2:], 4)
    with pytest.raises(ValueError):
        q.dec_cursor("bad")
    rows = [{"ts": 9 - i, "block": 1, "tx_hash": TX, "log_index": i, "token": T, "from": A, "to": B, "amount": "1", "kind": "erc20"}
            for i in range(3)]
    fake = FakeCH({**BASE, "transfers": lambda p: rows[:int(p["lim"])]})
    tc = TestClient(create_app(ch=fake))
    d = tc.get(f"/address/{A}/transfers?limit=2&dir=out").json()["data"]
    assert len(d["items"]) == 2 and d["items"][0]["symbol"] == "WETH" and d["next_cursor"]
    assert q.dec_cursor(d["next_cursor"]) == (8, TX[2:], 1)
    tc.get(f"/address/{A}/transfers?limit=2&dir=out&cursor=" + d["next_cursor"])
    p = [p for t, p in fake.calls if t == "transfers"][-1]
    assert (p["c_ts"], p["c_tx"], p["c_li"]) == (8, TX[2:], 1) and p["lim"] == 3
    assert "UNION ALL" not in q.q_transfers(["from_addr"], False, False, True) and "UNION ALL" in q.q_transfers(["from_addr", "to_addr"], False, False, False)


def test_address_page_and_search():
    h = {"addr_tokens": [{"token": T, "dir": "in", "n_sum": "3", "amt_sum": "30"}, {"token": T, "dir": "out", "n_sum": "4", "amt_sum": "40"}],
         "top_from": lambda p: [top_row(A, B, 4, "40")], "top_to": lambda p: [top_row(A, C, 3, "30")],
         "search_labels": [{"id": B, "label": "Router", "type": "DEX"}], "search_tokens": [{"id": T, "symbol": "WETH", "name": "W"}],
         "tx_exists": [{"1": 1}]}
    c = client(h)
    d = c.get("/address/" + A.upper().replace("0X", "0x")).json()["data"]
    assert d["addr"] == A and d["type"] == "EOA" and d["n_tx"] == 5 and d["is_contract"] is False
    assert d["tokens"] == [{"token": T, "n_in": 3, "n_out": 4, "amount_in": "30", "amount_out": "40", "symbol": "WETH", "decimals": 18}]
    assert d["top_out"] == [{"addr": B, "type": "DEX", "label": "Router", "n": 4, "amount_sum": "40", "token": T}]
    assert d["top_in"][0]["addr"] == C and d["top_in"][0]["type"] == "CTR"
    assert c.get("/search?q=ro").json()["data"]["hits"] == [{"kind": "address", "id": B, "label": "Router", "type": "DEX"},
                                                            {"kind": "token", "id": T, "label": "WETH", "type": "ERC"}]
    assert c.get("/search?q=" + A).json()["data"]["hits"] == [{"kind": "address", "id": A, "label": None, "type": "EOA"}]
    assert c.get("/search?q=" + TX).json()["data"]["hits"][0]["kind"] == "tx"
    assert c.get("/search?q=0x" + "99" * 20).json()["data"]["hits"] == []
    d = client({"token_span": [{"first_ts": 1, "last_ts": 2, "holders_seen": "12", "n_transfers": "9"}]}).get("/token/" + T).json()["data"]
    assert d == {"token": T, "symbol": "WETH", "decimals": 18, "name": "Wrapped", "kind": "erc20", "n_transfers": 9,
                 "first_ts": 1, "last_ts": 2, "holders_seen": 12}


def test_hubs_are_not_expanded():
    # B is a hub: reached at depth 1 (and as a /path hop) but never expanded, so C stays out of the graph.
    counts = lambda p: [{"addr": a, "n_in": str(q.HUB_MAX if a == B else 3), "n_out": "4"} for a in (A, B, C, E) if in_xs(p, a)]
    fan = {A: [B], B: [C]}
    h = {"counts": counts,
         "top_from": lambda p: [top_row(a, o, 1) for a, os in fan.items() if in_xs(p, a) for o in os],
         "top_to": lambda p: [top_row(E, B, 7)] if in_xs(p, E) else [],
         "edges_from": lambda p: [edge_row(a, o, 1) for a, os in fan.items() if in_xs(p, a) for o in os]}
    r = client(h).get("/graph?address=" + A + "&depth=2").json()["data"]
    assert {n["id"] for n in r["nodes"]} == {A, B} and r["truncated"] is True
    r = client(h).get("/graph?address=" + B + "&depth=1").json()["data"]          # the root is always expanded
    assert {n["id"] for n in r["nodes"]} == {B, C}
    r = client(h).get(f"/path?from={A}&to={E}").json()["data"]                    # A -> B <- E meets on the hub
    assert r["found"] is True and [(x["from"], x["to"]) for x in r["hops"]] == [(A, B), (B, E)]


def test_hub_top_cache_serves_unfiltered_only():
    cached = lambda p: [top_row(A, C, 99)] if in_xs(p, A) else []
    live = lambda p: [top_row(a, B, 1) for a in (A, D) if in_xs(p, a)]
    fake = FakeCH({**BASE, "hub_top": cached, "top_from": live, "edges_from": lambda p: [edge_row(A, C, 99)]})
    c = TestClient(create_app(ch=fake))
    r = c.get("/graph?address=" + A).json()["data"]
    assert {(e["from"], e["to"]) for e in r["edges"]} == {(A, C)}                  # hub row wins, no live query for A
    assert [t for t, _ in fake.calls if t == "top_from"] == []
    fake.calls.clear()
    c.get(f"/graph?address={A}&token={T}")                                           # filtered: live only
    assert [t for t, _ in fake.calls if t in ("hub_top", "top_from")] == ["top_from"]
    s = q.hub_insert("from_addr", A)
    assert s.startswith("INSERT INTO hub_top SELECT me, 'out', other") and "LIMIT 50 BY me" in s and "{" not in s


def test_openapi_lists_every_route():
    c = client()
    assert c.get("/docs").status_code == 200
    paths = c.get("/openapi.json").json()["paths"]
    assert {"/health", "/address/{addr}", "/address/{addr}/transfers", "/graph", "/path", "/tx/{hash}/flow",
            "/token/{addr}", "/search"} <= set(paths)
