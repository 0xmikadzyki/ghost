"""Enrich: ABI decoding (string / bytes32 / uint8), error rows stored, batches ≤ 50 calls, eth_getCode →
is_contract, labels upsert keeps a known is_contract. Fake RPC and fake CH, no network."""
import json

from rhflow import enrich

W = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
STR_WETH = "0x" + "20".rjust(64, "0") + "04".rjust(64, "0") + "57455448".ljust(64, "0")   # string "WETH"
B32_MKR = "0x" + "4d4b52".ljust(64, "0")                                                  # bytes32 "MKR"
DEC18 = "0x" + "12".rjust(64, "0")


def test_decode_string_and_bytes32():
    assert enrich.decode_string(STR_WETH) == "WETH"
    assert enrich.decode_string(B32_MKR) == "MKR"
    assert enrich.decode_string("0x") is None and enrich.decode_string(None) is None
    assert enrich.decode_string("0x" + "20".rjust(64, "0") + "ff".rjust(64, "0")) is None   # length past end


def test_decode_decimals():
    assert enrich.decode_uint8(DEC18) == 18
    assert enrich.decode_uint8("0x" + "ff".rjust(64, "0")) == 255
    assert enrich.decode_uint8("0x" + "100".rjust(64, "0")) is None
    assert enrich.decode_uint8("0x") is None


class FakeRPC:
    def __init__(self, answers):
        self.answers, self.batches = answers, []
    def call(self, calls):
        self.batches.append(calls)
        out = []
        for m, p in calls:
            key = (m, p[0]["to"] if m == "eth_call" else p[0], p[0].get("data") if m == "eth_call" else None)
            a = self.answers.get(key, {"error": {"code": -32000, "message": "execution reverted"}})
            out.append({"result": a} if isinstance(a, str) else a)
        return out


class FakeCH:
    def __init__(self, rows):
        self.rows, self.inserts = rows, []
    def query(self, sql, params=None):
        if "any(kind)" in sql:
            return [{"token": r["token"], "kind": "erc721" if r["token"] == W else "erc20"} for r in self.rows]
        return self.rows
    def insert_rows(self, table, rows):
        self.inserts.append((table, rows))


def test_enrich_tokens_rows_and_batches():
    toks = ["0x" + f"{i:040x}" for i in range(1, 21)]
    ans = {("eth_call", W, enrich.SYMBOL): STR_WETH, ("eth_call", W, enrich.DECIMALS): DEC18,
           ("eth_call", W, enrich.NAME): STR_WETH,
           ("eth_call", toks[0], enrich.SYMBOL): B32_MKR, ("eth_call", toks[0], enrich.DECIMALS): DEC18}
    rpc, ch = FakeRPC(ans), FakeCH([{"token": t, "n": 1} for t in [W] + toks])
    done, err = enrich.enrich_tokens(ch, rpc, top=100)
    assert done == 21 and err == 19
    assert all(len(b) <= enrich.BATCH for b in rpc.batches) and len(rpc.batches) == 2
    rows = [r for _, rs in ch.inserts for r in rs]
    assert ch.inserts[0][0] == enrich.TOKENS_T
    assert rows[0] == {"token": W, "symbol": "WETH", "decimals": 18, "name": "WETH", "source": "eth_call",
                       "fetched_at": rows[0]["fetched_at"], "kind": "erc721", "n_transfers": 1}
    assert rows[1]["symbol"] == "MKR" and rows[1]["name"] == "" and rows[1]["source"] == "eth_call"
    assert rows[2] == {"token": toks[1], "symbol": "", "decimals": 0, "name": "", "source": "eth_call:err",
                       "fetched_at": rows[2]["fetched_at"], "kind": "erc20", "n_transfers": 1}


def test_enrich_addresses_is_contract():
    a, b = "0x" + "aa" * 20, "0x" + "bb" * 20
    rpc = FakeRPC({("eth_getCode", a, None): "0x6001", ("eth_getCode", b, None): "0x"})
    ch = FakeCH([{"addr": a}, {"addr": b}])
    assert enrich.enrich_addresses(ch, rpc, min_n=5) == 2
    (table, rows), = ch.inserts
    assert table == enrich.ADDRS_T
    assert [(r["addr"], r["is_contract"], r["source"]) for r in rows] == [(a, 1, "eth_getCode"), (b, 0, "eth_getCode")]


def test_load_labels_keeps_known_is_contract(tmp_path):
    p = tmp_path / "labels.json"
    p.write_text(json.dumps([{"addr": "0x" + "AA" * 20, "label": "x", "type": "DEX", "source": "s"},
                             {"addr": "0x" + "bb" * 20, "label": "y", "type": "BRG", "source": "s", "is_contract": 1}]))
    ch = FakeCH([{"addr": "0x" + "aa" * 20, "is_contract": 1}])
    assert enrich.load_labels(ch, p) == 2
    (table, rows), = ch.inserts
    assert table == enrich.ADDRS_T
    assert rows[0]["addr"] == "0x" + "aa" * 20 and rows[0]["is_contract"] == 1 and rows[0]["label"] == "x"
    assert rows[1]["is_contract"] == 1 and rows[1]["type"] == "BRG"


def test_shipped_labels_are_well_formed():
    labels = json.loads(enrich.LABELS.read_text())
    assert labels and all({"addr", "label", "type", "source"} <= set(l) for l in labels)
    assert all(l["type"] in ("WAL", "CTR", "ERC", "EOA", "BRG", "DEX") for l in labels)
    assert len({l["addr"] for l in labels}) == len(labels)
