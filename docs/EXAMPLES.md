# Examples: real requests, real responses

Every number below is a field from an actual response of the API on the full chain, captured on 2026-09-18
(`meta.dataset_head` 66,207,692 – 66,208,003 across the calls; `took_ms` is the server-side time of that
one call). Nothing is rounded except where marked with `≈`; amounts are shown in token units computed from
the raw integer and `decimals`. The contract is in [`ARCHITECTURE.md`](ARCHITECTURE.md); the interactive
version is `/docs` on a running `rhflow serve`.

## 1. A labelled contract: the canonical bridge

`GET /address/0x1d187c3e2da52d72bc9c41e3aba0fdfa6a7bf055` — 134 ms

| field | value |
|---|---|
| `type` / `label` | `BRG` / `Arbitrum L2 ERC20 gateway (canonical bridge)` (from `rhflow/labels.json`) |
| `is_contract` | `true` |
| `first_ts` → `last_ts` | 2026-06-25 07:25:25 → 2026-09-18 09:46:27 UTC |
| `n_in` / `n_out` / `n_tx` | 2,716 / 2,705 / 2,716 |
| `tokens` | 10 entries |
| `top_out[0]` | `0x07ae8551be970cb1cca11dd7a11f47ae82e70e67` (EOA), 1,518 transfers, 105,825.996 WETH |

The bridge moves few transfers but large amounts; a hub (next) is the opposite.

## 2. A hub: 289M transfers in one address

`GET /address/0x8366a39cc670b4001a1121b8f6a443a643e40951` — 337 ms

| field | value |
|---|---|
| `type` / `label` | `CTR` / `null` (contract by `eth_getCode`, no label yet) |
| `first_ts` → `last_ts` | 2026-05-22 21:15:34 → 2026-09-18 11:59:41 UTC |
| `n_in` / `n_out` / `n_tx` | 127,178,278 / 162,187,163 / 112,925,364 |
| `tokens[0]` | USDG: 20,970,002 in / 23,548,035 out; 6,932,869,857.10 in / 6,873,290,073.60 out |
| `tokens[1]` | WETH: 7,222,985 in / 7,936,287 out |

`top_in` / `top_out` for this address come from `hub_top` (precomputed by `rhflow store hubs`), which is why
it answers in 337 ms instead of the 5–10 s a live top-k over ~13M (counterparty, token) groups takes.

## 3. Transfers of an EOA, paginated

`GET /address/0x1e832ba8e59f1182fdd36218f6889887eec641fe/transfers?limit=2` — 130 ms

`items[0]`:

```json
{"ts": 1789552747, "block": 64419574,
 "tx_hash": "0x8a6115a666d9d24c7b99c880dfeb4da56c6b6207c13bbb11b0dab38f7bb6b87f", "log_index": 887,
 "token": "0x9a79abc72b54c902d7cebdcc4c8109b873651697",
 "from": "0x9a79abc72b54c902d7cebdcc4c8109b873651697",
 "to": "0x1e832ba8e59f1182fdd36218f6889887eec641fe",
 "amount": "1000000000000000000", "kind": "erc20", "symbol": null, "decimals": null}
```

`next_cursor` is an opaque base64 string (`WzE3ODk1Mzc4NjcsICIweGMz…`); pass it back as `cursor=` for the
next page. `symbol`/`decimals` are `null` because this token is outside the enriched top-5,000.

## 4. One transaction, every transfer in it: an airdrop

`GET /tx/0x8a6115a666d9d24c7b99c880dfeb4da56c6b6207c13bbb11b0dab38f7bb6b87f/flow` — 653 ms

| field | value |
|---|---|
| `block` / `ts` | 64,419,574 / 2026-09-16 09:59:07 UTC |
| `nodes` | 2,001 |
| `edges` | 2,000, all from the token contract `0x9a79…1697` itself, one token, `amount` `1000000000000000000` each |
| `log_index` | 65 … 2,064 |

The address from example 3 is one of the 2,000 recipients (its row is `log_index` 887). The response is
~500 KB; `/tx/{hash}/flow` never aggregates, it returns the log as it is.

## 5. Graph around an EOA

`GET /graph?address=0x1e832ba8e59f1182fdd36218f6889887eec641fe&depth=1&per_node=5` — 72 ms:
7 nodes, 72 edges, `truncated: true` (5 counterparties kept out of 61; edges are per token, so 72 > 7).

`GET /graph?address=0x1e832ba8e59f1182fdd36218f6889887eec641fe&depth=2` — 1,006 ms:
61 nodes, 1,500 edges, `truncated: true` — 1,500 is the hard cap on edges; the graph was cut there.

`nodes[1]`: `0xd91355fc43f92ef1f128d0c551885560ed5b1634`, `EOA`, `n_in` 46,448, `n_out` 47,196.

## 6. Path: found in one hop, found in three, not found

`GET /path?from=0x1e83…41fe&to=0xd91355fc43f92ef1f128d0c551885560ed5b1634&max_hops=2` — 27 ms

```json
{"found": true, "explored_nodes": 4,
 "hops": [{"from": "0x1e832ba8e59f1182fdd36218f6889887eec641fe", "to": "0xd91355fc43f92ef1f128d0c551885560ed5b1634",
           "token": "0xeaa9abb805db03b6859662354abfe0c2a30902ae", "n": 17, "amount_sum": "120228760444472471778814",
           "first_ts": 1783896273, "last_ts": 1784240843}]}
```

`GET /path?from=0x1e83…41fe&to=0x8366a39cc670b4001a1121b8f6a443a643e40951&max_hops=3` — 285 ms, `found: true`,
21 nodes explored:

| hop | from → to | token | n | window (UTC) |
|---|---|---|---|---|
| 1 | `0x1e83…41fe` → `0x0560…e284` | `0x948a…126b` (SUMMER) | 37 | 2026-07-10 11:47 → 2026-07-11 22:30 |
| 2 | `0x0560…e284` → `0x6505…40dc` | WETH | 6,725 (653.649 WETH) | 2026-07-08 12:41 → 2026-09-15 22:39 |
| 3 | `0x6505…40dc` → the hub | USDG | 2,806,299 (455,202,174.02 USDG) | 2026-07-03 03:31 → 2026-09-18 02:15 |

A found path is a chain of aggregated edges, not a chain of transactions: hop 2 starts before hop 1 does.
Ordering in time is the caller's job (`from_ts`/`to_ts` on `/graph`).

`GET /path?from=0x1e83…41fe&to=0x1d187c3e2da52d72bc9c41e3aba0fdfa6a7bf055&max_hops=3` — 86 ms,
`found: false`, `explored_nodes: 43`. With `max_hops=4` the same pair is `found: true` in four hops. `/path`
walks at most 5,000 nodes over the per-node top-k, so `found: false` means "not found within the cap",
never "no path exists".

## 7. Token page

`GET /token/0x5fc5360d0400a0fd4f2af552add042d716f1d168` — 88 ms

```json
{"data": {"token": "0x5fc5360d0400a0fd4f2af552add042d716f1d168", "symbol": "USDG", "decimals": 6,
          "name": "Global Dollar", "kind": "erc20", "n_transfers": 216650123,
          "first_ts": 1778608203, "last_ts": 1789732781, "holders_seen": 939969},
 "meta": {"dataset_head": 66207692, "took_ms": 88}}
```

`n_transfers` and `holders_seen` come from the `token_stats` aggregate (live, fed by every insert);
`holders_seen` is `uniq(to_addr)`: addresses that ever received the token, not current balances.

## 8. Search, and why labels matter

`GET /search?q=uniswap` — 16 ms: 5 hits, all `kind: "address"`, `type: "DEX"`, e.g.
`0x89e5db8b5aa49aa85ac63f691524311aeb649eba` "Uniswap V2-style router".

`GET /search?q=USD` — 20 hits (the cap), **18 of them tokens whose symbol is `USDG`**. Only
`0x5fc5360d0400a0fd4f2af552add042d716f1d168` is the Global Dollar with 216M transfers; the other 17 are
contracts that chose the same symbol. Search matches the symbol prefix and says so; it does not rank by
legitimacy. Check the token page (`n_transfers`, `holders_seen`) before trusting a symbol.

## 9. Negative cases

| request | status | body |
|---|---|---|
| `GET /address/0xfeedfacefeedfacefeedfacefeedfacefeedface` | 404 | `{"detail": "unknown address"}` — no transfer touches it |
| `GET /tx/0x111…111/flow` (64 hex) | 404 | `{"detail": "unknown tx"}` |
| `GET /address/0x123` | 400 | `{"detail": "String should match pattern '^0x[0-9a-fA-F]{40}$'", "loc": ["path", "addr"]}` |
| `GET /graph?address=…&depth=3` | 400 | `{"detail": "Input should be less than or equal to 2", "loc": ["query", "depth"]}` |

Two things that look like negatives but are not:

- `0x1111111111111111111111111111111111111111` is **200**: an EOA with 56 transfers in and 1 out
  (2026-07-11 → 2026-09-18). People send tokens to vanity addresses. Existence in this dataset means
  "appears in a `Transfer` log", nothing more.
- A hub with a filter runs live instead of reading `hub_top`:
  `GET /graph?address=0x8366…0951&depth=1&token=0x5fc5…d168&per_node=5` returned 200 with 7 nodes in
  827 ms on this run. The 10 s statement budget (HTTP 503) is reachable with wider filters
  (`depth=2`, time windows on the busiest hubs); it was not hit here.
