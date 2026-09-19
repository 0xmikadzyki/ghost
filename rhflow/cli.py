"""`rhflow` command line."""
import argparse, asyncio


def main(argv=None):
    ap = argparse.ArgumentParser(prog="rhflow", description="Fund-flow data pipeline for Robinhood Chain (4663).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("backfill", help="pull all ERC-20 Transfer logs from HyperSync into gzip shards")
    p.add_argument("--out", default="data/transfers")
    p.add_argument("--from-block", type=int, default=0)
    p.add_argument("--to-block", type=int, default=None, help="exclusive; default: head minus --head-lag")
    p.add_argument("--conc", type=int, default=8, help="parallel shards; 8 stays under the account rate limit")
    p.add_argument("--shard", type=int, default=10_000, help="blocks per shard")
    p.add_argument("--head-lag", type=int, default=1000, help="blocks kept back from the head (reorg margin)")

    p = sub.add_parser("stats", help="exact shard/log/tx counts over a backfill directory")
    p.add_argument("--out", default="data/transfers")
    p.add_argument("--jobs", type=int, default=None)

    for name, help_ in (("store", "ClickHouse: init schema, load shards (rhflow store -h)"),
                       ("tail", "follow the chain head into ClickHouse (rhflow tail -h)"),
                       ("enrich", "token symbols/decimals, contract flags, labels (rhflow enrich -h)")):
        sub.add_parser(name, help=help_, add_help=False)

    p = sub.add_parser("serve", help="read-only HTTP API for the explorer")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8791)

    a, rest = ap.parse_known_args(argv)
    if a.cmd == "backfill":
        from . import backfill
        asyncio.run(backfill.run(a.out, a.conc, a.shard, a.from_block, a.to_block, a.head_lag))
    elif a.cmd == "stats":
        from . import stats
        stats.run(a.out, a.jobs)
    elif a.cmd == "serve":
        from .api.app import serve
        serve(a.host, a.port)
    else:                                   # store / tail / enrich own their argparse
        import importlib
        return importlib.import_module(f".{a.cmd}", __package__).main(rest)
    return 0
