# deploy

ClickHouse on the data box, loopback only, no password (see the comment in `docker-compose.yml`).

```sh
# on the server, as root
mkdir -p /root/rhflow/ch /root/rhflow/logs && chown 101:101 /root/rhflow/ch   # clickhouse uid in the image
rsync -a deploy/ root@<ingest-box>:/root/rhflow/deploy/                      # from the repo
cd /root/rhflow/deploy && docker compose up -d
curl -s 127.0.0.1:8123/ping                                                    # → Ok.
ss -ltnp | grep -E ':(8123|9000) '                                             # both on 127.0.0.1 only
```

Then, with the package on the box (`rsync -a rhflow/ root@…:/root/rhflow/app/rhflow/`, venv has httpx):

```sh
cd /root/rhflow/app
/root/rhflow/venv/bin/python -m rhflow.store init
setsid nohup /root/rhflow/venv/bin/python -m rhflow.store load --shards /root/rhflow/data/transfers \
    >> /root/rhflow/logs/load.log 2>&1 &
tail -3 /root/rhflow/logs/load.log            # progress; rerun the same command to resume
/root/rhflow/venv/bin/python -m rhflow.store stats
```

Stop / upgrade: `docker compose down` keeps `/root/rhflow/ch`; change the pinned tag, `up -d` again.

## Services

```sh
cp deploy/rhflow-*.service /etc/systemd/system/ && systemctl daemon-reload
systemctl enable --now rhflow-api          # 127.0.0.1:8791
systemctl enable --now rhflow-tail         # only after `rhflow store load` printed [done]
journalctl -u rhflow-api -f                # slow statements are logged as `[api] slow q:<tag> <ms> ms`
echo '17 4 * * * root cd /root/rhflow/app && /root/rhflow/venv/bin/python -m rhflow store hubs >> /root/rhflow/logs/hubs.log 2>&1' > /etc/cron.d/rhflow-hubs
```

`store hubs` refreshes `hub_top` (top-50 counterparties of every address above 5M transfers, ~2 minutes for
42 hubs); the API serves a hub live while its rows are being replaced.
