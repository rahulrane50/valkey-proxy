# Local replica integration tests

Build and run from the repository root. Requires Docker and Python 3.9+;
no Python packages or remote cluster credentials are used.

```sh
docker build -f test/replica/Dockerfile -t valkey-proxy:replica-local .
python3 test/replica/integration.py --backend-image valkey/valkey:9.0
python3 test/replica/integration.py --backend-image redis:7
```

`--proxy-image` selects another locally available image; `--output` selects
the log and JSON result directory. Each invocation creates its own Docker
network, six backend containers (three primaries and three replicas), and
proxy containers. Ports bind only to localhost. Resources are removed in a
`finally` block; `--keep` retains them for debugging.

The suite covers default primary routing, the supported read command matrix,
cross-slot MGET/EXISTS/TOUCH, write-like reads and scripts, WATCH and MULTI/EXEC,
binary data, concurrent ordered RESP2/RESP3 pipelines, replica loss with primary
fallback, planned and automatic promotion, rejoin, and global broadcasts after
role changes. Replica mode runs with backend multiplexing enabled and disabled.
Value assertions wait for replication offsets instead of assuming synchronous
replication. Backend command-counter deltas prove where reads execute. Writes
must succeed and increment primary counters; replica counters also include
writes applied through replication and cannot distinguish that from client
traffic by themselves.

Mixed SETEX/GET, HSET/HGET and INCRBY/GET pipelines additionally verify ordered
replies and eventual convergence. They explicitly allow a missing value from
an immediate replica read: replica-preferred mode does not provide
read-your-writes, even within a mixed pipeline. Transactional reads stay on
primaries. Applications requiring stronger consistency should use the default
primary policy.

The image build also runs configuration tests and builds the existing C
integration runner (`run-test`). Logs and results describe the image tested;
run the existing integration suites as an additional regression check.
