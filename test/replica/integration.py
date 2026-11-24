#!/usr/bin/env python3
"""Disposable Docker cluster tests. Uses only Python's standard library.

Never accepts a remote endpoint: all six backends and the proxy are created
on an isolated, uniquely named network and removed afterwards.
"""
import argparse
import binascii
import concurrent.futures
import json
import pathlib
import socket
import subprocess
import tempfile
import threading
import time
import uuid


class ResponseError(Exception):
    pass


def encode(args):
    args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
    return b"*%d\r\n" % len(args) + b"".join(
        b"$%d\r\n" % len(a) + a + b"\r\n" for a in args)


class Connection:
    def __init__(self, port, host="127.0.0.1"):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.file = self.sock.makefile("rb")

    def close(self):
        self.file.close()
        self.sock.close()

    def read(self):
        line = self.file.readline()
        if not line.endswith(b"\r\n"):
            raise EOFError(repr(line))
        kind, data = line[:1], line[1:-2]
        if kind == b"+":
            return data
        if kind == b"-":
            return ResponseError(data.decode(errors="replace"))
        if kind == b":":
            return int(data)
        if kind == b"$":
            n = int(data)
            if n < 0:
                return None
            value = self.file.read(n)
            assert self.file.read(2) == b"\r\n"
            return value
        if kind in (b"*", b"~", b">"):
            n = int(data)
            return None if n < 0 else [self.read() for _ in range(n)]
        if kind == b"%":
            return {self.read(): self.read() for _ in range(int(data))}
        if kind == b"_":
            return None
        if kind == b"#":
            return data == b"t"
        if kind == b",":
            return float(data)
        if kind == b"=":
            value = self.file.read(int(data))
            assert self.file.read(2) == b"\r\n"
            return value[4:]
        raise AssertionError((kind, data))

    def pipeline(self, commands):
        self.sock.sendall(b"".join(encode(c) for c in commands))
        return [self.read() for _ in commands]

    def call(self, *args):
        value = self.pipeline([args])[0]
        if isinstance(value, ResponseError):
            raise value
        return value


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True,
                                   stderr=subprocess.STDOUT).strip()


def eventually(fn, timeout=30):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
        except (OSError, EOFError, ResponseError, AssertionError) as exc:
            last = exc
        time.sleep(.1)
    raise AssertionError(f"condition timed out; last error: {last}")


def info(conn, section):
    return dict(line.split(b":", 1) for line in conn.call("INFO", section).splitlines()
                if b":" in line and not line.startswith(b"#"))


def slot(key):
    key = key if isinstance(key, bytes) else key.encode()
    start = key.find(b"{")
    end = key.find(b"}", start + 1)
    if start >= 0 and end > start + 1:
        key = key[start + 1:end]
    return binascii.crc_hqx(key, 0) % 16384


class Cluster:
    def __init__(self, args):
        self.args = args
        self.prefix = "vp-replica-" + uuid.uuid4().hex[:10]
        self.names = []
        self.nodes = []
        self.proxies = []
        self.tmp = tempfile.TemporaryDirectory(prefix=self.prefix)
        self.results = []

    def start(self):
        docker("network", "create", self.prefix)
        server = "valkey-server" if "valkey" in self.args.backend_image else "redis-server"
        cli = server.replace("server", "cli")
        for i in range(6):
            name = f"{self.prefix}-{i}"
            self.names.append(name)
            docker("run", "-d", "--name", name, "--network", self.prefix,
                   "-p", "127.0.0.1::6379", self.args.backend_image,
                   server, "--cluster-enabled", "yes", "--cluster-node-timeout", "1000",
                   "--appendonly", "no", "--save", "", "--protected-mode", "no")
            inspected = json.loads(docker("inspect", name))[0]
            port = int(inspected["NetworkSettings"]["Ports"]["6379/tcp"][0]["HostPort"])
            node = {"name": name, "port": port, "ip": inspected["NetworkSettings"]["Networks"][self.prefix]["IPAddress"], "up": True}
            node["c"] = eventually(lambda: Connection(port))
            node["id"] = node["c"].call("CLUSTER", "MYID")
            self.nodes.append(node)
        docker("exec", self.names[0], cli, "--cluster", "create",
               *(n["ip"] + ":6379" for n in self.nodes), "--cluster-replicas", "1", "--cluster-yes")
        eventually(lambda: all(b"cluster_state:ok" in n["c"].call("CLUSTER", "INFO") for n in self.nodes))
        self.roles()
        self.sync()
        print("Cluster ready:", self.args.backend_image, flush=True)

    def roles(self):
        for n in self.nodes:
            if n["up"]:
                n["role"] = n["c"].call("ROLE")[0]

    def primary_for(self, key):
        s = slot(key)
        for n in self.nodes:
            if not n["up"]:
                continue
            for group in n["c"].call("CLUSTER", "SLOTS"):
                if group[0] <= s <= group[1]:
                    return next(x for x in self.nodes if x["id"] == group[2][2])
        raise AssertionError("no owner")

    def replica_for(self, primary):
        return next(n for n in self.nodes if n["up"] and n["role"] != b"master"
                    and info(n["c"], "replication").get(b"master_host") == primary["ip"].encode())

    def sync(self):
        self.roles()
        for p in self.nodes:
            if not p["up"] or p["role"] != b"master":
                continue
            offset = int(info(p["c"], "replication")[b"master_repl_offset"])
            replicas = [n for n in self.nodes if n["up"] and n["role"] != b"master"
                        and info(n["c"], "replication").get(b"master_host") == p["ip"].encode()]
            for r in replicas:
                def caught_up():
                    state = info(r["c"], "replication")
                    return (state.get(b"master_link_status") == b"up" and
                            state.get(b"master_sync_in_progress") == b"0" and
                            int(state.get(b"slave_repl_offset", b"-1")) >= offset)
                eventually(caught_up)

    def proxy(self, policy="replica-preferred", multiplex=True):
        name = f"{self.prefix}-proxy-{len(self.proxies)}"
        conf = pathlib.Path(self.tmp.name) / (name + ".conf")
        conf.write_text(f"""server-ip {self.nodes[0]['ip']}
server-port 6379
bind 0.0.0.0
port 7777
worker-thread-no 4
cluster-refresh-interval-ms 50
cluster-node-fail-grace-ms 200
backend-connect-timeout-ms 150
backend-multiplexing {'yes' if multiplex else 'no'}
min-normal-connection 1
min-blocking-connection 0
shared-connection-per-node 2
""" + (f"read-policy {policy}\n" if policy else ""))
        self.names.append(name)
        self.proxies.append(name)
        docker("run", "-d", "--name", name, "--network", self.prefix,
               "-p", "127.0.0.1::7777", "-v", f"{conf}:/etc/proxy.conf:ro",
               self.args.proxy_image, "/etc/proxy.conf")
        inspected = json.loads(docker("inspect", name))[0]
        port = int(inspected["NetworkSettings"]["Ports"]["7777/tcp"][0]["HostPort"])
        def ready():
            c = Connection(port)
            try:
                return c.call("PING") == b"PONG"
            finally:
                c.close()
        eventually(ready)
        c = Connection(port)
        state = info(c, "proxy")
        assert state[b"proxy_read_policy"] == (policy or "primary").encode()
        assert state[b"proxy_shards"] == b"3"
        assert state[b"proxy_replicas"] == (b"3" if policy == "replica-preferred" else b"0")
        return c, port, name

    def stats(self):
        result = {}
        for n in self.nodes:
            if n["up"]:
                result[n["name"]] = {
                    k.removeprefix(b"cmdstat_"): int(v.split(b",", 1)[0].split(b"=")[1])
                    for k, v in info(n["c"], "commandstats").items()}
        return result

    def restart(self, node):
        docker("start", node["name"])
        inspected = json.loads(docker("inspect", node["name"]))[0]
        # Docker can allocate a different ephemeral host port on restart.
        node["port"] = int(inspected["NetworkSettings"]["Ports"]["6379/tcp"][0]["HostPort"])
        node["ip"] = inspected["NetworkSettings"]["Networks"][self.prefix]["IPAddress"]
        node["up"] = True
        node["c"] = eventually(lambda: Connection(node["port"]))

    def routed(self, c, command, role, expected=None, check=True, replicated=False):
        before = self.stats()
        value = c.call(*command)
        after = self.stats()
        cmd = str(command[0]).lower().encode()
        if cmd == b"object":
            cmd += b"|" + str(command[1]).lower().encode()
        counts = {n["name"]: after[n["name"]].get(cmd, 0) - before[n["name"]].get(cmd, 0)
                  for n in self.nodes if n["up"]}
        used = [n for n in self.nodes if n["up"] and counts[n["name"]] > 0]
        assert used, (command, "command did not execute", counts)
        # Replicas also count writes replayed from the replication stream.
        # A successful client write must execute on a primary; READONLY
        # replica connections reject direct writes instead of forwarding them.
        if replicated:
            used = [n for n in used if n["role"] == b"master"]
            assert used, (command, "no primary executed the write", counts)
        assert all(n["role"] == role for n in used), (command, role, counts, [(n["name"], n["role"]) for n in used])
        if check:
            assert value == expected, (command, value, expected)
        return value

    def record(self, name, fn):
        start = time.monotonic()
        try:
            fn()
        except BaseException as exc:
            self.results.append({"test": name, "status": "failed", "error": str(exc)})
            raise
        result = {"test": name, "status": "passed", "seconds": round(time.monotonic() - start, 3)}
        self.results.append(result)
        print(json.dumps(result), flush=True)

    def clean(self):
        for n in self.nodes:
            n["c"].close()
        for name in reversed(self.names):
            subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "network", "rm", self.prefix], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.tmp.cleanup()

    def save_logs(self):
        out = pathlib.Path(self.args.output)
        out.mkdir(parents=True, exist_ok=True)
        for name in self.proxies:
            try:
                contents = docker("logs", name)
            except subprocess.CalledProcessError as exc:
                contents = "Could not collect container logs: " + exc.output
            (out / (name + ".log")).write_text(contents)
        (out / "results.json").write_text(json.dumps(self.results, indent=2) + "\n")


def routing_suite(cluster, c, replicas=True):
    role = b"slave" if replicas else b"master"
    # Redis and Valkey ROLE both use the legacy 'slave' token.
    tag = "{routing}"
    s, h, l, st, z, geo, pf = [tag + suffix for suffix in ("s", "h", "l", "set", "z", "geo", "pf")]
    c.call("FLUSHALL")
    for command in (("SET", s, "100", "EX", 3600), ("HSET", h, "f", "v"),
                    ("RPUSH", l, "1", "2"), ("SADD", st, "1"),
                    ("ZADD", z, "1", "1"), ("GEOADD", geo, "13.361389", "38.115556", "Palermo"),
                    ("PFADD", pf, "a", "b")):
        cluster.routed(c, command, b"master", check=False, replicated=True)
    # PFCOUNT may propagate an internal cached-cardinality update. Prime it
    # on the owner before measuring routing, then synchronize the replicas.
    cluster.primary_for(pf)["c"].call("PFCOUNT", pf)
    cluster.sync()
    # Exact values where deterministic; route counters still prove every case.
    cases = [
        (("GET", s), b"100"), (("MGET", s, s), [b"100", b"100"]),
        (("GETBIT", s, 0), 0), (("GETRANGE", s, 0, 1), b"10"), (("STRLEN", s), 3),
        (("TYPE", s), b"string"), (("EXISTS", s), 1), (("TOUCH", s), 1),
        (("BITCOUNT", s), 7), (("BITPOS", s, 1), 2), (("BITFIELD_RO", s, "GET", "u8", 0), [49]),
        (("HEXISTS", h, "f"), 1), (("HGET", h, "f"), b"v"), (("HMGET", h, "f"), [b"v"]),
        (("HGETALL", h), [b"f", b"v"]), (("HKEYS", h), [b"f"]), (("HLEN", h), 1),
        (("HSTRLEN", h, "f"), 1), (("HVALS", h), [b"v"]), (("HRANDFIELD", h), b"f"),
        (("HSCAN", h, 0), [b"0", [b"f", b"v"]]),
        (("LINDEX", l, 0), b"1"), (("LLEN", l), 2), (("LPOS", l, "2"), 1),
        (("LRANGE", l, 0, -1), [b"1", b"2"]), (("LCS", s, s), b"100"),
        (("SCARD", st), 1), (("SISMEMBER", st, "1"), 1), (("SMISMEMBER", st, "1", "2"), [1, 0]),
        (("SMEMBERS", st), [b"1"]), (("SRANDMEMBER", st), b"1"),
        (("SSCAN", st, 0), [b"0", [b"1"]]), (("SDIFF", st, st), []),
        (("SINTER", st, st), [b"1"]), (("SINTERCARD", 2, st, st), 1), (("SUNION", st, st), [b"1"]),
        (("ZCARD", z), 1), (("ZCOUNT", z, "-inf", "+inf"), 1), (("ZLEXCOUNT", z, "-", "+"), 1),
        (("ZMSCORE", z, "1"), [b"1"]), (("ZRANGE", z, 0, -1), [b"1"]),
        (("ZRANGEBYLEX", z, "-", "+"), [b"1"]), (("ZRANGEBYSCORE", z, "-inf", "+inf"), [b"1"]),
        (("ZRANK", z, "1"), 0), (("ZREVRANGE", z, 0, -1), [b"1"]),
        (("ZREVRANGEBYLEX", z, "+", "-"), [b"1"]), (("ZREVRANGEBYSCORE", z, "+inf", "-inf"), [b"1"]),
        (("ZREVRANK", z, "1"), 0), (("ZSCORE", z, "1"), b"1"), (("ZRANDMEMBER", z), b"1"),
        (("ZSCAN", z, 0), [b"0", [b"1", b"1"]]), (("ZDIFF", 2, z, z), []),
        (("ZINTER", 2, z, z), [b"1"]), (("ZINTERCARD", 2, z, z), 1), (("ZUNION", 2, z, z), [b"1"]),
        (("PFCOUNT", pf), 2), (("GEODIST", geo, "Palermo", "Palermo"), b"0.0000"),
        (("GEOHASH", geo, "Palermo"), [b"sqc8b49rny0"]),
        (("GEOSEARCH", geo, "FROMMEMBER", "Palermo", "BYRADIUS", 1, "km"), [b"Palermo"]),
        (("GEORADIUS_RO", geo, "13.361389", "38.115556", 1, "km"), [b"Palermo"]),
        (("GEORADIUSBYMEMBER_RO", geo, "Palermo", 1, "km"), [b"Palermo"]),
        (("SORT_RO", l), [b"1", b"2"]),
    ]
    for command, expected in cases:
        cluster.routed(c, command, role, expected)
    for command in (("DUMP", s), ("TTL", s), ("PTTL", s), ("EXPIRETIME", s),
                    ("PEXPIRETIME", s), ("GEOPOS", geo, "Palermo"), ("OBJECT", "ENCODING", s)):
        assert cluster.routed(c, command, role, check=False) is not None
    # Hash-field expiry commands are available in both targeted backend versions.
    c.call("HPEXPIRE", h, 3600000, "FIELDS", 1, "f")
    cluster.sync()
    for command in ("HTTL", "HPTTL", "HEXPIRETIME", "HPEXPIRETIME"):
        assert cluster.routed(c, (command, h, "FIELDS", 1, "f"), role, check=False)[0] > 0
    # Every shard participates in a cross-slot fan-out; result order matters.
    keys = [f"{{shard-{i}}}:binary\x00".encode() for i in range(40)]
    keys = list({cluster.primary_for(k)["name"]: k for k in keys}.values())
    assert len(keys) == 3
    for i, k in enumerate(keys):
        c.call("SET", k, bytes([i, 0, 255]) * 3000)
    cluster.sync()
    cluster.routed(c, ("MGET", *keys, keys[0]), role,
                   [bytes([i, 0, 255]) * 3000 for i in range(3)] + [bytes([0, 0, 255]) * 3000])
    cluster.routed(c, ("EXISTS", *keys), role, 3)
    cluster.routed(c, ("TOUCH", *keys), role, 3)
    # Mutating reads and scripts must never escape to replicas.
    for command in (("GETEX", s, "EX", 3600), ("GETSET", s, "100"),
                    ("BITFIELD", s, "INCRBY", "u8", 0, 1),
                    ("EVAL", "return redis.call('GET',KEYS[1])", 1, s),
                    ("SORT", l, "STORE", tag + "sorted"), ("GETDEL", s)):
        cluster.routed(c, command, b"master", check=False, replicated=True)


def transaction_suite(cluster, c):
    key = "{txn}:value"
    c.call("SET", key, "before")
    cluster.sync()
    c.call("WATCH", key)
    cluster.routed(c, ("GET", key), b"master", b"before")
    assert c.call("MULTI") == b"OK"
    assert c.call("SET", key, "after") == b"QUEUED"
    assert c.call("GET", key) == b"QUEUED"
    assert c.call("EXEC") == [b"OK", b"after"]
    c.call("WATCH", key)
    other = Connection(cluster.proxy_port)
    try:
        other.call("SET", key, "conflict")
    finally:
        other.close()
    c.call("MULTI")
    c.call("GET", key)
    assert c.call("EXEC") is None
    cluster.sync()
    cluster.routed(c, ("GET", key), b"slave", b"conflict")


def pipeline_suite(cluster, port):
    seed = Connection(port)
    keys = [f"pipeline-{i}" for i in range(24)]
    values = [bytes([0, 255, i]) * 2000 for i in range(24)]
    for k, v in zip(keys, values):
        seed.call("SET", k, v)
    cluster.sync()
    seed.close()

    def run(i):
        c = Connection(port)
        try:
            if i % 2:
                assert isinstance(c.call("HELLO", 3), dict)
            for batch in range(20):
                commands, expected = [], []
                for j in range(24):
                    idx = (batch + j + i) % len(keys)
                    commands.extend((("GET", keys[idx]), ("SETEX", f"writer-{i}-{j}", 60, values[idx])))
                    expected.extend((values[idx], b"OK"))
                assert c.pipeline(commands) == expected, (i, batch)
        finally:
            c.close()
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        list(executor.map(run, range(16)))


def eventual_batch_suite(cluster, port):
    """Mixed batches preserve reply order but do not promise read-your-writes."""
    for resp in (2, 3):
        c = Connection(port)
        try:
            c.call("HELLO", resp)
            for i in range(40):
                key = f"eventual-{resp}-{i}-{uuid.uuid4().hex}"
                replies = c.pipeline([
                    ("SETEX", key, 60, "value"), ("GET", key),
                    ("HSET", key + "h", "field", "map-value"),
                    ("HGET", key + "h", "field"),
                    ("INCRBY", key + "counter", 100), ("GET", key + "counter"),
                ])
                assert replies[0] == b"OK" and replies[2] == 1 and replies[4] == 100, replies
                assert replies[1] in (None, b"value"), replies
                assert replies[3] in (None, b"map-value"), replies
                assert replies[5] in (None, b"100"), replies
            cluster.sync()
            assert c.call("GET", key) == b"value"
            assert c.call("HGET", key + "h", "field") == b"map-value"
            assert c.call("GET", key + "counter") == b"100"
        finally:
            c.close()


def topology_changes(cluster, c, stop_load):
    key = "{failover}:value"
    c.call("SET", key, "stable")
    cluster.sync()
    p = cluster.primary_for(key)
    r = cluster.replica_for(p)
    # Confirm replica selection, then loss and primary fallback.
    cluster.routed(c, ("GET", key), b"slave", b"stable")
    docker("stop", "-t", "0", r["name"])
    r["up"] = False
    r["c"].close()
    eventually(lambda: info(c, "proxy").get(b"proxy_replicas") == b"2")
    eventually(lambda: c.call("GET", key) == b"stable")
    cluster.routed(c, ("GET", key), b"master", b"stable")
    cluster.restart(r)
    cluster.sync()
    eventually(lambda: info(c, "proxy").get(b"proxy_replicas") == b"3")
    eventually(lambda: c.call("GET", key) == b"stable")
    cluster.routed(c, ("GET", key), b"slave", b"stable")
    # Planned promotion changes pool roles while keeping stable topology indices.
    r["c"].call("CLUSTER", "FAILOVER")
    eventually(lambda: r["c"].call("ROLE")[0] == b"master")
    eventually(lambda: p["c"].call("ROLE")[0] == b"slave")
    cluster.sync()
    eventually(lambda: c.call("SET", key, "promoted") == b"OK")
    cluster.sync()
    eventually(lambda: c.call("GET", key) == b"promoted")
    cluster.routed(c, ("GET", key), b"slave", b"promoted")
    cluster.routed(c, ("SET", key, "promoted"), b"master", b"OK", replicated=True)
    # Kill the new primary: automatic promotion and seed-independent refresh.
    docker("stop", "-t", "0", r["name"])
    r["up"] = False
    r["c"].close()
    eventually(lambda: p["c"].call("ROLE")[0] == b"master", timeout=40)
    eventually(lambda: c.call("SET", key, "recovered") == b"OK", timeout=40)
    cluster.roles()
    eventually(lambda: info(c, "proxy").get(b"proxy_replicas") == b"2")
    cluster.routed(c, ("GET", key), b"master", b"recovered")
    cluster.restart(r)
    eventually(lambda: r["c"].call("ROLE")[0] == b"slave")
    cluster.sync()
    eventually(lambda: info(c, "proxy").get(b"proxy_replicas") == b"3")
    cluster.routed(c, ("GET", key), b"slave", b"recovered")
    stop_load()
    # Global broadcasts must target three primaries, never both roles per shard.
    expected_size = sum(n["c"].call("DBSIZE") for n in cluster.nodes if n["role"] == b"master")
    assert c.call("DBSIZE") == expected_size
    keys = c.call("KEYS", "*")
    assert len(keys) == len(set(keys)) == expected_size
    assert c.call("FLUSHALL") == b"OK"
    cluster.sync()
    assert c.call("DBSIZE") == 0


def topology_suite(cluster, c, port):
    stop = threading.Event()
    # Initialize before background readers start. Replica reads can be stale,
    # but after synchronization this key must exist until a replica restarts.
    c.call("SET", "{failover}:value", "stable")
    cluster.sync()

    def load(i):
        conn = Connection(port)
        success, errors = 0, 0
        try:
            if i % 2:
                conn.call("HELLO", 3)
            while not stop.is_set():
                replies = conn.pipeline([
                    # Keep GET's backend counters exclusive to the routing
                    # assertions: other workers can briefly have older roles.
                    ("GETRANGE", "{failover}:value", 0, -1),
                    ("SET", f"{{failover}}:load-{i}", "load"),
                    ("MGET", "{failover}:value", "{other-shard}:absent"),
                ])
                for j, reply in enumerate(replies):
                    if isinstance(reply, ResponseError):
                        # In-flight operations are not retried during failover.
                        assert str(reply).startswith(("ERR backend", "ERR fan-out backend error", "ERR slot not mapped", "MOVED ",
                                                      "ASK ", "CLUSTERDOWN ", "READONLY ",
                                                      "LOADING ", "MASTERDOWN ")), str(reply)
                        errors += 1
                        continue
                    if j == 0:
                        assert reply in (b"", b"stable", b"promoted", b"recovered"), reply
                    elif j == 1:
                        assert reply == b"OK", reply
                    else:
                        assert (isinstance(reply, list) and len(reply) == 2 and reply[1] is None
                                and reply[0] in (None, b"stable", b"promoted", b"recovered")), reply
                    success += 1
                time.sleep(.002)
        finally:
            conn.close()
        return success, errors

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(load, i) for i in range(8)]

        def stop_load():
            stop.set()
            counts = [f.result(timeout=10) for f in futures]
            assert all(passed > 0 for passed, _ in counts), counts
            print(json.dumps({"failover_inflight_successes": sum(p for p, _ in counts),
                              "failover_inflight_errors": sum(e for _, e in counts)}), flush=True)

        try:
            topology_changes(cluster, c, stop_load)
        finally:
            stop.set()
            # Surface reader exceptions even when a topology assertion fails.
            for future in futures:
                future.result(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proxy-image", default="valkey-proxy:replica-local")
    parser.add_argument("--backend-image", default="valkey/valkey:9.0")
    parser.add_argument("--output", default="/tmp/valkey-proxy-replica-results")
    parser.add_argument("--keep", action="store_true", help="keep disposable containers for debugging")
    args = parser.parse_args()
    cluster = Cluster(args)
    try:
        cluster.start()
        c, _, _ = cluster.proxy(policy=None)
        cluster.record("default primary routing", lambda: routing_suite(cluster, c, False))
        c.close()
        for multiplex in (True, False):
            c, port, _ = cluster.proxy(multiplex=multiplex)
            cluster.proxy_port = port
            cluster.record(f"replica command matrix multiplex={multiplex}", lambda: routing_suite(cluster, c))
            cluster.record(f"transactions and WATCH multiplex={multiplex}", lambda: transaction_suite(cluster, c))
            cluster.record(f"binary RESP2/RESP3 ordered pipelines multiplex={multiplex}", lambda: pipeline_suite(cluster, port))
            cluster.record(f"mixed write/read batches eventually converge multiplex={multiplex}", lambda: eventual_batch_suite(cluster, port))
            cluster.record(f"replica loss, promotion, rejoin under load multiplex={multiplex}", lambda: topology_suite(cluster, c, port))
            c.close()
    finally:
        try:
            cluster.save_logs()
        finally:
            if args.keep:
                print("Kept resources:", cluster.prefix, flush=True)
            else:
                cluster.clean()


if __name__ == "__main__":
    main()
