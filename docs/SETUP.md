# Setup

## Preparation

In order to allow ecChronos to run there are a few tables that need to be present.
The keyspace name is configurable and is `ecchronos` by default.
It is important that the keyspace is configured to replicate to all data centers.
It is also highly recommended to use `NetworkTopologyStrategy`.

The required tables are shown below:

```cql
CREATE KEYSPACE IF NOT EXISTS ecchronos WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1};

CREATE TABLE IF NOT EXISTS ecchronos.lock (
    resource text,
    node uuid,
    metadata map<text,text>,
    PRIMARY KEY(resource))
    WITH default_time_to_live = 600
    AND gc_grace_seconds = 0;

CREATE TABLE IF NOT EXISTS ecchronos.lock_priority (
    resource text,
    node uuid,
    priority int,
    PRIMARY KEY(resource, node))
    WITH default_time_to_live = 600
    AND gc_grace_seconds = 0;

CREATE TABLE IF NOT EXISTS ecchronos.reject_configuration (
    keyspace_name text,
    table_name text,
    start_hour int,
    start_minute int,
    end_hour int,
    end_minute int,
    PRIMARY KEY(keyspace_name, table_name, start_hour, start_minute));

CREATE TYPE IF NOT EXISTS ecchronos.token_range (
    start text,
    end text);

CREATE TYPE IF NOT EXISTS ecchronos.table_reference (
    id uuid,
    keyspace_name text,
    table_name text);

CREATE TABLE IF NOT EXISTS ecchronos.on_demand_repair_status (
    host_id uuid,
    job_id uuid,
    table_reference frozen<table_reference>,
    token_map_hash int,
    repaired_tokens frozen<set<frozen<token_range>>>,
    status text,
    completed_time timestamp,
    repair_type text,
    PRIMARY KEY(host_id, job_id))
    WITH default_time_to_live = 2592000
    AND gc_grace_seconds = 0;

CREATE TABLE IF NOT EXISTS ecchronos.repair_history(
    table_id uuid,
    node_id uuid,
    repair_id timeuuid,
    job_id uuid,
    coordinator_id uuid,
    range_begin text,
    range_end text,
    participants set<uuid>,
    status text,
    started_at timestamp,
    finished_at timestamp,
    PRIMARY KEY((table_id,node_id), repair_id))
    WITH compaction = {'class': 'TimeWindowCompactionStrategy'}
    AND default_time_to_live = 2592000
    AND CLUSTERING ORDER BY (repair_id DESC);

CREATE TABLE IF NOT EXISTS ecchronos.nodes_sync(
    ecchronos_id TEXT,
    datacenter_name TEXT,
    node_id UUID,
    node_endpoint TEXT,
    node_status TEXT,
    last_connection TIMESTAMP,
    next_connection TIMESTAMP,
    PRIMARY KEY(ecchronos_id, datacenter_name, node_id))
    WITH CLUSTERING ORDER BY(datacenter_name DESC, node_id DESC);
```

A sample file is located in `conf/create_keyspace_sample.cql` which can be executed by running:

```bash
cqlsh -f conf/create_keyspace_sample.cql
```

It is recommended to modify `NetworkTopologyStrategy` with a replication factor according to your configuration.

## Installation

The package can be found in
[maven central](https://mvnrepository.com/artifact/com.ericsson.bss.cassandra.ecchronos/ecchronos-binary)
or in the [github releases section](https://github.com/Ericsson/ecchronos/releases).

Unpack `ecchronos-binary-<version>.tar.gz`.
The root directory should contain the following directories:

```
bin/
conf/
lib/
licenses/
statistics/
LICENSE.txt
NOTICE.txt
```

## Configuration

Change the configuration in `conf/ecc.yml`.

### Connection

ecChronos uses an agent-based connection model. Each instance must have a unique `instanceName`, which is used as the partition key (`ecchronos_id`) in the `nodes_sync` table.

The `type` property defines which nodes this instance is responsible for:

- `datacenterAware` — all nodes in the specified datacenters
- `rackAware` — nodes in specific racks within a datacenter
- `hostAware` — a specific list of hosts

```yaml
connection:
  cql:
    instanceName: unique_identifier
    type: datacenterAware
    localDatacenter: datacenter1
    contactPoints:
      - host: 127.0.0.1
        port: 9042
      - host: 127.0.0.2
        port: 9042

    datacenterAware:
      datacenters:
        - name: datacenter1
        - name: datacenter2

    rackAware:
      racks:
        - datacenterName: datacenter1
          rackName: rack1
        - datacenterName: datacenter1
          rackName: rack2

    hostAware:
      hosts:
        - host: 127.0.0.1
          port: 9042
        - host: 127.0.0.2
          port: 9042
```

#### Topology Reload

ecChronos periodically reloads the cluster topology to detect node additions or removals that may have been missed by driver events. The interval is configurable:

```yaml
connection:
  cql:
    reloadSchedule:
      initialDelay: 1
      fixedDelay: 1
      unit: days
```

For more details on topology change management see [ARCHITECTURE.md](ARCHITECTURE.md).

### JMX Connection

By default ecChronos connects to Cassandra JMX via native RMI on port `7199`:

```yaml
connection:
  jmx:
    port: 7199
```

#### Jolokia

ecChronos supports connecting to Cassandra JMX over HTTP/HTTPS using the [Jolokia](https://jolokia.org/) protocol instead of native RMI. This is the recommended approach for containerized environments.

```yaml
connection:
  jmx:
    jolokia:
      enabled: false
      port: 8778
      usePem: false   # Enable TLS with PEM certificates (Jolokia only, requires reverse proxy)
    reverseDNSResolution: false
```

- `usePem` enables TLS communication using PEM certificates. Only supported with Jolokia in reverse proxy scenarios (e.g., NGINX). Certificate paths are configured in `conf/security.yml`.
- `reverseDNSResolution` enables reverse DNS lookups to resolve IP addresses to hostnames. Useful in containerized environments where certificates use DNS names instead of IPs.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full Jolokia + reverse proxy architecture and PEM certificate setup.

### Lock Factory

The CAS lock factory controls distributed repair locking via Cassandra lightweight transactions:

```yaml
lock_factory:
  cas:
    keyspace: ecchronos
    cache_expiry_time_in_seconds: 30
    consistencySerial: "SERIAL"   # or "LOCAL" for LOCAL_SERIAL
    locks_per_resource: 3
```

`locks_per_resource` defines how many parallel repairs can run per datacenter. All ecChronos instances managing the same cluster **must use the same value**. Increase this if jobs are falling behind (LATE/OVERDUE), but monitor compaction throughput and read latencies.

### Scheduler

```yaml
scheduler:
  frequency:
    time: 30
    unit: SECONDS
  session_window:
    time: 5
    unit: MINUTES
  cooldown:
    time: 0
    unit: SECONDS
```

- `session_window` — how long a node holds a distributed lock and executes repair tasks in a single batch before releasing. Reduces lock acquisition overhead in large clusters.
- `cooldown` — wait time after a session completes before competing for a new lock. Set to `0` to disable.

### Security

If you have authentication or TLS enabled, modify `conf/security.yml`:

```yaml
cql:
  credentials:
    enabled: true
    username: cassandra
    password: cassandra
  tls:
    enabled: false
    keystore: /path/to/keystore
    keystore_password: ecchronos
    truststore: /path/to/truststore
    truststore_password: ecchronos
    protocol: TLSv1.2,TLSv1.3
    algorithm:
    store_type: JKS
    cipher_suites:
    require_endpoint_verification: false

jmx:
  credentials:
    enabled: true
    username: cassandra
    password: cassandra
  tls:
    enabled: false
    keystore: /path/to/keystore
    keystore_password: ecchronos
    truststore: /path/to/truststore
    truststore_password: ecchronos
    protocol: TLSv1.2,TLSv1.3
    cipher_suites:
```

CQL also supports certificates in PEM format (EC and RSA algorithms only):

```yaml
cql:
  tls:
    enabled: true
    certificate: /path/to/certificate
    certificate_private_key: /path/to/certificate_key
    trust_certificate: /path/to/certificate_authorities
    protocol: TLSv1.2,TLSv1.3
    cipher_suites:
    require_endpoint_verification: false
```

> **Note:** If both keystore and PEM certificates are declared in `conf/security.yml` for CQL, PEM certificates take precedence.

JMX PEM certificate support is available **only when Jolokia is enabled** with a reverse proxy:

```yaml
jmx:
  tls:
    certificate: /path/to/certificate
    certificate_private_key: /path/to/certificate_key
    trust_certificate: /path/to/certificate_authorities
    algorithm: "EC"
```

#### Certificate Revocation Lists (CRL)

To use CRL for CQL connections, add the following to the `cql.tls` section in `conf/security.yml`:

```yaml
cql:
  tls:
    crl:
      enabled: true
      path: /path/to/crl/file.crl
      strict: false   # true = reject if CRL is missing or empty
      attempts: 5
      interval: 300   # seconds between CRL file rescans
```

The security parameters are reloaded at runtime and picked up automatically by ecChronos.

### Custom Connection Providers

It is possible to override the default connection providers if needed.
More information about custom connection providers can be found in [STANDALONE.md](STANDALONE.md).

### Java Driver Configuration

For advanced use-cases, the java-driver configuration can be overridden.
See the [reference configuration](https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/configuration/reference/) for available options.

Example `application.conf` in the `conf` directory:

```
datastax-java-driver {
  advanced.prepared-statements {
    prepare-on-all-nodes = false
    reprepare-on-up {
      enabled = false
    }
  }
}
```

Or via system properties in `conf/jvm.options`:

```
-Ddatastax-java-driver.advanced.prepared-statements.prepare-on-all-nodes=false
-Ddatastax-java-driver.advanced.prepared-statements.reprepare-on-up.enabled=false
```

## Running ecChronos

To run ecChronos execute `bin/ecctool start` from the root directory.
It is also possible to run `bin/ecc` directly, using the flag `-f` to keep the process running in the foreground.
With the default setup a logfile will be created in the root directory called `ecc.log`.
