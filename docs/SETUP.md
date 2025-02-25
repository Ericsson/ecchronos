# Setup

## Preparation

In order to allow ecChronos to run there are a few tables that needs to be present.
The keyspace name is configurable and is `ecchronos` by default.
It is important that the keyspace is configured to replicate to all data centers.
It is also highly recommended to use `NetworkTopologyStrategy`.

The required tables are shown below:
```
CREATE KEYSPACE IF NOT EXISTS ecchronos WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

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
```

A sample file is located in `conf/create_keyspace_sample.cql` which can be executed by running `cqlsh -f conf/create_keyspace_sample.cql`.
It is recommended to modify `SimpleStrategy` to `NetworkTopologyStrategy` with a replication factor according to your configuration.

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

Change the configuration in `conf/ecc.yml`.
To get started the connection configuration needs to match your local setup:

```yaml
connection:
  cql:
    host: localhost
    port: 9042
  jmx:
    host: localhost
    port: 7199
```

If ecChronos is deployed in a multi-site environment where clients can't connect to Cassandra nodes in remote sites
the remoteRouting must be disabled. If remote routing is enabled, locks will be taken in the remote data center.
Disabling remote routing will cause locks to be taken locally but with SERIAL consistency instead of LOCAL_SERIAL consistency.

```yaml
cql:
  remoteRouting: false
```

If you have authentication/tls enabled you need to modify `conf/security.yml`:

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

CQL also supports certificates in PEM format.

```yaml
cql:
  credentials:
    enabled: true
    username: cassandra
    password: cassandra
  tls:
    enabled: false
    certificate: /path/to/certificate
    certificate_private_key: /path/to/certificate_key
    trust_certificate: /path/to/certificate_authorities
    protocol: TLSv1.2,TLSv1.3
    cipher_suites:
    require_endpoint_verification: false
```

> **Note**
>
> In case certificate stores and PEM certificates are declared in `conf/security.yml` for CQL,
> PEM certificates takes precedence.

If Certificate Revocation Lists (CRL) is to be used for the CQL connections, add the following
section to the cql/tls section.

```yaml
    crl:
      enabled: true
      path: /path/to/crl/file.crl
      strict: true/false
      attempts: 5
      interval: 300
```

The security parameters can be updated during runtime and will automatically be picked up by ecc.

It's possible to override the default connection providers if needed.
More information about the custom connection provider can be found [here](STANDALONE.md).

For more advanced use-cases, it's possible to override the java-driver configuration,
please see [reference configuration](https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/configuration/reference/) for available configuration options.
To override default java-driver configuration,
follow any of the supported methods documented at [datastax docs](https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/configuration/#default-implementation-typesafe-config).

Examples:

`application.conf` in `conf` directory of ecChronos:

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

system properties (you can put these in `jvm.options` file of ecChronos:

```
-Ddatastax-java-driver.advanced.prepared-statements.prepare-on-all-nodes=false -Ddatastax-java-driver.advanced.prepared-statements.reprepare-on-up.enabled=false
```



## Running ecChronos

To run ecChronos execute `bin/ecc` or `bin/ecctool start` from the root directory.
It is possible to use the flag `-f` to keep the process running in the foreground.
With the default setup a logfile will be created in the root directory called `ecc.log`.
