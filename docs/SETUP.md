# Setup

## Preparation

In order to allow ecChronos to run there are a few tables that needs to be present.
The keyspace name is configurable and is `ecchronos` by default.
It is important that the keyspace is configured to replicate to all data centers.
It is also highly recommended to use `NetworkTopologyStategy`.

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
    PRIMARY KEY(host_id, job_id))
    WITH default_time_to_live = 2592000
    AND gc_grace_seconds = 0;
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

```
connection:
  cql:
    host: localhost
    port: 9042
  jmx:
    host: localhost
    port: 7199
```

If you have authentication/tls enabled you need to modify `conf/security.yml`:

```
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
    protocol: TLSv1.2
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
    protocol: TLSv1.2
    cipher_suites:
```

The security parameters can be updated during runtime and will automatically be picked up by ecc.

It's possible to override the default connection providers if needed.
More information about the custom connection provider can be found [here](STANDALONE.md).

## Running ecChronos

To run ecChronos execute `bin/ecc` from the root directory.
It is possible to use the flag `-f` to keep the process running in the foreground.
With the default setup a logfile will be created in the root directory called `ecc.log`.