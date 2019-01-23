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
```

A sample file is located in `conf/create_keyspace_sample.cql` which can be executed by running `cqlsh -f conf/create_keyspace_sample.cql`.
It is recommended to modify `SimpleStrategy` to `NetworkTopologyStrategy` and replication factor according to your configuration.

## Installation

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

Change the configuration in `conf/ecChronos.cfg`.
To get started the connection properties needs to match your local setup:

```
### Native connection properties
#connection.native.host=localhost
#connection.native.port=9042

### JMX connection properties
#connection.jmx.host=localhost
#connection.jmx.port=7199
```

If additional properties like SSL or authentication are needed it's possible to create custom connection providers.
More information about the custom connection provider can be found [here](STANDALONE.md).

## Running ecChronos

To run ecChronos execute `bin/ecChronos` from the root directory.
It is possible to use the flag `-f` to keep the process running in the foreground.
With the default setup a logfile will be created in the root directory called `ecChronos.log`.