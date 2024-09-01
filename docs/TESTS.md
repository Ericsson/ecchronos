# Tests

Make sure to add tests to the relevant phase(s) when possible.
If mocks or a single Apache Cassandra instance is necessary it may be run as a unit test.
If multiple Apache Cassandra instances are necessary then test cases should be added to `standalone-integration` and/or `osgi-integration` tests.
If HTTP related functionality is changed then `ecchronos-binary` test cases should be changed.

## Running the tests

There are a few different tests that can be run:

* Unit tests
* Docker tests
    * Integration tests
    * Acceptance tests

It's recommended to run all tests `mvn clean verify -Dprecommit.tests` before creating a PR.
Make sure to add test cases to the appropriate test suite.

### Unit tests

The unit tests are run by `mvn clean test`.
They are running simple tests that sometimes utilize a single embedded Cassandra node.

### Docker tests

The current test setup employs Testcontainers to create a simplified Cassandra instance for unit testing and a full Cassandra cluster for integration and manual tests. To conduct manual tests, you can utilize the template provided in the [cassandra-test-image](../cassandra-test-image/src/main/docker/docker-compose.yml). To set up the cluster, navigate to the directory containing the docker-compose.yml file and execute the following command:

```bash
docker-compose -f docker-compose.yml up --build
```

For integration tests, the system uses the [AbstractCassandraCluster](../cassandra-test-image/src/test/java/cassandracluster/AbstractCassandraCluster.java) class. This class can be imported into other modules by including the cassandra-test-image test package in the `pom.xml`, as follows:

```xml
<dependency>
    <groupId>com.ericsson.bss.cassandra.ecchronos</groupId>
    <artifactId>cassandra-test-image</artifactId>
    <classifier>tests</classifier>
    <version>${project.version}</version>
    <scope>test</scope>
</dependency>
```

Before executing tests in other modules, ensure that the cassandra-test-image JAR file has been generated. This can be done by running the following Maven command:

```bash
mvn package -Dbuild-cassandra-test-jar -DskipTests
```

### Integration tests

The integration tests tries to start ecChronos with a cluster of nodes and verfiy that repairs are run.
They are activated by using `-P osgi-integration-tests` or `-P standalone-integration-tests`.
It is possible to run either OSGi integration tests or the standalone tests without the other.
This can be done by running either `mvn clean install -P docker-integration-test,osgi-integration-tests` or `mvn clean install -P docker-integration-test,standalone-integration-tests`.

### Acceptance tests

The acceptance test use behave to verify the python scripts as well as the REST server in ecChronos.
They are activated by using `-P python-integration-tests` in combination with the docker flag.

### Running acceptance/integration tests towards local ccm cluster

Running acceptance and integration tests towards local ccm cluster will save a lot of time because the Cassandra instances
will be reused for the tests.
This is especially useful during test development to have shorter feedback loop.

**NOTE: These tests does not use TLS or auth therefore,
it's recommended to run all the tests using `mvn clean verify -Dprecommit.tests` once you're done with development**

1. Setup local ccm cluster

```
ccm create test -n 4 -v 4.0 --vnodes; ccm updateconf "num_tokens: 16"; ccm start;
```

2. Prepare keyspaces and tables:

```
ccm node1 cqlsh -f cassandra-test-image/src/main/docker/create_keyspaces.cql
```

3. Run the tests.

Python integration tests
```
mvn clean install -P local-python-integration-tests
```

Standalone integration tests
```
mvn clean install -P local-standalone-integration-tests
```

OSGi integration tests
```
mvn clean install -P local-osgi-integration-tests
```

All the above in one property.
```
mvn clean install -Dlocalprecommit.tests
```

### Running acceptance tests towards local ecChronos&Cassandra

For development, it's much faster to run behave tests without needing to do the ecChronos/Cassandra setup each time.
Running behave tests manually is roughly 7x faster than running through `python-integration-tests`.

1. Install behave and dependencies (this is only needed first time)

```
pip install behave
pip install requests
pip install jsonschema
pip install cassandra-driver
```

2. Prepare tables and configuration for tests

Create keyspaces and tables in Cassandra (make sure to replace datacenter name with whatever you have).
It is important that `ecchronos` keyspace has replication factor of 1 (tests depend on this).

```
CREATE KEYSPACE IF NOT EXISTS ecchronos WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};
CREATE TYPE IF NOT EXISTS ecchronos.token_range (start text, end text);
CREATE TYPE IF NOT EXISTS ecchronos.table_reference (id uuid, keyspace_name text, table_name text);
CREATE TABLE IF NOT EXISTS ecchronos.on_demand_repair_status (host_id uuid, job_id uuid, table_reference frozen<table_reference>, token_map_hash int, repaired_tokens frozen<set<frozen<token_range>>>, status text, completed_time timestamp, PRIMARY KEY(host_id, job_id)) WITH default_time_to_live = 2592000 AND gc_grace_seconds = 0;
CREATE TABLE IF NOT EXISTS ecchronos.lock (resource text, node uuid, metadata map<text,text>, PRIMARY KEY(resource)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0;
CREATE TABLE IF NOT EXISTS ecchronos.lock_priority (resource text, node uuid, priority int, PRIMARY KEY(resource, node)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0;
CREATE TABLE IF NOT EXISTS ecchronos.reject_configuration (keyspace_name text, table_name text, start_hour int, start_minute int, end_hour int, end_minute int, PRIMARY KEY(keyspace_name, table_name, start_hour, start_minute));
CREATE TABLE IF NOT EXISTS ecchronos.repair_history(table_id uuid, node_id uuid, repair_id timeuuid, job_id uuid, coordinator_id uuid, range_begin text, range_end text, participants set<uuid>, status text, started_at timestamp, finished_at timestamp, PRIMARY KEY((table_id,node_id), repair_id)) WITH compaction = {'class': 'TimeWindowCompactionStrategy'} AND default_time_to_live = 1728000 AND CLUSTERING ORDER BY (repair_id DESC);
CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
CREATE TABLE IF NOT EXISTS test.table1 (key1 text, key2 int, value int, PRIMARY KEY(key1, key2));
CREATE TABLE IF NOT EXISTS test.table2 (key1 text, key2 int, value int, PRIMARY KEY(key1, key2));
CREATE KEYSPACE IF NOT EXISTS test2 WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
CREATE TABLE IF NOT EXISTS test2.table1 (key1 text, key2 int, value int, PRIMARY KEY(key1, key2));
CREATE TABLE IF NOT EXISTS test2.table2 (key1 text, key2 int, value int, PRIMARY KEY(key1, key2));
```

To speed up tests it is recommended to change scheduler frequency to 5 seconds in ecc.yml.

```
scheduler:
  frequency:
    time: 5
    unit: SECONDS
```

3. Start ecChronos

**ecChronos must be listening on `http://localhost:8080`**

```
ecctool start
```

4. Change working directory to `ecchronos-binary/src/test/behave`

```
cd ecchronos-binary/src/test/behave
```

5. Run the tests

**Make sure to change define variables before running.**

- ecctool - should point to your local ecctool (Mandatory)
- cassandra_address - should point to your Cassandra IP (Mandatory)
- cql_user - should be the CQL user that has permissions for ecchronos keyspace
  (Mandatory if auth is enabled, otherwise skip defining).
  This will be used to clean up the ecchronos.on_demand_repair_status table.
- cql_password - should be the password for CQL user that has permissions for ecchronos keyspace
  (Mandatory if auth is enabled, otherwise skip defining).
  This will be used to clean up the ecchronos.on_demand_repair_status table.
- no_tls - disables secure connection towards Cassandra.

Example (no auth, no tls), run all behave tests

```
behave --define ecctool=/usr/bin/ecctool --define cassandra_address="127.0.0.1" --define no_tls
```

Example (no auth, no tls), run specific tests (feature)

```
behave --define ecctool=/usr/bin/ecctool --define cassandra_address="127.0.0.1" --define no_tls --include features/ecc-spring.feature
```

Example (auth, tls), run all behave tests

```
behave --define ecctool=/usr/bin/ecctool --define cassandra_address="127.0.0.1" --define cql_user="cassandra" --define cql_password="cassandra"
```

Example (auth, tls), run specific tests (feature)

```
behave --define ecctool=/usr/bin/ecctool --define cassandra_address="127.0.0.1" --define cql_user="cassandra" --define cql_password="cassandra" --include features/ecc-spring.feature
```

### Maven configuration properties

| Property                   | Default    | Description                                              |
|----------------------------|------------|----------------------------------------------------------|
| it.cassandra.memory        | 1073741824 | Memory limit for the docker instance                     |
| it.cassandra.heap          | 256M       | Amount of heap memory Cassandra should use at most       |

### Running within IDEA/Eclipse

If you already have a three node cluster setup (through docker, ccm, etc) this can be used instead.

Make sure to run the following command (with correct data centers specified in the create_keyspaces.cql) to prepare the cluster.
```
cqlsh -f standalone-integration/src/test/resources/create_keyspaces.cql
```

The test cases needs to be run with the following system properties set:

| Variable                 |  Description                                  |
|--------------------------|-----------------------------------------------|
| it-cassandra.ip          | The host ip of one of the Cassandra instances |
| it-cassandra.native.port | The native port used                          |
| it-cassandra.jmx.port    | The JMX port used                             |

### Break down into end to end tests

The `standalone-integration` tests runs a setup similar to the standalone application to verify automated repairs.
The `osgi-integration` tests runs in a OSGi environment to verify that all services are available and verifies automated repairs.