# Contributing

You are most welcome to create pull requests and issues to ecChronos.
Before creating pull requests it is recommended to first discuss your idea with at least one of the owners of the repository.
For keeping track of the history it is recommended that most communication is performed or summarized within pull requests or issues.

## Development

### Prerequisites

* Maven
* Java 17 / 21
* Docker (for test setup)
* Python 3

### Branches

Target the lowest applicable version of ecChronos when fixing an issue.
Bug fixes should be targeted to the lowest maintained version where the bug reside.
New features should, in general, be added to the master branch.

### Code Style

This project uses the cassandra code style which is based on Sun’s Java coding conventions.
Formatting rules for eclipse can be found [here](../code_style.xml).

Provided patches should be contained and should not modify code outside the scope of the patch.
This will make it quicker to perform reviews and merging the pull requests.

### Logging

The table below describe the main criteria(s) for each debugging level. The levels are in the order of (from most to least):
<br>
<pre>
  all > trace > debug > info > warn > error > off
</pre>

| Log&nbsp;level | Description                                                                                                                                                                                                                                                                                                                                          |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| all            | All levels will be logged <br> <b>Example:</b> -                                                                                                                                                                                                                                                                                                     |
| trace          | Detailed debugging (flows, request/response, details, etc). Will have a performance impact and is therefore not for production, unless it is a planned troubleshooting activity. Mainly used during development. <br>  <b>Example:</b> Every method call is logged in detail for a certain request and response flow. Used data is logged in detail. |
| debug          | Simple debug logging which can be turned on and used in production if necessary (should have no impact on performance). The logs at this level should be of the type a developer might need to spot a quick fix to a problem or to at least isolate the problem further. <br> <b>Example:</b> Specific events with contextual details.               |
| info           | For logging the normal flow and operation of the service(s). <br> <b>Example:</b> Service health, progress of requests/responses etc.                                                                                                                                                                                                                |
| warn           | Behaviors in the service(s) which are unexpected and potentially could lead to errors, but were handled for the moment. However, the service(s) as such are still working normally and as expected. <br> <b>Example:</b> A primary service switching to a secondary one, connection retries, reverting to defaults etc.                              |
| error          | A service or dependency have failed in the sense no requests can be served and/or data processed cannot be trusted. <br> <b>Example:</b> Connection attempts that ultimate fail. Crucial resources not available.                                                                                                                                    |
| off            | No levels will be logged at all. <br> <b>Example:</b> -                                                                                                                                                                                                                                                                                              |                                                                                                                                                                                                                                                |

If the log message may require lengthy calculations, method calls to collect data or concatenations, use an <i>is...Enabled</i> block to guard it. An example would be:
<pre>
  if (LOG.isDebugEnabled())
  {
    LOG.debug("Environment status: {}", <b>getSyncEnvironmentStatus()</b>);
  }
</pre>

### Builds

The builds required to merge a pull request are contained within the [Github configuration](../.github/workflows/actions.yml) and include tests, code coverage as well as PMD checks.

All checks need to pass before merging a pull request.
The current PMD rules are constantly evolving.
If you encounter a PMD rule that seems odd or non-relevant feel free to discuss it within an issue or pull request.

#### Built with

* [Maven](https://maven.apache.org) - Dependency and build management
* [docker-maven-plugin](https://github.com/fabric8io/docker-maven-plugin) - For integration tests

### Maintained versions

The following table state what versions of ecChronos is still under active maintenance.

| Version |  First Release   | Status |
|:-------:|:----------------:|:------:|
|  1.x.x  | Not yet released |   -    |


### REST API

Whenever there are any changes to the REST API, the OpenAPI specification must be generated.
Generation of the OpenAPI specification is done by running the `python-integration-tests` tests.
The generated specification is stored in `docs/autogenerated`.

### ecctool documentation

If changes have been made to ecctool, the documentation must be generated using `mvn clean install -P generate-ecctool-doc -DskipUTs`.
On top of that, if there's any change in the output of ecctool the [examples](ECCTOOL_EXAMPLES.md) must be updated manually.

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

The acceptance tests use pytest and behave to verify the Python scripts as well as the REST server in ecChronos.

#### Full Integration Tests (`python-integration-tests`)
This profile is designed for CI environments and provides complete test coverage:
- Creates certificates and sets up TLS authentication
- Uses Docker containers with full security configuration
- Does **not** create a virtual environment automatically
- Installs Python dependencies directly to the system/current environment

To run locally:
```bash
mvn clean verify -Dprecommit.tests
```

**Prerequisites:**
- Install Python dependencies manually: `pip install -r ecchronos-binary/target/test/requirements.txt`
- OR create and activate your own virtual environment before running

#### Local Development Tests (`local-python-integration-tests`)
This profile is optimized for local development:
- Automatically creates a Python virtual environment
- Installs all required dependencies in isolation
- Uses simplified configuration (no TLS/auth)
- Faster execution for development cycles

To run:
```bash
mvn clean install -Dlocalprecommit.tests

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
CREATE KEYSPACE IF NOT EXISTS ecchronos WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2': 1};
CREATE TYPE IF NOT EXISTS ecchronos.token_range (start text, end text);
CREATE TYPE IF NOT EXISTS ecchronos.table_reference (id uuid, keyspace_name text, table_name text);
CREATE TABLE IF NOT EXISTS ecchronos.nodes_sync(ecchronos_id TEXT, datacenter_name TEXT, node_id UUID, node_endpoint TEXT, node_status TEXT, last_connection TIMESTAMP, next_connection TIMESTAMP, PRIMARY KEY(ecchronos_id, datacenter_name, node_id)) WITH CLUSTERING ORDER BY(datacenter_name DESC, node_id DESC);
CREATE TABLE IF NOT EXISTS ecchronos.on_demand_repair_status (host_id uuid, job_id uuid, table_reference frozen<table_reference>, token_map_hash int, repaired_tokens frozen<set<frozen<token_range>>>, status text, completed_time timestamp, repair_type text, PRIMARY KEY(host_id, job_id)) WITH default_time_to_live = 2592000 AND gc_grace_seconds = 0;
CREATE TABLE IF NOT EXISTS ecchronos.lock (resource text, node uuid, metadata map<text,text>, PRIMARY KEY(resource)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0;
CREATE TABLE IF NOT EXISTS ecchronos.lock_priority (resource text, node uuid, priority int, PRIMARY KEY(resource, node)) WITH default_time_to_live = 600 AND gc_grace_seconds = 0;
CREATE TABLE IF NOT EXISTS ecchronos.reject_configuration (keyspace_name text, table_name text, start_hour int, start_minute int, end_hour int, end_minute int, PRIMARY KEY(keyspace_name, table_name, start_hour, start_minute));
CREATE TABLE IF NOT EXISTS ecchronos.repair_history(table_id uuid, node_id uuid, repair_id timeuuid, job_id uuid, coordinator_id uuid, range_begin text, range_end text, participants set<uuid>, status text, started_at timestamp, finished_at timestamp, PRIMARY KEY((table_id,node_id), repair_id)) WITH compaction = {'class': 'TimeWindowCompactionStrategy'} AND default_time_to_live = 1728000 AND CLUSTERING ORDER BY (repair_id DESC);
CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2': 1};
CREATE TABLE IF NOT EXISTS test.table1 (key1 text, key2 int, value int, PRIMARY KEY(key1, key2));
CREATE TABLE IF NOT EXISTS test.table2 (key1 text, key2 int, value int, PRIMARY KEY(key1, key2));
CREATE KEYSPACE IF NOT EXISTS test2 WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2': 1};
CREATE TABLE IF NOT EXISTS test2.table1 (key1 text, key2 int, value int, PRIMARY KEY(key1, key2));
CREATE TABLE IF NOT EXISTS test2.table2 (key1 text, key2 int, value int, PRIMARY KEY(key1, key2));
CREATE KEYSPACE IF NOT EXISTS "keyspaceWithCamelCase" WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2, 'datacenter2': 1};
CREATE TABLE IF NOT EXISTS "keyspaceWithCamelCase"."tableWithCamelCase" (key1 text, key2 int, value int, PRIMARY KEY(key1, key2));
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