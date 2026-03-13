# cassandra-test-image Module

The `cassandra-test-image` module provides a managed, Docker-based Apache Cassandra cluster for integration testing. It encapsulates all the complexity of setting up a realistic multi-node, multi-datacenter cluster, and provides a Java base class that other test modules extend.

---

## Module Structure

```
cassandra-test-image/
├── src/main/
│   ├── docker/
│   │   ├── Dockerfile                        # Custom Cassandra image
│   │   ├── docker-compose.yml               # 4-node, 2-DC cluster
│   │   ├── ecc-entrypoint.sh                # Cassandra startup + config script
│   │   ├── setup_db.sh                      # Runs CQL initialisation scripts
│   │   ├── create_keyspaces.cql             # ecChronos + test keyspace schemas
│   │   ├── users.cql                        # eccuser role creation
│   │   └── cassandra-rackdc-*.properties    # Rack/DC topology files
│   └── resources/
│       └── generate_certificates.sh         # TLS certificate generation
└── src/test/java/cassandracluster/
    ├── AbstractCassandraCluster.java        # Base class for integration tests
    └── TestCassandraCluster.java            # Example usage
```

---

## Cluster Topology

The Docker Compose file defines a 4-node cluster across 2 datacenters using `GossipingPropertyFileSnitch`:

| Node | Datacenter | Rack | IP |
|---|---|---|---|
| `cassandra-seed-dc1-rack1-node1` | datacenter1 | rack1 | 172.29.0.2 |
| `cassandra-seed-dc2-rack1-node1` | datacenter2 | rack1 | 172.29.0.3 |
| `cassandra-node-dc1-rack1-node2` | datacenter1 | rack1 | 172.29.0.4 |
| `cassandra-node-dc2-rack1-node2` | datacenter2 | rack1 | 172.29.0.5 |

**Configuration:** 16 vnodes per node, auto-snapshots disabled, JMX auth disabled, zero ring/gossip delays for fast startup.

---

## Keyspace Schema (`create_keyspaces.cql`)

| Keyspace | Replication | Purpose |
|---|---|---|
| `ecchronos` | NTS: dc1=1, dc2=1 | ecChronos system tables (lock, repair_history, etc.) |
| `test` | NTS: dc1=2, dc2=1 | Integration test tables (table1, table2, table3) |
| `test2` | NTS: dc1=2, dc2=1 | Additional test tables |
| `keyspaceWithCamelCase` | NTS: dc1=2, dc2=1 | Tests for case-sensitive identifiers |

---

## AbstractCassandraCluster

The Java base class used by integration test modules.

**Lifecycle:**
1. Starts Docker Compose container (shared across all tests in a class)
2. Waits up to 90 seconds for all 4 nodes to be `UN` (up/normal)
3. Updates `system_auth` keyspace for multi-DC auth
4. Runs a full cluster repair
5. Runs `setup_db.sh` to initialise keyspaces, tables, and the `eccuser` role
6. Verifies the `ecchronos` keyspace exists before returning

**Key members:**

| Member | Type | Purpose |
|---|---|---|
| `composeContainer` | `DockerComposeContainer` | The running cluster |
| `containerIP` | `String` | Seed node IP for CQL connections |
| `mySession` | `CqlSession` | Optional shared session |

**Helper methods:**

| Method | Purpose |
|---|---|
| `createDefaultSession()` | Creates a CQL session with `cassandra`/`cassandra` credentials |
| `defaultBuilder()` | Returns a pre-configured `CqlSessionBuilder` |
| `decommissionNode(String)` | Runs `nodetool decommission` inside the container |
| `waitForNodesToBeUp(String, int, long)` | Polls `nodetool status` until expected node count is reached |

---

## TLS/Certificate Support

`generate_certificates.sh` creates three certificate chains:
- **CQL certificates** — for Cassandra client encryption (JKS keystore/truststore)
- **REST/server certificates** — for ecChronos REST API TLS (PKCS12)
- **Nginx PEM certificates** — for secure Jolokia HTTP proxy (mutual TLS)

`ecc-entrypoint.sh` configures TLS if certificates are present in `/etc/certificates`.

---

## Usage by Other Modules

### standalone-integration

Extends `AbstractCassandraCluster` via `SharedCassandraCluster` (a thread-safe singleton wrapper). Test classes extend `TestBase`, which calls `SharedCassandraCluster.ensureInitialized()`.

### ecchronos-binary

Uses the `docker-compose.yml` for BDD acceptance tests and extracts certificate resources from the test JAR using `maven-dependency-plugin`.

---

## Building

The module produces a test JAR (classifier `tests`) for use by other modules:

```bash
# Required before running integration tests in an IDE
mvn package -Dbuild-cassandra-test-jar -DskipTests
```

The test JAR is not published to Maven Central.
