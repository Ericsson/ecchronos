# CLAUDE.md — cassandra-test-image

This file provides guidance to Claude Code (claude.ai/code) when working with the `cassandra-test-image` module.

## Module Overview

`cassandra-test-image` provides the Docker-based Cassandra cluster infrastructure used by integration tests. It produces a test JAR (`cassandra-test-image-agent-tests`) consumed by `standalone-integration` and `ecchronos-binary`. It is not a runtime dependency.

**Maven artifact:** `cassandra-test-image-agent` (test JAR classifier: `tests`)
**Java class package:** `cassandracluster`

## Build Commands

```bash
# Create the test JAR (required before running integration tests in IDE)
mvn package -Dbuild-cassandra-test-jar -DskipTests

# Normal build (no test JAR unless -Dbuild-cassandra-test-jar is set)
mvn install -DskipTests=true
```

## Key Files

| File | Purpose |
|---|---|
| `src/main/docker/docker-compose.yml` | 4-node, 2-DC cluster definition |
| `src/main/docker/Dockerfile` | Custom Cassandra image (adds curl, Jolokia) |
| `src/main/docker/ecc-entrypoint.sh` | Startup script: auth, TLS, Jolokia, Nginx |
| `src/main/docker/create_keyspaces.cql` | **Schema for all ecChronos and test tables** |
| `src/main/docker/users.cql` | Creates `eccuser` role |
| `src/main/resources/generate_certificates.sh` | Generates TLS certs for all components |
| `src/test/java/cassandracluster/AbstractCassandraCluster.java` | Base class for integration tests |

## Schema Changes

**`create_keyspaces.cql` is the single source of truth** for all Cassandra schema used in tests, including:
- `ecchronos` keyspace (lock, repair_history, nodes_sync, on_demand_repair_status, reject_configuration)
- `test`, `test2`, `keyspaceWithCamelCase` keyspaces

When modifying schema:
1. Edit `create_keyspaces.cql`
2. Rebuild the test image: `mvn package -Dbuild-cassandra-test-jar -DskipTests`
3. Re-run integration tests to verify

**Also update `data` module prepared statements** if any `ecchronos.*` table schema changes.

## AbstractCassandraCluster

### Extending it

```java
public class MyCluster extends AbstractCassandraCluster {
    private static volatile boolean initialized = false;

    public static void ensureInitialized() {
        if (!initialized) {
            synchronized (MyCluster.class) {
                if (!initialized) {
                    setup();  // inherited @BeforeClass method
                    initialized = true;
                }
            }
        }
    }
}
```

The `setup()` method is a JUnit 4 `@BeforeClass` — call it from your own `@BeforeClass` or wrapper.

### Startup Timeout

The cluster waits up to `DEFAULT_WAIT_TIME_IN_MS = 90000` (90 seconds) for all 4 nodes to reach `UN` state. If tests fail with cluster startup errors, check:
- Docker daemon is running and has sufficient memory (at least 6 GB for 4 nodes)
- `MAX_HEAP_SIZE` env var is not set too low (default 3 GB per node)
- `DEFAULT_WAIT_TIME_IN_MS` may need increasing in slow CI environments

### Node Operations

```java
// Decommission a node for topology tests
decommissionNode("cassandra-node-dc2-rack1-node2");

// Wait for exactly 3 nodes to be up
waitForNodesToBeUp("cassandra-seed-dc1-rack1-node1", 3, 60000);
```

## Cluster Configuration

**System properties for test configuration:**

| Property | Default | Purpose |
|---|---|---|
| `it.cassandra.version` | `4.0` | Cassandra Docker image version |
| `it.jolokia.enabled` | `false` | Download and enable Jolokia agent |
| `project.build.directory` | `target` | Used to locate certificate directory |

**Node tuning (hardcoded in `ecc-entrypoint.sh`):**
- `num_tokens: 16` — 16 vnodes per node
- `cassandra.skip_wait_for_gossip_to_settle=0`
- `cassandra.ring_delay_ms=0`
- JMX auth disabled

## TLS/Certificate Setup

When TLS tests are needed, certificates must be generated before the cluster starts:

```bash
cd cassandra-test-image/src/main/resources
./generate_certificates.sh
```

This generates three chains:
- CQL: JKS keystore/truststore in `cert/`
- REST server: PKCS12 in `cert/server*`
- Nginx PEM: in `cert/pem/` (for Jolokia HTTPS proxy)

All keystores use password `ecctest`.

Certificates are mounted into containers at `/etc/certificates`. `ecc-entrypoint.sh` detects their presence and enables TLS automatically.

## Jolokia Support

To enable Jolokia (HTTP-based JMX), set `JOLOKIA=true` in docker-compose or via the `it.jolokia.enabled` system property. The startup script downloads `jolokia-agent-jvm` from Maven Central and attaches it to the JVM on port 8778.

For PEM/mutual TLS Jolokia (`PEM_ENABLED=true`), Nginx is installed as a reverse proxy on port 8443.

## Common Modification Scenarios

### Adding a new test keyspace or table
1. Add the `CREATE KEYSPACE` and `CREATE TABLE` statements to `create_keyspaces.cql`
2. Rebuild test JAR: `mvn package -Dbuild-cassandra-test-jar -DskipTests`

### Changing cluster topology
- Edit `docker-compose.yml` — add/remove services
- Update `AbstractCassandraCluster.CASSANDRA_SEED_NODE_NAME` if seed changes
- Update `waitForNodesToBeUp()` call to expect the new node count

### Adding a new user/role
- Add `CREATE ROLE` and `GRANT` statements to `users.cql`

### Bumping the Cassandra version
- Change `CASSANDRA_VERSION` in `docker-compose.yml` or pass `-Dit.cassandra.version=X.Y` at test time
- Verify that schema syntax is compatible with the new version

## Code Style

Follows the Cassandra code style (Sun Java conventions). CheckStyle and PMD run on `mvn compile`.
