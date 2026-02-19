# standalone-integration Module

The `standalone-integration` module contains end-to-end integration tests that verify the full repair scheduling flow against a real multi-node Cassandra cluster managed by Docker Compose (via `cassandra-test-image`).

---

## Module Structure

```
standalone-integration/src/test/
├── java/com/ericsson/bss/cassandra/ecchronos/standalone/
│   ├── SharedCassandraCluster.java         # Thread-safe cluster singleton
│   ├── TestBase.java                       # Shared test infrastructure
│   ├── ITSchedules.java                    # Scheduled vnode/sub-range repair tests
│   ├── ITOnDemandRepairJob.java            # On-demand vnode repair tests
│   ├── ITIncrementalSchedules.java         # Scheduled incremental repair tests
│   ├── ITIncrementalOnDemandRepairJob.java # On-demand incremental repair tests
│   └── ITRepairInfo.java                   # Repair statistics tests
└── resources/
    ├── logback-test.xml                    # Test logging configuration
    └── security.yml                        # CQL credentials for tests
```

---

## Test Classes

### ITSchedules

Tests the automatic repair scheduler for vnode and sub-range repairs:
- `repairSingleTable()` — basic single table vnode repair
- `repairSingleTableInParallel()` — parallel vnode repair
- `repairSingleTableRepairedInSubRanges()` — sub-range repair detection
- `repairMultipleTables()` — multiple concurrent tables
- `partialTableRepair()` — repair with pre-existing partial history

### ITOnDemandRepairJob

Tests user-triggered repairs that bypass the scheduler:
- `repairSingleTable()` — trigger repair on one table
- `repairMultipleTables()` — trigger repairs on multiple tables
- `repairSameTableTwice()` — queue multiple jobs on the same table

### ITIncrementalSchedules

Tests incremental repair scheduling using Cassandra's native `percentRepaired`/`maxRepairedAt` metrics:
- `repairSingleTable()` — waits for metrics to reach ≥95% before asserting `COMPLETED`
- Timeout: 300 seconds (longer due to JMX metric update latency)

### ITIncrementalOnDemandRepairJob

Tests on-demand incremental repairs:
- `repairSingleTable()`, `repairMultipleTables()`, `repairSameTableTwice()`
- Inserts and flushes data before triggering, then monitors `percentRepaired` and `maxRepairedAt`

### ITRepairInfo

Tests repair statistics retrieval from both history sources (parameterised):
- `repairInfoForRepaired()` — full repair stats
- `repairInfoForRepairedInSubRanges()` — sub-range repair stats
- `repairInfoForHalfOfRangesRepaired()` — partial repair stats

---

## Test Infrastructure

### SharedCassandraCluster

A thread-safe singleton that wraps `AbstractCassandraCluster`. All test classes call `ensureInitialized()` in their `@BeforeClass`, sharing a single Docker cluster for the entire test run.

### TestBase

Provides shared state for all test classes:

| Member | Purpose |
|---|---|
| `myNativeConnectionProvider` | CQL session (eccuser credentials) |
| `myAdminNativeConnectionProvider` | CQL session (cassandra/cassandra) |
| `myJmxConnectionProvider` | JMX connections to all nodes |
| `myJmxProxyFactory` | Factory for JMX proxies |
| `myEccNodesSync` | Node ownership tracking |

Helper methods:
- `insertSomeDataAndFlush(tableRef, session, node)` — inserts 1000 UUID rows and flushes to disk
- `getSession()`, `getNode()`, `getJmxProxyFactory()`, etc.

---

## Repair State Verification

Tests verify repairs in two ways:

**For vnode repairs:** Inspect `ecchronos.repair_history` for completed entries covering the expected token ranges.

**For incremental repairs:** Poll `CassandraMetrics` for `percentRepaired >= 95%` and `maxRepairedAt >= testStartTime`.

Both use Awaitility for async polling:
```java
await().pollInterval(1, SECONDS).atMost(180, SECONDS).until(() -> isRepairedSince(tableRef, startTime));
```

---

## Running the Tests

```bash
# Via Maven (requires Docker)
mvn verify -P standalone-integration-tests -DskipUTs

# Single test class
mvn test -Dtest=ITSchedules

# Single test method
mvn test -Dtest=ITSchedules#repairSingleTable

# Against a local CCM cluster
ccm create test -n 4 -v 4.0 --vnodes && ccm start
mvn clean install -Dlocalprecommit.tests
```

**System properties:**

| Property | Default | Purpose |
|---|---|---|
| `it.cassandra.version` | `4.0` | Cassandra version for Docker cluster |
| `it.jolokia.enabled` | `false` | Enable Jolokia JMX bridge |

---

## Dependencies

All dependencies are `test` scope:

| Dependency | Purpose |
|---|---|
| `cassandra-test-image-agent` (tests) | `AbstractCassandraCluster`, Docker cluster |
| `application-agent` | Full Spring Boot wiring for repair stack |
| `core-agent`, `core.impl-agent` | Repair scheduling classes under test |
| `connection-agent`, `connection.impl-agent` | CQL/JMX connections |
| `data-agent` | Repair history service |
| Awaitility | Async repair completion polling |
| Mockito | Mocking `TableRepairMetrics` |
