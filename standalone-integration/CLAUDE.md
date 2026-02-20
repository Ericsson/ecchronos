# CLAUDE.md — standalone-integration

This file provides guidance to Claude Code (claude.ai/code) when working with the `standalone-integration` module.

## Module Overview

`standalone-integration` contains end-to-end integration tests (`IT*.java`) that verify the full repair scheduling pipeline against a real 4-node, 2-datacenter Cassandra cluster. All tests are `test` scope only — no production code lives here.

**Maven artifact:** `standalone-integration-agent` (not published)
**Test package:** `com.ericsson.bss.cassandra.ecchronos.standalone`

## Build Commands

```bash
# Run all standalone integration tests (requires Docker)
mvn verify -P standalone-integration-tests -DskipUTs

# Single test class
mvn test -pl standalone-integration -Dtest=ITSchedules

# Single test method
mvn test -pl standalone-integration -Dtest=ITSchedules#repairSingleTable

# Against a local CCM cluster
mvn clean install -Dlocalprecommit.tests
```

## Maven Profiles

| Profile | Activation | Purpose |
|---|---|---|
| `standalone-integration-tests` | `-Dprecommit.tests` | Runs `IT*.java` via Failsafe |
| `local-standalone-integration-tests` | `-Dlocalprecommit.tests` | Same, but against CCM |

## Test Class Structure

All test classes extend `TestBase` which provides:
- `myNativeConnectionProvider` — CQL session (eccuser)
- `myAdminNativeConnectionProvider` — CQL session (cassandra/cassandra admin)
- `myJmxConnectionProvider` — JMX connections
- `myJmxProxyFactory` — JMX proxies for repair operations
- `insertSomeDataAndFlush()` — inserts 1000 rows and forces flush (required before incremental repair)

`SharedCassandraCluster.ensureInitialized()` is called in `TestBase.initialize()` (`@BeforeClass`). All test classes share the **same Docker cluster instance** — do not tear down the cluster between test classes.

## Repair Verification Patterns

### Vnode Repairs
```java
// Inject partial repair history
injectRepairHistory(tableRef, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

// Start scheduler and wait
await().pollInterval(1, SECONDS).atMost(180, SECONDS)
       .until(() -> isRepairedSince(tableRef, startTime));
```
Check `ecchronos.repair_history` for entries covering all token ranges.

### Incremental Repairs
```java
// Insert data and flush first
insertSomeDataAndFlush(tableRef, session, node);

// Start on-demand job, then poll metrics
await().pollInterval(5, SECONDS).atMost(300, SECONDS)
       .until(() -> cassandraMetrics.getPercentRepaired(nodeId, tableRef) >= 95.0);
```
Monitor `percentRepaired` and `maxRepairedAt` via JMX. Allow 300 seconds — metric updates have latency.

### On-Demand Repairs
```java
OnDemandRepairJobView job = onDemandScheduler.scheduleJob(tableRef, RepairOptions.newBuilder().build());
await().atMost(90, SECONDS).until(() -> onDemandScheduler.getAllRepairJobs().isEmpty());
```
Wait for the job queue to drain, then verify `job.getStatus() == COMPLETED`.

## Adding a New Integration Test

1. Create `IT<FeatureName>.java` in `src/test/java/com/ericsson/bss/cassandra/ecchronos/standalone/`
2. Extend `TestBase`
3. Follow the naming convention `IT*.java` — Failsafe discovers tests by this pattern
4. Call `SharedCassandraCluster.ensureInitialized()` in `@BeforeClass` (or rely on `TestBase` doing it)
5. Use `getSession()`, `getNode()`, etc. from `TestBase`
6. Use Awaitility for all async assertions — never `Thread.sleep()`

## Common Failure Modes

### Cluster startup timeout
- Symptom: `waitForNodesToBeUp` times out after 90 seconds
- Cause: Docker daemon memory is low, or another test left orphaned containers
- Fix: Increase Docker memory allocation; run `docker ps` and stop leftover containers

### Repair not completing within timeout
- Symptom: Awaitility assertion times out after 180 seconds
- Cause: Lock contention, node health issues, or very slow repair
- Fix: Check `ecchronos.repair_history` and `ecchronos.lock` tables; verify all 4 nodes are UP

### Incremental repair metrics not updating
- Symptom: `percentRepaired` stays at 0% or below threshold
- Cause: Data was not flushed before repair, or Cassandra metric latency
- Fix: Always call `insertSomeDataAndFlush()` before triggering incremental repair; increase timeout to 300s

### JMX connection refused
- Symptom: JMX proxy creation fails
- Cause: JMX port 7199 not ready or blocked
- Fix: Check `it.jolokia.enabled` — if using Jolokia, verify port 8778 is exposed

## Test Data Constants

```java
static final String ECCHRONOS_KEYSPACE = "ecchronos";
static final String TEST_KEYSPACE = "test";
static final String TEST_TABLE_ONE_NAME = "table1";
static final String TEST_TABLE_TWO_NAME = "table2";
static final int DEFAULT_INSERT_DATA_COUNT = 1000;
```

Tables `test.table1`, `test.table2`, `test.table3` are created by `cassandra-test-image/src/main/docker/create_keyspaces.cql` with NTS (dc1=2, dc2=1).

## Code Style

Follows the Cassandra code style (Sun Java conventions). CheckStyle and PMD run on `mvn compile`. Tests use JUnit 4 style (`@BeforeClass`/`@AfterClass`).
