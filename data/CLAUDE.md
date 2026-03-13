# CLAUDE.md — data

This file provides guidance to Claude Code (claude.ai/code) when working with the `data` module.

## Module Overview

`data` is the data access layer for ecChronos. It owns all CQL prepared statements for the `ecchronos.repair_history` and `ecchronos.nodes_sync` tables, and provides IP translation for containerised Cassandra deployments.

**Maven artifact:** `data-agent`
**Root package:** `com.ericsson.bss.cassandra.ecchronos.data`

## Key Dependencies

| Dependency | Purpose |
|---|---|
| `connection-agent` | `DistributedNativeConnectionProvider` |
| `core-agent` | `RepairHistory`, `RepairHistoryProvider` interfaces |
| `utils-agent` | `RepairStatus`, `NodeStatus` |
| Cassandra Java Driver | CQL session, query builder |
| Guava | `Preconditions`, `Functions` |

## Build Commands

```bash
# Unit tests for this module only (Testcontainers — requires Docker)
mvn test -pl data

# Single test class
mvn test -pl data -Dtest=TestRepairHistoryService

# Build without tests
mvn install -pl data -DskipTests=true
```

## Package Structure

```
com.ericsson.bss.cassandra.ecchronos.data
├── repairhistory/
│   ├── RepairHistoryService           # Implements RepairHistory + RepairHistoryProvider
│   ├── CassandraRepairHistoryService  # Implements RepairHistoryProvider (read-only)
│   └── RepairHistoryData              # Immutable DTO with builder
├── sync/
│   └── EccNodesSync                   # ecchronos.nodes_sync table operations
└── iptranslator/
    └── IpTranslator                   # Extends NodeStateListenerBase for IP mapping
```

## Cassandra Schema Details

All tables are created by `cassandra-test-image/src/main/docker/create_keyspaces.cql`. Schema changes must be made there.

### ecchronos.repair_history

Primary key: `(table_id, node_id, repair_id)` — range partitioned by table and node.

Uses `TimeWindowCompactionStrategy` and a 20-day TTL. All write operations use `LOCAL_QUORUM`.

### ecchronos.nodes_sync

Primary key: `(ecchronos_id, datacenter_name, node_id)`.

`ecchronos_id` is the UUID of the ecChronos instance, allowing multiple instances to coexist without interfering.

## Key Implementation Details

### RepairHistoryService

Uses four `PreparedStatement`s: INSERT, UPDATE (finish), SELECT (filtered), and ITERATE (scan).

The inner `RepairEntryIterator` lazily fetches rows — do not hold a reference longer than needed or the underlying `ResultSet` may time out.

`RepairSessionImpl` tracks `startedAt` in memory; `finishedAt` is set on `finish()` call. If `finish()` is never called (crash, etc.), the row stays with status `STARTED` and no `finished_at` — this is handled by the TTL and is not a bug.

### CassandraRepairHistoryService

Read-only; only implements `RepairHistoryProvider`. Used when `repair.history.provider: cassandra` is set in `ecc.yml`. Resolves participant IPs to `DriverNode` objects via `NodeResolver`.

### EccNodesSync

Uses `LOCAL_QUORUM` for all operations. The builder is required — do not call the constructor directly.

`acquireNodes()` must be called after connection setup and before the scheduler starts. It registers all managed nodes with this instance's UUID.

### IpTranslator

Call `isActive()` before calling `getInternalIp()`. If `isActive()` returns `false`, the gossip_info table has no translation mappings and you should use the original IP directly.

IP translation is used in `connection.impl/builders/DistributedJmxBuilder.java` during JMX connection setup.

## Testing

Tests use Testcontainers to spin up a real single-node Cassandra 4.1.5 container. `AbstractCassandraTest` is the shared base class.

This means tests **require Docker to be running**. They are slower than pure unit tests — run the full suite only when modifying data access logic.

`TestRepairHistoryService` tests:
- Write/read/update lifecycle
- Time period, status, and lookback filtering
- Empty result sets

`TestEccNodesSync` tests:
- Node acquisition and status updates
- Builder null-safety validation (NullPointerException on missing fields)

## Common Modification Scenarios

### Adding a new repair history filter
1. Add a `PreparedStatement` field in `RepairHistoryService`
2. Prepare it in the constructor with `session.prepare(...)`
3. Add the public method returning an `Iterator<RepairEntry>`
4. Add a test in `TestRepairHistoryService` with real Cassandra data

### Changing the nodes_sync schema
1. Update `create_keyspaces.cql` in `cassandra-test-image`
2. Update `EccNodesSync` prepared statements to match
3. Rebuild the test image if running integration tests against Docker

### Supporting a new IP translation format
- Extend `IpTranslator.refreshIpMap()` with the new parsing logic
- `gossip_info` schema is Cassandra-controlled; do not assume column names will not change across versions

## Code Style

Follows the Cassandra code style (Sun Java conventions). CheckStyle and PMD run on `mvn compile`. Tests follow JUnit 4 style (`@BeforeClass`/`@AfterClass`).
