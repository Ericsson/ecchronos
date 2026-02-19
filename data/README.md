# data Module

The `data` module is the data access layer for ecChronos. It manages three concerns: repair history persistence and querying, node ownership tracking, and IP address translation for containerised environments.

---

## Module Structure

```
com.ericsson.bss.cassandra.ecchronos.data
├── repairhistory/
│   ├── RepairHistoryService           # Primary repair history (ecchronos.repair_history)
│   ├── CassandraRepairHistoryService  # Alternative: native system_distributed.repair_history
│   └── RepairHistoryData              # Immutable data model with builder
├── sync/
│   └── EccNodesSync                   # Node ownership table (ecchronos.nodes_sync)
└── iptranslator/
    └── IpTranslator                   # External-to-internal IP mapping
```

---

## Cassandra Tables

| Table | Managed By | Purpose |
|---|---|---|
| `ecchronos.repair_history` | `RepairHistoryService` | Repair operation audit trail |
| `ecchronos.nodes_sync` | `EccNodesSync` | Tracks which ecChronos instance owns each node |
| `system_views.gossip_info` | `IpTranslator` (read-only) | Maps external IPs to internal IPs |
| `system_distributed.repair_history` | `CassandraRepairHistoryService` (read-only) | Native Cassandra repair history |

All CQL operations use `LOCAL_QUORUM` consistency.

---

## RepairHistoryService

Implements both `RepairHistory` (write operations) and `RepairHistoryProvider` (read/iterate operations) from `core`.

**Write operations:**
- `RepairSession.start()` — writes an entry with status `STARTED`
- `RepairSession.finish(status)` — updates the entry with final status and timestamps

**Read operations / filtering:**
- `getRepairHistoryByTimePeriod(nodeId, tableRef, startDate, endDate)`
- `getRepairHistoryByStatus(nodeId, tableRef, status)`
- `getRepairHistoryByLookBackTime(nodeId, tableRef, lookbackMs)`

---

## EccNodesSync

Manages the `ecchronos.nodes_sync` table to track node ownership across multiple ecChronos instances.

**Key operations:**
- `acquireNodes(nodes)` — registers all managed nodes for this instance
- `updateNodeStatus(nodeId, status)` — marks a node `AVAILABLE` or `UNAVAILABLE`
- `getAllByLocalInstance()` — returns nodes owned by this ecChronos instance
- `deleteNodeStatus(nodeId)` — removes a node record

The table uses `(ecchronos_id, datacenter_name, node_id)` as its primary key. A configurable connection delay (default 30 minutes) controls heartbeat timing.

---

## IpTranslator

Handles Docker/Kubernetes environments where Cassandra nodes advertise different IP addresses than they are reachable at.

- Reads from `system_views.gossip_info` to build an external→internal IP map
- Refreshes on node state changes (extends `NodeStateListenerBase`)
- `isActive()` returns `false` if no translation mappings exist — callers skip translation in that case
- Thread-safe via `ConcurrentHashMap`

---

## RepairHistoryData

Immutable data transfer object for repair history entries. Uses a builder pattern:

```java
RepairHistoryData data = RepairHistoryData.newBuilder()
    .withTableId(tableId)
    .withNodeId(nodeId)
    .withRepairId(repairId)
    .withStatus(RepairStatus.SUCCESS)
    .withStartedAt(startInstant)
    .withFinishedAt(finishInstant)
    .build();
```

---

## Dependencies

| Dependency | Purpose |
|---|---|
| `connection-agent` | `DistributedNativeConnectionProvider` for node list |
| `core-agent` | `RepairHistory`, `RepairHistoryProvider` interfaces |
| `utils-agent` | `RepairStatus`, `NodeStatus` enums |
| Cassandra Java Driver | CQL sessions and query builder |
| Guava | `Preconditions`, `Functions` |
