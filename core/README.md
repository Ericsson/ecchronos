# core Module

The `core` module defines all the interfaces, data classes, and abstractions for ecChronos repair scheduling. It is interfaces-only; all implementations live in `core.impl`. Every other module that needs scheduling or repair state logic depends on the contracts defined here.

---

## Module Structure

```
com.ericsson.bss.cassandra.ecchronos.core
├── repair.scheduler/   # Job scheduling interfaces and data classes
├── state/              # Repair state and history interfaces
├── repair.config/      # Repair configuration model
├── repair.types/       # JSON-serialisable REST/CLI data transfer objects
├── repair/             # Repair resources, lock factories, stats
├── repair.multithread/ # Repair event types
├── table/              # Table reference and table policy interfaces
├── metrics/            # Metrics collection interfaces
├── metadata/           # Node resolver and Cassandra driver metadata helpers
├── locks/              # Distributed lock interface
└── jmx/                # JMX proxy interface
```

---

## Key Interfaces

### Scheduling (`repair.scheduler`)

| Interface | Purpose |
|---|---|
| `ScheduleManager` | Schedules and deschedules repair jobs per node |
| `RepairScheduler` | Factory for creating/updating repair configurations; returns current jobs |
| `OnDemandRepairScheduler` | Triggers one-off user-requested repairs |
| `RunPolicy` | Validates whether a job is allowed to run right now |

### Repair State (`state`)

| Interface | Purpose |
|---|---|
| `RepairState` | Maintains and exposes an immutable snapshot of a table's repair state |
| `RepairHistory` | Records repair sessions (start/finish); has a no-op implementation |
| `RepairHistoryProvider` | Iterates repair history entries from `ecchronos.repair_history` |
| `ReplicationState` | Maps token ranges to replica node sets |
| `VnodeRepairStateFactory` | Creates per-vnode repair states from history |
| `RepairStateFactory` | Factory for creating `RepairState` instances |

### Table (`table`)

| Interface | Purpose |
|---|---|
| `TableReference` | Identifies a keyspace/table pair with metadata |
| `TableReferenceFactory` | Creates `TableReference` from names or driver metadata |
| `ReplicatedTableProvider` | Lists all user tables replicated on the local node |
| `TableRepairPolicy` | Pluggable policy to suppress repairs on specific tables |
| `TableStorageStates` | Estimates vnode data sizes via JMX metrics |

### Locking (`locks`)

| Interface | Purpose |
|---|---|
| `LockFactory` | Creates distributed locks with priority competition |

### JMX (`jmx`)

| Interface | Purpose |
|---|---|
| `DistributedJmxProxy` | Executes Cassandra repair operations and reads metrics via JMX |
| `DistributedJmxProxyFactory` | Factory for creating `DistributedJmxProxy` instances |

---

## Key Data Classes

| Class | Description |
|---|---|
| `RepairConfiguration` | Immutable repair settings (interval, thresholds, type, parallelism). Includes a `DISABLED` constant and a builder. |
| `RepairStateSnapshot` | Immutable snapshot of all vnode repair states for a table at a point in time |
| `VnodeRepairState` | State of a single vnode: token range, replicas, last repair timestamp |
| `ReplicaRepairGroup` | (Record) Groups of vnodes sharing the same replica set |
| `LongTokenRange` | Cassandra token range with wrap-around handling |
| `ScheduledJob` | Abstract base for all repair jobs; defines `Priority` and `State` enums |
| `ScheduledRepairJobView` | View object exposing job status (`COMPLETED`, `ON_TIME`, `LATE`, `OVERDUE`, `BLOCKED`) |
| `OnDemandRepairJobView` | View for on-demand jobs (`COMPLETED`, `IN_QUEUE`, `WARNING`, `ERROR`, `BLOCKED`) |

### REST/CLI Data Transfer Objects (`repair.types`)

| Class | Purpose |
|---|---|
| `Schedule` | Scheduled repair job for JSON serialisation |
| `OnDemandRepair` | On-demand repair job for JSON serialisation |
| `RepairInfo` | Repair history aggregation for a time window |
| `RepairStats` | Per-table repair statistics |
| `VirtualNodeState` | JSON representation of a vnode's repair state |

---

## Key Constants

| Constant | Value | Meaning |
|---|---|---|
| `VnodeRepairState.UNREPAIRED` | `-1L` | Marker for a vnode that has never been repaired |
| `RepairConfiguration.NO_UNWIND` | `0.0d` | No delay between repair sessions |
| `RepairConfiguration.FULL_REPAIR_SIZE` | `Long.MAX_VALUE` | Repair full vnodes without splitting |
| `ScheduledJob.DEFAULT_BACKOFF_IN_MINUTES` | `30` | Backoff on job failure |

---

## Dependencies

| Dependency | Purpose |
|---|---|
| `utils-agent` | Shared enums and exceptions |
| Cassandra Java Driver | `Node`, `TokenRange`, driver metadata |
| Micrometer | Metrics gauge registration |
| Guava | `ImmutableSet`, `ImmutableList` |
| Caffeine | Caching annotations |
| Jakarta Validation | Bean validation (`@NotNull`, etc.) |
