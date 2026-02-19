# core.impl Module

The `core.impl` module is the operational heart of ecChronos, implementing all the interfaces defined in `core/`. It contains the concrete logic for distributed repair scheduling, locking, state management, and job execution.

---

## Module Structure

```
core.impl/
├── jmx/           # JMX proxy implementations
├── locks/         # Distributed CAS-based locking
├── logging/       # Throttled logging utilities
├── metadata/      # Node resolver and metadata
├── metrics/       # Repair statistics and metrics
├── multithreads/  # Per-node worker thread management
├── refresh/       # Node add/remove event handling
├── repair/
│   ├── incremental/   # Incremental repair jobs
│   ├── scheduler/     # Central scheduling orchestration
│   ├── state/         # Repair state computation
│   └── vnode/         # Vnode-based repair logic
├── table/         # Table discovery, run policies
└── utils/         # Shared utilities
```

---

## 1. Scheduling Layer (`repair/scheduler/`)

**`ScheduleManagerImpl`** — The central orchestrator. It maintains a `ScheduledJobQueue` per Cassandra node and runs a background task every 30 seconds that:
- Checks `RunPolicy` constraints (e.g. time-based blackouts)
- Acquires distributed locks via `CASLockFactory`
- Executes the highest-priority job from the queue

**`RepairSchedulerImpl`** — A factory and controller for repair jobs. It maps each `(NodeID, TableReference)` to a set of `ScheduledRepairJob`s, responds to table configuration changes, and decides whether to create a `TableRepairJob` (vnode) or `IncrementalRepairJob`.

**`OnDemandRepairSchedulerImpl`** — Handles user-triggered repairs. Polls `ecchronos.on_demand_repair_status` every 10 seconds and creates `VnodeOnDemandRepairJob` or `IncrementalOnDemandRepairJob` instances as needed.

**`ScheduledJobQueue`** — A priority queue (`PriorityQueue` with `DefaultJobComparator`) ensuring the most urgent job executes first.

---

## 2. Repair Job Implementations (`repair/`)

**`TableRepairJob`** — The primary vnode/sub-range repair job. It:
- Holds a `RepairState` snapshot of all vnode repair statuses
- On each scheduling cycle, yields `RepairGroup` tasks (one per replica set)
- Computes its UI status as `OVERDUE`, `LATE`, `ON_TIME`, `BLOCKED`, or `COMPLETED`
- Supports sub-range splitting when `repair.size.target` is configured

**`IncrementalRepairJob`** — Uses Cassandra's native `maxRepairedAt`/`percentRepaired` JMX metrics instead of ecChronos' own repair history. No `RepairState` snapshot; it reads live metrics.

**`VnodeOnDemandRepairJob` / `IncrementalOnDemandRepairJob`** — User-triggered variants of the above two job types.

**`RepairGroup`** — Coordinates repair for one specific replica set. It acquires the distributed lock then lazily creates and yields `RepairTask` instances via an inner `RepairTaskIterator`.

**`RepairTask`** (abstract) — Executes Cassandra repair via JMX. It:
- Initiates repair via JMX and waits on a `CountDownLatch` for completion
- Handles progress notifications from Cassandra
- Has hang prevention: monitors node health every 10 minutes, force-terminates after 30 minutes of inactivity

**`VnodeRepairTask` / `IncrementalRepairTask`** — Concrete `RepairTask` subclasses for vnode and incremental repair respectively.

---

## 3. Distributed Locking (`locks/`)

**`CASLockFactory`** — Implements distributed locking using Cassandra Lightweight Transactions (LWT/CAS):
- Uses two tables: `ecchronos.lock` (the lock itself) and `ecchronos.lock_priority` (node competition)
- Verifies a quorum of nodes are reachable before attempting to lock
- Wraps locks in a `LockCache` (Caffeine) to avoid redundant acquisitions

**`CASLock`** — Represents an acquired lock. It:
- Competes with other nodes by inserting into `ecchronos.lock_priority`
- Atomically acquires the lock via CAS INSERT into `ecchronos.lock`
- Schedules automatic renewal every 60 seconds (TTL is 600 seconds)
- Releases lock and removes priority record on `close()`

**`RepairLockType`** (enum) — Selects lock granularity: `VNODE` (lock per vnode) or `DATACENTER` (lock per datacenter).

---

## 4. Repair State Computation (`repair/state/`)

**`RepairStateImpl`** — Maintains an `AtomicReference<RepairStateSnapshot>` for one table. On `update()`, it:
1. Queries repair history from `ecchronos.repair_history`
2. Gets current topology from `ReplicationStateImpl`
3. Delegates to `VnodeRepairStateFactoryImpl` to build per-vnode states
4. Groups vnodes by replica set via `VnodeRepairGroupFactory`
5. Fires `AlarmPostUpdateHook` if repairs are overdue

**`RepairStateSnapshot`** — An immutable snapshot containing: all `VnodeRepairState` objects, `ReplicaRepairGroup` lists (vnodes grouped by common replicas), last completed timestamp, and estimated repair duration.

**`ReplicationStateImpl`** — Queries Cassandra topology to determine which nodes hold which token ranges.

**`HostStatesImpl`** — Caches node UP/DOWN status (via JMX polling) with a 10-second TTL, used to skip repairs when replicas are unavailable.

**`AlarmPostUpdateHook`** — Fires fault reports (via `RepairFaultReporter`) when repair overdue conditions are detected after a state update.

---

## 5. Vnode-Specific Logic (`repair/vnode/`)

**`VnodeRepairStateFactoryImpl`** — Builds the collection of `VnodeRepairState` objects by correlating repair history with current topology.

**`VnodeRepairGroupFactory`** — Groups vnodes that share the same replica set, sorts groups by urgency (oldest last-repaired first), and produces an ordered `List<ReplicaRepairGroup>`.

**`NormalizedRange` / `NormalizedBaseRange`** — Handle token range arithmetic including splitting for sub-range repairs.

**`SubRangeRepairStates`** — Splits vnodes into smaller sub-ranges when data size exceeds the configured `repair.size.target`.

---

## 6. Table Management (`table/`)

**`TimeBasedRunPolicy`** — Enforces scheduled repair blackout windows from `ecchronos.reject_configuration`. Caches windows per table (10-second TTL) and returns how many milliseconds until the next allowed run when blocked.

**`ReplicatedTableProviderImpl`** — Discovers all user tables that need repair (excludes system keyspaces and non-replicated tables).

**`TableReferenceFactoryImpl`** — Validates and creates `TableReference` identity objects.

**`TableStorageStatesImpl`** — Estimates data size per vnode via JMX metrics, used by sub-range repair to decide how to split ranges.

---

## 7. Multi-threaded Workers (`multithreads/`)

**`NodeWorkerManager`** — Maintains a dedicated `NodeWorker` background thread per Cassandra node.

**`NodeWorker`** — A continuously-running background thread per node. It:
- Discovers all replicated tables for its node
- Fetches repair configurations per table
- Calls `RepairScheduler.putConfigurations()` to keep jobs up to date
- Loops on a configurable refresh interval

---

## 8. Metrics & JMX (`metrics/`, `jmx/`)

**`CassandraMetrics`** — Queries Cassandra JMX metrics, including `maxRepairedAt` (for incremental repair) and load/size information (for sub-range splitting).

**`RepairStatsProviderImpl`** — Computes repair statistics (repaired ratio, estimated repair time) for the REST API.

**`DistributedJmxProxyFactoryImpl`** — Creates JMX connections to individual Cassandra nodes.

**`JolokiaNotificationController`** — Receives Cassandra repair progress notifications via HTTP/Jolokia as an alternative to direct JMX, useful when firewalls block JMX ports.

---

## Key Execution Flow

```
NodeWorker (per-node loop)
  → RepairSchedulerImpl.putConfigurations()
    → Creates TableRepairJob / IncrementalRepairJob
      → ScheduleManager.schedule(nodeId, job)

ScheduleManagerImpl.JobRunTask (every 30s per node)
  → ScheduledJobQueue.poll()   ← highest priority job
    → RunPolicy.validate()     ← check blackout windows
    → job.iterator()           ← get RepairGroup tasks
      → RepairGroup.getLock()  ← CASLockFactory acquires distributed lock
        → RepairTask.execute() ← JMX repair call to Cassandra
          → handleNotification() ← process Cassandra progress events
```

---

## Thread Safety

| Component | Mechanism |
|---|---|
| `RepairStateImpl` | `AtomicReference<RepairStateSnapshot>` |
| `ScheduleManagerImpl` | `ConcurrentHashMap` for job queues |
| `RepairSchedulerImpl` | Synchronized blocks on job maps |
| `LockCache` | Caffeine thread-safe cache |
| `HostStatesImpl` | `ConcurrentHashMap` for cached statuses |
