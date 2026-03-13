# CLAUDE.md — core.impl

This file provides guidance to Claude Code (claude.ai/code) when working with the `core.impl` module.

## Module Overview

`core.impl` is the concrete implementation of all interfaces defined in the `core` module. It contains the scheduling engine, distributed CAS-based locking, repair job implementations (vnode, sub-range, incremental), repair state computation, and per-node worker thread management. It is the operational heart of ecChronos.

**Maven artifact:** `core.impl-agent`
**Root package:** `com.ericsson.bss.cassandra.ecchronos.core.impl`

## Key Dependencies

| Dependency | Purpose |
|---|---|
| `core-agent` | Interfaces this module implements |
| `connection-agent` | CQL/JMX connection interfaces |
| `data-agent` | Repair history and run policy data models |
| `fault.manager-agent` | Fault reporting interfaces |
| Cassandra Java Driver | CQL queries for locking and state |
| Caffeine | Lock caching |
| Jolokia | HTTP-based JMX adapter |
| Micrometer | Metrics collection |
| Spring Context | Dependency injection wiring |

## Build Commands

Run from the repository root or the `core.impl/` directory:

```bash
# Unit tests for this module only
mvn test -pl core.impl

# Single test class
mvn test -pl core.impl -Dtest=TestClassName

# Single test method
mvn test -pl core.impl -Dtest=TestClassName#testMethodName

# Compile and run style checks
mvn compile -pl core.impl

# Build without tests
mvn install -pl core.impl -DskipTests=true
```

## Package Structure

```
com.ericsson.bss.cassandra.ecchronos.core.impl
├── jmx/                 # JMX proxy factory and Jolokia HTTP notification controller
│   └── http/            # Jolokia-specific HTTP adapter
├── locks/               # CAS distributed locking (CASLockFactory, CASLock, LockCache)
├── logging/             # ThrottlingLogger — prevents log spam for repeated messages
├── metadata/            # NodeResolverImpl — resolves node UUIDs to addresses
├── metrics/             # CassandraMetrics, RepairStatsProviderImpl
├── multithreads/        # NodeWorkerManager, NodeWorker — per-node repair loop threads
├── refresh/             # Handles Cassandra node join/leave events
├── repair/
│   ├── incremental/     # IncrementalRepairJob, IncrementalOnDemandRepairJob, IncrementalRepairTask
│   ├── scheduler/       # ScheduleManagerImpl, RepairSchedulerImpl, OnDemandRepairSchedulerImpl
│   ├── state/           # RepairStateImpl, RepairStateSnapshot, ReplicationStateImpl, HostStatesImpl
│   └── vnode/           # TableRepairJob, VnodeRepairTask, VnodeRepairGroupFactory, sub-range logic
├── table/               # TimeBasedRunPolicy, ReplicatedTableProviderImpl, TableStorageStatesImpl
└── utils/               # RepairLockFactoryImpl and other shared utilities
```

## Architecture & Key Patterns

### Interface/Implementation Separation

Every class in this module implements an interface from `core`. Never add business logic directly to interfaces — keep it here in `core.impl`. When adding a new feature, define the interface in `core` first, then implement it here.

### Scheduling Flow

```
NodeWorker (background thread per node)
  → RepairSchedulerImpl.putConfigurations()
    → Creates TableRepairJob or IncrementalRepairJob
      → ScheduleManager.schedule(nodeId, job)

ScheduleManagerImpl.JobRunTask (runs every 30s per node)
  → ScheduledJobQueue.poll()      ← highest-priority job
    → RunPolicy.validate()        ← enforces time-based blackouts
    → job.iterator()              ← yields RepairGroup tasks
      → RepairGroup.getLock()     ← CASLockFactory acquires distributed lock
        → RepairTask.execute()    ← triggers JMX repair on Cassandra
          → handleNotification()  ← processes Cassandra progress events
```

### Distributed Locking

Locking uses Cassandra LWT (Compare-And-Set) across two tables:
- `ecchronos.lock` — the actual lock, TTL 600s
- `ecchronos.lock_priority` — priority competition between nodes

**Important:** `CASLock` schedules automatic renewal every 60 seconds. Always call `close()` on a lock (use try-with-resources) to avoid stale locks blocking other nodes.

`LockCache` (Caffeine-backed) caches both acquired locks and lock failures to avoid hammering Cassandra. Be aware of this when writing tests — cached failures can cause unexpected test behaviour.

### Repair State Snapshots

`RepairStateImpl` holds an `AtomicReference<RepairStateSnapshot>`. The snapshot is immutable and replaced atomically on each `update()` call. When modifying state-related code:
- Never mutate a snapshot in place — always create a new one
- `update()` is the only entry point for refreshing state; it queries history, topology, and vnode states from scratch

### Repair Job Selection

`RepairSchedulerImpl.createScheduledRepairJob()` decides between `TableRepairJob` (vnode/sub-range) and `IncrementalRepairJob` based on `RepairConfiguration.repairType()`. If adding a new repair type, this is the factory method to update.

### Sub-range Repair

Sub-range repair is triggered automatically when `repair.size.target` is configured. `TableStorageStatesImpl` estimates vnode data sizes via JMX; `SubRangeRepairStates` and `NormalizedRange` handle the token range arithmetic. Avoid changing the splitting logic without also updating the corresponding unit tests in `repair/vnode/`.

### Run Policies

`TimeBasedRunPolicy` reads blackout windows from `ecchronos.reject_configuration` and caches them with a 10-second TTL. When `validate()` returns a positive value, the scheduler backs off for that many milliseconds. If adding a new run policy, implement the `RunPolicy` interface from `core` and register it in `ScheduleManagerImpl`.

## Testing

Tests use JUnit 5 + Mockito. Some tests spin up a real Cassandra node via Testcontainers (particularly in `locks/` and `repair/state/`).

### Common Test Patterns

- **Mock-heavy unit tests** — most classes are tested with Mockito mocks for all dependencies
- **Testcontainers tests** — `CASLock*` and some state tests use a real single-node Cassandra cluster; these are slower and require Docker
- **Awaitility** — used for asynchronous assertions (e.g. waiting for lock renewal or background threads to act)

### Running Tests with a Local CCM Cluster

```bash
ccm create test -n 4 -v 4.0 --vnodes
ccm updateconf "num_tokens: 16"
ccm start
ccm node1 cqlsh -f cassandra-test-image/src/main/docker/create_keyspaces.cql
mvn install -Dlocalprecommit.tests
```

## Common Modification Scenarios

### Adding a new repair job type
1. Define the interface in `core`
2. Implement in `repair/` under the appropriate sub-package
3. Add a factory branch in `RepairSchedulerImpl.createScheduledRepairJob()`
4. Add a corresponding on-demand variant if needed in `OnDemandRepairSchedulerImpl`

### Changing lock behaviour
- `CASLockFactory` — lock acquisition and priority logic
- `CASLock` — renewal scheduling and release
- `CASLockStatement` — CQL statements; update these if the schema changes
- Always update `ecchronos.lock` / `ecchronos.lock_priority` schema in `cassandra-test-image/src/main/docker/create_keyspaces.cql`

### Modifying repair state computation
- Entry point: `RepairStateImpl.generateNewRepairState()`
- Vnode grouping: `VnodeRepairGroupFactory`
- Per-vnode state building: `VnodeRepairStateFactoryImpl`
- Snapshots are immutable — produce a new `RepairStateSnapshot` rather than mutating

### Adding a new run policy
1. Implement `RunPolicy` from `core`
2. Register in `ScheduleManagerImpl` (add to the list of policies checked in `JobRunTask`)

## Code Style

Follows the Cassandra code style (Sun Java conventions). CheckStyle and PMD run automatically on `mvn compile`. The formatter config is `code_style.xml` at the repository root.
