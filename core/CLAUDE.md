# CLAUDE.md — core

This file provides guidance to Claude Code (claude.ai/code) when working with the `core` module.

## Module Overview

`core` is the interfaces and data-model module for ecChronos. It defines all scheduling abstractions, repair state models, table metadata interfaces, distributed lock contracts, and JMX proxy interfaces. Implementations live exclusively in `core.impl`.

**Maven artifact:** `core-agent`
**Root package:** `com.ericsson.bss.cassandra.ecchronos.core`

## Key Dependencies

| Dependency | Purpose |
|---|---|
| `utils-agent` | `RepairStatus`, `RepairType`, `RepairParallelism`, exceptions |
| Cassandra Java Driver | `Node`, `TokenRange`, driver metadata types |
| Micrometer | Gauge and metric registration |
| Guava | `ImmutableSet`, `ImmutableList` |
| Jakarta Validation | `@NotNull` and other bean validation annotations |
| Caffeine | Cache interfaces |

## Build Commands

```bash
mvn compile -pl core
mvn install -pl core -DskipTests=true
```

No unit tests in this module (interfaces and data classes only).

## Package Guide

### repair.scheduler
Scheduling contracts. `ScheduleManager` is the top-level orchestrator interface; `RepairScheduler` is the factory for repair jobs; `RunPolicy` is the pluggable blackout check. `ScheduledJob` is the abstract base class all jobs extend.

### state
Repair history and state snapshot contracts. `RepairState` wraps an `AtomicReference<RepairStateSnapshot>` pattern (defined here as a contract; `AtomicReference` is used in `core.impl`). `RepairStateSnapshot` is the immutable snapshot. Never mutate a snapshot — always create a new one.

### repair.config
`RepairConfiguration` is immutable and uses a builder. It has a `DISABLED` constant for tables excluded from repair. All repair parameters (interval, warning/error thresholds, type, parallelism, unwind ratio, size target) live here.

### repair.types
JSON-serialisable DTOs for REST API and `ecctool` CLI. These are the response models for `rest` controllers. When adding a new field to a REST response, add it here first.

### table
`TableReference` is the central identity type for tables throughout the system. It carries `gcGraceSeconds` and `twcs` metadata that influence repair decisions. `TableRepairPolicy` is the extension point for suppressing repairs on specific table types (e.g. TWCS tables).

### locks
`LockFactory` defines the distributed locking contract. `DistributedLock` (inner interface) is `Closeable` — always use try-with-resources.

### jmx
`DistributedJmxProxy` defines all JMX operations ecChronos performs on Cassandra: repair execution, metrics reading (`maxRepairedAt`, `percentRepaired`, `liveDiskSpaceUsed`), and node status queries. When Cassandra exposes a new JMX attribute ecChronos needs, add a method here first.

## Design Patterns Used

- **Factory pattern** — `RepairStateFactory`, `TableReferenceFactory`, `LockFactory`, `DistributedJmxProxyFactory`
- **Builder pattern** — `RepairConfiguration.Builder`, `ScheduledJob.Configuration.Builder`, `RepairStateSnapshot.Builder`
- **Strategy pattern** — `RunPolicy`, `TableRepairPolicy`, `PostUpdateHook`
- **Immutable data** — `RepairStateSnapshot`, `VnodeRepairState`, `RepairEntry` use defensive copying
- **No-op pattern** — `RepairHistory.NoOpRepairHistory` for cases where repair tracking is disabled

## Common Modification Scenarios

### Adding a new repair configuration option
1. Add field to `RepairConfiguration` with a sensible default
2. Add getter and builder method
3. Update `RepairConfiguration.newBuilder()` Javadoc
4. Update `application/config/repair/RepairConfig.java` to parse the new YAML field
5. Use the field in `core.impl` where the behaviour is implemented

### Adding a new REST response field
1. Add field to the appropriate class in `repair.types` (e.g. `Schedule`, `OnDemandRepair`)
2. Ensure JSON serialisation works (Jackson annotations if needed)
3. Update the corresponding REST controller in `rest` to populate the field
4. Regenerate the OpenAPI spec: `mvn verify -P python-integration-tests`

### Adding a new JMX operation
1. Add method to `DistributedJmxProxy`
2. Implement in `core.impl/jmx/DistributedJmxProxyFactoryImpl` and the proxy class
3. Add to `DistributedJmxProxy` interface — all mocks in tests must be updated

### Adding a new scheduling interface
1. Define the interface here in the appropriate package
2. Implement it in `core.impl`
3. Wire it in `application/spring/ECChronos.java`

## Code Style

Follows the Cassandra code style (Sun Java conventions). CheckStyle and PMD run on `mvn compile`. Javadoc is required on all public methods — `mvn javadoc:jar` will fail without it.
