# utils Module

The `utils` module is the foundational library layer of ecChronos. It contains shared exceptions, domain enumerations, and lightweight utility classes used by every other module. It has no business logic and no dependencies on Cassandra or Spring.

---

## Module Structure

```
com.ericsson.bss.cassandra.ecchronos.utils
├── exceptions/    # Custom exception hierarchy
├── enums/
│   ├── repair/    # RepairStatus, RepairType, RepairParallelism, RepairHistoryProvider
│   ├── connection/ # ConnectionType (agent modes)
│   ├── history/   # SessionState
│   └── sync/      # NodeStatus
├── converter/     # UnitConverter, ManyToOneIterator
└── dns/           # ReverseDNS (with Kubernetes support)
```

---

## Exceptions

| Class | Type | Purpose |
|---|---|---|
| `EcChronosException` | Checked | Generic base exception for scheduler operations |
| `ConfigurationException` | Checked | Configuration parsing and validation errors |
| `LockException` | Checked | Distributed lock acquisition failures |
| `ScheduledJobException` | Checked | Scheduled repair job execution failures |
| `InternalException` | Unchecked | Unexpected internal bugs / invariant violations |
| `RetryPolicyException` | Unchecked | Cassandra connection overload or unavailability |

---

## Enums

### Repair

| Enum | Values | Purpose |
|---|---|---|
| `RepairStatus` | `STARTED`, `SUCCESS`, `FAILED`, `UNKNOWN` | Repair session lifecycle tracking |
| `RepairType` | `VNODE`, `PARALLEL_VNODE`, `INCREMENTAL` | Repair strategy selection |
| `RepairParallelism` | `PARALLEL` | Repair concurrency model |
| `RepairHistoryProvider` | `CASSANDRA`, `UPGRADE`, `ECC` | Source of repair history data |

### Connection

| Enum | Values | Purpose |
|---|---|---|
| `ConnectionType` | `datacenterAware`, `rackAware`, `hostAware` | ecChronos control scope for node management |

### State

| Enum | Values | Purpose |
|---|---|---|
| `SessionState` | `NO_STATE`, `STARTED`, `DONE` | State machine for repair session transitions |
| `NodeStatus` | `UNAVAILABLE`, `AVAILABLE`, `UNREACHABLE` | Node connectivity state |

---

## Utility Classes

**`UnitConverter`** — Parses byte size strings with SI suffixes (`k`/`K`/`m`/`M`/`g`/`G`) into `long`. Used to parse `repair.size.target` configuration values (e.g. `"100m"` → `104857600`).

**`ManyToOneIterator`** — Merges multiple sorted `Iterable<T>` instances into a single sorted iterator using a `Comparator<T>`. Used internally by `ScheduledJobQueue` to merge priority-ordered repair job queues without allocating all jobs upfront.

**`ReverseDNS`** — Performs reverse DNS lookups with special handling for Kubernetes hostnames. Supports:
- Standard dot-separated IP prefixes: `10.244.1.5.hostname.cluster.local` → `hostname.cluster.local`
- Kubernetes hyphen-separated IP prefixes: `10-244-1-5.hostname.cluster.local` → `hostname.cluster.local`
- IPv6 formats in both styles
- Falls back to the original host string on failure

---

## Dependencies

| Dependency | Purpose |
|---|---|
| Guava | `AbstractIterator` base for `ManyToOneIterator` |
| SLF4J | Logging in `ReverseDNS` |

All other modules depend on `utils-agent`. It has no internal module dependencies.
