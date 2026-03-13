# CLAUDE.md — utils

This file provides guidance to Claude Code (claude.ai/code) when working with the `utils` module.

## Module Overview

`utils` is the foundational library of ecChronos. It provides shared exception types, domain enumerations, and lightweight utilities (byte-size parsing, multi-iterable merging, reverse DNS) that are consumed by every other module. It contains no business logic, no Cassandra queries, and no Spring wiring.

**Maven artifact:** `utils-agent`
**Root package:** `com.ericsson.bss.cassandra.ecchronos.utils`

## Key Dependencies

| Dependency | Purpose |
|---|---|
| Guava | `AbstractIterator` for `ManyToOneIterator` |
| SLF4J | Logging in `ReverseDNS` |

This module has **no internal module dependencies** — it is the lowest layer of the dependency graph.

## Build Commands

```bash
# Unit tests for this module only
mvn test -pl utils

# Single test class
mvn test -pl utils -Dtest=TestReverseDNS

# Build without tests
mvn install -pl utils -DskipTests=true
```

## Package Structure

```
com.ericsson.bss.cassandra.ecchronos.utils
├── exceptions/     # Exception hierarchy (6 classes)
├── enums/
│   ├── repair/     # RepairStatus, RepairType, RepairParallelism, RepairHistoryProvider
│   ├── connection/ # ConnectionType
│   ├── history/    # SessionState
│   └── sync/       # NodeStatus
├── converter/      # UnitConverter, ManyToOneIterator
└── dns/            # ReverseDNS
```

## Exception Design

The exception hierarchy is intentionally stratified:

| Class | Type | When to use |
|---|---|---|
| `EcChronosException` | Checked | General scheduler errors that callers must handle |
| `ConfigurationException` | Checked | Invalid configuration at startup; fatal |
| `LockException` | Checked | Distributed lock failure; may be transient |
| `ScheduledJobException` | Checked | Job execution failure; handled by scheduler |
| `InternalException` | Unchecked | Invariant violations / bugs; should not be caught |
| `RetryPolicyException` | Unchecked | Cassandra overload; propagates to retry logic |

**Rule:** Do not collapse these into a single exception type. Callers in `core.impl` and `application` depend on being able to distinguish configuration errors (fatal, fix the config) from lock errors (transient, retry) from internal bugs (don't catch).

## Enum Design

Enums define the shared vocabulary across all modules. When adding a new value:
1. Ensure it is truly shared — if only one module uses it, define it there
2. Update any `switch` statements in `core.impl` and `connection.impl` that handle all enum values (CheckStyle will warn about exhaustive checks)
3. Add a `RepairStatus.getFromStatus(String)` style factory method if the enum needs safe string parsing

### ConnectionType

Defines the three ecChronos agent modes. Adding a new mode requires changes in:
- `connection.impl/builders/DistributedNativeBuilder.java` — `createNodesMap()` switch
- `application/config/connection/DistributedNativeConnection.java` — validation
- Integration tests in `standalone-integration/`

## Utility Classes

### UnitConverter

- Only one static method: `toBytes(String)` → `long`
- Throws `IllegalArgumentException` for unrecognised formats
- Supported suffixes: `k`/`K` (×1024), `m`/`M` (×1024²), `g`/`G` (×1024³), or no suffix (bytes)
- Used for `repair.size.target` config parsing in `application`

### ManyToOneIterator

- Merges any number of sorted `Iterable<T>` sources into one sorted stream
- Comparator-driven; does not deduplicate
- Used by `ScheduledJobQueue` in `core.impl`
- Lazy: does not materialise all elements; keeps one "next" element per source iterable

### ReverseDNS

- `fromHostString(String host)` is the only public method
- Falls back to original `host` string silently on any DNS failure
- Handles four hostname formats: standard dotted IPv4, Kubernetes hyphen-IPv4, IPv6, K8s hyphen-IPv6
- Controlled by the `jmx.reverse-dns-resolution` flag in `ecc.yml`; do not call unless that flag is set

## Testing

Only `ReverseDNS` has a test class (`TestReverseDNS`). It uses reflection to access the private `cleanHostname()` method directly.

Other classes (exceptions, enums, `UnitConverter`) are effectively tested through their consuming modules' tests. There is no need to add separate unit tests for simple enum values or exception constructors.

## Code Style

Follows the Cassandra code style (Sun Java conventions). CheckStyle and PMD run on `mvn compile`. No test-only code should be added to main sources.

## Common Modification Scenarios

### Adding a new exception type
- Extend `EcChronosException` (checked) or `RuntimeException` (unchecked)
- Document when it is thrown vs. when callers should catch it
- Add to this module only if it is used by more than one other module

### Adding a new enum value
- Add to the appropriate enum in `enums/`
- Search the codebase for existing `switch` statements on that enum and update them
- If the enum is serialised to Cassandra (e.g. `RepairStatus`), verify the string representation does not break existing data

### Adding a new utility class
- Only add to `utils` if the class is genuinely needed by two or more other modules
- Keep it stateless or use thread-safe patterns (`ConcurrentHashMap`, `AtomicReference`)
- Add a unit test in `utils/src/test/`
