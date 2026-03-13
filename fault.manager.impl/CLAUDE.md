# CLAUDE.md — fault.manager.impl

This file provides guidance to Claude Code (claude.ai/code) when working with the `fault.manager.impl` module.

## Module Overview

`fault.manager.impl` provides `LoggingFaultReporter`, the default implementation of `RepairFaultReporter` from `fault.manager`. It uses SLF4J for output and an in-memory `HashMap` to deduplicate alarm events.

**Maven artifact:** `fault.manager.impl-agent`
**Root package:** `com.ericsson.bss.cassandra.ecchronos.fm.impl`

## Key Dependencies

| Dependency | Purpose |
|---|---|
| `fault.manager-agent` | The interface to implement |
| SLF4J | Logging |

## Build Commands

```bash
# Unit tests for this module only
mvn test -pl fault.manager.impl

# Single test class
mvn test -pl fault.manager.impl -Dtest=TestLoggingFaultReporter

# Build without tests
mvn install -pl fault.manager.impl -DskipTests=true
```

## Implementation Details

### Deduplication Logic

```java
private final Map<Integer, FaultCode> alarms = new HashMap<>();
```

- **Key:** `data.hashCode()` — deterministic for the same `{KEYSPACE, TABLE}` pair
- **Value:** Current `FaultCode`
- Logs only on: first raise, or escalation from WARNING → ERROR

### Alarm Escalation Rule

```java
if (oldCode == null || (oldCode == REPAIR_WARNING && faultCode == REPAIR_ERROR)) {
    LOG.error("Raising alarm: {} - {}", faultCode, data);
}
```

This means:
- `raise(WARNING)` → logs once, then silent
- `raise(ERROR)` after `raise(WARNING)` → logs escalation
- `raise(WARNING)` after `raise(ERROR)` → silent (not a downgrade)

### Atomic Remove in cease()

```java
alarms.remove(data.hashCode(), code);
```

Uses `Map.remove(key, value)` (atomic compare-and-remove) to prevent a race where `raise(ERROR)` and `cease(WARNING)` could accidentally clear an ERROR alarm.

## Testing

`TestLoggingFaultReporter` covers:
- Escalation from WARNING to ERROR (two log calls)
- Repeated `raise()` with same severity (one log call)
- `cease()` removes from alarm map
- Multiple alarm entries for different tables are independent

Tests inspect `getAlarms()` (a test-visible getter) to verify internal state rather than capturing log output.

## Common Modification Scenarios

### Handling a new FaultCode
If `FaultCode` gains a new value in `fault.manager`:
1. Update the escalation logic in `raise()` if severity ordering changes
2. Update `TestLoggingFaultReporter` to cover the new value
3. Verify `cease()` correctly removes any new code

### Making alarm state persistent
If alarm state needs to survive restarts:
- Create a new implementation of `RepairFaultReporter` (do not modify `LoggingFaultReporter`)
- Store state in Cassandra or another durable store
- Register it via `ecc.yml`

### Connecting to an external alarm system
Same approach: create a new implementation and set it in `ecc.yml`. The default `LoggingFaultReporter` is not meant to be extended — implement `RepairFaultReporter` directly.

## Code Style

Follows the Cassandra code style (Sun Java conventions). CheckStyle and PMD run on `mvn compile`.
