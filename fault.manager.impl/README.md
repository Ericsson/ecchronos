# fault.manager.impl Module

The `fault.manager.impl` module provides the default reference implementation of the `RepairFaultReporter` interface: `LoggingFaultReporter`. It logs alarm events via SLF4J and tracks in-memory alarm state to prevent duplicate log messages.

---

## Module Structure

```
com.ericsson.bss.cassandra.ecchronos.fm.impl
└── LoggingFaultReporter.java    (implements RepairFaultReporter)
```

---

## LoggingFaultReporter

### Behaviour

Tracks active alarms in an internal `HashMap<Integer, FaultCode>`, keyed by `data.hashCode()` (which is deterministic for the same keyspace/table pair).

**Raising alarms:**
- If the alarm is new (`oldCode == null`) → logs at `ERROR` level
- If escalating from `REPAIR_WARNING` to `REPAIR_ERROR` → logs at `ERROR` level
- Repeated calls with the same severity → silent (no duplicate logs)

**Ceasing alarms:**
- If an alarm exists for the table → logs at `INFO` level and removes it
- Repeated `cease()` calls after the alarm is cleared → silent

### Log Output Examples

```
ERROR Raising alarm: REPAIR_WARNING - {KEYSPACE=mykeyspace, TABLE=mytable}
ERROR Raising alarm: REPAIR_ERROR - {KEYSPACE=mykeyspace, TABLE=mytable}
INFO  Ceasing alarm: REPAIR_ERROR - {KEYSPACE=mykeyspace, TABLE=mytable}
```

### Configuration

This is the default implementation, referenced in `ecc.yml`:

```yaml
repair:
  alarm:
    faultReporter: com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter
    warn: 8d
    error: 10d
```

To suppress or redirect alarm logging, configure Logback:

```xml
<logger name="com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter" level="OFF"/>
```

---

## Alarm Lifecycle Example

| Event | Elapsed Since Repair | Action | Log |
|---|---|---|---|
| State update 1 | 9 days | `raise(WARNING)` | `ERROR Raising alarm: REPAIR_WARNING` |
| State update 2 | 9.5 days | `raise(WARNING)` | *(silent)* |
| State update 3 | 11 days | `raise(ERROR)` | `ERROR Raising alarm: REPAIR_ERROR` |
| Table repaired | 0 days | `cease(WARNING)` | `INFO Ceasing alarm: REPAIR_ERROR` |

---

## Notes on State

Alarm state is **in-memory only** and is lost on ecChronos restart. This is intentional: on restart, repair states are re-evaluated immediately and alarms are re-raised as needed.

---

## Dependencies

| Dependency | Purpose |
|---|---|
| `fault.manager-agent` | `RepairFaultReporter` interface |
| SLF4J | Logging |
