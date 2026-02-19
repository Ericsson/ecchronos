# fault.manager Module

The `fault.manager` module defines the pluggable API contract for fault and alarm reporting in ecChronos. It is an intentionally minimal interfaces-only module that decouples repair monitoring from any specific alarm delivery system.

---

## Module Structure

```
com.ericsson.bss.cassandra.ecchronos.fm
└── RepairFaultReporter.java    (interface)
```

---

## Core Interface: RepairFaultReporter

The single interface in this module defines how ecChronos raises and ceases repair-related alarms.

```java
public interface RepairFaultReporter {
    String FAULT_KEYSPACE = "KEYSPACE";
    String FAULT_TABLE    = "TABLE";

    enum FaultCode { REPAIR_WARNING, REPAIR_ERROR }

    void raise(FaultCode faultCode, Map<String, Object> data);
    void cease(FaultCode faultCode, Map<String, Object> data);
}
```

### Fault Codes

| Code | Default Threshold | Meaning |
|---|---|---|
| `REPAIR_WARNING` | 8 days since last repair | Table is overdue for repair |
| `REPAIR_ERROR` | 10 days since last repair | Table is significantly overdue |

### Contract

- `raise()` and `cease()` **may be called repeatedly** with the same arguments. Implementations are responsible for deduplication.
- The `data` map always contains at minimum `FAULT_KEYSPACE` and `FAULT_TABLE`. Custom implementations may inspect or extend it.
- Neither method declares checked exceptions. Implementations should handle failures internally.

---

## How Alarms Are Triggered

Alarms are fired by `AlarmPostUpdateHook` in `core.impl` after every repair state update:

1. The hook calculates elapsed time since the table was last repaired
2. If elapsed time ≥ error threshold → `raise(REPAIR_ERROR, data)`
3. If elapsed time ≥ warning threshold → `raise(REPAIR_WARNING, data)`
4. If elapsed time < warning threshold → `cease(REPAIR_WARNING, data)`

---

## Configuration

The fault reporter implementation class is selected in `ecc.yml`:

```yaml
repair:
  alarm:
    faultReporter: com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter
    warn: 8d
    error: 10d
```

The class is instantiated via reflection (zero-arg constructor required). The default is `LoggingFaultReporter` from the `fault.manager.impl` module.

---

## Implementing a Custom Reporter

```java
public class MyFaultReporter implements RepairFaultReporter {
    @Override
    public void raise(FaultCode faultCode, Map<String, Object> data) {
        // Send alert to PagerDuty, Kafka, Prometheus gauge, etc.
    }

    @Override
    public void cease(FaultCode faultCode, Map<String, Object> data) {
        // Resolve the alert in the target system
    }
}
```

Update `ecc.yml` to use your class, then deploy. No other changes are required.

---

## Dependencies

This module has **zero runtime dependencies** by design, ensuring maximum portability for custom implementations.
