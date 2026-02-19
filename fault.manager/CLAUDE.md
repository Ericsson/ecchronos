# CLAUDE.md — fault.manager

This file provides guidance to Claude Code (claude.ai/code) when working with the `fault.manager` module.

## Module Overview

`fault.manager` defines the single interface `RepairFaultReporter` that ecChronos uses for all alarm/fault reporting. It is an interfaces-only module with **zero runtime dependencies**, intentionally isolated so that any implementation can be plugged in without transitive dependency issues.

**Maven artifact:** `fault.manager-agent`
**Root package:** `com.ericsson.bss.cassandra.ecchronos.fm`

## Build Commands

```bash
# Compile and check style
mvn compile -pl fault.manager

# Build without tests
mvn install -pl fault.manager -DskipTests=true
```

There are no unit tests in this module (interfaces only).

## The Interface

`RepairFaultReporter` has exactly two methods and one nested enum:

```java
void raise(FaultCode faultCode, Map<String, Object> data);
void cease(FaultCode faultCode, Map<String, Object> data);

enum FaultCode { REPAIR_WARNING, REPAIR_ERROR }
```

**Constants for data map keys:**
- `FAULT_KEYSPACE = "KEYSPACE"`
- `FAULT_TABLE = "TABLE"`

## How It Is Used

The only call site is `AlarmPostUpdateHook` in `core.impl`. The hook fires after every `RepairState.update()` call and decides whether to raise or cease an alarm based on elapsed time since last repair.

The `RepairFaultReporter` bean is created in `application/spring/BeanConfigurator.java` via reflection from the class name in `ecc.yml`:

```java
return ReflectionUtils.construct(config.getRepairConfig().getAlarm().getFaultReporterClass());
```

The class must have a **zero-arg constructor**.

## Key Design Rules

- **No runtime dependencies** — do not add any. This is the point of the module: implementations bring their own.
- **Idempotent contract** — `raise()` and `cease()` can be called multiple times with identical parameters. Any implementation must tolerate this.
- **No checked exceptions** — methods must not declare checked exceptions; implementations handle errors internally.

## Common Modification Scenarios

### Adding a new FaultCode
1. Add the value to the `FaultCode` enum
2. Update `AlarmPostUpdateHook` in `core.impl` to use the new code where appropriate
3. Update `LoggingFaultReporter` in `fault.manager.impl` to handle it
4. Document the threshold configuration in `application/config/repair/Alarm.java`

### Adding a new data key constant
- Add it as a `public static final String` to `RepairFaultReporter`
- Document when it is populated in the `data` map

### Replacing the default implementation
- Create a class implementing `RepairFaultReporter` with a zero-arg constructor
- Set `alarm.faultReporter` in `ecc.yml` to the fully-qualified class name
- The `fault.manager.impl` module provides the default `LoggingFaultReporter`

## Code Style

Follows the Cassandra code style (Sun Java conventions). CheckStyle and PMD run on `mvn compile`.
