# CLAUDE.md — connection

This file provides guidance to Claude Code (claude.ai/code) when working with the `connection` module.

## Module Overview

`connection` is the interfaces-only module for CQL and JMX connectivity. It defines the contracts that `connection.impl` implements and that `core`, `core.impl`, and `application` depend on. It contains no business logic.

**Maven artifact:** `connection-agent`
**Root package:** `com.ericsson.bss.cassandra.ecchronos.connection`

## Key Dependencies

| Dependency | Purpose |
|---|---|
| `utils-agent` | `ConnectionType` enum |
| Cassandra Java Driver | `CqlSession`, `Node`, `SslEngineFactory`, `EndPoint` |
| Guava | Collections utilities |
| SLF4J | Logging |

## Build Commands

```bash
mvn compile -pl connection
mvn install -pl connection -DskipTests=true
```

No unit tests in this module (interfaces only).

## The Three Interfaces

### DistributedNativeConnectionProvider

- Single `CqlSession` shared across the application
- `getNodes()` returns only nodes in the managed scope (filtered by agent type)
- `confirmNodeValid(Node)` is the gating check used by `connection.impl` builders
- `getConnectionType()` drives scheduling decisions in `core.impl`

### DistributedJmxConnectionProvider

- One `JMXConnector` per node, stored in a `ConcurrentHashMap`
- `getJmxConnector(UUID)` must handle reconnection if the connection has dropped
- `isConnected()` should be a lightweight check (e.g., call `getConnectionId()`)

### CertificateHandler

- Extends `SslEngineFactory` from the Cassandra driver
- Used for hot-reloadable TLS certificates
- Implemented in `application` as `ReloadingCertificateHandler`

## Common Modification Scenarios

### Adding a method to an existing interface
1. Add the method with a clear contract documented in the Javadoc
2. Implement it in `connection.impl` (`DistributedNativeConnectionProviderImpl` or `DistributedJmxConnectionProviderImpl`)
3. Update any test mocks that implement the interface

### Adding a new interface
- Only add here if the interface is used by two or more other modules
- Follow the pattern: interface in `connection`, implementation in `connection.impl`

### Adding a new ConnectionType value
- Add it to `ConnectionType` in `utils`
- Add the corresponding filtering logic in `connection.impl/builders/DistributedNativeBuilder.java` (`createNodesMap()` switch)
- Add validation in `application/config/connection/DistributedNativeConnection.java`

## Code Style

Follows the Cassandra code style (Sun Java conventions). CheckStyle and PMD run on `mvn compile`.
