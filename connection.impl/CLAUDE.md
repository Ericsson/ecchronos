# CLAUDE.md — connection.impl

This file provides guidance to Claude Code (claude.ai/code) when working with the `connection.impl` module.

## Module Overview

`connection.impl` implements the `DistributedNativeConnectionProvider` and `DistributedJmxConnectionProvider` interfaces from `connection`. It handles node filtering by agent type, CQL session creation, JMX connection setup (both standard RMI and Jolokia), IP translation, and reverse DNS.

**Maven artifact:** `connection.impl-agent`
**Root package:** `com.ericsson.bss.cassandra.ecchronos.connection.impl`

## Key Dependencies

| Dependency | Purpose |
|---|---|
| `connection-agent` | Interfaces to implement |
| `data-agent` | `IpTranslator`, `EccNodesSync` |
| `utils-agent` | `ConnectionType`, `NodeStatus` |
| Cassandra Java Driver | `CqlSession`, `Metadata`, `Node` |
| Jolokia client/JMX adapter | HTTP-based JMX |
| `java-driver-metrics-micrometer` | Driver metrics with Micrometer |

## Build Commands

```bash
# Unit tests for this module only
mvn test -pl connection.impl

# Single test class
mvn test -pl connection.impl -Dtest=TestDistributedNativeConnectionProviderImpl

# Build without tests
mvn install -pl connection.impl -DskipTests=true
```

## Package Structure

```
com.ericsson.bss.cassandra.ecchronos.connection.impl
├── builders/
│   ├── DistributedNativeBuilder     # CQL builder (agent type, auth, TLS, listeners)
│   ├── DistributedJmxBuilder        # JMX builder (Jolokia, TLS, IP translation, DNS)
│   └── ContactEndPoint              # Implements driver EndPoint for contact points
└── providers/
    ├── DistributedNativeConnectionProviderImpl
    └── DistributedJmxConnectionProviderImpl
```

## Key Architectural Points

### Node Filtering (`DistributedNativeBuilder`)

`createNodesMap(CqlSession)` contains the switch statement for all three agent types:
- `datacenterAware` → filter by `node.getDatacenter()`
- `rackAware` → filter by both `node.getDatacenter()` and `node.getRack()`
- `hostAware` → filter by `node.getEndPoint().resolve()` matching a configured `InetSocketAddress`

`confirmNodeValid(Node)` delegates to the same logic and is called dynamically when nodes join the cluster.

### JMX Connection Strategy (`DistributedJmxBuilder`)

The `reconnect(Node)` private method handles both transports:
- **Standard JMX**: queries `system_views.system_properties` for the JMX port, then uses `JMXConnectorFactory.connect()`
- **Jolokia**: uses `JolokiaJmxConnectionProvider` with a 20-second async timeout

Connection validation calls `getConnectionId()` and `getMBeanServerConnection()`. If either fails, the node is marked `UNAVAILABLE` in `EccNodesSync`.

### IP Translation

The `IpTranslator` from `data` reads `system_views.gossip_info` and maps external IPs to internal IPs. This handles Docker container networking where nodes advertise different addresses than they are reachable at.

Call `translator.isActive()` before calling `getInternalIp()` — if no translation mapping exists, the method returns the original IP unchanged.

### LOCAL_QUORUM Verification

After creating the `CqlSession`, the builder checks:
```java
isKeyspaceReplicationFactorOK(rfMap, localDatacenter)
```
This validates `RF >= (RF/2) + 1` nodes are available for quorum. If the ecchronos keyspace is under-replicated for the local DC, startup fails. Do not bypass this check.

## Testing

Tests use JUnit 5 + Mockito. They mock `CqlSession`, `Metadata`, and `Node` to test the filtering logic without a real Cassandra cluster.

`TestDistributedNativeConnectionProviderImpl` sets up a 2×2 grid (2 DCs × 2 racks) of mock nodes and verifies correct filtering for all three agent types.

`TestNodeInclusion` focuses on `confirmNodeValid()` for the same scenarios.

## Common Modification Scenarios

### Adding a new agent type

1. Add the value to `ConnectionType` in `utils`
2. Add a case to the `switch` in `DistributedNativeBuilder.createNodesMap()`
3. Add a case to `DistributedNativeBuilder.confirmNodeValid()`
4. Add configuration parsing in `application/config/connection/DistributedNativeConnection.java`
5. Add test cases in `TestDistributedNativeConnectionProviderImpl` and `TestNodeInclusion`

### Changing JMX connection behaviour

All JMX logic is in `DistributedJmxBuilder.reconnect(Node)`:
- Default JMX port query: `system_views.system_properties` → `cassandra.jmx.remote.port` or `cassandra.jmx.local.port`
- Fallback port: 7199
- Jolokia port: configurable via `withJolokiaPort()`, default 8778

### Supporting a new JMX transport

Implement the new transport in `reconnect(Node)` and add a builder flag alongside `withJolokiaEnabled()`.

## Code Style

Follows the Cassandra code style (Sun Java conventions). CheckStyle and PMD run on `mvn compile`.
