# connection Module

The `connection` module defines the interfaces for managing CQL and JMX connections to Apache Cassandra nodes. It is an interfaces-only module; implementations live in `connection.impl`.

---

## Module Structure

```
com.ericsson.bss.cassandra.ecchronos.connection
├── DistributedNativeConnectionProvider    (CQL session management)
├── DistributedJmxConnectionProvider       (JMX connection management)
└── CertificateHandler                     (SSL/TLS certificate provider)
```

---

## Interfaces

### DistributedNativeConnectionProvider

Manages CQL (Cassandra Query Language) connections to the cluster.

| Method | Purpose |
|---|---|
| `getCqlSession()` | Returns the shared `CqlSession` for all CQL operations |
| `getNodes()` | Returns `Map<UUID, Node>` of all managed nodes |
| `addNode(Node)` | Dynamically registers a new node |
| `removeNode(Node)` | Dynamically de-registers a node |
| `confirmNodeValid(Node)` | Returns `true` if the node is within the managed scope |
| `getConnectionType()` | Returns the agent type (`datacenterAware`, `rackAware`, `hostAware`) |
| `close()` | Closes the CQL session |

### DistributedJmxConnectionProvider

Manages JMX connections, one per Cassandra node.

| Method | Purpose |
|---|---|
| `getJmxConnections()` | Returns `ConcurrentHashMap<UUID, JMXConnector>` of all active connections |
| `getJmxConnector(UUID)` | Retrieves or reconnects to a specific node's JMX port |
| `isConnected(JMXConnector)` | Validates whether a connector is still active |
| `add(Node)` | Adds a JMX connection for a new node |
| `close(UUID)` | Closes a specific node's connection |
| `close()` | Closes all JMX connections |

### CertificateHandler

Extends the Cassandra driver's `SslEngineFactory` to provide custom TLS certificate handling with hot-reload support.

---

## Agent Types

The `ConnectionType` enum (from `utils`) defines the three ecChronos control modes:

| Mode | Scope |
|---|---|
| `datacenterAware` | Manages all nodes in one or more datacenters |
| `rackAware` | Manages nodes in specific datacenter + rack combinations |
| `hostAware` | Manages specific individual hosts by IP address |

---

## Dependencies

| Dependency | Purpose |
|---|---|
| `utils-agent` | `ConnectionType` enum |
| Cassandra Java Driver | `CqlSession`, `Node`, `SslEngineFactory` |
| SLF4J | Logging |
| Guava | Utilities |
