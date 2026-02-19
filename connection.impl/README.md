# connection.impl Module

The `connection.impl` module provides the concrete implementations of the CQL and JMX connection interfaces defined in `connection`. It uses the Cassandra Java Driver for CQL sessions and supports both standard RMI-based JMX and HTTP-based Jolokia for JMX access.

---

## Module Structure

```
com.ericsson.bss.cassandra.ecchronos.connection.impl
├── builders/
│   ├── DistributedNativeBuilder     # Fluent builder for CQL connections
│   ├── DistributedJmxBuilder        # Fluent builder for JMX connections
│   └── ContactEndPoint              # Implements driver EndPoint interface
└── providers/
    ├── DistributedNativeConnectionProviderImpl  # CQL implementation
    └── DistributedJmxConnectionProviderImpl     # JMX implementation
```

---

## How CQL Connections Are Established

`DistributedNativeBuilder` produces a `DistributedNativeConnectionProviderImpl`:

1. Configure contact points, local datacenter, agent type, auth, and TLS
2. `build()` creates a `CqlSession` via the Cassandra Java Driver
3. All cluster nodes are discovered from session metadata
4. Nodes are filtered by agent type:
   - **`datacenterAware`** — keeps nodes where `node.getDatacenter()` matches the configured list
   - **`rackAware`** — keeps nodes where datacenter **and** rack both match
   - **`hostAware`** — keeps nodes where `node.getEndPoint().resolve()` matches configured IPs
5. The ecchronos keyspace is verified to have sufficient replication for `LOCAL_QUORUM`

### Key Builder Methods

| Method | Purpose |
|---|---|
| `withInitialContactPoints(List<InetSocketAddress>)` | Seed nodes for driver bootstrap |
| `withAgentType(ConnectionType)` | Node filtering mode |
| `withLocalDatacenter(String)` | Driver routing datacenter |
| `withDatacenterAware(List<String>)` | DC names to manage |
| `withRackAware(List<Map<String,String>>)` | DC+rack pairs to manage |
| `withHostAware(List<InetSocketAddress>)` | Specific hosts to manage |
| `withAuthProvider(AuthProvider)` | Cassandra authentication |
| `withSslEngineFactory(SslEngineFactory)` | TLS configuration |
| `withSchemaChangeListener(SchemaChangeListener)` | Listener for schema events |
| `withNodeStateListener(NodeStateListener)` | Listener for node up/down events |

---

## How JMX Connections Are Established

`DistributedJmxBuilder` produces a `DistributedJmxConnectionProviderImpl`:

1. Iterates all nodes from the native connection provider
2. For each node, calls `reconnect(Node)`:
   - Extracts the node's broadcast RPC address (falls back to listen address)
   - Applies IP translation if configured
   - Applies reverse DNS resolution if configured
   - **Standard JMX:** Queries `system_views.system_properties` for JMX port (default 7199), connects via `service:jmx:rmi:///jndi/rmi://{host}:{port}/jmxrmi`
   - **Jolokia:** Connects via `service:jmx:jolokia://{host}:{port}/jolokia/` with 20-second timeout
3. Validates the connection by calling `getConnectionId()`
4. Updates `EccNodesSync` with `AVAILABLE` or `UNAVAILABLE` status

### Key Builder Methods

| Method | Purpose |
|---|---|
| `withNativeConnection(DistributedNativeConnectionProvider)` | Source of node list |
| `withJolokiaEnabled(boolean)` | Use HTTP Jolokia instead of RMI JMX |
| `withJolokiaPort(int)` | Jolokia port (default 8778) |
| `withDNSResolution(boolean)` | Enable reverse DNS for hostnames |
| `withIpTranslator(IpTranslator)` | Map external IPs to internal IPs |
| `withCredentials(Supplier<String[]>)` | JMX username/password |
| `withTLS(Supplier<Map<String,String>>)` | JMX TLS properties |

---

## LOCAL_QUORUM Quorum Validation

On CQL connection setup, the builder verifies that the ecchronos keyspace has sufficient replication in the local datacenter for `LOCAL_QUORUM` consistency. Quorum is calculated as `(replicationFactor / 2) + 1`. If replication is insufficient, startup fails with a clear error.

---

## Dependencies

| Dependency | Purpose |
|---|---|
| `connection-agent` | Interfaces implemented here |
| `data-agent` | `IpTranslator`, `EccNodesSync` |
| `utils-agent` | `ConnectionType`, `NodeStatus` |
| Cassandra Java Driver | `CqlSession`, `JMXConnector` |
| Jolokia client libraries | HTTP-based JMX transport |
| Micrometer | Driver metrics integration |
