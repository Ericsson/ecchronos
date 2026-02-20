# application Module

The `application` module is the Spring Boot entry point for ecChronos. It assembles all other modules into a running service by wiring beans, loading YAML configuration, setting up TLS and authentication, and starting the repair scheduler.

---

## Module Structure

```
com.ericsson.bss.cassandra.ecchronos.application
├── SpringBooter.java                  # Main entry point (@SpringBootApplication)
├── spring/                            # Spring @Configuration classes
│   ├── BeanConfigurator               # Primary wiring hub (connections, security, metrics)
│   ├── ECChronos                      # Repair orchestration (scheduler, lock factory, policies)
│   ├── ECChronosInternals             # Core infrastructure (JMX factory, metrics, schedule manager)
│   ├── MetricBeans                    # Prometheus, JMX, and CSV meter registries
│   └── TomcatWebServerCustomizer      # TLS and optional metrics server on separate port
├── providers/                         # Connection provider implementations
│   ├── AgentNativeConnectionProvider  # CQL: auth, TLS, certificate reload, agent type
│   └── AgentJmxConnectionProvider     # JMX: Jolokia or RMI, PEM certificates
└── config/                            # YAML configuration model (~45 classes)
    ├── Config.java                    # Root model (all subsettings)
    ├── ConfigurationHelper.java       # YAML file loader (Jackson + YAMLFactory)
    ├── ConfigRefresher.java           # File watcher for security.yml hot-reload
    ├── connection/                    # CQL, JMX, thread pool config
    ├── security/                      # TLS, credentials, CRL, certificate hot-reload
    ├── repair/                        # Default repair settings and per-table overrides
    ├── scheduler/                     # Scheduler frequency
    ├── runpolicy/                     # Time-based blackout configuration
    ├── lockfactory/                   # CAS lock factory settings
    ├── metrics/                       # Statistics and reporting
    └── rest/                          # REST server host/port
```

---

## Configuration Files

| File | Purpose |
|---|---|
| `ecc.yml` | Master configuration: connection, repair, statistics, scheduler, lock factory, REST server, run policy |
| `security.yml` | CQL and JMX security: credentials, TLS (keystore or PEM), CRL |
| `application.yml` | Spring Boot settings: Springdoc, SSL for REST server, metrics server |
| `schedule.yml` | Per-table repair schedules (keyspace/table pattern matching, intervals, alarm thresholds) |

---

## Startup Flow

1. **`SpringBooter.main()`** — Spring Boot entry point
2. **`BeanConfigurator`** constructor — loads `ecc.yml` + `security.yml`, starts `ConfigRefresher`
3. **Bean creation:**
   - CQL session via `AgentNativeConnectionProvider` (auth, TLS, agent type filtering)
   - JMX connections via `AgentJmxConnectionProvider` (Jolokia or RMI, PEM support)
   - Fault reporter via reflection from configured class name
   - Composite meter registry (Prometheus, JMX, CSV)
4. **`ECChronos`** wires repair schedulers, on-demand scheduler, run policies, lock factory, and thread pool
5. **`TomcatWebServerCustomizer`** optionally adds a separate metrics server connector with independent TLS
6. **Scheduler starts** — `ScheduleManager.createScheduleFutureForNodeIDList()` for all managed nodes

---

## TLS & Security

### CQL TLS

Configured in `security.yml`. Supports two modes (PEM takes precedence):
- **Keystore:** `keystore`, `truststore` paths and passwords
- **PEM:** `certificate`, `private-key`, `trusted-certificate` file paths

Certificates are handled by `ReloadingCertificateHandler` — file changes trigger hot-reload without restart.

### JMX TLS

Same keystore/PEM options. PEM also works via Jolokia reverse proxy (Nginx/mTLS).

### REST Server TLS

Configured in `application.yml` via Spring Boot `server.ssl.*` settings. Supports keystore and PEM.

### Credentials

`ReloadingAuthProvider` wraps username/password; credential changes in `security.yml` are picked up on next new connection.

### Certificate Revocation (CRL)

Enabled in `security.yml` under `cql.tls.crl`. Supports strict mode (all certs must have valid CRL) and non-strict mode (only explicitly revoked certs are rejected). Configurable refresh interval.

---

## Metrics

`MetricBeans` creates a `CompositeMeterRegistry` with:
- **Prometheus** (if enabled) — metrics at `/metrics`
- **JMX** (if enabled) — metrics as JMX MBeans
- **CSV/file** (if enabled) — periodic CSV files in the configured directory

Metrics can be filtered by name and tags via `MeterFilterImpl`.

---

## Per-Table Schedule Configuration (`schedule.yml`)

```yaml
keyspaces:
  - name: mykeyspace
    tables:
      - name: mytable
        enabled: true
        interval: 7d
        alarm:
          warn: 8d
          error: 10d
        repair_type: vnode
```

Keyspace and table names support regex patterns.

---

## Dependencies

| Dependency | Purpose |
|---|---|
| All other `*-agent` modules | Full dependency set |
| Spring Boot Web | REST API server |
| Springdoc OpenAPI | Swagger UI |
| Jackson YAML | YAML configuration parsing |
| Micrometer (Prometheus, JMX) | Metrics registries |
| Logback | Logging |
| BouncyCastle (test) | Certificate generation for TLS tests |
