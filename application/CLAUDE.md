# CLAUDE.md — application

This file provides guidance to Claude Code (claude.ai/code) when working with the `application` module.

## Module Overview

`application` is the Spring Boot entry point. It owns all `@Configuration` classes, YAML configuration models, connection provider implementations, security (TLS, auth, CRL), and metrics setup. It has no business logic — it wires and configures everything from the other modules.

**Maven artifact:** `application-agent`
**Root package:** `com.ericsson.bss.cassandra.ecchronos.application`

## Key Dependencies

| Dependency | Purpose |
|---|---|
| All other `*-agent` modules | Full runtime dependency set |
| Spring Boot Web | Embedded Tomcat, REST API |
| Springdoc OpenAPI | Swagger UI at `/swagger-ui.html` |
| Jackson YAML | `ecc.yml` / `security.yml` parsing |
| Micrometer (Prometheus, JMX) | Metrics registries |
| Logback | Logging |
| BouncyCastle (test) | Certificate generation in TLS tests |

## Build Commands

```bash
# Unit tests for this module only
mvn test -pl application

# Single test class
mvn test -pl application -Dtest=TestConfig

# Build without tests
mvn install -pl application -DskipTests=true
```

## Configuration Model (`config/`)

The `Config` class is the root YAML model. Every `ecc.yml` section maps to a sub-class:

| YAML section | Java class | Location |
|---|---|---|
| `connection.cql` | `DistributedNativeConnection` | `config/connection/` |
| `connection.jmx` | `DistributedJmxConnection` | `config/connection/` |
| `connection.threadPool` | `ThreadPoolTaskConfig` | `config/connection/` |
| `repair` | `GlobalRepairConfig` → `RepairConfig` | `config/repair/` |
| `statistics` | `StatisticsConfig` | `config/metrics/` |
| `scheduler` | `SchedulerConfig` | `config/scheduler/` |
| `run_policy` | `RunPolicyConfig` → `TimeBasedConfig` | `config/runpolicy/` |
| `lock_factory.cas` | `CasLockFactoryConfig` | `config/lockfactory/` |
| `rest_server` | `RestServerConfig` | `config/rest/` |

**Rule:** When adding a new `ecc.yml` setting, add the field to the appropriate sub-class. Validate it in the constructor or a `validate()` method. Never add defaults via Spring `@Value` — use the Java configuration model.

## Spring Configuration Classes

### BeanConfigurator

The primary wiring hub. Loads `ecc.yml` and `security.yml`, creates connection providers, wires fault reporters, configures the web server factory.

Key beans created here:
- `AgentNativeConnectionProvider` (CQL session + node filtering)
- `AgentJmxConnectionProvider` (JMX connections)
- `RepairFaultReporter` (via `ReflectionUtils.construct()` from configured class name)
- `TomcatServletWebServerFactory` (main REST server)

Hot-reload beans use `AtomicReference<Security>` to allow security config refresh without restart:
```java
AtomicReference<Security> securityRef = new AtomicReference<>(loadSecurity());
// ConfigRefresher calls securityRef.set(reloadSecurity()) on file change
```

### ECChronos

Assembles the repair stack:
- `RepairSchedulerImpl` and `OnDemandRepairSchedulerImpl`
- `TimeBasedRunPolicy` (blackout windows)
- `CASLockFactory` (distributed leasing)
- `NodeWorkerManager` with `ThreadPoolTaskExecutor`
- Calls `scheduleManager.createScheduleFutureForNodeIDList()` at startup

### MetricBeans

Creates `CompositeMeterRegistry` with reporters based on `statistics.*` config. Applies `MeterFilterImpl` to exclude metrics by name/tag patterns.

### TomcatWebServerCustomizer

Adds a second Tomcat connector for the metrics server (optional, configurable port + independent TLS). Only active when `metricsServer.enabled=true` in `application.yml`.

## Security & TLS

### CQL TLS (`AgentNativeConnectionProvider`)

Two modes are supported; PEM takes precedence:
1. **Keystore** — `keystore`/`truststore` JKS files
2. **PEM** — `certificate`, `private-key`, `trusted-certificate` files

TLS is applied via `ReloadingCertificateHandler` (implements `CertificateHandler` from `connection`). File changes in `security.yml` trigger hot-reload.

CRL (Certificate Revocation List) is optional:
- `strict=true` — all certs must have a valid CRL entry
- `strict=false` — only explicitly revoked certs are rejected

### JMX TLS (`AgentJmxConnectionProvider`)

Same keystore/PEM pattern. JMX via Jolokia also supports PEM via Nginx mTLS.

### Credentials (`ReloadingAuthProvider`)

Wraps `AuthProvider`; credentials reload automatically when `security.yml` changes. The `ConfigRefresher` thread detects file modification time changes.

## Per-Table Repair Configuration (`schedule.yml`)

`FileBasedRepairConfiguration` reads `schedule.yml` and maps per-table settings to `RepairConfiguration` objects. Table names support regex patterns. The configuration is reloaded periodically by `ReloadSchedulerService`.

```yaml
keyspaces:
  - name: my_keyspace           # Regex supported
    tables:
      - name: my_table          # Regex supported
        enabled: true
        interval: 7d
        alarm:
          warn: 8d
          error: 10d
        repair_type: vnode      # vnode | parallel_vnode | incremental
        size_target: 100m       # Optional: enables sub-range repair
```

## Testing

Tests use JUnit 5 + Mockito. Many tests use `@SpringBootTest` with `@MockitoBean` for integration-level testing.

**TLS tests** (`TestTomcatWebServerCustomizer*`) use BouncyCastle to generate real certificates in-memory and test actual TLS handshakes. There are 4 variants: RSA keystore, EC keystore, RSA PEM, EC PEM.

**Configuration tests** (`TestConfig`) load `all_set.yml` (all options set) and `nothing_set.yml` (all defaults) from test resources and assert on parsed values.

## Common Modification Scenarios

### Adding a new ecc.yml setting
1. Add field to the appropriate config class in `config/`
2. Add getter and optionally a setter
3. Provide a sensible default value
4. Add to `TestConfig` with both explicit value and default assertion
5. Use the value in the appropriate Spring `@Configuration` class

### Adding a new Spring bean
1. Add `@Bean` method to the appropriate `@Configuration` class (`BeanConfigurator`, `ECChronos`, etc.)
2. Use constructor injection — no `@Autowired` on fields
3. Add a test that verifies the bean is created correctly (use `@SpringBootTest` or pure unit test with mocks)

### Adding a new metrics reporter
1. Add reporter config class in `config/metrics/`
2. Add `@Bean` method in `MetricBeans`
3. Add `enabled` flag following the existing JMX/Prometheus pattern
4. Add YAML config mapping in `StatisticsConfig`

### Changing TLS handling
- CQL TLS: `AgentNativeConnectionProvider` + `ReloadingCertificateHandler`
- JMX TLS: `AgentJmxConnectionProvider`
- REST TLS: `TomcatWebServerCustomizer` + `application.yml` `server.ssl.*`
- All TLS config models are in `config/security/`

## Code Style

Follows the Cassandra code style (Sun Java conventions). CheckStyle and PMD run on `mvn compile`. Javadoc required on all public methods in non-test code.
