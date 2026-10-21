# Upgrade to 1.1.x

## scheduler.session_window: new structure, per-task lock mode, and force_refresh

`scheduler.session_window` has been extended from a bare interval into an object with two fields:

- `enabled` (boolean, default `true`) — selects how distributed (CAS) repair locks are scoped:
  - `true` (default, unchanged behaviour): "batched" lock-session mode. When a node starts a
    repair session it acquires the distributed locks for the replica resources it touches and
    holds them for the whole window, reusing shared locks across tasks. Note that, for a single
    agent managing many nodes, holding the locks for the whole window can serialize work across
    those nodes (one node's session holds the shared replica-set locks the others need).
  - `false`: per-task mode. The lock is acquired for a task, the task runs, and the
    lock is released immediately, so a replica-set lock is held only for the duration of a single
    repair task. This maximises cross-node repair concurrency for an agent managing many nodes, at
    the cost of more frequent lock acquisitions. It mirrors the lock scope of the old sidecar
    architecture.
- `interval` (`{time, unit}`, default `5 MINUTES`) — the previous session-window duration, now
  nested under `interval`.

New form:

```yaml
scheduler:
  session_window:
    enabled: true
    interval:
      time: 5
      unit: MINUTES
```

### Backward compatibility

The old flat form is still accepted, so existing configuration files keep their configured window
and are **not** silently reset to the default:

```yaml
scheduler:
  session_window:        # legacy form, still honoured
    time: 10
    unit: MINUTES
```

A legacy file is read as `enabled: true` (the historical behaviour) with the configured
`time`/`unit` used as the window. If both the legacy `time`/`unit` and the new `interval` are
present, `interval` takes precedence. No action is required to upgrade; set `enabled: false` only
if you want the per-task lock mode.

### `force_refresh` (new, default `false`)

`scheduler.session_window` gains a third field, `force_refresh` (boolean, default `false`):

```yaml
scheduler:
  session_window:
    enabled: true
    interval:
      time: 5
      unit: MINUTES
    force_refresh: false   # new; opt-in
```

- `false` (default, unchanged behaviour): within a session a job's current task snapshot is
  drained once, and the repair state is refreshed at most once afterwards (subject to the normal
  refresh throttle). After the snapshot is drained the node waits for the next (throttled) refresh
  before it can "see" more work. With a 24h repair interval that throttle is ~14 minutes, so a node
  can sit idle for several minutes between bursts even when there is more work to do and the
  distributed locks are free.
- `true`: when a session drains a job's current task snapshot **within the window** and executed
  work, the scheduler immediately forces a repair-state refresh (bypassing the throttle) and keeps
  draining the **same** job in the same session while it still reports repairable ranges. This
  removes the idle wait between bursts and markedly improves scheduler utilization for an agent
  managing many nodes. The forced refresh happens at most **once per drained snapshot** (roughly
  once per session, not once per task), so the extra repair-state recompute cost stays amortised.

#### Relationship with `enabled`

`force_refresh` and `enabled` are **independent** and control different things:

- `enabled` controls the **lock scope** (how long a distributed lock is held): batched for the
  whole window (`true`) vs per task (`false`).
- `force_refresh` controls **when the repair state is refreshed** so the session can pick up more
  work (throttled only, `false`; or forced once a snapshot is drained, `true`).

Because they are orthogonal, `force_refresh: true` works with **either** `enabled: true` **or**
`enabled: false` — it does not require batched sessions. All four combinations are valid:

| `enabled` | `force_refresh` | Behaviour |
| --- | --- | --- |
| `true`  | `false` | Default. Batched locks, single drain per session, throttled refresh. |
| `false` | `false` | Per-task locks, single drain per session, throttled refresh. |
| `true`  | `true`  | Batched locks, re-drive the job after a forced refresh while work remains. |
| `false` | `true`  | Per-task locks, re-drive after a forced refresh — highest cross-node throughput for a many-node agent. |

Note that `force_refresh` lives under `session_window` purely for grouping; it is not gated by
`enabled`. The `interval` still bounds how long a single session (including any force-refresh
re-drives) may run, regardless of `enabled` or `force_refresh`.

#### Recommendation

Leave `force_refresh: false` on upgrade (no behaviour change). Enable it (ideally together with
`enabled: false`) for a standalone agent managing many nodes whose repairs lag because nodes sit
idle between refreshes. When enabling it, watch the agent CPU and Cassandra load (pending
compactions, read latency): sustaining higher repair concurrency puts more continuous load on
Cassandra, and the forced refresh adds an amortised (once-per-session) repair-state recompute.

## nodes_sync self-healing (heartbeat + TTL)

From this version, each ecChronos instance periodically renews (heartbeats) its own rows in
`ecchronos.nodes_sync`, writing them with a TTL. While an instance is running its rows are
continuously refreshed and persist; if an instance is permanently stopped (for example when a
datacenter is decommissioned), its rows are no longer renewed and expire automatically via the
TTL. This resolves where orphaned rows previously remained `AVAILABLE` indefinitely.

No schema change is required: the TTL is applied per write (`USING TTL`), not via
`default_time_to_live`. Any pre-existing rows written by an older version have no TTL. Rows still
owned by a running instance are healed automatically on the first heartbeat after upgrade (they are
rewritten with a TTL). Rows belonging to instances that no longer exist were never going to be
renewed, so they must be removed once, manually:

```
DELETE FROM ecchronos.nodes_sync WHERE ecchronos_id = '<retired-instance-id>';
```

The heartbeat is configurable in `ecc.yml` under `connection.cql.nodesSyncHeartbeat`. Both
`interval` and `ttl` are expressed in the same `unit` (defaults: `interval: 1`, `unit: hours`).
If `ttl` is left unset it defaults to 3x the interval (so 3 hours by default). The TTL must be
greater than the interval so a single missed heartbeat does not prematurely expire a live node's
row; the application fails to start if an explicitly configured TTL violates this. The TTL is
converted to seconds internally for Cassandra's `USING TTL`.

Additionally, `ecctool nodes state` (and the `/state/nodes` REST endpoint) now expose node
freshness. The legacy `Node Status` column is unchanged (it keeps showing the raw status such as
`AVAILABLE`). A new `Stale For` column shows, for a node whose row has not been refreshed within
roughly two heartbeat intervals, how long it has been since its last successful heartbeat (e.g.
`0 day(s) 00h 05m 31s`); for a healthy node it shows `---`. The REST/JSON output carries two new
fields: `stale` (boolean) and `lastHeartbeatAgeMs` (always the time since the last successful
heartbeat, in milliseconds), the latter being useful for trend monitoring. The underlying
`nodeStatus` value is left untouched.

## Incremental Repairs

During the fix of [Incremental Repair does not works properly](https://github.com/Ericsson/ecchronos/issues/1777) table ecchronos.repair_history has gotten a new column `repair_type`.
The `ecchronos.repair_history` table must be updated before performing the upgrade to version 1.X.

The command to add the column is shown below:
```
ALTER TABLE ecchronos.repair_history ADD repair_type text;
```

# Upgrading from ecChronos Sidecar to ecChronos Agent (1.x.x)

> ⚠️ **This is a major upgrade. Test thoroughly in a non-production environment before rolling out to production.**

ecChronos 1.x.x introduces an agent-based connection model replacing the previous sidecar architecture.
The agent runs as a JVM agent attached to the Cassandra process itself, rather than as a standalone sidecar process.

## What Changed

- Connection model changed from sidecar (standalone process with JMX) to agent (one ecchronos managing multiple nodes).
- A new `nodes_sync` table is required for distributed coordination.
- Each agent instance must have a unique `instanceName`.
- Topology awareness (`datacenterAware`, `rackAware`, or `hostAware`) must be explicitly configured.

### Migration Steps

#### 1. Create the `nodes_sync` table

The `nodes_sync` table is **mandatory** for the agent. Without it, the agent will fail to start.

Refer to [SETUP.md](SETUP.md) for the full CQL schema and keyspace creation instructions.

#### 2. Configure the agent

In `ecc.yml`, set the following required fields:

```yaml
instanceName: "<unique-name-per-node>"   # Must be unique across all nodes in the cluster

connection:
  type: "datacenterAware"                # One of: datacenterAware, rackAware, hostAware
```

- `instanceName` must be unique per node. A common convention is to use the node's hostname or IP.
- `type` controls how the agent selects which tables to repair — choose based on your replication strategy and operational preference.

See [SETUP.md](SETUP.md) for the full `ecc.yml` reference.

#### 3. Remove the old sidecar instances

Once the agent is deployed and verified on a node, stop and remove the sidecar process from that node.
Do **not** run both the sidecar and the agent on the same node simultaneously.

#### 4. Verify

After deploying the agent on each node:

- Check that the `nodes_sync` table is being populated.
- Confirm repairs are being scheduled via `ecctool` or the REST API.
- Monitor logs for any connectivity or configuration errors.

---

### Reference

- Sidecar upgrade documentation (ecChronos 6.x): https://github.com/Ericsson/ecchronos/blob/ecchronos-6.1/docs/UPGRADE.md
- Setup guide (current): [SETUP.md](SETUP.md)
