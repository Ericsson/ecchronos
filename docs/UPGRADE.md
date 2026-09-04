# Upgrade to 1.1.x

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
