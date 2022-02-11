# Upgrade

## From 2.0.0

A new column has been added to the table `ecchronos.on_demand_repair_status`, this must be added before upgrading.

The command to add the column is shown below:
```
ALTER TABLE ecchronos.on_demand_repair_status ADD completed_time timestamp;
```

## From <=2.0.4

A new column has been added to the table `ecchronos.on_demand_repair_status`, this must be added before upgrading.

The command to add the column is shown below:
```
ALTER TABLE ecchronos.on_demand_repair_status ADD started_time timestamp;
```

Note: Make sure that you create the column with the cql_type timestamp since its not possible to change cql_type on an existing column.

## From versions before 2.0.0

A new table has been introduced and must be present before upgrading.

The required table is shown below:
```
CREATE TABLE IF NOT EXISTS ecchronos.on_demand_repair_status (
    host_id uuid,
    job_id uuid,
    table_reference frozen<table_reference>,
    token_map_hash int,
    repaired_tokens frozen<set<frozen<token_range>>>,
    status text,
    completed_time timestamp,
    started_time timestamp,
    PRIMARY KEY(host_id, job_id))
    WITH default_time_to_live = 2592000
    AND gc_grace_seconds = 0;
```

An optional configuration parameter for remote routing has been introduced, the default value is true.

This can be configured in `conf/ecc.yml`:
```
cql:
  remoteRouting: false
```