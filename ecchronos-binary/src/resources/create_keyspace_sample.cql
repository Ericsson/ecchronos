CREATE KEYSPACE IF NOT EXISTS ecchronos WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

CREATE TABLE IF NOT EXISTS ecchronos.lock (
    resource text,
    node uuid,
    metadata map<text,text>,
    PRIMARY KEY(resource))
    WITH default_time_to_live = 600
    AND gc_grace_seconds = 0;

CREATE TABLE IF NOT EXISTS ecchronos.lock_priority (
    resource text,
    node uuid,
    priority int,
    PRIMARY KEY(resource, node))
    WITH default_time_to_live = 600
    AND gc_grace_seconds = 0;

CREATE TABLE IF NOT EXISTS ecchronos.reject_configuration (
    keyspace_name text,
    table_name text,
    start_hour int,
    start_minute int,
    end_hour int,
    end_minute int,
    PRIMARY KEY(keyspace_name, table_name, start_hour, start_minute));

CREATE TYPE IF NOT EXISTS ecchronos.token_range (
    start text,
    end text);

CREATE TYPE IF NOT EXISTS ecchronos.table_reference (
    id uuid,
    keyspace_name text,
    table_name text);

CREATE TABLE IF NOT EXISTS ecchronos.on_demand_repair_status (
    host_id uuid,
    job_id uuid,
    table_reference frozen<table_reference>,
    token_map_hash int,
    repaired_tokens frozen<set<frozen<token_range>>>,
    status text,
    completed_time timestamp,
    repair_type text,
    PRIMARY KEY(host_id, job_id))
    WITH compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 1, 'compaction_window_unit': 'DAYS'}
    AND default_time_to_live = 2592000
    AND gc_grace_seconds = 0;

CREATE TABLE IF NOT EXISTS ecchronos.repair_history(
    table_id uuid,
    node_id uuid,
    repair_id timeuuid,
    job_id uuid,
    coordinator_id uuid,
    range_begin text,
    range_end text,
    participants set<uuid>,
    status text,
    started_at timestamp,
    finished_at timestamp,
    PRIMARY KEY((table_id,node_id), repair_id))
    WITH compaction = {'class': 'TimeWindowCompactionStrategy'}
    AND default_time_to_live = 2592000
    AND CLUSTERING ORDER BY (repair_id DESC);
