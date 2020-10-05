CREATE ROLE IF NOT EXISTS eccuser WITH PASSWORD = 'eccpassword' AND LOGIN = true;

GRANT SELECT ON KEYSPACE ecchronos TO eccuser;
GRANT MODIFY ON KEYSPACE ecchronos TO eccuser;
GRANT SELECT ON system_distributed.repair_history TO eccuser;
