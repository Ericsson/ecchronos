# REST interfaces

## Repair scheduler

The REST interface for the repair scheduler is located under the path `<host>/repair-management/v1/`.

The interface is exposing state and configuration for scheduled tables and allows triggering manual repairs.


### Resources

* &lt;host&gt;/repair-management/v1/status
  - Valid verbs: GET
* &lt;host&gt;/repair-management/v1/status/ids/&lt;id&gt;
  - Valid verbs: GET
* &lt;host&gt;/repair-management/v1/status/keyspaces/&lt;keyspace&gt;
  - Valid verbs: GET
* &lt;host&gt;/repair-management/v1/status/keyspaces/&lt;keyspace&gt;/tables/&lt;table&gt;
  - Valid verbs: GET
* &lt;host&gt;/repair-management/v1/config
  - Valid verbs: GET
* &lt;host&gt;/repair-management/v1/config/ids/&lt;id&gt;
  - Valid verbs: GET
* &lt;host&gt;/repair-management/v1/config/keyspaces/&lt;keyspace&gt;
  - Valid verbs: GET
* &lt;host&gt;/repair-management/v1/config/keyspaces/&lt;keyspace&gt;/tables/&lt;table&gt;
  - Valid verbs: GET
* &lt;host&gt;/repair-management/v1/schedule/keyspaces/&lt;keyspace&gt;/tables/&lt;table&gt;
  - Valid verbs: POST


### Get specific table repair status

When performing GET on `<host>/repair-management/v1/status/keyspaces/mykeyspace/tables/mytable` or a GET on `<host>/repair-management/v1/status/ids/d53c2490-548a-11ea-8366-d174199d777a` a JSON object of the [RepairJob](../ecchronos-binary/src/test/features/repair_job.json) type will be returned.

*Note: The field virtualNodeStates will only be used when showing a specific table.
When listing multiple table repair jobs the field will not be used.*


### Get table repair job status

When performing GET on `<host>/repair-management/v1/status` a [JSON list of RepairJobs](../ecchronos-binary/src/test/features/repair_job_list.json) for all keyspaces will be returned.

When performing GET on `<host>/repair-management/v1/status/keyspaces/mykeyspace` a JSON list of RepairJobs for the keyspace "mykeyspace" will be returned.


### Get scheduled table configuration

When performing GET on `<host>/repair-management/v1/config` a [JSON list of RepairConfig](../ecchronos-binary/src/test/features/repair_config_list.json) for all keyspaces will be returned.

When performing GET on `<host>/repair-management/v1/config/keyspaces/mykeyspace` a JSON list of RepairConfig for the keyspace "mykeyspace" will be returned.

When performing GET on `<host>/repair-management/v1/config/keyspaces/mykeyspace/tables/mytable` or `<host>/repair-management/v1/config/ids/d53c2490-548a-11ea-8366-d174199d777a` a JSON object of the [RepairConfig](../ecchronos-binary/src/test/features/repair_config.json) type will be returned.

### Schedule table repair

When performing POST on `<host>/repair-management/v1/schedule/keyspaces/mykeyspace/tables/mytable` a JSON object of the [RepairJob](../ecchronos-binary/src/test/features/repair_job.json) type will be returned.


### Types

RepairJob:

| Key                    | Type                   | Example value                           | Optional  |
|------------------------|------------------------|-----------------------------------------|-----------|
| id                     | UUID                   | d53c2490-548a-11ea-8366-d174199d777a    | Mandatory |
| keyspace               | String                 | mykeyspace                              | Mandatory |
| table                  | String                 | mytable                                 | Mandatory |
| status                 | String                 | COMPLETED                               | Mandatory |
| repairedRatio          | double                 | 1.0 (100%)                              | Mandatory |
| lastRepairedAtInMs     | long                   | 1553099547852 (2019-03-16T20:32:27.852) | Mandatory |
| nextRepairInMs         | long                   | 1553531547000 (2019-03-16T16:32:27.000) | Mandatory |
| virtualNodeStates      | list(VirtualNodeState) | VirtualNode state example below         | Optional  |

VirtualNodeState:

| Key                | Type               | Example value                           |
|--------------------|--------------------|-----------------------------------------|
| startToken         | long               | -1                                      |
| endToken           | long               | 1                                       |
| replicas           | list(inet address) | [127.0.0.1, 127.0.0.2]                  |
| lastRepairedAtInMs | long               | 1553099547852 (2019-03-16T16:32:27.852) |
| repaired           | boolean            | true                                    |

RepairConfig:

| Key                    | Type       | Example value                           | Optional  |
|------------------------|------------|-----------------------------------------|-----------|
| id                     | UUID       | d53c2490-548a-11ea-8366-d174199d777a    | Mandatory |
| keyspace               | String     | mykeyspace                              | Mandatory |
| table                  | String     | mytable                                 | Mandatory |
| repairIntervalInMs     | long       | 432000000 (5 days)                      | Mandatory |
| repairParallelism      | String     | PARALLEL                                | Mandatory |
| repairUnwindRatio      | double     | 0.5 (50%)                               | Mandatory |
| repairWarningTimeInMs  | long       | 604800000 (7 days)                      | Mandatory |
| repairErrorTimeInMs    | long       | 864000000 (10 days)                     | Mandatory |
