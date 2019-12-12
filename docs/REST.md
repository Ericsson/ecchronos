# REST interfaces

## Repair scheduler

The REST interface for the repair scheduler is located under the path `<host>/repair-scheduler/v1/`.
The following sub-paths exists:
* `/get/<keyspace>/<table>`
* `/list[/<keyspace>]`
* `/config`

The interface is only exposing state and configuration for scheduled tables.


### Show specific table repair status

When performing GET on `<host>/repair-scheduler/v1/get/mykeyspace/mytable` a JSON object of the [RepairJob](../ecchronos-binary/src/test/features/repair_job.json) type will be returned.

*Note: The field virtualNodeStates will only be used when showing a specific table.
When listing table repair jobs the field will not be used.*


### List table repair jobs

When performing GET on `<host>/repair-scheduler/v1/list` a [JSON list of RepairJobs](../ecchronos-binary/src/test/features/repair_job_list.json) for all keyspaces will be returned.
When performing GET on `<host>/repair-scheduler/v1/list/mykeyspace` a JSON list of RepairJobs for that specific keyspace will be returned.


### List repair config

When performing GET on `<host>/repair-scheduler/v1/config` a [JSON list of RepairConfig](../ecchronos-binary/src/test/features/repair_config.json) for all tables will be returned.


### Types

RepairJob:

| Key                    | Type                   | Example value                           | Optional  |
|------------------------|------------------------|-----------------------------------------|-----------|
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

| Key                    | Type       | Example value       | Optional  |
|------------------------|------------|---------------------|-----------|
| keyspace               | String     | mykeyspace          | Mandatory |
| table                  | String     | mytable             | Mandatory |
| repairIntervalInMs     | long       | 432000000 (5 days)  | Mandatory |
| repairParallelism      | String     | PARALLEL            | Mandatory |
| repairUnwindRatio      | double     | 0.5 (50%)           | Mandatory |
| repairWarningTimeInMs  | long       | 604800000 (7 days)  | Mandatory |
| repairErrorTimeInMs    | long       | 864000000 (10 days) | Mandatory |
