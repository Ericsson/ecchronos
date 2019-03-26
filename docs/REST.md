# REST interfaces

## Repair scheduler

The REST interface for the repair scheduler is located under the path `<host>/repair-scheduler/v1/`.
The following sub-paths exists:
* `/get/<keyspace>/<table>`
* `/list[/<keyspace>]`

The interface is only exposing state for scheduled tables.


### Get

When performing GET on `<host>/repair-scheduler/v1/get/mykeyspace/mytable` a JSON object of the RepairJob type will be returned.

The vnodeStates field will be set when `get` is used.


### List

When performing GET on `<host>/repair-scheduler/v1/list` a JSON list of RepairJobs for all keyspaces will be returned.
When performing GET on `<host>/repair-scheduler/v1/list/mykeyspace` a JSON list of RepairJobs for that specific keyspace will be returned.

The virtualNodeStates field will not be set when `list` is used.


### Types

RepairJob:

| Key                    | Type                   | Example value                           | Optional  |
|------------------------|------------------------|-----------------------------------------|-----------|
| keyspace               | String                 | mykeyspace                              | Mandatory |
| table                  | String                 | mytable                                 | Mandatory |
| repairIntervalInMs     | long                   | 604800000 (7 days)                      | Mandatory |
| lastRepairedAtInMs     | millis since epoch     | 1553099547852 (2019-03-16T16:32:27.852) | Mandatory |
| repairedRatio          | double                 | 1.0 (100%)                              | Mandatory |
| virtualNodeStates      | list(VirtualNodeState) | VirtualNode state example below         | Optional  |

VirtualNodeState:

| Key                | Type               | Example value                    |
|--------------------|--------------------|----------------------------------|
| startToken         | long               | -1                               |
| endToken           | long               | 1                                |
| replicas           | list(inet address) | [127.0.0.1, 127.0.0.2]           |
| lastRepairedAtInMs | unix timestamp     | 1552468659 (2019-03-13T09:17:39) |
| repaired           | boolean            | true                             |
