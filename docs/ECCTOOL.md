# ECChronos tool

`ecctool` is a command line utility which can be used to perform actions towards a local ecChronos instance.
The actions are implemented in form of subcommands with arguments.

All visualization is displayed in form of human-readable tables.
For more information refer to sections below.

## Subcommands

The following subcommands are available:

| Command               | Description                              |
|-----------------------|------------------------------------------|
| `ecctool repairs`     | Repair status overview                   |
| `ecctool schedules`   | Status of schedules                      |
| `ecctool run-repair`  | Trigger a single repair                  |
| `ecctool repair-info` | Show information about repairs per table |
| `ecctool start`       | Start ecChronos service                  |
| `ecctool stop`        | Stop ecChronos service                   |
| `ecctool status`      | Show status of ecChronos service         |

For more information about each subcommand refer to the specific sections below.

### repairs

`ecctool repairs` subcommand is used to show the status of all manual repairs.
This subcommand has no mandatory parameters.

#### Example

In this example, we will use `ecctool repairs` to check the status of manual repairs.
The output shows all manual repairs for all ecChronos instances.

```bash
---------------------------------------------------------------------------------------------------------------------------------------------------
| Id                                   | Host Id                              | Keyspace | Table  | Status    | Repaired(%) | Completed at        |
---------------------------------------------------------------------------------------------------------------------------------------------------
| f4fb2b38-b9d0-4390-97ca-eeb284391f80 | ee32d9c7-1a4e-40c2-9b28-1000544011ae | test     | table1 | IN_QUEUE  | 0.00        | -                   |
| f4fb2b38-b9d0-4390-97ca-eeb284391f80 | ba7665b2-5a7b-42b5-9f38-037f2da1e80a | test     | table1 | COMPLETED | 100.00      | 2022-09-22 13:40:07 |
---------------------------------------------------------------------------------------------------------------------------------------------------
Summary: 1 completed, 1 in queue, 0 blocked, 0 warning, 0 error
```

Looking at the example output above, the columns are:

`Id` - the manual repair ID, manual repair triggered on several hosts will have the same ID.

`Host Id` - the host id of the Cassandra instance ecChronos responsible for performing manual repair is connected to.

`Keyspace` - the keyspace the manual repair is run on.

`Table` - the table the manual repair is run on.

`Status` - the status of the manual repair.
The possible statuses are:
* `IN_QUEUE` - the manual repair is awaiting execution or is currently running
* `ERROR` - the manual repair failed, some ranges might've failed or the ranges have changed.
* `COMPLETED` - the manual repair is completed, all ranges have been repaired.

`Repaired(%)` - the number of ranges repaired vs total ranges.
For manual repairs this value should never go down.

`Completed at` - the time when the manual repair has finished.

#### Arguments

| Short-form | Long-form    | Default value           | Description                                                                                                                                                                      | 
|------------|--------------|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-h`       | `--help`     |                         | Shows the help message and exits.                                                                                                                                                |
| `-k`       | `--keyspace` |                         | Show repairs for the specified keyspace. This argument is mutually exclusive with `-i` and `--id`.                                                                               |
| `-t`       | `--table`    |                         | Show repairs for the specified table. Keyspace argument `-k` or `--keyspace` becomes mandatory if using this argument. This argument is mutually exclusive with `-i` and `--id`. |
| `-u`       | `--url`      | `http://localhost:8080` | The ecChronos host to connect to, specified in the format `http://<host>:<port>`.                                                                                                |
| `-i`       | `--id`       |                         | Show repairs matching the specified ID. This argument is mutually exclusive with `-k`, `--keyspace`, `-t` and `--table`.                                                         |
| `-l`       | `--limit`    | -1                      | Limits the number of tables printed in the output. Specified as a number, -1 to disable limit.                                                                                   |
|            | `--hostid`   |                         | Show repairs for the specified host id. The host id corresponds to the Cassandra instance ecChronos is connected to.                                                             |

### schedules

`ecctool schedules` subcommand is used to show the status of schedules.
This subcommand has no mandatory parameters.

#### Example

In this example we will use `ecctool schedules` to check the status of schedules.
The output shows all schedules the local ecChronos instance has.

```bash
Snapshot as of 2022-09-22 14:05:12
-----------------------------------------------------------------------------------------------------------------------------------------------------------
| Id                                   | Keyspace              | Table              | Status    | Repaired(%) | Completed at        | Next repair         |
-----------------------------------------------------------------------------------------------------------------------------------------------------------
| f7bf2960-3a6d-11ed-b3c3-4d376b8456cd | test                  | table1             | COMPLETED | 100.00      | 2022-09-16 14:05:10 | 2022-09-23 14:05:10 |
| f8ec37b0-3a6d-11ed-b3c3-4d376b8456cd | test                  | table2             | COMPLETED | 100.00      | 2022-09-16 14:05:10 | 2022-09-23 14:05:10 |
| fbe5efb0-3a6d-11ed-b3c3-4d376b8456cd | keyspaceWithCamelCase | tableWithCamelCase | COMPLETED | 100.00      | 2022-09-16 14:05:10 | 2022-09-23 14:05:10 |
| fa060c20-3a6d-11ed-b3c3-4d376b8456cd | test2                 | table1             | COMPLETED | 100.00      | 2022-09-16 14:05:10 | 2022-09-23 14:05:10 |
| fab2edf0-3a6d-11ed-b3c3-4d376b8456cd | test2                 | table2             | COMPLETED | 100.00      | 2022-09-16 14:05:10 | 2022-09-23 14:05:10 |
-----------------------------------------------------------------------------------------------------------------------------------------------------------
Summary: 5 completed, 0 on time, 0 blocked, 0 late, 0 overdue
```

Looking at the example output above, the first line is `Snapshot as of 2022-09-22 14:05:12`.
This means that the output is tied to a point in time,
the output might change if the subcommand is run at a different time.

Looking at the table, the columns are:

`Id` - the schedule ID, this corresponds to the table id.

`Keyspace` - the keyspace the repair is run on.

`Table` - the table the repair is run on.

`Status` - the status of the repair.
The possible statuses are:
* `ON_TIME` - the schedule is awaiting execution or is currently running
* `LATE` - the schedule is late, warning time specified in the configuration has passed.
* `OVERDUE` - the schedule is overdue, error time specified in the configuration has passed.
* `COMPLETED` - the schedule is completed, all ranges have been repaired within the interval.

`Repaired(%)` - the number of ranges repaired within the interval vs total ranges.
For schedules this value can go up and down as ranges become unrepaired.

`Completed at` - the time when the all ranges for the schedule are repaired.
ecChronos assumes all ranges are repaired if there's no repair history.

`Next repair` - the time when the schedule will be made ready for execution.
This is based on the (oldest range repair time + interval) - repair time taken for the ranges.
This is updated each time a repair group is completed.

#### Arguments

| Short-form | Long-form    | Default value           | Description                                                                                                                                                                        |
|------------|--------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-h`       | `--help`     |                         | Shows the help message and exits.                                                                                                                                                  |
| `-k`       | `--keyspace` |                         | Show schedules for the specified keyspace. This argument is mutually exclusive with `-i` and `--id`.                                                                               |
| `-t`       | `--table`    |                         | Show schedules for the specified table. Keyspace argument `-k` or `--keyspace` becomes mandatory if using this argument. This argument is mutually exclusive with `-i` and `--id`. |
| `-u`       | `--url`      | `http://localhost:8080` | The ecChronos host to connect to, specified in the format `http://<host>:<port>`.                                                                                                  |
| `-i`       | `--id`       |                         | Show schedules matching the specified ID. This argument is mutually exclusive with `-k`, `--keyspace`, `-t` and `--table`.                                                         |
| `-f`       | `--full`     | False                   | Show full schedules, can only be used with `-i` or `--id`. Full schedules include schedule configuration and repair state per vnode.                                               |
| `-l`       | `--limit`    | -1                      | Limits the number of tables printed in the output. Specified as a number, -1 to disable limit.                                                                                     |

### run-repair

`ecctool run-repair` subcommand is used to run a manual repair.
The manual repair will be triggered in ecChronos, which will group sub-ranges by nodes that have them in common.
Afterwards, each sub-range will be repaired once through Cassandra JMX interface.
This subcommand has no mandatory parameters.

#### Example

In this example we will use `ecctool run-repair` to run a manual repair for all ecChronos instances,
for all keyspaces and tables.
The output shows created manual repairs for all ecChronos instances.

```bash
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
| Id                                   | Host Id                              | Keyspace              | Table              | Status   | Repaired(%) | Completed at |
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
| 497eb4cf-9275-4216-9cca-12958bde28af | 6424a5fa-69ea-49a3-a542-4751d0283c9a | test2                 | table2             | IN_QUEUE | 0.00        | -            |
| 497eb4cf-9275-4216-9cca-12958bde28af | 045c01c1-ff50-4de2-8da3-1b9270c382b5 | test2                 | table2             | IN_QUEUE | 0.00        | -            |
| ea32c8b5-3e8a-466a-b5f5-f9248e73774c | 6424a5fa-69ea-49a3-a542-4751d0283c9a | test                  | table2             | IN_QUEUE | 0.00        | -            |
| ea32c8b5-3e8a-466a-b5f5-f9248e73774c | 045c01c1-ff50-4de2-8da3-1b9270c382b5 | test                  | table2             | IN_QUEUE | 0.00        | -            |
| 0d8845fd-84dc-435c-8b8b-83b701dd2cbd | 045c01c1-ff50-4de2-8da3-1b9270c382b5 | keyspaceWithCamelCase | tableWithCamelCase | IN_QUEUE | 0.00        | -            |
| 0d8845fd-84dc-435c-8b8b-83b701dd2cbd | 6424a5fa-69ea-49a3-a542-4751d0283c9a | keyspaceWithCamelCase | tableWithCamelCase | IN_QUEUE | 0.00        | -            |
| c5f830b0-533a-464f-80ed-aa8b90248ba3 | 6424a5fa-69ea-49a3-a542-4751d0283c9a | test2                 | table1             | IN_QUEUE | 0.00        | -            |
| c5f830b0-533a-464f-80ed-aa8b90248ba3 | 045c01c1-ff50-4de2-8da3-1b9270c382b5 | test2                 | table1             | IN_QUEUE | 0.00        | -            |
| 73c27554-58a5-47b9-a2ab-01b9fcfad4f0 | 6424a5fa-69ea-49a3-a542-4751d0283c9a | test                  | table1             | IN_QUEUE | 0.00        | -            |
| 73c27554-58a5-47b9-a2ab-01b9fcfad4f0 | 045c01c1-ff50-4de2-8da3-1b9270c382b5 | test                  | table1             | IN_QUEUE | 0.00        | -            |
--------------------------------------------------------------------------------------------------------------------------------------------------------------------
Summary: 0 completed, 10 in queue, 0 blocked, 0 warning, 0 error
```

Looking at the example output above, the columns are:

`Id` - the manual repair ID, manual repair triggered on several hosts will have the same ID.

`Host Id` - the host id of the Cassandra instance ecChronos responsible for performing manual repair is connected to.

`Keyspace` - the keyspace the manual repair is run on.

`Table` - the table the manual repair is run on.

`Status` - the status of the manual repair. This will always be `IN_QUEUE` for newly run manual repairs.

`Repaired(%)` - the number of ranges repaired vs total ranges.
For manual repairs this value should never go down.
This will always be `0` for newly run manual repairs.

`Completed at` - the time when the manual repair has finished. This will always be `-` for newly run manual repairs.

After running this subcommand, to check the progress of running manual repairs use `ecctool repairs`.

#### Arguments

| Short-form | Long-form    | Default value           | Description                                                                                                                         |
|------------|--------------|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `-h`       | `--help`     |                         | Shows the help message and exits.                                                                                                   |
| `-k`       | `--keyspace` |                         | Run repair for the specified keyspace. Repair will be run for all tables within the keyspace with replication factor higher than 1. |
| `-t`       | `--table`    |                         | Run repair for the specified table. Keyspace argument `-k` or `--keyspace` becomes mandatory if using this argument.                |
| `-u`       | `--url`      | `http://localhost:8080` | The ecChronos host to connect to, specified in the format `http://<host>:<port>`.                                                   |
|            | `--local`    |                         | Run repair for the local node only, i.e repair will only be performed for the ranges that the local node is a replica for.          |

### repair-info

`ecctool repair-info` subcommand is used to get information about repairs for tables.
The repair information is based on repair history,
meaning that both manual repairs and schedules will contribute to the repair information.
This subcommand requires the user to provide either `--since` or `--duration`.

#### Example

In this example we will use `ecctool repair-info --duration 5m` to check how much each table is repaired.
The output shows the cluster wide repair information for all tables in the past 5 minutes.

```bash
Time window between '2022-09-23 13:12:54' and '2022-09-23 13:17:54'
---------------------------------------------------------------------------------
| Keyspace              | Table              | Repaired (%) | Repair time taken |
---------------------------------------------------------------------------------
| keyspaceWithCamelCase | tableWithCamelCase | 73.73        | 3 seconds         |
| test                  | table1             | 73.73        | 3 seconds         |
| test                  | table2             | 73.73        | 4 seconds         |
| test2                 | table1             | 73.73        | 3 seconds         |
| test2                 | table2             | 73.73        | 4 seconds         |
---------------------------------------------------------------------------------
```

Looking at the example output above, the columns are:

`Keyspace` - the keyspace the repair information corresponds to.

`Table` - the table the repair information corresponds to.

`Repaired (%)` - the repaired ranges vs total ranges of the table in %.

`Repair time taken` - the time taken for the Cassandra to finish the repairs.

By default, repair-info fetches the information on a cluster level.
To check the repair information for the local node use `--local` flag.

#### Arguments

| Short-form | Long-form    | Default value           | Description                                                                                                                                                                                                                                   |
|------------|--------------|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-h`       | `--help`     |                         | Shows the help message and exits.                                                                                                                                                                                                             |
| `-k`       | `--keyspace` |                         | Show repair information for all tables in the specified keyspace.                                                                                                                                                                             |
| `-t`       | `--table`    |                         | Show repair information for the specified table. Keyspace argument `-k` or `--keyspace` becomes mandatory if using this argument.                                                                                                             |
| `-s`       | `--since`    |                         | Show repair information since the specified date to now. Date must be specified in ISO8601 format. The time-window will be `since` to `now`. Mandatory if `-d` and `--duration` is not specified.                                             |
| `-d`       | `--duration` |                         | Show repair information for the duration. Duration can be specified as ISO8601 format or as simple format in form: `5s`, `5m`, `5h`, `5d`. The time-window will be `now-duration` to `now`. Mandatory if `-s` and `--since` is not specified. |
|            | `--local`    | False                   | Show repair information only for the local node.                                                                                                                                                                                              |
| `-u`       | `--url`      | `http://localhost:8080` | The ecChronos host to connect to, specified in the format `http://<host>:<port>`.                                                                                                                                                             |
| `-l`       | `--limit`    | -1                      | Limits the number of tables printed in the output. Specified as a number, -1 to disable limit.                                                                                                                                                |

### start

`ecctool start` subcommand is used to start the ecChronos instance.
This subcommand has no mandatory parameters.

#### Arguments

| Short-form | Long-form      | Default value           | Description                                                                                  |
|------------|----------------|-------------------------|----------------------------------------------------------------------------------------------|
| `-h`       | `--help`       |                         | Shows the help message and exits.                                                            |
| `-f`       | `--foreground` | False                   | Start the ecChronos instance in foreground mode (exec in current terminal and log to stdout) |
| `-p`       | `--pidfile`    | $ECCHRONOS_HOME/ecc.pid | Start the ecChronos instance and store the pid in the specified pid file.                    |

### stop

`ecctool stop` subcommand is used to stop the ecChronos instance.
Stopping of ecChronos is done by using `kill` with `SIGTERM` signal (same as kill in shell) for the pid.
This subcommand has no mandatory parameters.

#### Arguments

| Short-form | Long-form   | Default value           | Description                                                              |
|------------|-------------|-------------------------|--------------------------------------------------------------------------|
| `-h`       | `--help`    |                         | Shows the help message and exits.                                        |
| `-p`       | `--pidfile` | $ECCHRONOS_HOME/ecc.pid | Stops the ecChronos instance by pid fetched from the specified pid file. |

### status

`ecctool status` subcommand is used to view status of ecChronos instance.
This subcommand has no mandatory parameters.

#### Optional arguments

| Short-form | Long-form | Default value           | Description                                                  |
|------------|-----------|-------------------------|--------------------------------------------------------------|
| `-h`       | `--help`  |                         | Shows the help message and exits.                            |
| `-u`       | `--url`   | `http://localhost:8080` | Check the status of ecChronos running on the specified host. |
