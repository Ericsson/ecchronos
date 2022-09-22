# ECChronos tool

Standalone ecChronos includes a commandline utility (ecctool).

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

#### Example output

```bash
TODO
```

In the example output above ...TODO

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

#### Example output

```bash
TODO
```

In the example output above ...TODO

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
This subcommand has no mandatory parameters.

#### Example output

```bash
TODO
```

In the example output above ...TODO

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
This subcommand requires the user to provide either `--since` or `--duration`.

#### Example output

```bash
TODO
```

In the example output above ...TODO

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

#### Example output

```bash
TODO
```

In the example output above ...TODO

#### Arguments

| Short-form | Long-form      | Default value           | Description                                                                                  |
|------------|----------------|-------------------------|----------------------------------------------------------------------------------------------|
| `-h`       | `--help`       |                         | Shows the help message and exits.                                                            |
| `-f`       | `--foreground` | False                   | Start the ecChronos instance in foreground mode (exec in current terminal and log to stdout) |
| `-p`       | `--pidfile`    | $ECCHRONOS_HOME/ecc.pid | Start the ecChronos instance and store the pid in the specified pid file.                    |

### stop

`ecctool stop` subcommand is used to stop the ecChronos instance.
This subcommand has no mandatory parameters.

#### Example output

```bash
TODO
```

In the example output above ...TODO

#### Arguments

| Short-form | Long-form   | Default value           | Description                                                              |
|------------|-------------|-------------------------|--------------------------------------------------------------------------|
| `-h`       | `--help`    |                         | Shows the help message and exits.                                        |
| `-p`       | `--pidfile` | $ECCHRONOS_HOME/ecc.pid | Stops the ecChronos instance by pid fetched from the specified pid file. |

### status

`ecctool status` subcommand is used to view status of ecChronos instance.
This subcommand has no mandatory parameters.

#### Example output

```bash
TODO
```

In the example output above ...TODO

#### Optional arguments

| Short-form | Long-form | Default value           | Description                                                  |
|------------|-----------|-------------------------|--------------------------------------------------------------|
| `-h`       | `--help`  |                         | Shows the help message and exits.                            |
| `-u`       | `--url`   | `http://localhost:8080` | Check the status of ecChronos running on the specified host. |
