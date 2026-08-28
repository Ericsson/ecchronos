# ecctool

ecctool is a command line utility used to perform operations toward an ecChronos instance. Run ‘ecctool <subcommand> –help’ to get more information about each subcommand.

```console
usage: ecctool [-h]
               {config,rejections,repair-info,repairs,run-repair,running-job,schedules,start,state,status,stop} ...
```


### -h, --help
show this help message and exit

## ecctool config

Show or update ecChronos configuration.

```console
usage: ecctool config [-h] [--session-window SESSION_WINDOW]
                      [--cooldown COOLDOWN]
                      [--locks-per-resource LOCKS_PER_RESOURCE] [-u URL]
```


### -h, --help
show this help message and exit


### --session-window <session_window>
session window duration (e.g. 5m, 30s, 300000)


### --cooldown <cooldown>
cooldown duration (e.g. 5m, 30s, 300000)


### --locks-per-resource <locks_per_resource>
locks per resource


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)

## ecctool rejections

Manage ecchronos rejections. Use ‘ecctool rejections <action> –help’ for action information.

```console
usage: ecctool rejections [-h] [-u URL] [-c COLUMNS] [-o OUTPUT]
                          {create,delete,get,update} ...
```


### -h, --help
show this help message and exit


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)


### -c <columns>, --columns <columns>
table columns to display (format: 0,1,2,…,N)


### -o <output>, --output <output>
output formats: json, table (default)

## ecctool rejections create

```console
usage: ecctool rejections create [-h] -k KEYSPACE -t TABLE -sh START_HOUR
                                 -sm START_MINUTE -eh END_HOUR -em END_MINUTE
                                 -dcs DC_EXCLUSIONS [DC_EXCLUSIONS ...]
                                 [-u URL]
```


### -h, --help
show this help message and exit


### -k <keyspace>, --keyspace <keyspace>
keyspace


### -t <table>, --table <table>
table


### -sh <start_hour>, --start-hour <start_hour>
start hour


### -sm <start_minute>, --start-minute <start_minute>
start minute


### -eh <end_hour>, --end-hour <end_hour>
end hour


### -em <end_minute>, --end-minute <end_minute>
end minute


### -dcs <dc_exclusions>, --dc-exclusions <dc_exclusions>
datacenters to exclude (format: <dc1> <dc2> … <dcN>)


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)

## ecctool rejections delete

```console
usage: ecctool rejections delete [-h] [-a ALL] [-k KEYSPACE] [-t TABLE]
                                 [-sh START_HOUR] [-sm START_MINUTE]
                                 [-dcs DC_EXCLUSIONS [DC_EXCLUSIONS ...]]
                                 [-u URL]
```


### -h, --help
show this help message and exit


### -a <all>, --all <all>
delete all


### -k <keyspace>, --keyspace <keyspace>
keyspace


### -t <table>, --table <table>
table


### -sh <start_hour>, --start-hour <start_hour>
start hour


### -sm <start_minute>, --start-minute <start_minute>
start minute


### -dcs <dc_exclusions>, --dc-exclusions <dc_exclusions>
datacenters to exclude (format: <dc1> <dc2> … <dcN>)


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)

## ecctool rejections get

```console
usage: ecctool rejections get [-h] [-k KEYSPACE] [-t TABLE] [-u URL]
```


### -h, --help
show this help message and exit


### -k <keyspace>, --keyspace <keyspace>
keyspace


### -t <table>, --table <table>
table


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)

## ecctool rejections update

```console
usage: ecctool rejections update [-h] -k KEYSPACE -t TABLE -sh START_HOUR
                                 -sm START_MINUTE
                                 [-dcs DC_EXCLUSIONS [DC_EXCLUSIONS ...]]
                                 [-u URL]
```


### -h, --help
show this help message and exit


### -k <keyspace>, --keyspace <keyspace>
keyspace


### -t <table>, --table <table>
table


### -sh <start_hour>, --start-hour <start_hour>
start hour


### -sm <start_minute>, --start-minute <start_minute>
start minute


### -dcs <dc_exclusions>, --dc-exclusions <dc_exclusions>
datacenters to exclude (format: <dc1> <dc2> … <dcN>)


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)

## ecctool repair-info

Get information about repairs for tables. The repair information is based on repair history, meaning both manual and scheduled repairs will be a part of the repair information. This subcommand requires the user to provide either –since or –duration if –keyspace and –table is not provided. If repair info is fetched for a specific table using –keyspace and –table, the duration will default to the table’s GC_GRACE_SECONDS.

```console
usage: ecctool repair-info [-h] [-c COLUMNS] [-n NODE] [-k KEYSPACE]
                           [-t TABLE] [-s SINCE] [-d DURATION] [-u URL]
                           [-l LIMIT] [-o OUTPUT]
```


### -h, --help
show this help message and exit


### -c <columns>, --columns <columns>
table columns to display (format: 0,1,2,…,N)


### -n <node>, --node <node>
only matching node id


### -k <keyspace>, --keyspace <keyspace>
keyspace


### -t <table>, --table <table>
table


### -s <since>, --since <since>
repair information from specified date (ISO8601 format) to now (required unless using –duration or –keyspace/–table)


### -d <duration>, --duration <duration>
repair information for specified duration (ISO8601 or simple format: 5s, 5m, 5h, 5d) from now-duration to now (required unless using –since or –keyspace/–table)


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)


### -l <limit>, --limit <limit>
limit output rows (use -1 for no limit)


### -o <output>, --output <output>
output formats: json, table (default)

## ecctool repairs

Show the status of all manual repairs.

```console
usage: ecctool repairs [-h] [-c COLUMNS] [-k KEYSPACE] [-t TABLE] [-u URL]
                       [-n NODE] [-i ID] [-l LIMIT] [-o OUTPUT]
```


### -h, --help
show this help message and exit


### -c <columns>, --columns <columns>
table columns to display (format: 0,1,2,…,N)


### -k <keyspace>, --keyspace <keyspace>
keyspace (mutually exclusive with -n/–node)


### -t <table>, --table <table>
table (requires -k/–keyspace and is mutually exclusive with -n/–node)


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)


### -n <node>, --node <node>
only matching node id (mutually exclusive with -k/–keyspace and -t/–table)


### -i <id>, --id <id>
only matching job id (mutually exclusive with -k/–keyspace and -t/–table)


### -l <limit>, --limit <limit>
limit output rows (use -1 for no limit)


### -o <output>, --output <output>
output formats: json, table (default)

## ecctool run-repair

Triggers a manual repair in ecChronos. This will be done through the Cassandra JMX interface.

```console
usage: ecctool run-repair [-h] [-c COLUMNS] [-n NODE] [-u URL] [-o OUTPUT]
                          [-r REPAIR_TYPE] [-f] [-e] [-a] [-k KEYSPACE]
                          [-t TABLE]
```


### -h, --help
show this help message and exit


### -c <columns>, --columns <columns>
table columns to display (format: 0,1,2,…,N)


### -n <node>, --node <node>
only matching node id (mutually exclusive with -k/–keyspace and -t/–table)


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)


### -o <output>, --output <output>
output formats: json, table (default)


### -r <repair_type>, --repair_type <repair_type>
type of repair (accepted values: vnode, parallel_vnode and incremental)


### -f, --forceRepairTWCS
force repair of TWCS tables


### -e, --forceRepairDisabled
force repair of disabled tables


### -a, --all
run repair for all nodes


### -k <keyspace>, --keyspace <keyspace>
keyspace (applies to all tables within the keyspace with a replication factor greater than 1)


### -t <table>, --table <table>
table (requires -k/–keyspace)

## ecctool running-job

Show which (if any) job is currently running.

```console
usage: ecctool running-job [-h] [-o OUTPUT] [-u URL]
```


### -h, --help
show this help message and exit


### -o <output>, --output <output>
output formats: json (defaults to no format)


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)

## ecctool schedules

Show the status of schedules.

```console
usage: ecctool schedules [-h] [-c COLUMNS] [-f] [-n NODE] [-i ID]
                         [-k KEYSPACE] [-l LIMIT] [-o OUTPUT] [-t TABLE]
                         [-u URL]
```


### -h, --help
show this help message and exit


### -c <columns>, --columns <columns>
table columns to display (format: 0,1,2,…,N)


### -f, --full
show full schedules with configuration and vnode state (requires -n/–node)


### -n <node>, --node <node>
only matching node id (mutually exclusive with -k/–keyspace and -t/–table)


### -i <id>, --id <id>
only matching job id (mutually exclusive with -k/–keyspace and -t/–table)


### -k <keyspace>, --keyspace <keyspace>
keyspace (mutually exclusive with -n/–node)


### -l <limit>, --limit <limit>
limit output rows (use -1 for no limit)


### -o <output>, --output <output>
output formats: json, table (default)


### -t <table>, --table <table>
table (requires -k/–keyspace and is mutually exclusive with -n/–node)


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)

## ecctool start

Start the ecChronos service.

```console
usage: ecctool start [-h] [-f] [-o OUTPUT] [-p PIDFILE]
```


### -h, --help
show this help message and exit


### -f, --foreground
run in foreground (executes in current terminal and logs to stdout)


### -o <output>, --output <output>
output formats: json (defaults to no format)


### -p <pidfile>, --pidfile <pidfile>
file for storing process id

## ecctool state

Get information of ecChronos internal state.

```console
usage: ecctool state [-h] [-c COLUMNS] [-o OUTPUT] [-u URL] {nodes} ...
```


### -h, --help
show this help message and exit


### -c <columns>, --columns <columns>
table columns to display (format: 0,1,2,…,N)


### -o <output>, --output <output>
output formats: json (defaults to no format)


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)

## ecctool state nodes

```console
usage: ecctool state nodes [-h] [-u URL]
```


### -h, --help
show this help message and exit


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)

## ecctool status

View cluster-wide status of nodes registered in nodes_sync across all ecChronos instances.

```console
usage: ecctool status [-h] [-c COLUMNS] [--local] [-u URL] [-o OUTPUT]
```


### -h, --help
show this help message and exit


### -c <columns>, --columns <columns>
table columns to display (format: 0,1,2,…,N)


### --local
check whether the local ecChronos instance is running (legacy status check)


### -u <url>, --url <url>
ecchronos host URL (format: [http:/](http:/)/<host>:<port>)


### -o <output>, --output <output>
output formats: json, table (default)

## ecctool stop

Stop the ecChronos service (sends SIGTERM to the process).

```console
usage: ecctool stop [-h] [-o OUTPUT] [-p PIDFILE]
```


### -h, --help
show this help message and exit


### -o <output>, --output <output>
output formats: json (defaults to no format)


### -p <pidfile>, --pidfile <pidfile>
file containing process id

# Examples

For example usage and explanation about output refer to [ECCTOOL_EXAMPLES.md](../ECCTOOL_EXAMPLES.md)
