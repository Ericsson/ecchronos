# ecctool

ecctool is a command line utility used to perform operations toward an ecChronos instance. Run ‘ecctool &lt;subcommand&gt; –help’ to get more information about each subcommand.

```console
usage: ecctool [-h]
               {config,metrics,rejections,repair-info,repair-sessions,repairs,run-repair,running-job,schedules,start,state,status,stop,version}
               ...
```


### -h, --help
show this help message and exit

## ecctool config

Show or update ecChronos configuration.

```console
usage: ecctool config [-h] [--session-window SESSION_WINDOW]
                      [--cooldown COOLDOWN]
                      [--locks-per-resource LOCKS_PER_RESOURCE]
                      [--max-concurrency MAX_CONCURRENCY]
                      [--max-wait-time MAX_WAIT_TIME]
                      [--hung-repair-recovery {on,off}]
                      [--hung-repair-threshold HUNG_REPAIR_THRESHOLD]
                      [--hung-repair-bypass-coordinator {on,off}]
                      [--hung-repair-force {on,off}] [-u URL] [-T TIMEOUT]
```


### -h, --help
show this help message and exit


### --session-window &lt;session_window&gt;
session window duration (e.g. 5m, 30s, 300000)


### --cooldown &lt;cooldown&gt;
cooldown duration (e.g. 5m, 30s, 300000)


### --locks-per-resource &lt;locks_per_resource&gt;
locks per resource


### --max-concurrency &lt;max_concurrency&gt;
max concurrent scheduler threads (0 = unbounded, one thread per managed node)


### --max-wait-time &lt;max_wait_time&gt;
max wait time in minutes for a repair (&gt; 0)


### --hung-repair-recovery {on,off}
enable or disable automatic recovery of hung incremental repair sessions


### --hung-repair-threshold &lt;hung_repair_threshold&gt;
stall threshold before a hung repair session is cancelled (e.g. 30m, 1800s, 1800000)


### --hung-repair-bypass-coordinator {on,off}
bypass the coordinator check and send the fail command to all managed nodes


### --hung-repair-force {on,off}
value of the force flag passed to failSession, used for all requests


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)

## ecctool metrics

Fetch the agent metrics exposition text (native Prometheus/OpenMetrics passthrough).

```console
usage: ecctool metrics [-h] [--name SUBSTR]
                       [--format {prometheus,openmetrics}] [--raw] [-u URL]
                       [-T TIMEOUT]
```


### -h, --help
show this help message and exit


### --name &lt;substr&gt;
only show metrics whose name contains the given substring (repeatable, case-insensitive, ‘.’ and ‘_’ are equivalent)


### --format {prometheus,openmetrics}
exposition format to request (default: prometheus)


### --raw
suppress ‘# HELP’ and ‘# TYPE’ comment lines, showing only sample lines


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)

## ecctool rejections

Manage ecchronos rejections. Use ‘ecctool rejections &lt;action&gt; –help’ for action information.

```console
usage: ecctool rejections [-h] [-u URL] [-T TIMEOUT] [-c COLUMNS] [-o OUTPUT]
                          {create,delete,get,update} ...
```


### -h, --help
show this help message and exit


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json, table (default)

## ecctool rejections create

```console
usage: ecctool rejections create [-h] -k KEYSPACE -t TABLE -sh START_HOUR -sm
                                 START_MINUTE -eh END_HOUR -em END_MINUTE -dcs
                                 DC_EXCLUSIONS [DC_EXCLUSIONS ...] [-u URL]
                                 [-T TIMEOUT]
```


### -h, --help
show this help message and exit


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
keyspace


### -t &lt;table&gt;, --table &lt;table&gt;
table


### -sh &lt;start_hour&gt;, --start-hour &lt;start_hour&gt;
start hour


### -sm &lt;start_minute&gt;, --start-minute &lt;start_minute&gt;
start minute


### -eh &lt;end_hour&gt;, --end-hour &lt;end_hour&gt;
end hour


### -em &lt;end_minute&gt;, --end-minute &lt;end_minute&gt;
end minute


### -dcs &lt;dc_exclusions&gt;, --dc-exclusions &lt;dc_exclusions&gt;
datacenters to exclude (format: &lt;dc1&gt; &lt;dc2&gt; … &lt;dcN&gt;)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)

## ecctool rejections delete

```console
usage: ecctool rejections delete [-h] [-a ALL] [-k KEYSPACE] [-t TABLE]
                                 [-sh START_HOUR] [-sm START_MINUTE]
                                 [-dcs DC_EXCLUSIONS [DC_EXCLUSIONS ...]]
                                 [-u URL] [-T TIMEOUT]
```


### -h, --help
show this help message and exit


### -a &lt;all&gt;, --all &lt;all&gt;
delete all


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
keyspace


### -t &lt;table&gt;, --table &lt;table&gt;
table


### -sh &lt;start_hour&gt;, --start-hour &lt;start_hour&gt;
start hour


### -sm &lt;start_minute&gt;, --start-minute &lt;start_minute&gt;
start minute


### -dcs &lt;dc_exclusions&gt;, --dc-exclusions &lt;dc_exclusions&gt;
datacenters to exclude (format: &lt;dc1&gt; &lt;dc2&gt; … &lt;dcN&gt;)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)

## ecctool rejections get

```console
usage: ecctool rejections get [-h] [-k KEYSPACE] [-t TABLE] [-u URL]
                              [-T TIMEOUT]
```


### -h, --help
show this help message and exit


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
keyspace


### -t &lt;table&gt;, --table &lt;table&gt;
table


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)

## ecctool rejections update

```console
usage: ecctool rejections update [-h] -k KEYSPACE -t TABLE -sh START_HOUR -sm
                                 START_MINUTE
                                 [-dcs DC_EXCLUSIONS [DC_EXCLUSIONS ...]]
                                 [-u URL] [-T TIMEOUT]
```


### -h, --help
show this help message and exit


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
keyspace


### -t &lt;table&gt;, --table &lt;table&gt;
table


### -sh &lt;start_hour&gt;, --start-hour &lt;start_hour&gt;
start hour


### -sm &lt;start_minute&gt;, --start-minute &lt;start_minute&gt;
start minute


### -dcs &lt;dc_exclusions&gt;, --dc-exclusions &lt;dc_exclusions&gt;
datacenters to exclude (format: &lt;dc1&gt; &lt;dc2&gt; … &lt;dcN&gt;)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)

## ecctool repair-info

Get information about repairs for tables. The repair information is based on repair history, meaning both manual and scheduled repairs will be a part of the repair information. This subcommand requires the user to provide either –since or –duration if –keyspace and –table is not provided. If repair info is fetched for a specific table using –keyspace and –table, the duration will default to the table’s GC_GRACE_SECONDS.

```console
usage: ecctool repair-info [-h] [-c COLUMNS] [-n NODE] [-k KEYSPACE]
                           [-t TABLE] [-s SINCE] [-d DURATION] [-u URL]
                           [-T TIMEOUT] [-l LIMIT] [-o OUTPUT]
```


### -h, --help
show this help message and exit


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -n &lt;node&gt;, --node &lt;node&gt;
only matching node id


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
keyspace


### -t &lt;table&gt;, --table &lt;table&gt;
table


### -s &lt;since&gt;, --since &lt;since&gt;
repair information from specified date (ISO8601 format) to now (required unless using –duration or –keyspace/–table)


### -d &lt;duration&gt;, --duration &lt;duration&gt;
repair information for specified duration (ISO8601 or simple format: 5s, 5m, 5h, 5d) from now-duration to now (required unless using –since or –keyspace/–table)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)


### -l &lt;limit&gt;, --limit &lt;limit&gt;
limit output rows (use -1 for no limit)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json, table (default)

## ecctool repair-sessions

List and fail incremental repair sessions. Use ‘ecctool repair-sessions &lt;action&gt; –help’ for action information.

```console
usage: ecctool repair-sessions [-h] [-u URL] [-T TIMEOUT] [-c COLUMNS]
                               [-o OUTPUT]
                               {list,fail} ...
```


### -h, --help
show this help message and exit


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json, table (default)

## ecctool repair-sessions fail

```console
usage: ecctool repair-sessions fail [-h] -s SESSION [-f] [-n NODE] [-y]
                                    [-u URL] [-T TIMEOUT] [-o OUTPUT]
```


### -h, --help
show this help message and exit


### -s &lt;session&gt;, --session &lt;session&gt;
repair session id


### -f, --force
force-fail the session on all managed nodes that report it, not only the coordinator


### -n &lt;node&gt;, --node &lt;node&gt;
only matching node id


### -y, --yes
skip the confirmation prompt


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json, table (default)

## ecctool repair-sessions list

```console
usage: ecctool repair-sessions list [-h] [-n NODE] [-u URL] [-T TIMEOUT]
                                    [-c COLUMNS] [-o OUTPUT]
```


### -h, --help
show this help message and exit


### -n &lt;node&gt;, --node &lt;node&gt;
only matching node id


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json, table (default)

## ecctool repairs

Show the status of all manual repairs.

```console
usage: ecctool repairs [-h] [-c COLUMNS] [-k KEYSPACE] [-t TABLE] [-u URL]
                       [-T TIMEOUT] [-n NODE] [-i ID] [-l LIMIT] [-o OUTPUT]
```


### -h, --help
show this help message and exit


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
keyspace (mutually exclusive with -n/–node)


### -t &lt;table&gt;, --table &lt;table&gt;
table (requires -k/–keyspace and is mutually exclusive with -n/–node)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)


### -n &lt;node&gt;, --node &lt;node&gt;
only matching node id (mutually exclusive with -k/–keyspace and -t/–table)


### -i &lt;id&gt;, --id &lt;id&gt;
only matching job id (mutually exclusive with -k/–keyspace and -t/–table)


### -l &lt;limit&gt;, --limit &lt;limit&gt;
limit output rows (use -1 for no limit)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json, table (default)

## ecctool run-repair

Triggers a manual repair in ecChronos. This will be done through the Cassandra JMX interface.

```console
usage: ecctool run-repair [-h] [-c COLUMNS] [-n NODE] [-u URL] [-T TIMEOUT]
                          [-o OUTPUT] [-r REPAIR_TYPE] [-f] [-e] [-a]
                          [-k KEYSPACE] [-t TABLE]
```


### -h, --help
show this help message and exit


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -n &lt;node&gt;, --node &lt;node&gt;
only matching node id (mutually exclusive with -k/–keyspace and -t/–table)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json, table (default)


### -r &lt;repair_type&gt;, --repair_type &lt;repair_type&gt;
type of repair (accepted values: vnode, parallel_vnode and incremental)


### -f, --forceRepairTWCS
force repair of TWCS tables


### -e, --forceRepairDisabled
force repair of disabled tables


### -a, --all
run repair for all nodes


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
keyspace (applies to all tables within the keyspace with a replication factor greater than 1)


### -t &lt;table&gt;, --table &lt;table&gt;
table (requires -k/–keyspace)

## ecctool running-job

Show which (if any) job is currently running.

```console
usage: ecctool running-job [-h] [-o OUTPUT] [-u URL] [-T TIMEOUT]
```


### -h, --help
show this help message and exit


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json (defaults to no format)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)

## ecctool schedules

Show the status of schedules.

```console
usage: ecctool schedules [-h] [-c COLUMNS] [-f] [-n NODE] [-i ID]
                         [-k KEYSPACE] [-l LIMIT] [-o OUTPUT] [-t TABLE]
                         [-u URL] [-T TIMEOUT]
```


### -h, --help
show this help message and exit


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -f, --full
show full schedules with configuration and vnode state (requires -n/–node)


### -n &lt;node&gt;, --node &lt;node&gt;
only matching node id (mutually exclusive with -k/–keyspace and -t/–table)


### -i &lt;id&gt;, --id &lt;id&gt;
only matching job id (mutually exclusive with -k/–keyspace and -t/–table)


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
keyspace (mutually exclusive with -n/–node)


### -l &lt;limit&gt;, --limit &lt;limit&gt;
limit output rows (use -1 for no limit)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json, table (default)


### -t &lt;table&gt;, --table &lt;table&gt;
table (requires -k/–keyspace and is mutually exclusive with -n/–node)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)

## ecctool start

Start the ecChronos service.

```console
usage: ecctool start [-h] [-f] [-o OUTPUT] [-p PIDFILE]
```


### -h, --help
show this help message and exit


### -f, --foreground
run in foreground (executes in current terminal and logs to stdout)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json (defaults to no format)


### -p &lt;pidfile&gt;, --pidfile &lt;pidfile&gt;
file for storing process id

## ecctool state

Get information of ecChronos internal state.

```console
usage: ecctool state [-h] [-c COLUMNS] [-o OUTPUT] [-u URL] [-T TIMEOUT]
                     {nodes} ...
```


### -h, --help
show this help message and exit


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json (defaults to no format)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)

## ecctool state nodes

```console
usage: ecctool state nodes [-h] [-u URL] [-T TIMEOUT]
```


### -h, --help
show this help message and exit


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)

## ecctool status

View status of the ecChronos instance.

```console
usage: ecctool status [-h] [-u URL] [-T TIMEOUT] [-o OUTPUT]
```


### -h, --help
show this help message and exit


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json (defaults to no format)

## ecctool stop

Stop the ecChronos service (sends SIGTERM to the process).

```console
usage: ecctool stop [-h] [-o OUTPUT] [-p PIDFILE]
```


### -h, --help
show this help message and exit


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json (defaults to no format)


### -p &lt;pidfile&gt;, --pidfile &lt;pidfile&gt;
file containing process id

## ecctool version

Show the ecctool client version and the connected ecChronos node’s version.

```console
usage: ecctool version [-h] [-u URL] [-T TIMEOUT] [-o OUTPUT]
```


### -h, --help
show this help message and exit


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -T &lt;timeout&gt;, --timeout &lt;timeout&gt;
request timeout in seconds (default: 30, or set ECCTOOL_TIMEOUT_SECONDS)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json (defaults to no format)

# Examples

For example usage and explanation about output refer to [ECCTOOL_EXAMPLES.md](../ECCTOOL_EXAMPLES.md)
