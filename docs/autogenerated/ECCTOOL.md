# ecctool

ecctool is a command line utility used to perform operations toward an ecChronos instance. Run ‘ecctool &lt;subcommand&gt; –help’ to get more information about each subcommand.

```console
usage: ecctool [-h]
               {rejections,repair-info,repairs,run-repair,running-job,schedules,start,state,status,stop}
               ...
```


### -h, --help
show this help message and exit

## ecctool rejections

Manage ecchronos rejections. Use ‘ecctool rejections &lt;action&gt; –help’ for action information.

```console
usage: ecctool rejections [-h] [-u URL] [-c COLUMNS] [-o OUTPUT]
                          {create,delete,get,update} ...
```


### -h, --help
show this help message and exit


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json, table (default)

## ecctool rejections create

```console
usage: ecctool rejections create [-h] -k KEYSPACE -t TABLE -sh START_HOUR -sm
                                 START_MINUTE -eh END_HOUR -em END_MINUTE -dcs
                                 DC_EXCLUSIONS [DC_EXCLUSIONS ...] [-u URL]
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

## ecctool rejections delete

```console
usage: ecctool rejections delete [-h] [-a ALL] [-k KEYSPACE] [-t TABLE]
                                 [-sh START_HOUR] [-sm START_MINUTE]
                                 [-dcs DC_EXCLUSIONS [DC_EXCLUSIONS ...]]
                                 [-u URL]
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

## ecctool rejections get

```console
usage: ecctool rejections get [-h] [-k KEYSPACE] [-t TABLE] [-u URL]
```


### -h, --help
show this help message and exit


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
keyspace


### -t &lt;table&gt;, --table &lt;table&gt;
table


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)

## ecctool rejections update

```console
usage: ecctool rejections update [-h] -k KEYSPACE -t TABLE -sh START_HOUR -sm
                                 START_MINUTE
                                 [-dcs DC_EXCLUSIONS [DC_EXCLUSIONS ...]]
                                 [-u URL]
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

## ecctool repair-info

Get information about repairs for tables. The repair information is based on repair history, meaning both manual and scheduled repairs will be a part of the repair information. This subcommand requires the user to provide either –since or –duration if –keyspace and –table is not provided. If repair info is fetched for a specific table using –keyspace and –table, the duration will default to the table’s GC_GRACE_SECONDS.

```console
usage: ecctool repair-info [-h] [-c COLUMNS] [-i ID] [-k KEYSPACE] [-t TABLE]
                           [-s SINCE] [-d DURATION] [-u URL] [-l LIMIT]
                           [-o OUTPUT]
```


### -h, --help
show this help message and exit


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -i &lt;id&gt;, --id &lt;id&gt;
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


### -l &lt;limit&gt;, --limit &lt;limit&gt;
limit output rows (use -1 for no limit)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json, table (default)

## ecctool repairs

Show the status of all manual repairs.

```console
usage: ecctool repairs [-h] [-c COLUMNS] [-k KEYSPACE] [-t TABLE] [-u URL]
                       [-i ID] [-j JOB] [-l LIMIT] [-o OUTPUT]
```


### -h, --help
show this help message and exit


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
keyspace (mutually exclusive with -i/–id)


### -t &lt;table&gt;, --table &lt;table&gt;
table (requires -k/–keyspace and is mutually exclusive with -i/–id)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


### -i &lt;id&gt;, --id &lt;id&gt;
only matching id (mutually exclusive with -k/–keyspace and -t/–table)


### -j &lt;job&gt;, --job &lt;job&gt;
only matching job id (mutually exclusive with -k/–keyspace and -t/–table)


### -l &lt;limit&gt;, --limit &lt;limit&gt;
limit output rows (use -1 for no limit)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json, table (default)

## ecctool run-repair

Triggers a manual repair in ecChronos. This will be done through the Cassandra JMX interface.

```console
usage: ecctool run-repair [-h] [-c COLUMNS] [-i ID] [-u URL] [-o OUTPUT]
                          [-r REPAIR_TYPE] [-f] [-e] [-a] [-k KEYSPACE]
                          [-t TABLE]
```


### -h, --help
show this help message and exit


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -i &lt;id&gt;, --id &lt;id&gt;
only matching id (mutually exclusive with -k/–keyspace and -t/–table)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


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
usage: ecctool running-job [-h] [-o OUTPUT] [-u URL]
```


### -h, --help
show this help message and exit


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json (defaults to no format)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)

## ecctool schedules

Show the status of schedules.

```console
usage: ecctool schedules [-h] [-c COLUMNS] [-f] [-i ID] [-j JOB] [-k KEYSPACE]
                         [-l LIMIT] [-o OUTPUT] [-t TABLE] [-u URL]
```


### -h, --help
show this help message and exit


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -f, --full
show full schedules with configuration and vnode state (requires -i/–id)


### -i &lt;id&gt;, --id &lt;id&gt;
only matching id (mutually exclusive with -k/–keyspace and -t/–table)


### -j &lt;job&gt;, --job &lt;job&gt;
only matching job id (mutually exclusive with -k/–keyspace and -t/–table)


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
keyspace (mutually exclusive with -i/–id)


### -l &lt;limit&gt;, --limit &lt;limit&gt;
limit output rows (use -1 for no limit)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json, table (default)


### -t &lt;table&gt;, --table &lt;table&gt;
table (requires -k/–keyspace and is mutually exclusive with -i/–id)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)

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
usage: ecctool state [-h] [-c COLUMNS] [-o OUTPUT] [-u URL] {nodes} ...
```


### -h, --help
show this help message and exit


### -c &lt;columns&gt;, --columns &lt;columns&gt;
table columns to display (format: 0,1,2,…,N)


### -o &lt;output&gt;, --output &lt;output&gt;
output formats: json (defaults to no format)


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)

## ecctool state nodes

```console
usage: ecctool state nodes [-h] [-u URL]
```


### -h, --help
show this help message and exit


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)

## ecctool status

View status of the ecChronos instance.

```console
usage: ecctool status [-h] [-u URL] [-o OUTPUT]
```


### -h, --help
show this help message and exit


### -u &lt;url&gt;, --url &lt;url&gt;
ecchronos host URL (format: [http:/](http:/)/&lt;host&gt;:&lt;port&gt;)


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

# Examples

For example usage and explanation about output refer to [ECCTOOL_EXAMPLES.md](../ECCTOOL_EXAMPLES.md)
