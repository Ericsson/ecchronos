# ecctool

ecctool is a command line utility which can be used to perform actions towards a local ecChronos instance. The actions are implemented in form of subcommands with arguments. All visualization is displayed in form of human-readable tables.

```console
usage: ecctool [-h]
               {repairs,schedules,run-repair,repair-info,start,stop,status}
               ...
```


### -h, --help
show this help message and exit

## ecctool repair-info

Get information about repairs for tables. The repair information is based on repair history, meaning that both manual repairs and schedules will contribute to the repair information. This subcommand requires the user to provide either –since or –duration if –keyspace and –table is not provided. If repair info is fetched for a specific table using –keyspace and –table, the duration will default to the table’s GC_GRACE_SECONDS.

```console
usage: ecctool repair-info [-h] [-k KEYSPACE] [-t TABLE] [-s SINCE]
                           [-d DURATION] [--local] [-u URL] [-l LIMIT]
```


### -h, --help
show this help message and exit


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
Show repair information for all tables in the specified keyspace.


### -t &lt;table&gt;, --table &lt;table&gt;
Show repair information for the specified table. Keyspace argument -k or –keyspace becomes mandatory if using this argument.


### -s &lt;since&gt;, --since &lt;since&gt;
Show repair information since the specified date to now. Date must be specified in ISO8601 format. The time-window will be since to now. Mandatory if –duration or –keyspace and –table is not specified.


### -d &lt;duration&gt;, --duration &lt;duration&gt;
Show repair information for the duration. Duration can be specified as ISO8601 format or as simple format in form: 5s, 5m, 5h, 5d. The time-window will be now-duration to now. Mandatory if –since or –keyspace and –table is not specified.


### --local
Show repair information only for the local node.


### -u &lt;url&gt;, --url &lt;url&gt;
The ecChronos host to connect to, specified in the format [http:/](http:/)/&lt;host&gt;:&lt;port&gt;.


### -l &lt;limit&gt;, --limit &lt;limit&gt;
Limits the number of rows printed in the output. Specified as a number, -1 to disable limit.

## ecctool repairs

Show the status of all manual repairs. This subcommand has no mandatory parameters.

```console
usage: ecctool repairs [-h] [-k KEYSPACE] [-t TABLE] [-u URL] [-i ID]
                       [-l LIMIT] [--hostid HOSTID]
```


### -h, --help
show this help message and exit


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
Show repairs for the specified keyspace. This argument is mutually exclusive with -i and –id.


### -t &lt;table&gt;, --table &lt;table&gt;
Show repairs for the specified table. Keyspace argument -k or –keyspace becomes mandatory if using this argument. This argument is mutually exclusive with -i and –id.


### -u &lt;url&gt;, --url &lt;url&gt;
The ecChronos host to connect to, specified in the format [http:/](http:/)/&lt;host&gt;:&lt;port&gt;.


### -i &lt;id&gt;, --id &lt;id&gt;
Show repairs matching the specified ID. This argument is mutually exclusive with -k, –keyspace, -t and –table.


### -l &lt;limit&gt;, --limit &lt;limit&gt;
Limits the number of rows printed in the output. Specified as a number, -1 to disable limit.


### --hostid &lt;hostid&gt;
Show repairs for the specified host id. The host id corresponds to the Cassandra instance ecChronos is connected to.

## ecctool run-repair

Run a manual repair. The manual repair will be triggered in ecChronos. EcChronos will perform repair through Cassandra JMX interface. This subcommand has no mandatory parameters.

```console
usage: ecctool run-repair [-h] [-u URL] [--local] [-r REPAIR_TYPE]
                          [-k KEYSPACE] [-t TABLE]
```


### -h, --help
show this help message and exit


### -u &lt;url&gt;, --url &lt;url&gt;
The ecChronos host to connect to, specified in the format [http:/](http:/)/&lt;host&gt;:&lt;port&gt;.


### --local
Run repair for the local node only, i.e repair will only be performed for the ranges that the local node is a replica for.


### -r &lt;repair_type&gt;, --repair_type &lt;repair_type&gt;
The type of the repair, possible values are ‘vnode’, ‘parallel_vnode’, ‘incremental’


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
Run repair for the specified keyspace. Repair will be run for all tables within the keyspace with replication factor higher than 1.


### -t &lt;table&gt;, --table &lt;table&gt;
Run repair for the specified table. Keyspace argument -k or –keyspace becomes mandatory if using this argument.

## ecctool schedules

Show the status of schedules. This subcommand has no mandatory parameters.

```console
usage: ecctool schedules [-h] [-k KEYSPACE] [-t TABLE] [-u URL] [-i ID] [-f]
                         [-l LIMIT]
```


### -h, --help
show this help message and exit


### -k &lt;keyspace&gt;, --keyspace &lt;keyspace&gt;
Show schedules for the specified keyspace. This argument is mutually exclusive with -i and –id.


### -t &lt;table&gt;, --table &lt;table&gt;
Show schedules for the specified table. Keyspace argument -k or –keyspace becomes mandatory if using this argument. This argument is mutually exclusive with -i and –id.


### -u &lt;url&gt;, --url &lt;url&gt;
The ecChronos host to connect to, specified in the format [http:/](http:/)/&lt;host&gt;:&lt;port&gt;.


### -i &lt;id&gt;, --id &lt;id&gt;
Show schedules matching the specified ID. This argument is mutually exclusive with -k, –keyspace, -t and –table.


### -f, --full
Show full schedules, can only be used with -i or –id. Full schedules include schedule configuration and repair state per vnode.


### -l &lt;limit&gt;, --limit &lt;limit&gt;
Limits the number of rows printed in the output. Specified as a number, -1 to disable limit.

## ecctool start

Start the ecChronos service. This subcommand has no mandatory parameters.

```console
usage: ecctool start [-h] [-f] [-p PIDFILE]
```


### -h, --help
show this help message and exit


### -f, --foreground
Start the ecChronos instance in foreground mode (exec in current terminal and log to stdout)


### -p &lt;pidfile&gt;, --pidfile &lt;pidfile&gt;
Start the ecChronos instance and store the pid in the specified pid file.

## ecctool status

View status of ecChronos instance. This subcommand has no mandatory parameters.

```console
usage: ecctool status [-h] [-u URL]
```


### -h, --help
show this help message and exit


### -u &lt;url&gt;, --url &lt;url&gt;
The ecChronos host to connect to, specified in the format [http:/](http:/)/&lt;host&gt;:&lt;port&gt;.

## ecctool stop

Stop the ecChronos instance. Stopping of ecChronos is done by using kill with SIGTERM signal (same as kill in shell) for the pid. This subcommand has no mandatory parameters.

```console
usage: ecctool stop [-h] [-p PIDFILE]
```


### -h, --help
show this help message and exit


### -p &lt;pidfile&gt;, --pidfile &lt;pidfile&gt;
Stops the ecChronos instance by pid fetched from the specified pid file.

# Examples

For example usage and explanation about output refer to [ECCTOOL_EXAMPLES.md](../ECCTOOL_EXAMPLES.md)
