# Metrics

The metrics are controlled by `statistics` section in `ecc.yml` file.
The `statistics.enabled` controls if the metrics should be enabled.
The output directory for metrics is specified by `statistics.directory`.

**Note that statistics written to file are not rotated automatically.**

## Metric prefix

It's possible to define a global prefix for all metrics produced by ecChronos.
This is done by specifying a string in `statistics.prefix` in `ecc.yml`.
The prefix cannot start or end with a dot or any other path separator.

For example if the prefix is `ecChronos` and the metric name is `repaired.ratio`,
the metric name will be `ecChronos.repaired.ratio`.

By specifying an empty string or no value at all, the metric names will not be prefixed.

## Reporting formats

Metrics are exposed in several ways,
this is controlled by `statistics.reporting.jmx.enabled`, `statistics.reporting.file.enabled`
and `statistics.reporting.http.enabled` in `ecc.yml` file.
Metrics reported using different formats may look differently,
for reference please refer to ecChronos metrics section below.
Metrics reported using `file` will be written in CSV format.

## Metric exclusion
Metrics can be excluded from being reported, this is controlled by `statistics.reporting.jmx.excludedMetrics`
`statistics.reporting.file.excludedMetrics` `statistics.reporting.http.excludedMetrics` in `ecc.yml` file.
Exclusion can be performed based on the metric name (without the prefix) and optionally on tags.
The exclusion is performed using regular expressions.
If no tags are specified for exclusion, all metrics matching the name will be excluded.
If multiple tags are specified, all tags must match for the metric to be excluded.

### Examples

In this example we will be excluding metrics only for http reporting.
The same examples can be used for any reporter.

#### Exclude a metric on exact name

In this example `repaired.ratio` metric will be excluded for all tags.

```yaml
statistics:
  reporting:
    http:
      enabled: true
      excludedMetrics:
        - name: repaired\.ratio
```

#### Exclude metrics on name with regexp

In this example all metrics starting with `node.` will be excluded.

```yaml
statistics:
  reporting:
    http:
      enabled: true
      excludedMetrics:
        - name: node\..*
```

#### Exclude metrics on exact name and tag

In this example `repaired.ratio` metric will be excluded for all tables in keyspace `ecchronos`.

```yaml
statistics:
  reporting:
    http:
      enabled: true
      excludedMetrics:
        - name: repaired\.ratio
          tags:
            keyspace: ecchronos
```

#### Exclude metrics with name regexp and exact tag

In this example `node.*` metric will be excluded with tag `successful=true`.

```yaml
statistics:
  reporting:
    http:
      enabled: true
      excludedMetrics:
        - name: node\..*
          tags:
            successful: true
```

#### Exclude metrics with exact name and tag regexp

In this example `remaining.repair.time` metric will be excluded with tag keyspace matching value `test.*`.

```yaml
statistics:
  reporting:
    http:
      enabled: true
      excludedMetrics:
        - name: remaining\.repair\.time
          tags:
            keyspace: test.*
```

#### Exclude metric with exact name and multiple exact tags

In this example `time.since.last.repaired` metric will be excluded with tag `keyspace=test` and tag `table=table1`.

```yaml
statistics:
  reporting:
    http:
      enabled: true
      excludedMetrics:
        - name: time\.since\.last\.repaired
          tags:
            keyspace: test
            table: table1
```

## Driver metrics

The Cassandra driver used by ecChronos also has metrics on its own.
Driver metrics are exposed in the same way as ecChronos metrics and
can be excluded in the same way as ecChronos metrics.

For list of available driver metrics, refer to sections
`session-level metrics and node-level metrics` in [datastax reference configuration](https://docs.datastax.com/en/developer/java-driver/4.14/manual/core/configuration/reference/)

### Exclude all Driver metrics example

```yaml
statistics:
  reporting:
    http:
      enabled: true
      excludedMetrics:
        - name: nodes\..*
        - name: session\..*
```

## Spring Boot metrics

Spring Boot metrics are provided as well.
The metrics are exposed in the same way as ecChronos metrics and
can be excluded in the same way as ecChronos metrics.

For supported Spring Boot metrics, refer to section
`Supported Metrics and Meters` in [Spring Boot documentation](https://docs.spring.io/spring-boot/docs/2.7.5/reference/html/actuator.html#actuator.metrics.supported)

### Exclude all Spring Boot metrics example

```yaml
statistics:
  reporting:
    http:
      enabled: true
      excludedMetrics:
        - name: jvm\..*
        - name: logback\..*
        - name: executor\..*
        - name: application\..*
        - name: process\..*
        - name: tomcat\..*
        - name: disk\..*
        - name: system\..*
        - name: http\..*
```

## ecChronos metrics

| Metric name                   | Description                                                                                     | Tags                        |
|-------------------------------|-------------------------------------------------------------------------------------------------|-----------------------------|
| node.repaired.ratio           | Average repair ratio for all tables, aggregation of repaired.ratio                              |                             |
| repaired.ratio                | Ratio of repaired ranges vs total ranges                                                        | keyspace, table             |
| node.time.since.last.repaired | The longest time since a table has been fully repaired, aggregation of time.since.last.repaired |                             |
| time.since.last.repaired      | The amount of time since table was fully repaired                                               | keyspace, table             |
| node.remaining.repair.time    | A sum of remaining repair time for all tables, aggregation of remaining.repair.time             |                             |
| remaining.repair.time         | Estimated remaining repair time                                                                 | keyspace, table             |
| node.repair.sessions          | Time taken for all repair sessions for all tables to succeed or fail                            | successful                  |
| repair.sessions               | Time taken for repair sessions to succeed or fail                                               | keyspace, table, successful |

**All examples below assume keyspace `ks1` and table `tbl1`.**

### node.repaired.ratio

`node.repaired.ratio` metric represents the average repair ratio of all tables.
The value is a double between 0 and 1.

| Reporter type | Metric name(s)      |
|---------------|---------------------|
| jmx           | nodeRepairedRatio   |
| file          | nodeRepairedRatio   |
| http          | node_repaired_ratio |

#### Examples

In this example, the tables are on average `33%` repaired.

##### jmx

Object name: `metrics:name=nodeRepairedRatio,type=gauges`

```
Number: 0.33
Value: 0.33
```

##### file

File name: `nodeRepairedRatio.csv`

```
t,value
1669033092,0.33
```

* t - The timestamp in milliseconds when the metric was reported

* value - The average repaired ratio for all tables

##### http

```
node_repaired_ratio 0.33
```

### repaired.ratio

`repaired.ratio` metric represents the ratio of repaired ranges compared to total ranges within the run interval.
The value is a double between 0 and 1.

| Reporter type | Metric name(s)                        |
|---------------|---------------------------------------|
| jmx           | repairedRatio.keyspace.ks1.table.tbl1 |
| file          | repairedRatio.keyspace.ks1.table.tbl1 |
| http          | repaired_ratio                        |

#### Examples

In this example, the table has been `33%` repaired.

##### jmx

Object name: `metrics:name=repairedRatio.keyspace.ks1.table.tbl1,type=gauges`

```
Number: 0.33
Value: 0.33
```

##### file

File name: `repairedRatio.keyspace.ks1.table.tbl1.csv`

```
t,value
1669033092,0.33
```

* t - The timestamp in milliseconds when the metric was reported

* value - The repaired ratio

##### http

```
repaired_ratio{keyspace="ks1",table="tbl1",} 0.33
```

### node.time.since.last.repaired

`nodetime.since.last.repaired` metric represents the longest time since a table has been fully repaired
For `jmx` and `file` the time unit is milliseconds, while for `http` the time unit is seconds.

| Reporter type | Metric name(s)                        |
|---------------|---------------------------------------|
| jmx           | nodeTimeSinceLastRepaired             |
| file          | nodeTimeSinceLastRepaired             |
| http          | node_time_since_last_repaired_seconds |

#### Examples

In this example, the table that was repaired the longest time ago has been repaired `10 seconds` ago.

##### jmx

Object name: `metrics:name=nodeTimeSinceLastRepaired,type=gauges`

```
Number: 10000.0
Value: 10000.0
```

##### file

File name: `nodeTimeSinceLastRepaired.csv`

```
t,value
1669033092,10000.0
```

* t - The timestamp in milliseconds when the metric was reported

* value - The longest time ago a table was fully repaired in milliseconds

##### http

```
node_time_since_last_repaired_seconds 10.0
```

### time.since.last.repaired

`time.since.last.repaired` metric represents the duration since the table was fully repaired.
For `jmx` and `file` the time unit is milliseconds, while for `http` the time unit is seconds.

| Reporter type | Metric name(s)                                |
|---------------|-----------------------------------------------|
| jmx           | timeSinceLastRepaired.keyspace.ks1.table.tbl1 |
| file          | timeSinceLastRepaired.keyspace.ks1.table.tbl1 |
| http          | time_since_last_repaired_seconds              |

#### Examples

In this example, the table was repaired `10 seconds` ago.

##### jmx

Object name: `metrics:name=timeSinceLastRepaired.keyspace.ks1.table.tbl1,type=gauges`

```
Number: 10000.0
Value: 10000.0
```

##### file

File name: `timeSinceLastRepaired.keyspace.ks1.table.tbl1.csv`

```
t,value
1669033092,10000.0
```

* t - The timestamp in milliseconds when the metric was reported

* value - The time since the table was fully repaired in milliseconds

##### http

```
time_since_last_repaired_seconds{keyspace="ks1",table="tbl1",} 10.0
```

### node.remaining.repair.time

`node.remaining.repair.time` metric represents a sum of remaining repair time for all tables to be fully repaired.
This is the time ecChronos will have to wait for Cassandra to perform repair,
this is an estimation based on the last repair of each table.
The value should be 0 if there are no repairs ongoing.
For `jmx` and `file` the time unit is milliseconds, while for `http` the time unit is seconds.

| Reporter type | Metric name(s)                     |
|---------------|------------------------------------|
| jmx           | nodeRemainingRepairTime            |
| file          | nodeRemainingRepairTime            |
| http          | node_remaining_repair_time_seconds |

#### Examples

In this example, the remaining repair time for all ongoing repairs is `10 seconds`.

##### jmx

Object name: `metrics:name=nodeRemainingRepairTime,type=gauges`

```
Number: 10000.0
Value: 10000.0
```

##### file

File name: `nodeRemainingRepairTime.csv`

```
t,value
1669033092,10000.0
```

* t - The timestamp in milliseconds when the metric was reported

* value - The remaining repair time in milliseconds for all repairs to finish

##### http

```
remaining_repair_time_seconds 10.0
```

### remaining.repair.time

`remaining.repair.time` metric represents effective remaining repair time for the table to be fully repaired.
This is the time ecChronos will have to wait for Cassandra to perform repair,
this is an estimation based on the last repair of the table.
The value should be 0 if there is no repair ongoing for this table.
For `jmx` and `file` the time unit is milliseconds, while for `http` the time unit is seconds.

| Reporter type | Metric name(s)                              |
|---------------|---------------------------------------------|
| jmx           | remainingRepairTime.keyspace.ks1.table.tbl1 |
| file          | remainingRepairTime.keyspace.ks1.table.tbl1 |
| http          | remaining_repair_time_seconds               |

#### Examples

In this example, the remaining repair time for the table is `10 seconds`.

##### jmx

Object name: `metrics:name=remainingRepairTime.keyspace.ks1.table.tbl1,type=gauges`

```
Number: 10000.0
Value: 10000.0
```

##### file

File name: `remainingRepairTime.keyspace.ks1.table.tbl1.csv`

```
t,value
1669033092,10000.0
```

* t - The timestamp in milliseconds when the metric was reported

* value - The remaining repair time in milliseconds

##### http

```
remaining_repair_time_seconds{keyspace="ks1",table="tbl1",} 10.0
```

### node.repair.sessions

`node.repair.sessions` metric represents the time taken for all repair sessions for all tables to succeed or fail.
For `jmx` and `file` the time unit is milliseconds, while for `http` the time unit is seconds.

| Reporter type | Metric name(s)                                                                                       |
|---------------|------------------------------------------------------------------------------------------------------|
| jmx           | nodeRepairSessions.successful.true,repairSessions.successful.false                                   |
| file          | nodeRepairSessions.successful.true,repairSessions.successful.false                                   |
| http          | node_repair_sessions_seconds_count,node_repair_sessions_seconds_sum,node_repair_sessions_seconds_max |

#### Examples

In this example, we have run repair for all tables where there were `793` successful sessions and `793` failed sessions.

##### jmx

Object name: `metrics:name=nodeRepairSessions.successful.true,type=timers`

```
50thPercentile: 6.26
75thPercentile: 6.94
95thPercentile: 8.30
98thPercentile: 9.64
999thPercentile: 56.27
99thPercentile: 10.47
Count: 793
DurationUnit: milliseconds
FifteenMinuteRate: 12.04
FiveMinuteRate: 0.08
Max: 56.27
Mean: 6.69
MeanRate: 0.35
Min: 5.52
OneMinuteRate: 8.93E-15
RateUnit: events/second
StdDev: 2.15
```

Object name: `metrics:name=nodeRepairSessions.successful.false,type=timers`

```
50thPercentile: 6.26
75thPercentile: 6.94
95thPercentile: 8.30
98thPercentile: 9.64
999thPercentile: 56.27
99thPercentile: 10.47
Count: 793
DurationUnit: milliseconds
FifteenMinuteRate: 12.04
FiveMinuteRate: 0.08
Max: 56.27
Mean: 6.69
MeanRate: 0.35
Min: 5.52
OneMinuteRate: 8.93E-15
RateUnit: events/second
StdDev: 2.15
```

##### file

File name: `nodeRepairSessions.successful.true.csv`

```
t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit
1669036333,793,56.272057,6.693952,5.523480,2.153694,6.261243,6.945218,8.306778,9.646926,10.477962,56.272057,0.358667,0.000000,0.093323,12.519108,calls/second,milliseconds
```

* t - The timestamp in milliseconds when the metric was reported

* count - The number of repair sessions that succeeded

* max - Maximum time taken for a repair session to succeed

* mean - Mean time taken for a repair sessions to succeed

* min - Minimum time taken for a repair session to succeed

* stddev - Standard deviation for repair sessions to succeed

* p50 - 50 percentile (median) time taken for a repair session to succeed

* p75->p999 - 75->99.9 percentile time taken for repair sessions to succeed

* mean_rate - The mean rate for repair sessions to succeed per second

* m1_rate - The last minutes rate for repair sessions to succeed per second

* m5_rate - The last five minutes rate for repair sessions to succeed per second

* m15_rate - The last fifteen minutes rate for repair sessions to succeed per second

* rate_unit - The rate unit for the metric

* duration_unit - The duration unit for the metric

File name: `nodeRepairSessions.successful.false.csv`

```
t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit
1669036333,793,56.272057,6.693952,5.523480,2.153694,6.261243,6.945218,8.306778,9.646926,10.477962,56.272057,0.358667,0.000000,0.093323,12.519108,calls/second,milliseconds
```

* t - The timestamp in milliseconds when the metric was reported

* count - The number of repair sessions that failed

* max - Maximum time taken for a repair session to fail

* mean - Mean time taken for a repair sessions to fail

* min - Minimum time taken for a repair session to fail

* stddev - Standard deviation for repair sessions to fail

* p50 - 50 percentile (median) time taken for a repair session to fail

* p75->p999 - 75->99.9 percentile time taken for repair sessions to fail

* mean_rate - The mean rate for repair sessions to fail per second

* m1_rate - The last minutes rate for repair sessions to fail per second

* m5_rate - The last five minutes rate for repair sessions to fail per second

* m15_rate - The last fifteen minutes rate for repair sessions to fail per second

* rate_unit - The rate unit for the metric

* duration_unit - The duration unit for the metric

##### http

```
node_repair_sessions_seconds_count{successful="true",} 793.0
node_repair_sessions_seconds_sum{successful="true",} 5.317104685
node_repair_sessions_seconds_max{successful="true",} 0.0

node_repair_sessions_seconds_count{successful="false",} 793.0
node_repair_sessions_seconds_sum{successful="false",} 5.317104685
node_repair_sessions_seconds_max{successful="false",} 0.0
```

### repair.sessions

`repair.sessions` metric represents the time taken for repair sessions to succeed or fail.
For `jmx` and `file` the time unit is milliseconds, while for `http` the time unit is seconds.

| Reporter type | Metric name(s)                                                                                                 |
|---------------|----------------------------------------------------------------------------------------------------------------|
| jmx           | repairSessions.keyspace.ks1.successful.true.table.tbl1,repairSessions.keyspace.ks1.successful.false.table.tbl1 |
| file          | repairSessions.keyspace.ks1.successful.true.table.tbl1,repairSessions.keyspace.ks1.successful.false.table.tbl1 |
| http          | repair_sessions_seconds_count,repair_sessions_seconds_sum,repair_sessions_seconds_max                          |

#### Examples

In this example, we have run repair where there were `793` successful sessions and `793` failed sessions.

##### jmx

Object name: `metrics:name=repairSessions.keyspace.ks1.successful.true.table.tbl1,type=timers`

```
50thPercentile: 6.26
75thPercentile: 6.94
95thPercentile: 8.30
98thPercentile: 9.64
999thPercentile: 56.27
99thPercentile: 10.47
Count: 793
DurationUnit: milliseconds
FifteenMinuteRate: 12.04
FiveMinuteRate: 0.08
Max: 56.27
Mean: 6.69
MeanRate: 0.35
Min: 5.52
OneMinuteRate: 8.93E-15
RateUnit: events/second
StdDev: 2.15
```

Object name: `metrics:name=repairSessions.keyspace.ks1.successful.false.table.tbl1,type=timers`

```
50thPercentile: 6.26
75thPercentile: 6.94
95thPercentile: 8.30
98thPercentile: 9.64
999thPercentile: 56.27
99thPercentile: 10.47
Count: 793
DurationUnit: milliseconds
FifteenMinuteRate: 12.04
FiveMinuteRate: 0.08
Max: 56.27
Mean: 6.69
MeanRate: 0.35
Min: 5.52
OneMinuteRate: 8.93E-15
RateUnit: events/second
StdDev: 2.15
```

##### file

File name: `repairSessions.keyspace.test.successful.true.table.table1.csv`

```
t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit
1669036333,793,56.272057,6.693952,5.523480,2.153694,6.261243,6.945218,8.306778,9.646926,10.477962,56.272057,0.358667,0.000000,0.093323,12.519108,calls/second,milliseconds
```

* t - The timestamp in milliseconds when the metric was reported

* count - The number of repair sessions that succeeded

* max - Maximum time taken for a repair session to succeed

* mean - Mean time taken for a repair sessions to succeed

* min - Minimum time taken for a repair session to succeed

* stddev - Standard deviation for repair sessions to succeed

* p50 - 50 percentile (median) time taken for a repair session to succeed

* p75->p999 - 75->99.9 percentile time taken for repair sessions to succeed

* mean_rate - The mean rate for repair sessions to succeed per second

* m1_rate - The last minutes rate for repair sessions to succeed per second

* m5_rate - The last five minutes rate for repair sessions to succeed per second

* m15_rate - The last fifteen minutes rate for repair sessions to succeed per second

* rate_unit - The rate unit for the metric

* duration_unit - The duration unit for the metric

File name: `repairSessions.keyspace.test.successful.false.table.table1.csv`

```
t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit
1669036333,793,56.272057,6.693952,5.523480,2.153694,6.261243,6.945218,8.306778,9.646926,10.477962,56.272057,0.358667,0.000000,0.093323,12.519108,calls/second,milliseconds
```

* t - The timestamp in milliseconds when the metric was reported

* count - The number of repair sessions that failed

* max - Maximum time taken for a repair session to fail

* mean - Mean time taken for a repair sessions to fail

* min - Minimum time taken for a repair session to fail

* stddev - Standard deviation for repair sessions to fail

* p50 - 50 percentile (median) time taken for a repair session to fail

* p75->p999 - 75->99.9 percentile time taken for repair sessions to fail

* mean_rate - The mean rate for repair sessions to fail per second

* m1_rate - The last minutes rate for repair sessions to fail per second

* m5_rate - The last five minutes rate for repair sessions to fail per second

* m15_rate - The last fifteen minutes rate for repair sessions to fail per second

* rate_unit - The rate unit for the metric

* duration_unit - The duration unit for the metric

##### http

```
repair_sessions_seconds_count{keyspace="ks1",successful="true",table="tbl1",} 793.0
repair_sessions_seconds_sum{keyspace="ks1",successful="true",table="tbl1",} 5.317104685
repair_sessions_seconds_max{keyspace="ks1",successful="true",table="tbl1",} 0.0

repair_sessions_seconds_count{keyspace="ks1",successful="false",table="tbl1",} 793.0
repair_sessions_seconds_sum{keyspace="ks1",successful="false",table="tbl1",} 5.317104685
repair_sessions_seconds_max{keyspace="ks1",successful="false",table="tbl1",} 0.0
```

## Metric Status Logger
Whenever metric is enabled, a logger is triggered which monitor metrics for
repair failures within a defined time window. If number of repair failures 
overshoot the number(`repair_failures_count`) configured in ecc.yml within the time
window then ecchronos metrics are printed in debug logs. 

Repair failures threshold is handled by a property `statistics.repair_failures_count`. The
time window can be configured via `statistics.repair_failures_time_window`. There is
another field `statistics.trigger_interval_for_metric_inspection` which is used for controlling
the repeat time in which metric inspection will take place.