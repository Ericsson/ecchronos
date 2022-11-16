# Metrics

The metrics are controlled by `statistics` section in `ecc.yml` file.
The `statistics.enabled` controls if the metrics should be enabled.
The output directory for metrics is specified by `statistics.directory`.

**Note that statistics written to file are not rotated automatically.**

## Metric prefix

It's possible to define a global prefix for metrics produced by ecChronos and cassandra driver.
This is done by specifying a string in `statistics.prefix` in `ecc.yml`.
The prefix cannot start or end with a dot or any other path separator.

For example if the prefix is `ecChronos` and the metric name is `repaired.ratio`,
the metric name will be `ecChronos.repaired.ratio`.

By specifying an empty string or no value at all, the metric names will not be prefixed.

## Driver metrics

The cassandra driver used by ecChronos also has metrics on its own.
Driver metrics are exposed in the same way as ecChronos metrics and
can be excluded in the same way as ecChronos metrics.

For list of available driver metrics, refer to sections
`session-level metrics and node-level metrics` in [datastax reference configuration](https://docs.datastax.com/en/developer/java-driver/4.14/manual/core/configuration/reference/)

## Reporting formats

Metrics are exposed in several ways,
this is controlled by `statistics.reporting.jmx.enabled`, `statistics.reporting.file.enabled`
and `statistics.reporting.http.enabled` in `ecc.yml` file.
Metrics reported using `file` will be written in CSV format.

Metrics can be excluded from being reported, this is controlled by `statistics.reporting.jmx.excludedMetrics`
`statistics.reporting.file.excludedMetrics` `statistics.reporting.http.excludedMetrics` in `ecc.yml` file.
The `excludedMetrics` takes an array of quoted regexes, for example, `".*"` will exclude all metrics.

Metrics reported through different channels will look differently.

For example, assume we hava a metric `repaired.ratio` with tags `keyspace=ks1` and `table=tbl1`.
The metric name will be `repaired.ratio.keyspace.ks1.table.tbl1` for jmx and file reporters,
while for http reporter the metric name will be `repaired_ratio` with tags `keyspace=ks1` and `table=tbl1`.

## ecChronos metrics

The following metrics are available:

| Metric name           | Description                              | Tags                        |
|-----------------------|------------------------------------------|-----------------------------|
| repaired.ratio        | Ratio of repaired ranges vs total ranges | keyspace, table             |
| last.repaired.at      | Timestamp of last repair                 | keyspace, table             |
| remaining.repair.time | Estimated remaining repair time          | keyspace, table             |
| repair.time.taken     | Time taken to repair one range           | keyspace, table, successful |
| repair.tasks.run      | Number of repair tasks run               | keyspace, table, successful |

### File metrics examples

In the examples below we will be using keyspace `test` and table `table1`.

#### repairedRatio.keyspace.test.table.table1

| t          | value  |
|------------|--------|
| 1524472602 | 0.33   |

This metric shows the ratio of repaired ranges vs total ranges for the table.
In this case, the table has been `33%` repaired within the run interval.

#### lastRepairedAt.keyspace.test.table.table1

| t          | value          |
|------------|----------------|
| 1524472602 | 1524395220751  |

The value represents the last time the node perceived all of this tables ranges to be repaired.
The value is in milliseconds since UNIX epoch time.
If this value is beyond the alarm intervals an alarm should have been sent.

#### remainingRepairTime.keyspace.test.table.table1

| t          | value    |
|------------|----------|
| 1647956237 | 55740    |

The value represents the effective remaining repair time for the table to be fully repaired in milliseconds.
This is the time ecChronos will have to wait for Cassandra to perform repair,
this is an estimation based on the last repair of the table.
The value should be `0` if there is no repair ongoing for this table.

#### repairTimeTaken.keyspace.test.successful.true.table.table1

| t          | count | max | mean | min | stddev | p50 | p75 | p95 | p98 | p99 | p999 | mean_rate | m1_rate | m5_rate | m15_rate | rate_unit    | duration_unit |
|------------|-------|-----|------|-----|--------|-----|-----|-----|-----|-----|------|-----------|---------|---------|----------|--------------|---------------|
| 1524473322 | 102   | 933 | 218  | 51  | 206    | 105 | 282 | 701 | 769 | 845 | 933  | 0.065     | 1.4     | 0.32    | 0.11     | calls/second | milliseconds  |

This metric reports repair rate and timing for successful repair tasks.

* T

  The timestamp in seconds (UNIX Epoch time)

* Count

  The number of repair tasks

* Max

  Maximum time taken for repair tasks to complete/fail

* Mean

  Mean time taken for repair tasks to complete/fail

* Min

  Minimum time taken for repair tasks to complete/fail

* Stddev

  Standard deviation for repair tasks to complete/fail

* p50

  50 percentile (median) time taken for repair tasks to complete/fail

* p75->p999

  75->99.9 percentile time taken for repair tasks to complete/fail

* mean_rate

  The mean rate for repair tasks to complete/fail per second

* m1_rate

  The last minutes rate for repair tasks to complete/fail per second

* m5_rate

  The last five minutes rate for repair tasks to complete/fail per second

* m15_rate

  The last fifteen minutes rate for repair tasks to complete/fail per second

#### repairTasksRun.keyspace.test.successful.true.table.table1

| t          | count | mean_rate | m1_rate  | m5_rate   | m15_rate  | rate_unit     |
|------------|-------|-----------|----------|-----------|-----------|---------------|
| 1660213991 | 102   | 0.044732  | 3.000000 | 14.000000 | 42.000000 | events/second |

The count represents the total amount of failed/succeeded repair tasks for the table.
The mean rate is the rate at which events have occurred since the beginning.
The `m1_rate`, `m5_rate` and `m_15_rate` are the rates at which events have occurred for the past 1 minute,
5 minutes and 15 minutes.
For example an `m1_rate` of `15` would mean that 15 events have occurred in the past minute.