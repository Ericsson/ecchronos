# Metrics

The metrics are controlled by `statistics` section in `ecc.yml` file.
The `statistics.enabled` controls if the metrics should be enabled.
The output directory for metrics is specified by `statistics.directory`.

Which metrics should be enabled is controlled by `statistics.excluded` in `ecc.yml` file.
The `statistics.excluded` takes an array of quoted regexes to disable metrics.


## Driver metrics

The cassandra driver used by ecChronos also has metrics on its own.
Driver metrics are exposed in the same way as ecChronos metrics and
can be excluded in the same way as ecChronos metrics.

For list of available driver metrics, refer to sections
`session-level metrics and node-level metrics` in [datastax reference configuration](https://docs.datastax.com/en/developer/java-driver/4.14/manual/core/configuration/reference/)

## Files

There are four metric files on node-level and four metric files per table.

### Node-level

* TableRepairState

    The percentage of tables that have been repaired within the run interval (0-1).

* DataRepairState

    The percentage of data(estimate) that has been repaired within the run interval (0-1).

* RepairSuccessTime

    Timers for the repair tasks that were successful.
    A repair task is the repair of one virtual node (or token range).
    This is a metric that will show latencies and rates (repair tasks/s).
    The latency values are decayed over time and will roughly display the last five minutes of data. (See Exponentially Decaying Reservoirs in Dropwizard metrics)

* RepairFailedTime

    Timers for the repair tasks that were not successful.
    A repair task is the repair of one virtual node (or token range).
    This is a metric that will show latencies and rates (repair tasks/s).
    The latency values are decayed over time and will roughly display the last five minutes of data.

#### Examples

| t          | value  |
|------------|--------|
| 1524472602 | 0.033  |

TableRepairState

| t          | value  |
|------------|--------|
| 1524472602 | 0.1535 |

DataRepairState

In the CSV format the column t is the timestamp in seconds (UNIX Epoch time).
The values above shows that `3.3%` of tables and `15%` of the total data on the node have been repaired.
Whether or not table/data is deemed repaired in this case is based on the defined repair interval.

When all tables have been repaired within the defined repair interval a value of `1.0` should be observed for the two metrics.
Whenever a table passes the interval and becomes eligible for repair the metrics will be reduced accordingly based on the weight of the table.
If all tables exceed the interval both the metrics would drop down to `0.0`.

So these values does not represent if repair has passed the `gc_grace_seconds`.
If we are not able to manage that,
an alarm should be raised.
Instead this value shows that there is a repair backlog of `96.7%` of the tables and `85%` of the node data.


| t          | count | max | mean | min | stddev | p50 | p75 | p95 | p98 | p99 | p999 | mean_rate | m1_rate | m5_rate | m15_rate | rate_unit    | duration_unit |
|------------|-------|-----|------|-----|--------|-----|-----|-----|-----|-----|------|-----------|---------|---------|----------|--------------|---------------|
| 1524473322 | 102   | 933 | 218  | 51  | 206    | 105 | 282 | 701 | 769 | 845 | 933  | 0.065     | 1.4     | 0.32    | 0.11     | calls/second | milliseconds  |

RepairSuccessTime

| t          | count | max | mean | min | stddev | p50 | p75 | p95 | p98 | p99 | p999 | mean_rate | m1_rate | m5_rate | m15_rate | rate_unit    | duration_unit |
|------------|-------|-----|------|-----|--------|-----|-----|-----|-----|-----|------|-----------|---------|---------|----------|--------------|---------------|
| 1524473322 | 0     | 0   | 0    | 0   | 0      | 0   | 0   | 0   | 0   | 0   | 0    | 0         | 0       | 0       | 0        | calls/second | milliseconds  |

RepairFailedTime

Same as for DataRepairState and TableRepairState the t column is the timestamp.
The two CSV files described above contains the same type of metrics but have a slightly different meaning.
The RepairSuccessTime reports repair rate and timing for repair tasks that succeeds,
while the RepairFailedTime reports the same but for repair tasks that fail.
Usually the RepairFailedTime should be all zeros but if it's not the reason can usually be found in the system.log.

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


### Table level

  These metric files will be prefixed by the keyspace name, table name and table id they represent.

* RepairState

    The percentage of tables that have been repaired within the run interval (0-1).

* LastRepairedAt

    The time the table was last completely repaired according to the local node (milliseconds since epoch).

* RepairSuccessTime

    Timers for the repair sessions that were successful.

* RepairFailedTime

    Timers for the repair session that were not successful.

* RemainingRepairTime

    The effective remaining repair time (in milliseconds) for the table to be fully repaired (time ecChronos waits for cassandra to perform repair).

* FailedRepairTasks

    The counter for the repair tasks that have failed for the table.
    A repair task is the repair of one vnode (or token range).

* SucceededRepairTasks

    The counter for the repair tasks that have succeeded for the table.
    A repair task is the repair of one vnode (or token range).

#### Examples

| t          | value  |
|------------|--------|
| 1524472602 | 0.33   |

\<keyspace\>.\<table\>-\<table-id\>-RepairState

Similar to the value presented in TableRepairState and DataRepairState this value shows the percentage of ranges repaired for a specific table.
In this case the table has been `33%` repaired within the run interval.
If the local node initiated the repair this value should go to `1.0`.
If another node initiates the repair this value could differ as shown above.


| t          | value          |
|------------|----------------|
| 1524472602 | 1524395220751  |

\<keyspace\>.\<table\>-\<table-id\>-LastRepairedAt

This value represents the last time the node perceived all of this tables ranges to be repaired.
The value is in milliseconds since UNIX epoch time.
If this value is beyond the alarm intervals an alarm should have been sent.

| t          | value    |
|------------|----------|
| 1647956237 | 55740    |

\<keyspace\>.\<table\>-\<table-id\>-RemainingRepairTime

The value represents the effective remaining repair time for the table to be fully repaired in milliseconds.
This is the time ecChronos will have to wait for Cassandra to perform repair,
this is an estimation based on the last repair of the table.
The value should be `0` if there is no repair ongoing for this table.

| t          | count | mean_rate | m1_rate  | m5_rate  | m15_rate | rate_unit     |
|------------|-------|-----------|----------|----------|----------|---------------|
| 1660042648 | 0     | 0.000000  | 0.000000 | 0.000000 | 0.000000 | events/second |

\<keyspace\>.\<table\>-\<table-id\>-FailedRepairTasks


| t          | count | mean_rate | m1_rate  | m5_rate   | m15_rate  | rate_unit     |
|------------|-------|-----------|----------|-----------|-----------|---------------|
| 1660213991 | 102   | 0.044732  | 3.000000 | 14.000000 | 42.000000 | events/second |

\<keyspace\>.\<table\>-\<table-id\>-SucceededRepairTasks

The count represents the total amount of failed/succeeded repair tasks for the table.
The mean rate is the rate at which events have occurred since the beginning.
The `m1_rate`, `m5_rate` and `m_15_rate` are the rates at which events have occurred for the past 1 minute,
5 minutes and 15 minutes.
For example an `m1_rate` of `15` would mean that 15 events have occurred in the past minute.

For `\<keyspace\>.\<table\>-\<table-id\>-FailedRepairTasks`, `m1_rate`, `m5_rate` and `m15_rate` should be close to `0`
in healthy clusters. A positive rate might indicate a problem and warrant a look into the logs.
If repairs are not running, the `m1_rate`, `m5_rate` and `m15_rate` will show `0`.

For `\<keyspace\>.\<table\>-\<table-id\>-SucceededRepairTasks`, `m1_rate`, `m5_rate` and `m15_rate` should always be
a positive number.
If the `m1_rate`, `m5_rate` or `m15_rate` report `0`, that means that no repairs have succeeded or no repairs have run.

These 2 metrics could be viewed together to view the ratio between failed and succeeded repair tasks as well as total repair tasks.