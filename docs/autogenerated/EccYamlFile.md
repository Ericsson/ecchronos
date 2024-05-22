
## ecChronos configuration

# Connection
# Properties for connection to the local node
#
**connection:**
**cql:**
#
# Host and port properties for CQL.
# Primarily used by the default connection provider
#
* host: localhost
* port: 9042
#
# Connection Timeout for a CQL attempt.
# Specify a time to wait for cassandra to come up.
# Connection is tried based on retry policy delay calculations. Each connection attempt will use the timeout to calculate CQL connection process delay.
#
**timeout:**
* time: 60
* unit: seconds
**retryPolicy:**
# Max number of attempts ecChronos will try to connect with Cassandra.
* maxAttempts: 5
# Delay use to wait between an attempt and another, this value will be multiplied by the current attempt count powered by two.
# If the current attempt is 4 and the default delay is 5 seconds, so ((4(attempt) x 2) x 5(default delay)) = 40 seconds.
# If the calculated delay is greater than maxDelay, maxDelay will be used instead of the calculated delay.
* delay: 5
# Maximum delay before the next connection attempt is made.
# Setting it as 0 will disable maxDelay and the delay interval will
# be calculated based on the attempt count and the default delay.
* maxDelay: 30
* unit: seconds
#
# The class used to provide CQL connections to Apache Cassandra.
# The default provider will be used unless another is specified.
#
* provider: com.ericsson.bss.cassandra.ecchronos.application.DefaultNativeConnectionProvider
#
# The class used to provide an SSL context to the NativeConnectionProvider.
# Extending this allows to manipulate the SSLEngine and SSLParameters.
#
* certificateHandler: com.ericsson.bss.cassandra.ecchronos.application.ReloadingCertificateHandler
#
# The class used to decorate CQL statements.
# The default no-op decorator will be used unless another is specified.
#
* decoratorClass: com.ericsson.bss.cassandra.ecchronos.application.NoopStatementDecorator
#
# Allow routing requests directly to a remote datacenter.
# This allows locks for other datacenters to be taken in that datacenter instead of via the local datacenter.
# If clients are prevented from connecting directly to Cassandra nodes in other sites this is not possible.
# If remote routing is disabled, instead SERIAL consistency will be used for those request.
#
* remoteRouting: true
**jmx:**
#
# Host and port properties for JMX.
# Primarily used by the default connection provider.
#
* host: localhost
* port: 7199
#
# The class used to provide JMX connections to Apache Cassandra.
# The default provider will be used unless another is specified.
#
* provider: com.ericsson.bss.cassandra.ecchronos.application.DefaultJmxConnectionProvider

# Repair configuration
# This section defines default repair behavior for all tables.
#
**repair:**
#
# A class for providing repair configuration for tables.
# The default FileBasedRepairConfiguration uses a schedule.yml file to define per-table configurations.
#
* provider: com.ericsson.bss.cassandra.ecchronos.application.FileBasedRepairConfiguration
#
# How often repairs should be triggered for tables.
#
**interval:**
* time: 7
* unit: days
#
# Initial delay for new tables. New tables are always assumed to have been repaired in the past by the interval.
# However, a delay can be set for the first repair. This will not affect subsequent repairs and defaults to one day.
#
**initial_delay:**
* time: 1
* unit: days
#
# The unit of time granularity for priority calculation, can be HOURS, MINUTES, or SECONDS.
# This unit is used in the calculation of priority.
# Default is HOURS for backward compatibility.
# Ensure to pause repair operations prior to changing the granularity.
# Not doing so may lead to inconsistencies as some ecchronos instances
# could have different priorities compared to others for the same repair.
# Possible values are HOURS, MINUTES, or SECONDS.
#
**priority:**
* granularity_unit: HOURS
#
# Specifies the type of lock to use for repairs.
# "vnode" will lock each node involved in a repair individually and increase the number of
# parallel repairs that can run in a single data center.
# "datacenter" will lock each data center involved in a repair and only allow a single repair per data center.
# "datacenter_and_vnode" will combine both options and allow a smooth transition between them without allowing
# multiple repairs to run concurrently on a single node.
#
* lock_type: vnode
#
# Alarms are triggered when tables have not been repaired for a long amount of time.
# The warning alarm is meant to indicate early that repairs are falling behind.
# The error alarm is meant to indicate that gc_grace has passed between repairs.
#
# With the defaults where repairs triggers once every 7 days for each table a warning alarm would be raised
# if the table has not been properly repaired within one full day.
#
**alarm:**
#
# The class used for fault reporting
# The default LoggingFaultReporter will log when alarm is raised/ceased
#
* faultReporter: com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter
#
# If a table has not been repaired for the following duration an warning alarm will be raised.
# The schedule will be marked as late if the table has not been repaired within this interval.
#
**warn:**
* time: 8
* unit: days
#
# If a table has not been repaired for the following duration an error alarm will be raised.
# The schedule will be marked as overdue if the table has not been repaired within this interval.
#
**error:**
* time: 10
* unit: days
#
# Specifies the unwind ratio to smooth out the load that repairs generate.
# This value is a ratio between 0 -> 100% of the execution time of a repair session.
#
# 100% means that the executor will wait to run the next session for as long time as the previous session took.
# The 'unwind_ratio' setting configures the wait time between repair tasks as a proportion of the previous task's execution time.
#
# Examples:
# - unwind_ratio: 0
#   Explanation: No wait time between tasks. The next task starts immediately after the previous one finishes.
#   Total Repair Time: T1 (10s) + T2 (20s) = 30 seconds.
#
# - unwind_ratio: 1.0 (100%)
#   Explanation: The wait time after each task equals its duration.
#   Total Repair Time: T1 (10s + 10s wait) + T2 (20s + 20s wait) = 60 seconds.
#
# - unwind_ratio: 0.5 (50%)
#   Explanation: The wait time is half of the task's duration.
#   Total Repair Time: T1 (10s + 5s wait) + T2 (20s + 10s wait) = 45 seconds.
#
#  A higher 'unwind_ratio' reduces system load by adding longer waits, but increases total repair time.
#  A lower 'unwind_ratio' speeds up repairs but may increase system load.
#
* unwind_ratio: 0.0
#
# Specifies the lookback time for when the repair_history table is queried to get initial repair state at startup.
# The time should match the "expected TTL" of the system_distributed.repair_history table.
#
**history_lookback:**
* time: 30
* unit: days
#
# Specifies a target for how much data each repair session should process.
# This is only supported if using 'vnode' as repair_type.
# This is an estimation assuming uniform data distribution among partition keys.
# The value should be either a number or a number with a unit of measurement:
# 12  (12 B)
# 12k (12 KiB)
# 12m (12 MiB)
# 12g (12 GiB)
#
**size_target:**
#
# Specifies the repair history provider used to determine repair state.
# The "cassandra" provider uses the repair history generated by the database.
# The "upgrade" provider is an intermediate state reading history from "cassandra" and producing history for "ecc"
# The "ecc" provider maintains and uses an internal repair history in a dedicated table.
# The main context for the "ecc" provider is an environment where the ip address of nodes might change.
# Possible values are "ecc", "upgrade" and "cassandra".
#
# The keyspace parameter is only used by "ecc" and "upgrade" and points to the keyspace where the custom
# 'repair_history' table is located.
#
**history:**
* provider: ecc
* keyspace: ecchronos
#
# Specifies if tables with TWCS (TimeWindowCompactionStrategy) should be ignored for repair
#
* ignore_twcs_tables: false
#
# Specifies the backoff time for a job.
# This is the time that the job will wait before trying to run again after failing.
#
**backoff:**
* time: 30
* unit: MINUTES
#
# Specifies the default repair_type.
# Possible values are: vnode, parallel_vnode, incremental
# vnode = repair 1 vnode at a time (supports size_target to split the vnode further, in this case there will be 1 repair session per subrange)
# parallel_vnode = repair vnodes in parallel, this will combine vnodes into a single repair session per repair group
# incremental = repair vnodes incrementally (incremental repair)
#
* repair_type: vnode

**statistics:**
* enabled: true
#
# Decides how statistics should be exposed.
# If all reporting is disabled, the statistics will be disabled as well.
#
**reporting:**
**jmx:**
* enabled: true
#
# The metrics can be excluded on name and on tag values using quoted regular expressions.
# Exclusion on name should be done without the prefix.
# If an exclusion is without tags, then metric matching the name will be excluded.
# If both name and tags are specified, then the metric must match both to be excluded.
# If multiple tags are specified, the metric must match all tags to be excluded.
# By default, no metrics are excluded.
# For list of available metrics and tags refer to the documentation.
#
* excludedMetrics: []
**file:**
* enabled: true
#
# The metrics can be excluded on name and on tag values using quoted regular expressions.
# Exclusion on name should be done without the prefix.
# If an exclusion is without tags, then metric matching the name will be excluded.
# If both name and tags are specified, then the metric must match both to be excluded.
# If multiple tags are specified, the metric must match all tags to be excluded.
# By default, no metrics are excluded.
# For list of available metrics and tags refer to the documentation.
#
* excludedMetrics: []
**http:**
* enabled: true
#
# The metrics can be excluded on name and on tag values using quoted regular expressions.
# Exclusion on name should be done without the prefix.
# If an exclusion is without tags, then metric matching the name will be excluded.
# If both name and tags are specified, then the metric must match both to be excluded.
# If multiple tags are specified, the metric must match all tags to be excluded.
# By default, no metrics are excluded.
# For list of available metrics and tags refer to the documentation.
#
* excludedMetrics: []
* directory: ./statistics
#
# Prefix all metrics with below string
# The prefix cannot start or end with a dot or any other path separator.
#
* prefix: ''

**lock_factory:**
**cas:**
#
# The keyspace used for the CAS lock factory tables.
#
* keyspace: ecchronos
#
# The number of seconds until the lock failure cache expires.
# If an attempt to secure a lock is unsuccessful,
# all subsequent attempts will be failed until
# the cache expiration time is reached.
#
* cache_expiry_time_in_seconds: 30
#
# Allow to override consistency level for LWT (lightweight transactions). Possible values are:
# "DEFAULT" - Use consistency level based on remoteRouting.
# "SERIAL" - Use SERIAL consistency for LWT regardless of remoteRouting.
# "LOCAL" - Use LOCAL_SERIAL consistency for LWT regardless of remoteRouting.
#
# if you use remoteRouting: false and LOCAL then all locks will be taken locally
# in DC. I.e There's a risk that multiple nodes in different datacenters will be able to lock the
# same nodes causing multiple repairs on the same range/node at the same time.
#
* consistencySerial: "DEFAULT"

**run_policy:**
**time_based:**
#
# The keyspace used for the time based run policy tables.
#
* keyspace: ecchronos

**scheduler:**
#
# Specifies the frequency the scheduler checks for work to be done
#
**frequency:**
* time: 30
* unit: SECONDS

**rest_server:**
#
# The host and port used for the HTTP server
#
* host: localhost
* port: 8080
