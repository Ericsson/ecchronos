# Changes

## Version 4.0.0

* Add metric for failed and succeeded repair tasks - Issue #295
* Migrate to datastax driver-4.14.1 - Issue #269

## Version 3.0.0

* Add support for repairs without keyspace/table - Issue #158
* Add Cassandra health indicator and enable probes - Issue #192
* Add support for clusterwide repairs - Issue #299
* Add custom HTML error page
* Make fault reporter pluggable
* Fix JMX URI validation with new Java versions - Issue #306
* Reworked rest interface - Issue #257
* Bump springboot from 2.6.4 to 2.6.6
* Offset next runtime with repair time taken - Issue #121
* Remove deprecated scripts (ecc-schedule, ecc-status, ecc-config)
* Add blocked status - Issue #284

## Version 2.0.6

* Add ecc-schedule command - Issue #158
* Consolidate all scripts into single script with subcommands - Issue #170
* Bump springboot from 2.5.6 to 2.6.4 (CVE-2021-22060, CVE-2022-23181)
* Bump logback from 1.2.3 to 1.2.10 (CVE-2021-42550)
* Bump apache-karaf from 4.2.8 to 4.3.6 (CVE-2021-41766, CVE-2022-22932)
* Always create pidfile - Issue #253
* Sort repair-status on newest first - Issue #261

## Version 2.0.5

* Step log4j-api version to 2.15.0 - Issue #245

## Version 2.0.4

* Handle endpoints with changing IP addresses - Issue #243
* Log exceptions during creation/deletion of schedules
* Update gson to 2.8.9 - Issue #239
* Update netty to 4.1.69.Final
* Many exceptions if Ongoing Repair jobs cannot be fetched - Issue #236
* Update springbootframework to 2.5.6 - Issue #233
* Update netty to 4.1.68.Final - Issue #228
* Schedules are not deleted when dropping tables - Issue #230
* Step simpleclient to 0.10.0

## Version 2.0.3

* Fixed signing issue of karaf feature artifact

## Version 2.0.2 (Not Released)

* Support certificatehandler pluggability
* Improve logging - Issue #191
* Fix On Demand Repair Jobs always showing topology changed after restart
* Fix reoccurring flag in ecc-status showing incorrect value
* Update Netty io version to 4.1.62.Final
* Update commons-io to 2.7
* Update spring-boot-dependencies to 2.5.3 - Issue #223

### Merged from 1.0

* Step karaf to 4.2.8
* Improve Alarm logging - Issue #191

## Version 2.0.1

* Add possibilities to only take local locks - Issue #175
* Remove default limit of ecc-status
* Process hangs on timeout - Issue #190

## Version 2.0.0

* OnDemand Job throws NPE when scheduled on non-existing table/keyspace - Issue #183

### Merged from 1.2

* Repairs not scheduled when statistics disabled - Issue #176

## Version 2.0.0-beta

* Add Code Style - Issue #103
* Avoid using concurrent map - Issue #101
* Move alarm handling out of TableRepairJob
* Add State to each ScheduledJob
* Change executable file name from ecChronos to ecc
* Change configuration file name from ecChronos.cfg to ecc.cfg
* Add RepairJobView
* Add HTTP server with REST API - Issue #50
* Expose metrics through JMX - Issue #75
* Add ecc-config command that displays repair configuration
* Remove usage of joda time - Issue #95
* Proper REST interface - Issue #109
* Make scheduler interval configurable - Issue #122
* Add manual repair - Issue #14
* Use yaml format for configuration - Issue #126
* Use springboot for REST server - Issue #111
* Add Health Endpoint - Issue #131
* Use table id in metric names - Issue #120
* Add authentication for CQL and JMX - Issue #129
* Add TLS support for CQL and JMX - Issue #129
* Expose Springboot configuration - Issue #149
* Per table configurations - Issue #119

### Merged from 1.2

* Add support for sub range repairs - Issue #96
* Locks get stuck when unexpected exception occurs - Issue #177

### Merged from 1.1

* Add Karaf commands that exposes repair status

### Merged from 1.0

* Dynamic license header check - Issue #37
* Unwind ratio getting ignored - Issue #44
* Reduce memory footprint - Issue #54
* Locking failures log too much - Issue #58
* Fix log file rotation - Issue #61
* Correct initial replica repair group - Issue #60
* Fix statistics when no table/data to repair - Issue #59
* Cache locking failures to reduce unnecessary contention - Issue #70
* Trigger table repairs more often - Issue #72
* Reduce error logs to warn for some lock related failures
* Fix slow query of repair_history at start-up #86
* Reduce cache refresh time in TimeBasedRunPolicy to quicker react to configuration changes
* Avoid concurrent modification exception in RSI#close - Issue #99
* Support symlink of ecc binary - PR #114
* Close file descriptors in background mode - PR #115
* Add JVM options file
* Make policy changes quicker - Issue #117

## Version 1.0.0

* First release
