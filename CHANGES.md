# Changes

## Version 2.0.0

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

### Merged from 1.2

* Add support for sub range repairs - Issue #96

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
