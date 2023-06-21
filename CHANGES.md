# Changes

## Version 1.2.1

* Repairs not scheduled when statistics disabled - Issue #175

### Merged from 1.0

* Fix malformed IPv6 for JMX - Issue #306
* Step karaf to 4.2.8
* Improve Alarm logging - Issue #191

## Version 1.2.0

### Merged from 1.0

* Locks get stuck when unexpected exception occurs - Issue #177

## Version 1.1.2

* Add support for sub range repairs - Issue #96

### Merged from 1.0

* Add Code Style - Issue #103
* Avoid using concurrent map - Issue #101
* Avoid concurrent modification exception in RSI#close - Issue #99
* Support symlink of ecc binary - PR #114
* Close file descriptors in background mode - PR #115
* Add JVM options file
* Make policy changes quicker - Issue #117

## Version 1.1.1

### Merged from 1.0

* Reduce cache refresh time in TimeBasedRunPolicy to quicker react to configuration changes

## Version 1.1.0

* Add Karaf commands that exposes repair status

### Merged from 1.0

* Fix slow query of repair_history at start-up #86

## Version 1.0.4

* Reduce error logs to warn for some lock related failures

## Version 1.0.3

* Trigger table repairs more often - Issue #72
* Cache locking failures to reduce unnecessary contention - Issue #70

## Version 1.0.2

* Locking failures log too much - Issue #58
* Fix log file rotation - Issue #61
* Correct initial replica repair group - Issue #60
* Fix statistics when no table/data to repair - Issue #59

## Version 1.0.1

* Dynamic license header check - Issue #37
* Unwind ratio getting ignored - Issue #44
* Reduce memory footprint - Issue #54

## Version 1.0.0

* First release
