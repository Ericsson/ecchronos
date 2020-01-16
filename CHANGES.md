# Changes

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
