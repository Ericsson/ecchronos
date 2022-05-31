# Repair Monitoring

Once ecChronos is up and running its progress can be monitored by [metrics](METRICS.md), status commands or via log files,
see below depending on deployment.

## Standalone

The following commands (placed in the `bin` directory) are available:

| Command                                                       | Description                                         |
|---------------------------------------------------------------|-----------------------------------------------------|
| `ecctool repairs`                                             | Repair status overview for all tables               |
| `ecctool repairs --keyspace <keyspace> --table <table>`       | Detailed repair status for a given table            |
| `ecctool schedules`                                           | Schedule status overview for all tables             |
| `ecctool schedules -i <id> --full`                            | Gives more detailed information on a given schedule |
| `ecctool schedules --keyspace <keyspace> --table <table>`     | Detailed schedule status for a given table          |

Logging is configured in `conf/logback.xml`. By default `ecc.log` and `ecc.debug.log` files will be produce.
As the naming suggests - the debug-log contains more information but entries will be rotated out faster as the logs grow.

## OSGI

If running ecChronos inside a Karaf container the following console-commands are available:

| Command               | Description                           |
|-----------------------|---------------------------------------|
| `repair:status`       | Repair status overview for all tables |
| `repair:table-status` | Detailed status for a given table     |
| `repair:config`       | Configuration for all tables          |

Logging is configured in the OSGI runtime environment. By default for Karaf, ecChronos will log to `karaf.log`.
