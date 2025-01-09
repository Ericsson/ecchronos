# Compatibility

## Cassandra

Below matrix defines which ecChronos agent versions have been tested and verified with which Cassandra version.

| ecchronos version      | Cassandra 3.0.X | Cassandra 3.11.X | Cassandra 4.0.X | Cassandra 4.1.X | Cassandra 5.0-alpha1 |
|------------------------|-----------------|------------------|-----------------|-----------------|----------------------|
| &gt;= 1.0.0            |                 |                  | X               | X               | X                    |

## Jolokia

In ecChronos, a new feature has been introduced via issue #831, enabling the use of dynamic notification listeners in conjunction with Jolokia. This enhancement ensures that notifications can be monitored dynamically using Jolokia without affecting the existing architecture of RepairTask, which is implemented as a NotificationListener.

### Background

Traditionally, JMX notification listeners were registered using the following method:

```java
MBeanServerConnection().addNotificationListener(objectName, listener, filter, handback);
```

However, this approach is not supported by Jolokia, which relies on HTTP and JSON-based interactions instead of direct Java method calls. With Jolokia version 2 or later, it is now possible to add notification listeners dynamically using its JSON or JavaScript interface.

In the context of ecChronos, the RepairTask component is tightly coupled to JMX notification listeners, and integrating it with Jolokia required a new approach that preserved the architecture while extending functionality.

### Compatibility Requirements

This feature requires Jolokia version 2 or later. Additionally, any environment running this implementation, including Cassandra, must ensure that the Jolokia agent is updated to version 2 or above. Older versions of Jolokia do not support the dynamic listener functionality.

### Implementation Overview

This pull request introduces a dynamic notification listener feature that allows ecChronos to handle notifications via Jolokia seamlessly. The feature works by:

- Leveraging Jolokia's JSON interface to register listeners dynamically.
- Ensuring compatibility with the RepairTask implementation as a NotificationListener.
- Bridging the gap between traditional JMX listener registration and Jolokia's HTTP-based operations.
- This enhancement does not disrupt the core architecture of RepairTask, maintaining its design integrity while enabling modernized handling of notifications.
