# Architecture



## Overview
ecChronos is built to be continuously repairing data in the background. Each ecChronos instance keeps track of the repairs
of a single cassandra node. A lock table in cassandra makes sure that only a subset of repairs run at any one time. Repairs
can be configured to run only during certain time periods or not at all. Settings for backpressure are provided to
make sure repair is spread out over the interval time while alarms are provided to signal when a job has not run for
longer than expected.

## Concepts

### Leases

In order to perform distributed scheduling ecChronos utilize two things `deterministic priorities` and `distributed leases`.
Deterministic priorities means that all nodes use the same algorithm to decide how important the local work is.
By having the priorities deterministic it is possible to compare the priority between nodes and get a fair scheduling.
Each time a node wants to perform some work a lease needs to be acquired for the node.
The lease should typically go to the node with the highest priority.

The default implementation of leases in ecChronos is based on CAS (Compare-And-Set) with Apache Cassandra as backend.
When the local node tries to obtain a lease it first announces its own priority and check what other nodes have announced.
If the local node has the highest priority it will try to obtain the lease.
The announcement is done to avoid node starvation and to try to promote the highest prioritized work in the cluster.

The leases are created with a TTL of 10 minutes to avoid locking in case of failure.
As some jobs might take more than 10 minutes to run the lease is continuously updated every minute until the job finishes.

### Scheduling flow

The scheduling in ecChronos is handled by the `schedule manager`.
The schedule manager is responsible to keep track of the local work queue,
check with run policies if a job should run and also to acquire the leases for the jobs before running them.

### Scheduled jobs

The work a node needs to perform is split into different jobs.
The most common example of a job is to keep a single table repaired from the local nodes point of view.
The priority of the job is calculated based on the repair history in `system_distributed.repair_history`.
The history is used to determine when the repair should run.
This also has the effect that repairs performed outside of the local ecChronos instance would be included towards the progress.

When the job is executed the work is split into one or more tasks.
In the case of repairs one task could correspond to the repair of one virtual node.
When all virtual nodes are repaired the job is considered to be finished and will be added back to the work queue.

As repair is a resource intensive operation the leases are used to make sure that a node is only part of one repair at a time.
It is configurable if the leases should be on a _data center level_ or on a _node level_.

### Run policies

Run policies are used to prevent jobs from running.
Before a job is started the run policies are consulted to see if it is appropriate for the job to run at this time.

The default implementation is time based and reads configuration from a table in Apache Cassandra.

### Repair scheduling

The repair scheduling begins by providing a [RepairConfiguration](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/RepairConfiguration.java) to the [RepairScheduler](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/RepairSchedulerImpl.java).
The repair scheduler then creates a [TableRepairJob](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/TableRepairJob.java) and schedules it using the [ScheduleManager](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/scheduling/ScheduleManagerImpl.java).

Each table keeps a representation of the repair history in the [RepairState](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/state/RepairStateImpl.java).
This information is used to determine when the table is eligable for the next repair and when to send alarms if necessary.

When a table is able to run repair the RepairState calculates the next tokens to repair and collects it in an ordered list of [ReplicaRepairGroups](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/state/ReplicaRepairGroup.java).
The calculation is performed by the [VnodeRepairGroupFactory](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/state/VnodeRepairGroupFactory.java) by default.
The TableRepairJob then generates [RepairGroups](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/RepairGroup.java) which are snapshots from how the state was when it was calculated.
When the RepairGroup is executed it will generate one [RepairTask](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/RepairTask.java) per token range to repair.
The RepairTask is the class that will perform the repair.