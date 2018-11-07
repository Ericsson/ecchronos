# Architecture

ecChronos is designed to run as a helper application next to each instance of Apache Cassandra.
It handles maintenance operations for the local node.

## Concepts

In order to perform distributed scheduling ecChronos utilize two things `deterministic priorities` and `distributed leases`.
Deterministic priorities means that all nodes use the same algorithm to decide how important the local work is.
By having the priorities deterministic it is possible to compare the priority between nodes and get a fair scheduling.
Each time a node wants to perform some work a lease needs to be acquired for the node.
The lease should typically go to the node with the highest priority.

### Scheduled jobs

The work a node needs to perform is split into different jobs.
One example of a job is to keep a single table repaired from the local nodes point of view.
The priority of the job is calculated based on the repair history in `system_distributed.repair_history`.
The history is used to determine when the repair should run.
This also has the effect that repairs performed outside of the local ecChronos instance would be included towards the progress.

When the job is executed the work is split into one or more tasks.
In the case of repairs one task could correspond to the repair of one virtual node.
When all virtual nodes are repaired the job is considered to be finished and will be added back to the work queue.

As repair is a resource intensive operation the leases are on a _data center level*_.
This means that only a single repair session may run in the data center at a time.

_\* Note: There are plans to change this to node level leases to improve concurrency_

### Leases

The default implementation of leases in ecChronos is based on CAS (Compare-And-Set) with Apache Cassandra as backend.
When the local node tries to obtain a lease it first announces its own priority and check what other nodes have announced.
If the local node has the highest priority it will try to obtain the lease.
The announcement is done to avoid node starvation and to try to promote the highest prioritized work in the cluster.

The leases are created with a TTL of 10 minutes to avoid locking in case of failure.
As some jobs might take more than 10 minutes to run the lease is continuously updated every minute until the job finishes.

### Run policies

Run policies are used to prevent jobs from running.
Before a job is started the run policies are consulted to see if it is appropriate for the job to run at this time.

The default implementation is time based and reads configuration from a table in Apache Cassandra.

## Scheduling flow

The scheduling in ecChronos is handled by the `schedule manager`.
The schedule manager is responsible to keep track of the local work queue,
check with run policies if a job should run and also to acquire the leases for the jobs before running them.
