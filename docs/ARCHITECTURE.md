# Architecture

## Summary
- [Overview](#overview)
- [Concepts](#concepts)
    - [Leases](#leases)
    - [Scheduling flow](#scheduling-flow)
    - [Scheduled jobs](#scheduled-jobs)
    - [Run policies](#run-policies)
    - [Repair scheduling](#repair-scheduling)
        - [Vnode repairs](#vnode-repairs)
- [Sub-range repairs](#sub-range-repairs)
    - [Example](#example)
        - [Repair history](#repair-history)
- [Incremental repairs](#incremental-repairs)
- [References](#references)

## Overview
ecChronos is built to be continuously repairing data in the background. Each ecChronos instance keeps track of the repairs
of a single cassandra node. A lock table in cassandra makes sure that only a subset of repairs run at any one time. Repairs
can be configured to run only during certain time periods or not at all. Settings for backpressure are provided to
make sure repair is spread out over the interval time while alarms are provided to signal when a job has not run for
longer than expected.

<div align="center">

  ```mermaid
  flowchart RL
      A(((Cassandra Node)))---B(((ecChronos Instance)));
      A(((Cassandra Node)))---C(((ecChronos Instance)));
      B(((ecChronos Instance)))---D(((Cassandra Node)));
      C(((ecChronos Instance)))---E(((Cassandra Node)));
      D(((Cassandra Node)))---F(((ecChronos Instance)));
      E(((Cassandra Node)))---G(((ecChronos Instance)));
      F(((ecChronos Instance)))---H(((Cassandra Node)));
      G(((ecChronos Instance)))---H(((Cassandra Node)));
  ```

  <figcaption>Figure 1: ecChronos and Cassandra Nodes.</figcaption>
</div>

## Concepts

### Leases

In the context of Apache Cassandra, a "lease" refers to a mechanism employed during the repair process within Cassandra.

Upon the initiation of a repair in a Cassandra cluster, it's granted to one of the cluster nodes, which serves as the repair coordinator. This coordinating node is responsible for overseeing the repair process to ensure that all data replicas conform and any disparities are addressed.

It bestows upon the coordinating node the exclusive right to conduct repair during a specific time frame. Within this duration, coordinating node assesses the data, ensuring that all replicas are updated and consistent. Additionally, it helps prevent multiple nodes from concurrently initiating repairs on the same data, thereby mitigating potential consistency issues and cluster overload.

Once the repair is completed by the coordinating node, the "lease" is released, enabling other nodes to request and carry out their own repairs as needed. It helps in efficiently distributing the repair load within the cluster [\[1\]](#references).

<div align="center">

  ```mermaid
  flowchart TB
    A[Create Lease] -->|Failed| b[Sleap]
    b[Sleap] --> A[Create Lease]
    A[Create Lease] -->|Succeeded| c[Become Master]
    c[Become Master] --> D[Periodically Renew Lease]
    D[Periodically Renew Lease] -->|Failed| A[Create Lease]
    D[Periodically Renew Lease] -->|Succeeded|D[Periodically Renew Lease]
  ```

  <figcaption>Figure 2: Lease Typically Election.</figcaption>
</div>

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

<div align="center">

  ```mermaid
  flowchart LR
      A(((Local Node))) --> a[DeclarePriority]
      B(((Other Node))) --> b[DeclarePriority]
      a[DeclarePriority] --> CheckOtherNodePriority
      b[DeclarePriority] --> CheckOtherNodePriority
      CheckOtherNodePriority --> ObtainTheLease
  ```

  <figcaption>Figure 3: Compare-And-Set.</figcaption>
</div>

### Scheduling flow

The scheduling in ecChronos is handled by the `schedule manager`.
The schedule manager is responsible to keep track of the local work queue, check with run policies if a job should run and also to acquire the leases for the jobs before running them.

### Scheduled jobs

The work a node needs to perform is split into different jobs.
The most common example of a job is to keep a single table repaired from the local nodes point of view.
The priority of a job is calculated based on the last time the table was repaired.
Repairs performed outside of the local ecChronos instance would be included towards the progress.

When the job is executed the work is split into one or more tasks.
In the case of repairs one task could correspond to the repair of one virtual node.
When all virtual nodes are repaired the job is considered to be finished and will be added back to the work queue.

As repair is a resource intensive operation the leases are used to make sure that a node is only part of one repair at a time.
It is configurable if the leases should be on a _data center level_ or on a _node level_.

### Run policies

Run policies are used to prevent jobs from running.
Before a job is started the run policies are consulted to see if it is appropriate for the job to run at this time.

The default implementation is time based and reads configuration from a table in Apache Cassandra.
For more information about time based run policy refer to [Time based run policy](TIME_BASED_RUN_POLICY.md)

### Repair scheduling

The repair scheduling begins by providing a [RepairConfiguration](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/RepairConfiguration.java) to the [RepairScheduler](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/RepairSchedulerImpl.java).
The repair scheduler then creates a [TableRepairJob](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/TableRepairJob.java)
or [IncrementalRepairJob](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/IncrementalRepairJob.java)
and schedules it using the [ScheduleManager](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/scheduling/ScheduleManagerImpl.java) [\[2\]](#references).

<div align="center">

  ```mermaid
  stateDiagram
      direction LR
      state RepairScheduler {
        direction LR
        RepairConfiguration1
        RepairConfiguration2
        RepairConfiguration3
      }
      state SchedulerManager {
        [...]
      }
      state ShouldJobRunCondition <<choice>>
      RepairScheduler --> CreateRepairJob
      CreateRepairJob --> SchedulerManager
      SchedulerManager --> RefreshPriorities
      RefreshPriorities --> PickJobWithHighestPriority
      PickJobWithHighestPriority --> ShouldJobRun
      ShouldJobRun --> ShouldJobRunCondition
      ShouldJobRunCondition --> PickJobWithHighestPriority: No
      ShouldJobRunCondition --> CreateJobTask: Yes
      CreateJobTask --> ExecuteTasks
      ExecuteTasks --> SchedulerManager
  ```

  <figcaption>Figure 4: Scheduling flow.</figcaption>

</div>

#### Vnode repairs

Each TableRepairJob keeps a representation of the repair history in the [RepairState](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/state/RepairStateImpl.java).
This information is used to determine when the table is eligible for the next repair and when to send alarms if necessary.

When a table is able to run repair the RepairState calculates the next tokens to repair and collects it in an ordered list of [ReplicaRepairGroups](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/state/ReplicaRepairGroup.java).
The calculation is performed by the [VnodeRepairGroupFactory](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/state/VnodeRepairGroupFactory.java) by default.
The TableRepairJob then generates [RepairGroups](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/RepairGroup.java) which are snapshots from how the state was when it was calculated.
When the RepairGroup is executed it will generate one [VnodeRepairTask](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/VnodeRepairTask.java) per token range to repair.
The VnodeRepairTask is the class that will perform the repair.

## Sub-range repairs

As of [#96][i96] the repair scheduler in ecChronos has support for sub range repairs within virtual nodes.
This is activated by specifying a target repair size in the [RepairConfiguration](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/RepairConfiguration.java).
For the standalone version the option is called `repair.size.target` in the configuration file.
Each sub-range repair session will aim to handle the target amount of data.

*Note: Without this option specified the repair mechanism will handle full virtual nodes only (including how it interprets the repair history)*  
*Note: The target repair size is assuming a uniform data distribution across partitions on the local node*

### Example  
With a local table containing 100 bytes of data and a total of 100 tokens locally in a continuous range (0, 100].
When the repair target is set to 2 bytes the range will be divided into 50 sub ranges, each handling two tokens.
The sub ranges would be:  
(0, 2]  
(2, 4]  
...  
(96, 98]  
(98, 100]

#### Repair history


Sub-ranges are handled in two parts, one part is building an internal state of the repair history and the other is performing the repairs.
While building the internal repair history state all sub-ranges which are fully contained within a local virtual node are collected from the repair history.
This means that for a virtual node (1, 5] it will collect ranges such as (1, 3] and (2, 4].
It will not collect (0, 3] since it is not fully contained in the virtual node even though it is intersecting it.

As the sub-range repair mechanism is using dynamic sizes of the sub-ranges there is a need of handling overlapping sub-ranges.
E.g. there could be entries for both (1, 3] and (2, 4] within one virtual node that has been repaired at different times.
This is handled by splitting the history into (1, 2], (2, 3] and (3, 4] where the middle range gets the latest repair time of the two.
In order to keep the memory footprint small these ranges are later consolidated where adjacent ranges that has been repaired closely together are merged.

In a larger context this also works for repairs covering the full virtual node.
Given a virtual node (0, 30] that was repaired at timestamp X and a repair history entry containing the sub range (15, 20] repaired at Y.
Assuming that X is more than one hour before Y this will produce three sub ranges in the internal representation:

* (0, 15] repaired at X.
* (15, 20] repaired at Y
* (20, 30] repaired at X5f

## Incremental repairs

**Incremental repairs do not use ecchronos repair history**

Each IncrementalRepairJob uses metrics from Cassandra `maxRepairedAt` and `percentRepaired`.
This information is used to determine when the job is eligible for the next repair and when to send alarms if necessary.
The job jumps over intervals if there's nothing to repair, i.e `percentRepaired` is 100%.

When the job runs, it calculates the replicas that might be involved in the repair using
[ReplicationStateImpl](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/state/ReplicationStateImpl.java).
Afterwards a single [RepairGroups](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/RepairGroup.java) is created.
When the RepairGroup is executed it will generate one [IncrementalRepairTask](../core/src/main/java/com/ericsson/bss/cassandra/ecchronos/core/repair/IncrementalRepairTask.java).
The IncrementalRepairTask is the class that will perform the incremental repair [\[3\]](#references).

[i96]: https://github.com/Ericsson/ecchronos/issues/96

## References
 [1\]: [Consensus on Cassandra](https://www.datastax.com/blog/consensus-cassandra);

 [2\]: [Incremental and Full Repairs](https://cassandra.apache.org/doc/latest/cassandra/operating/repair.html#incremental-and-full-repairs)

 [3\]: [Cassandra Metrics](#https://cassandra.apache.org/doc/4.1/cassandra/operating/metrics.html#table-metrics)
