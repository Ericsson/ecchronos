# Changes

## Version 1.0.0 (Not yet Released)

* Add statistics configuration and metrics implementation Issue #1191
* An invalid node id should throw a useful exception Issue #1199
* Manual repair to obey disabled tables Issue #1109
* Resolve initial contact points at each connection attemt - Issue #1178
* Remove DatacenterAwarePolicy - Issue #1128
* Run Repair --all Fails With "Repair Request Failed" in DatacenterAware Mode - Issue #1158
* Separate Lock Failure Cache by Node to Prevent Cross-Node Lock Blocking - Issue #1159
* On-demand repair jobs in multi-agent environments may be executed by the wrong agent - Issue #1146
* ecchronos manual repairs via ecctool should obey the ignore_twcs_tables flag - Issue #1107
* Fail During Repair with Custom Jolokia Port - Issue #1149
* Create Integration Tests Using the New Jolokia Functionality - Issue #845
* ClassCastException when using Jolokia for JMX metrics retrieval - Issue #1126
* ecc.sh in evolved ecchronos uses old SpringBooter package reference - Issue #1123
* ecctool with node and keyspace gives an error -Issue #1055
* REST Interface to provide list of nodes - Issue #1045
* Fix REST interface only shows the schedules for the last table added - Issue #1030
* Enable Multithreading Support in the EcChronos Agent for Node Operations - Issue #959
* Modify ecctool to Allow Repairs to be Created Without Requiring a Node ID - Issue #903
* Create ecchronos-binary Module with ecctool for Interacting with the ecChronos REST API - Issue #867
* Update TestContainers Template to Enable or Disable Jolokia in Cassandra - Issue #844
* Reconcile Jolokia Notification Listener Implementation with RepairTask - Issue #831
* Introduce REST Module for Scheduling and Managing Cassandra Repairs - Issue #771
* Create On Demand Repair Job on Agent - Issue #775
* Modify DistributedNativeConnectionProvider to Return a Map<UUID, Node> - Issue #778
* Bump Spring, Tomcat, Jackson and other dependencies to Remove Vulnerabilities in Agent - Issue #776
* Add Locks In SchedulerManager - Issue #768
* Cassandra-Based Distributed Locks - Issue #741
* Create New Repair Type Called "VNODE" - Issue #755
* Create ReplicaRepairGroup Class for Grouping Replicas and Token Ranges - Issue #721
* Hot Reload of Nodes List - Issue #699
* Investigate Creation of RepairScheduler and ScheduleManager - Issue #714
* Implement ScheduledJobQueue for Prioritized Job Management and Execution - Issue #740
* Implement RepairGroup Class for Managing and Executing Repair Tasks - Issue #738
* Create IncrementalRepairTask Class - Issue #736
* Implement ScheduledRepairJob, ScheduledJob and ScheduledTask for Automated Recurring Task Scheduling in Cassandra - Issue #737
* Create RepairTask Abstract Class to Handle Repair Operations - Issue #717
* Create ReplicationState and ReplicationStateImpl Class for Managing Token-to-Replicas Mapping - Issue #722
* Create a RepairHistory to Store Information on Repair Operations Performed by ecChronos Agent - Issue #730
* Generate Unique EcChronos ID - Issue #678
* Create RepairConfiguration class for repair configurations - Issue #716
* Create DistributedJmxProxy and DistributedJmxProxyFactory - Issue #715
* Create a New Maven Module "utils" for Common Code Reuse - Issue #720
* Implement ReplicationStateImpl to Manage and Cache Token Range to Replica Mappings - Issue #719
* Implement NodeResolverImpl to Resolve Nodes by IP Address and UUID - Issue #718
* Specify Interval for Next Connection - Issue #674
* Retry Policy for Jmx Connection - Issue #700
* Update Architecture and Tests Documentations to Add the Agent Features and The cassandra-test-image - Issue #707
* Enhance Test Infrastructure by Adding Cassandra-Test-Image Module With Multi-Datacenter Cluster and Abstract Integration Test Class - Issue #706
* Investigate Introduction of testContainers - Issue #682
* Create EccNodesSync Object to Represent Table nodes_sync - Issue #672
* Expose AgentJMXConnectionProvider on Connection and Application Module - Issue #676
* Create JMXAgentConfig to add Hosts in JMX Session Through ecc.yml - Issue #675
* Expose AgentNativeConnectionProvider on Connection and Application Module - Issue #673
* Create DatacenterAwareConfig to add Hosts in CQL Session Through ecc.yml - Issue #671
* Create Initial project Structure for Agent - Issue #695
* Add CRL support for CQL connections - Issue #883
