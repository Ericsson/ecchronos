# Changes

## Version 1.0.0 (Not yet Released)

* Process multiple jobs per scheduler tick to improve throughput - Issue #1479
* Add host id to alarms - Issue #1491
* Fix alarm not raised when CQL session is broken - Issue #1477
* Intermittent Jolokia Communication Failures Cause Ecchronos to Incorrectly Mark Repairs as Failed - Issue #1473
* Reduce log noise: downgrade "Node not managed by local instance" messages from INFO to DEBUG - Issue #1469

## Version 1.0.0-beta4

* Repair schedules not updated when cluster topology changes (scale-out/scale-in) - Issue #1460
* Return default values if schema changes in Cassandra have not been completed - Issue #1458
* Jolokia connection does not reload certificates after expiry - Issue #1441
* Catch exception from older versions of Jolokia Agents - Issue #1440
* Rework the i, n, and j flags to keep backward compatibility - Issue #1430
* Add missing error messages in ecctool - Issue #1431

## Version 1.0.0-beta3

* Jolokia connection fails when TLS is enabled - Issue #1415
* Log yaml config at startup
* Improve concurrency in IpTranslator
* Create DatacenterAware Integration tests with Multi Agent setup - Issue #1382
* Strip port number from internal ip address in IpTranslator
* Fix stale JMX connections when a node restarts with a new IP address - Issue #1400
* Reftresh translation map in IpTranslator if maping is missing - Issue #1393
* Refactor Test Infrastructure to Support Multiple ecChronos Agent Instances - Issue #1371
* Migrate old test strategy to use containers instead of in host process - Issue #1349
* Add JSON output and columns filtering - Issue #1225
* Add extra exceptions to be caught for Jolokia jmx extensions - Issue #1324
* Ensure Jolokia connection does not hang forever during connection - Issue #1337
* Add Configurable Option to Disable TLS Hostname Verification in JMX/Jolokia Connections - Issue #1333
* Add ignore-unreplicated-keyspaces to avoid nothing to repair error message - Issue #1323

## Version 1.0.0-beta2

* Fix nullpointer issues in IpTranslator
* Make sure to close CqlSessions if can't get a fully working connection to the cluster
* Add option to translate broadcastRPCAddress to RPCAddress for jmx connections - Issue #1283
* ecChronos remains in not running status - Issue #1301
* On-demand repair fails with ERROR status instead of being blocked when repairs are disabled - Issue #1289

## Version 1.0.0-beta1

* Add runDelay and healthCheckInterval parameters to tune the repair connection durinbg repairs - Issue #1281
* Fix MalformedURLException for IPv6 addresses - Issue #1264
* Add timeout config option - Issue #1279
* Add Rejections sub-command - Issue #1015
* Investigate Creation of a REST Endpoint to Disable Repairs at DC Level - Issue #1009
* JMX Connection Using PEM Certificates Not Always Successful - Issue #1277
* Add Resolve Reverse DNS Logic to Jolokia Notification Controller - Issue #1262
* Add PEM Certificate Support for Jolokia TLS Configuration - Issue #1153
* Enable alarms in ecchronos agent - Issue #1108
* Add statistics configuration and metrics implementation - Issue #1191
* Restore retryPolicy location in ecc.yml to maintain backward compatibility - Issue 1180
* An invalid node id should throw a useful exception - Issue #1199
* Manual repair to obey disabled tables - Issue #1109
* Resolve initial contact points at each connection attempt - Issue #1178
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
