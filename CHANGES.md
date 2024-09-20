# Changes

## Version 1.0.0 (Not yet Released)

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
