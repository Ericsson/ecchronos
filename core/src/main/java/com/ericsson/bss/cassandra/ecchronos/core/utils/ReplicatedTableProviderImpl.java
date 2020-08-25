/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicatedTableProviderImpl implements ReplicatedTableProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(ReplicatedTableProviderImpl.class);

    private static final String STRATEGY_CLASS = "class";
    private static final String SIMPLE_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";
    private static final String NETWORK_TOPOLOGY_STRATEGY = "org.apache.cassandra.locator.NetworkTopologyStrategy";

    private static final String SIMPLE_STRATEGY_REPLICATION_FACTOR = "replication_factor";

    private static final String SYSTEM_AUTH_KEYSPACE = "system_auth";

    private final Host myLocalhost;
    private final Metadata myMetadata;
    private final TableReferenceFactory myTableReferenceFactory;

    public ReplicatedTableProviderImpl(Host host, Metadata metadata, TableReferenceFactory tableReferenceFactory)
    {
        myLocalhost = host;
        myMetadata = metadata;
        myTableReferenceFactory = tableReferenceFactory;
    }

    @Override
    public Set<TableReference> getAll()
    {
        return myMetadata.getKeyspaces().stream()
                .filter(k -> accept(k.getName()))
                .flatMap(k -> k.getTables().stream())
                .map(tb -> myTableReferenceFactory.forTable(tb.getKeyspace().getName(), tb.getName()))
                .collect(Collectors.toSet());
    }

    @Override
    public boolean accept(String keyspace)
    {
        if (keyspace.startsWith("system") && !SYSTEM_AUTH_KEYSPACE.equals(keyspace))
        {
            return false;
        }

        KeyspaceMetadata keyspaceMetadata = myMetadata.getKeyspace(keyspace);

        if (keyspaceMetadata != null)
        {
            Map<String, String> replication = keyspaceMetadata.getReplication();
            String replicationClass = replication.get(STRATEGY_CLASS);

            switch(replicationClass)
            {
                case SIMPLE_STRATEGY:
                    return validateSimpleStrategy(replication);
                case NETWORK_TOPOLOGY_STRATEGY:
                    return validateNetworkTopologyStrategy(keyspace, replication);
                default:
                    LOG.warn("Replication strategy of type {} is not supported", replicationClass);
                    break;
            }
        }

        return false;
    }

    private boolean validateSimpleStrategy(Map<String, String> replication)
    {
        int replicationFactor = Integer.parseInt(replication.get(SIMPLE_STRATEGY_REPLICATION_FACTOR));

        return replicationFactor > 1;
    }

    private boolean validateNetworkTopologyStrategy(String keyspace, Map<String, String> replication)
    {
        String localDc = myLocalhost.getDatacenter();

        if (localDc == null)
        {
            LOG.error("Local data center is not defined, ignoring keyspace {}", keyspace);
            return false;
        }

        if (!replication.containsKey(localDc))
        {
            LOG.debug("Keyspace {} not replicated by local node, ignoring.", keyspace);
            return false;
        }

        return definedReplicationInNetworkTopologyStrategy(replication) > 1;
    }

    private int definedReplicationInNetworkTopologyStrategy(Map<String, String> replication)
    {
        int replicationFactor = 0;

        for (Map.Entry<String, String> replicationEntry : replication.entrySet())
        {
            if (!STRATEGY_CLASS.equals(replicationEntry.getKey()))
            {
                replicationFactor += Integer.parseInt(replicationEntry.getValue());
            }
        }

        return replicationFactor;
    }
}
