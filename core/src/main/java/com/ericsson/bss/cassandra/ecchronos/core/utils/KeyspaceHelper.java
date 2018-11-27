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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

/**
 * A helper class for keyspace related operations.
 */
public final class KeyspaceHelper
{
    private static final String REPLICATION_CLASS = "class";

    private KeyspaceHelper()
    {
        // No instances
    }

    /**
     * Get the data centers that the keyspace replicates data in.
     *
     * @param metadata
     *            The cluster metadata.
     * @param keyspace
     *            The keyspace
     * @return The set of data centers that the keyspace replicates data to.
     */
    public static Set<String> getDatacentersForKeyspace(Metadata metadata, String keyspace)
    {
        Set<String> datacenters = new HashSet<>();

        Map<String, String> replication = metadata.getKeyspace(keyspace).getReplication();
        String replicationClass = replication.get(REPLICATION_CLASS);

        if (replicationClass.endsWith("NetworkTopologyStrategy"))
        {
            for (String key : replication.keySet())
            {
                if (!REPLICATION_CLASS.equals(key))
                {
                    datacenters.add(key);
                }
            }
        }
        else
        {
            for (Host host : metadata.getAllHosts())
            {
                datacenters.add(host.getDatacenter());
            }
        }

        return datacenters;
    }

    /**
     * Get all the data centers in the cluster.
     *
     * @param metadata
     *            The cluster metadata.
     * @return The set of data centers available.
     */
    public static Set<String> getDatacenters(Metadata metadata)
    {
        Set<String> datacenters = new HashSet<>();
        for (Host host : metadata.getAllHosts())
        {
            datacenters.add(host.getDatacenter());
        }
        return datacenters;
    }
}
