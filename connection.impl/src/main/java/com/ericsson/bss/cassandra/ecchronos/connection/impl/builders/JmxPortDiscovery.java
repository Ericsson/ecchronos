/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;

import java.util.Objects;

public final class JmxPortDiscovery
{
    private static final int DEFAULT_PORT = 7199;

    private JmxPortDiscovery()
    {
    }

    /**
     * Discover the JMX port for a node by querying system_views.system_properties.
     * Falls back to the default port (7199) if the query fails.
     */
    public static int getPort(final CqlSession session, final Node node)
    {
        try
        {
            SimpleStatement stmt = SimpleStatement
                    .builder("SELECT value FROM system_views.system_properties WHERE name = 'cassandra.jmx.remote.port';")
                    .setNode(node)
                    .build();
            Row row = session.execute(stmt).one();
            if (row == null || row.getString("value") == null)
            {
                stmt = SimpleStatement
                        .builder("SELECT value FROM system_views.system_properties WHERE name = 'cassandra.jmx.local.port';")
                        .setNode(node)
                        .build();
                row = session.execute(stmt).one();
            }
            if (row != null && row.getString("value") != null)
            {
                return Integer.parseInt(Objects.requireNonNull(row.getString("value")));
            }
            return DEFAULT_PORT;
        }
        catch (AllNodesFailedException e)
        {
            return DEFAULT_PORT;
        }
    }
}
