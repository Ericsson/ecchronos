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
package com.ericsson.bss.cassandra.ecchronos.connection;

import java.io.IOException;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;

import com.datastax.oss.driver.api.core.metadata.Node;

/**
 * Strategy interface for creating JMX connections to Cassandra nodes.
 */
public interface JmxConnectionStrategy
{
    /**
     * Establishes a JMX connection to the given node.
     *
     * @param node the Cassandra node to connect to.
     * @return a {@link ConnectionResult} containing the JMX connector and service URL.
     * @throws IOException if an I/O error occurs during connection.
     */
    ConnectionResult connect(Node node) throws IOException;

    /**
     * Result of a JMX connection attempt, containing the connector and the service URL used.
     *
     * @param connector the established JMX connector.
     * @param serviceURL the JMX service URL that was connected to.
     */
    public record ConnectionResult(JMXConnector connector, JMXServiceURL serviceURL)
    {
    }
}
