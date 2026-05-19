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

import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.util.Map;

public interface JmxConnectionFactory
{
    /**
     * Create a JMX connection to the given host and port.
     *
     * @param host the resolved host address.
     * @param port the JMX port.
     * @param env  the JMX environment (credentials, TLS settings).
     * @return a connected JMXConnector.
     * @throws IOException if connection fails.
     */
    JMXConnector connect(String host, int port, Map<String, Object> env) throws IOException;
}
