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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.Map;

public final class RmiJmxConnectionFactory implements JmxConnectionFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(RmiJmxConnectionFactory.class);
    private static final String JMX_FORMAT_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    @Override
    public JMXConnector connect(final String host, final int port, final Map<String, Object> env) throws IOException
    {
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(JMX_FORMAT_URL, host, port));
        LOG.info("Starting to instantiate JMXService with host: {} and port: {}", host, port);
        return JMXConnectorFactory.connect(jmxUrl, env);
    }
}
