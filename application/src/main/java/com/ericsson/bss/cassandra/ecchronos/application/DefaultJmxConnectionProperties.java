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
package com.ericsson.bss.cassandra.ecchronos.application;

import java.util.Properties;

public final class DefaultJmxConnectionProperties
{
    private static final String CONFIG_CONNECTION_JMX_HOST = "connection.jmx.host";
    private static final String CONFIG_CONNECTION_JMX_PORT = "connection.jmx.port";

    private static final String DEFAULT_JMX_HOST = "localhost";
    private static final String DEFAULT_JMX_PORT = "7199";

    private final String myJmxHost;
    private final int myJmxPort;

    private DefaultJmxConnectionProperties(String jmxHost, int jmxPort)
    {
        myJmxHost = jmxHost;
        myJmxPort = jmxPort;
    }

    public String getJmxHost()
    {
        return myJmxHost;
    }

    public int getJmxPort()
    {
        return myJmxPort;
    }

    @Override
    public String toString()
    {
        return String.format("JmxConnection(%s:%d)", myJmxHost, myJmxPort);
    }

    public static DefaultJmxConnectionProperties from(Properties properties)
    {
        String jmxHost = properties.getProperty(CONFIG_CONNECTION_JMX_HOST, DEFAULT_JMX_HOST);
        int jmxPort = Integer.parseInt(properties.getProperty(CONFIG_CONNECTION_JMX_PORT, DEFAULT_JMX_PORT));

        return new DefaultJmxConnectionProperties(jmxHost, jmxPort);
    }
}
