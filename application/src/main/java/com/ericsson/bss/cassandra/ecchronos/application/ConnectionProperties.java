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

public class ConnectionProperties
{
    private static final String CONFIG_CONNECTION_NATIVE_HOST = "connection.native.host";
    private static final String CONFIG_CONNECTION_NATIVE_PORT = "connection.native.port";
    private static final String CONFIG_CONNECTION_JMX_HOST = "connection.jmx.host";
    private static final String CONFIG_CONNECTION_JMX_PORT = "connection.jmx.port";

    private static final String DEFAULT_NATIVE_HOST = "localhost";
    private static final String DEFAULT_NATIVE_PORT = "9042";
    private static final String DEFAULT_JMX_HOST = "localhost";
    private static final String DEFAULT_JMX_PORT = "7199";

    private final String myNativeHost;
    private final String myJmxHost;
    private final int myNativePort;
    private final int myJmxPort;

    private ConnectionProperties(String nativeHost, int nativePort,
                                 String jmxHost, int jmxPort)
    {
        myNativeHost = nativeHost;
        myNativePort = nativePort;
        myJmxHost = jmxHost;
        myJmxPort = jmxPort;
    }

    public String getNativeHost()
    {
        return myNativeHost;
    }

    public int getNativePort()
    {
        return myNativePort;
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
        return String.format("(native=%s:%d, jmx=%s:%d)", myNativeHost, myNativePort, myJmxHost, myJmxPort);
    }

    public static ConnectionProperties from(Properties properties)
    {
        String nativeHost = properties.getProperty(CONFIG_CONNECTION_NATIVE_HOST, DEFAULT_NATIVE_HOST);
        int nativePort = Integer.parseInt(properties.getProperty(CONFIG_CONNECTION_NATIVE_PORT, DEFAULT_NATIVE_PORT));
        String jmxHost = properties.getProperty(CONFIG_CONNECTION_JMX_HOST, DEFAULT_JMX_HOST);
        int jmxPort = Integer.parseInt(properties.getProperty(CONFIG_CONNECTION_JMX_PORT, DEFAULT_JMX_PORT));

        return new ConnectionProperties(nativeHost, nativePort, jmxHost, jmxPort);
    }
}
