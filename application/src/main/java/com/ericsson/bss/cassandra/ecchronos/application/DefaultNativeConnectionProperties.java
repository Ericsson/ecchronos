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

public final class DefaultNativeConnectionProperties
{
    private static final String CONFIG_CONNECTION_NATIVE_HOST = "connection.native.host";
    private static final String CONFIG_CONNECTION_NATIVE_PORT = "connection.native.port";

    private static final String DEFAULT_NATIVE_HOST = "localhost";
    private static final String DEFAULT_NATIVE_PORT = "9042";

    private final String myNativeHost;
    private final int myNativePort;

    private DefaultNativeConnectionProperties(String nativeHost, int nativePort)
    {
        myNativeHost = nativeHost;
        myNativePort = nativePort;
    }

    public String getNativeHost()
    {
        return myNativeHost;
    }

    public int getNativePort()
    {
        return myNativePort;
    }

    @Override
    public String toString()
    {
        return String.format("NativeConnection(%s:%d)", myNativeHost, myNativePort);
    }

    public static DefaultNativeConnectionProperties from(Properties properties) throws ConfigurationException
    {
        String nativeHost = properties.getProperty(CONFIG_CONNECTION_NATIVE_HOST, DEFAULT_NATIVE_HOST);
        int nativePort = Integer.parseInt(properties.getProperty(CONFIG_CONNECTION_NATIVE_PORT, DEFAULT_NATIVE_PORT));

        return new DefaultNativeConnectionProperties(nativeHost, nativePort);
    }
}
