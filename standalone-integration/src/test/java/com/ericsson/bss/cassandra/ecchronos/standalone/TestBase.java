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
package com.ericsson.bss.cassandra.ecchronos.standalone;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.LocalJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.LocalNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactoryImpl;
import net.jcip.annotations.NotThreadSafe;
import org.junit.AfterClass;

import java.io.IOException;

@NotThreadSafe
public class TestBase
{
    private static final String CASSANDRA_HOST = System.getProperty("it-cassandra.ip");
    private static final int CASSANDRA_NATIVE_PORT = Integer.parseInt(System.getProperty("it-cassandra.native.port"));
    private static final int CASSANDRA_JMX_PORT = Integer.parseInt(System.getProperty("it-cassandra.jmx.port"));
    private static final String IS_LOCAL = System.getProperty("it-local-cassandra");

    private static LocalNativeConnectionProvider myNativeConnectionProvider;
    private static LocalNativeConnectionProvider myAdminNativeConnectionProvider;
    private static LocalJmxConnectionProvider myJmxConnectionProvider;
    private static JmxProxyFactoryImpl myJmxProxyFactory;
    protected static Boolean myRemoteRouting;

    public static void initialize() throws IOException
    {
        AuthProvider authProvider = new ProgrammaticPlainTextAuthProvider("eccuser", "eccpassword");
        AuthProvider adminAuthProvider = new ProgrammaticPlainTextAuthProvider("cassandra", "cassandra");
        if (IS_LOCAL != null)
        {
            authProvider = null;
            adminAuthProvider = null;
        }
        myNativeConnectionProvider = LocalNativeConnectionProvider.builder()
                .withPort(CASSANDRA_NATIVE_PORT)
                .withLocalhost(CASSANDRA_HOST)
                .withAuthProvider(authProvider)
                .build();
        myAdminNativeConnectionProvider = LocalNativeConnectionProvider.builder()
                .withPort(CASSANDRA_NATIVE_PORT)
                .withLocalhost(CASSANDRA_HOST)
                .withAuthProvider(adminAuthProvider)
                .withRemoteRouting(myRemoteRouting)
                .build();
        myJmxConnectionProvider = new LocalJmxConnectionProvider(CASSANDRA_HOST, CASSANDRA_JMX_PORT);

        myJmxProxyFactory = JmxProxyFactoryImpl.builder()
                .withJmxConnectionProvider(myJmxConnectionProvider)
                .build();
    }

    @AfterClass
    public static void cleanup() throws IOException
    {
        myJmxConnectionProvider.close();
        myAdminNativeConnectionProvider.close();
        myNativeConnectionProvider.close();
    }

    protected static LocalNativeConnectionProvider getNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }

    protected static LocalNativeConnectionProvider getAdminNativeConnectionProvider()
    {
        return myAdminNativeConnectionProvider;
    }

    protected static LocalJmxConnectionProvider getJmxConnectionProvider()
    {
        return myJmxConnectionProvider;
    }

    protected static JmxProxyFactoryImpl getJmxProxyFactory()
    {
        return myJmxProxyFactory;
    }
}
