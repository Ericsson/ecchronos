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


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.LocalJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.LocalNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import net.jcip.annotations.NotThreadSafe;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

@NotThreadSafe
public class TestBase
{
    private static final int CASSANDRA_NATIVE_PORT = 9042;
    private static final int CASSANDRA_JMX_PORT = 7199;
    private static final String IS_LOCAL = System.getProperty("it-local-cassandra");
    private static final int DEFAULT_INSERT_DATA_COUNT = 1000;

    private static LocalNativeConnectionProvider myNativeConnectionProvider;
    private static LocalNativeConnectionProvider myAdminNativeConnectionProvider;
    private static LocalJmxConnectionProvider myJmxConnectionProvider;
    private static JmxProxyFactoryImpl myJmxProxyFactory;
    protected static Boolean myRemoteRouting;

    public static void initialize() throws IOException
    {
        try
        {
            SharedCassandraCluster.ensureInitialized();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new IOException("Cluster initialization interrupted", e);
        }
        
        String containerIP = SharedCassandraCluster.getContainerIP();
        AuthProvider authProvider = new ProgrammaticPlainTextAuthProvider("eccuser", "eccpassword");
        AuthProvider adminAuthProvider = new ProgrammaticPlainTextAuthProvider("cassandra", "cassandra");
        if (IS_LOCAL != null)
        {
            authProvider = null;
            adminAuthProvider = null;
        }
        myNativeConnectionProvider = LocalNativeConnectionProvider.builder()
                .withPort(CASSANDRA_NATIVE_PORT)
                .withLocalhost(containerIP)
                .withAuthProvider(authProvider)
                .build();
        myAdminNativeConnectionProvider = LocalNativeConnectionProvider.builder()
                .withPort(CASSANDRA_NATIVE_PORT)
                .withLocalhost(containerIP)
                .withAuthProvider(adminAuthProvider)
                .withRemoteRouting(myRemoteRouting)
                .build();
        myJmxConnectionProvider = new LocalJmxConnectionProvider(containerIP, CASSANDRA_JMX_PORT);

        myJmxProxyFactory = JmxProxyFactoryImpl.builder()
                .withJmxConnectionProvider(myJmxConnectionProvider)
                .build();
    }

    @AfterClass
    public static void cleanup() throws IOException
    {
        if (myJmxConnectionProvider != null)
        {
            myJmxConnectionProvider.close();
        }
        if (myAdminNativeConnectionProvider != null)
        {
            myAdminNativeConnectionProvider.close();
        }
        if (myNativeConnectionProvider != null)
        {
            myNativeConnectionProvider.close();
        }
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

    protected void insertSomeDataAndFlush(TableReference tableReference, CqlSession session)
            throws ReflectionException, MalformedObjectNameException, InstanceNotFoundException, MBeanException,
            IOException
    {
        for (int i = 0; i < DEFAULT_INSERT_DATA_COUNT; i++)
        {
            UUID randomUUID = UUID.randomUUID();
            SimpleStatement statement = QueryBuilder.insertInto(tableReference.getKeyspace(), tableReference.getTable())
                    .value("key1", literal(randomUUID.toString()))
                    .value("key2", literal(randomUUID.hashCode()))
                    .value("value", literal(randomUUID.hashCode())).build();
            session.execute(statement);
        }
        forceFlush(tableReference);
    }

    private void forceFlush(TableReference tableReference)
            throws IOException, MalformedObjectNameException, ReflectionException, InstanceNotFoundException,
            MBeanException
    {
        try(JMXConnector jmxConnector = getJmxConnectionProvider().getJmxConnector())
        {
            String[] table = new String[]{tableReference.getTable()};
            jmxConnector.getMBeanServerConnection().invoke(new ObjectName("org.apache.cassandra.db:type=StorageService"),
                    "forceKeyspaceFlush",
                    new Object[]
                            {
                                    tableReference.getKeyspace(), table
                            },
                    new String[]
                            {
                                    String.class.getName(), String[].class.getName()
                            });
        }
    }
}
