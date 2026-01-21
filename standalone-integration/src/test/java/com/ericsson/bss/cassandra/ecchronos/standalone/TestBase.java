/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedJmxConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedNativeConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.DistributedJmxProxyFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;
import org.junit.AfterClass;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.util.UUID;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

@NotThreadSafe
abstract public class TestBase
{
    protected static final Logger LOG = LoggerFactory.getLogger(TestBase.class);
    private static final String ECCHRONOS_ID = "EcchronosID";
    private static final int CASSANDRA_NATIVE_PORT = 9042;
    protected static final String ECCHRONOS_KEYSPACE = "ecchronos";
    protected static final String TEST_KEYSPACE = "test";
    protected static final String TEST_TABLE_ONE_NAME = "table1";
    protected static final String TEST_TABLE_TWO_NAME = "table2";

    private static final String DC1 = "datacenter1";
    private static final String DC2 = "datacenter2";
    
    protected static final int DEFAULT_INSERT_DATA_COUNT = 1000;

    private static DistributedNativeConnectionProvider myNativeConnectionProvider;
    private static DistributedNativeConnectionProvider myAdminNativeConnectionProvider;


    private static DistributedJmxConnectionProvider myJmxConnectionProvider;
    private static DistributedJmxProxyFactoryImpl myJmxProxyFactory;
    protected static EccNodesSync myEccNodesSync;

    protected static Node MyLocalNode;
    private static final Object lock = new Object();
    private static boolean myJolokiaEnabled;

    @BeforeClass
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
        String jolokiaEnabled = System.getProperty("it.jolokia.enabled", "false");
        myJolokiaEnabled = jolokiaEnabled.equals("true");
        List<InetSocketAddress> contactPoints = new ArrayList<>();
        CqlSession initialSession = createDefaultSession();

        for (Node node : initialSession.getMetadata().getNodes().values())
        {
            String hostname = node.getBroadcastRpcAddress().get().getHostName();
            int port = node.getBroadcastRpcAddress().get().getPort();
            contactPoints.add(new InetSocketAddress(hostname, port));
        }
        initialSession.close();

        AuthProvider authProvider = new ProgrammaticPlainTextAuthProvider("eccuser", "eccpassword");
        AuthProvider adminAuthProvider = new ProgrammaticPlainTextAuthProvider("cassandra", "cassandra");

        myNativeConnectionProvider = DistributedNativeConnectionProviderImpl.builder()
                .withInitialContactPoints(contactPoints)
                .withAgentType(ConnectionType.datacenterAware)
                .withDatacenterAware(Arrays.asList(DC1, DC2))
                .withAuthProvider(authProvider)
                .build();

        myAdminNativeConnectionProvider = DistributedNativeConnectionProviderImpl.builder()
                .withInitialContactPoints(contactPoints)
                .withAgentType(ConnectionType.datacenterAware)
                .withDatacenterAware(Arrays.asList(DC1, DC2))
                .withAuthProvider(adminAuthProvider)
                .build();

        myEccNodesSync = EccNodesSync.newBuilder()
                .withSession(myNativeConnectionProvider.getCqlSession())
                .withNativeConnection(myNativeConnectionProvider)
                .withEcchronosID(ECCHRONOS_ID)
                .build();

        myJmxConnectionProvider = DistributedJmxConnectionProviderImpl.builder()
                .withCqlSession(myNativeConnectionProvider.getCqlSession())
                .withNativeConnection(myNativeConnectionProvider)
                .withJolokiaEnabled(myJolokiaEnabled)
                .withEccNodesSync(myEccNodesSync)
                .withIpTranslator(new IpTranslator())
                .build();

        Map<UUID, Node> nodesMap = myNativeConnectionProvider.getCqlSession().getMetadata().getNodes();
        myJmxProxyFactory = DistributedJmxProxyFactoryImpl.builder()
                .withJmxConnectionProvider(myJmxConnectionProvider)
                .withEccNodesSync(myEccNodesSync)
                .withNodesMap(nodesMap)
                .withJolokiaEnabled(myJolokiaEnabled)
                .withIpTranslator(new IpTranslator())
                .build();
        MyLocalNode = getNativeConnectionProvider()
            .getNodes()
            .values()
            .stream()
            .filter(node -> "0.0.0.0" != node.getBroadcastRpcAddress().get().getAddress().getHostAddress())
            .findFirst()
            .orElse(null);
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

    protected static DistributedNativeConnectionProvider getNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }

    protected static DistributedJmxConnectionProvider getJmxConnectionProvider()
    {
        return myJmxConnectionProvider;
    }

    protected static DistributedJmxProxyFactoryImpl getJmxProxyFactory()
    {
        return myJmxProxyFactory;
    }

    protected static DistributedNativeConnectionProvider getAdminNativeConnectionProvider()
    {
        return myAdminNativeConnectionProvider;
    }

    protected static Node getNode()
    {
        synchronized (lock)
        {
            return MyLocalNode;
        }
        
    }

    protected static CqlSession getSession()
    {
        return getNativeConnectionProvider().getCqlSession();
    }
    
    protected static boolean isJolokiaEnabled()
    {
        return myJolokiaEnabled;
    }

    private static CqlSession createDefaultSession()
    {
       return defaultBuilder().build();
    }

    private static CqlSessionBuilder defaultBuilder()
    {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(SharedCassandraCluster.getContainerIP(), CASSANDRA_NATIVE_PORT))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials("cassandra", "cassandra");
    }

    protected void insertSomeDataAndFlush(TableReference tableReference, CqlSession session, Node node)
                                                                                                        throws ReflectionException,
                                                                                                        MalformedObjectNameException,
                                                                                                        InstanceNotFoundException,
                                                                                                        MBeanException,
                                                                                                        IOException
    {
        for (int i = 0; i < DEFAULT_INSERT_DATA_COUNT; i++)
        {
            UUID randomUUID = UUID.randomUUID();
            SimpleStatement statement = QueryBuilder.insertInto(tableReference.getKeyspace(), tableReference.getTable())
                    .value("key1", literal(randomUUID.toString()))
                    .value("key2", literal(randomUUID.hashCode()))
                    .value("value", literal(randomUUID.hashCode()))
                    .build();
            session.execute(statement);
        }
        forceFlush(tableReference, node);
    }

    private void forceFlush(TableReference tableReference, Node node)
                                                                      throws IOException,
                                                                      MalformedObjectNameException,
                                                                      ReflectionException,
                                                                      InstanceNotFoundException,
                                                                      MBeanException
    {
        try (JMXConnector jmxConnector = getJmxConnectionProvider().getJmxConnector(node.getHostId()))
        {
            if (jmxConnector != null && jmxConnector.getMBeanServerConnection() != null) {
                String[] table = new String[] { tableReference.getTable() };
                jmxConnector.getMBeanServerConnection()
                        .invoke(new ObjectName("org.apache.cassandra.db:type=StorageService"),
                                "forceKeyspaceFlush",
                                new Object[] {
                                               tableReference.getKeyspace(), table
                                },
                                new String[] {
                                               String.class.getName(), String[].class.getName()
                                });
            }
            else
            {
                LOG.warn("JMX connector or MBeanServerConnection is null for node {}, skipping flush", node.getHostId());
            }
        }
    }
}
