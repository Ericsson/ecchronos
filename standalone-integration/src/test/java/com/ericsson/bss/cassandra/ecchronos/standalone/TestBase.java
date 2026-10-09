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
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionStrategy;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.utils.ConnectionUtils;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.utils.JolokiaConnectionStrategy;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.utils.RMIConnectionStrategy;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedJmxConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedNativeConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.DistributedJmxProxyFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.jmx.JolokiaNotificationController;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import java.net.InetSocketAddress;
import java.time.Duration;
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
import java.util.concurrent.atomic.AtomicInteger;
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
    private static JolokiaNotificationController myJolokiaNotificationController;
    protected static EccNodesSync myEccNodesSync;

    protected static Node MyLocalNode;
    private static final Object lock = new Object();
    private static final AtomicInteger UNIQUE_TABLE_COUNTER = new AtomicInteger();
    // DDL (CREATE/DROP TABLE) on this multi-DC cluster regularly takes longer than the 2s default
    // request timeout while schema migrations/compactions settle, so give schema statements their own
    // generous timeout to avoid spurious DriverTimeoutException during test setup/teardown.
    private static final Duration DDL_TIMEOUT = Duration.ofSeconds(30);
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

        ConnectionUtils connectionUtils = ConnectionUtils.newBuilder()
                .withIpTranslator(new IpTranslator())
                .build();

        JmxConnectionStrategy jmxConnectionStrategy = myJolokiaEnabled
                ? JolokiaConnectionStrategy.newBuilder().withConnectionUtils(connectionUtils).withPort(8778).build()
                : RMIConnectionStrategy.newBuilder().withConnectionUtils(connectionUtils).build();

        myJmxConnectionProvider = DistributedJmxConnectionProviderImpl.builder()
                .withNativeConnection(myNativeConnectionProvider)
                .withEccNodesSync(myEccNodesSync)
                .withConnectionStrategy(jmxConnectionStrategy)
                .build();

        Map<UUID, Node> nodesMap = myNativeConnectionProvider.getCqlSession().getMetadata().getNodes();

        if (myJolokiaEnabled)
        {
            myJolokiaNotificationController = JolokiaNotificationController.newBuilder()
                    .withNativeConnection(myNativeConnectionProvider)
                    .withJolokiaPort(8778)
                    .withIpTranslator(new IpTranslator())
                    .build();
        }

        myJmxProxyFactory = DistributedJmxProxyFactoryImpl.builder()
                .withJmxConnectionProvider(myJmxConnectionProvider)
                .withEccNodesSync(myEccNodesSync)
                .withNodesMap(nodesMap)
                .withJolokiaEnabled(myJolokiaEnabled)
                .withIpTranslator(new IpTranslator())
                .withJolokiaNotificationController(myJolokiaNotificationController)
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
        if (myJolokiaNotificationController != null)
        {
            myJolokiaNotificationController.close();
        }
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
        // The shared cluster raises the system_auth replication factor during setup; the default
        // 'cassandra' superuser is read at QUORUM and its credentials may momentarily not be present
        // on the newly required replicas, yielding a transient AuthenticationException here. Retry a
        // bounded number of times so a brief propagation delay does not fail the whole test class.
        AllNodesFailedException lastError = null;
        for (int attempt = 1; attempt <= 15; attempt++)
        {
            try
            {
                return defaultBuilder().build();
            }
            catch (AllNodesFailedException e)
            {
                lastError = e;
                LOG.warn("Attempt {} to build default session failed (likely transient auth propagation): {}",
                        attempt, e.getMessage());
                try
                {
                    Thread.sleep(2000);
                }
                catch (InterruptedException ie)
                {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        throw lastError;
    }

    private static CqlSessionBuilder defaultBuilder()
    {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(SharedCassandraCluster.getContainerIP(), CASSANDRA_NATIVE_PORT))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials("cassandra", "cassandra");
    }

    /**
     * Create a unique table in the {@link #TEST_KEYSPACE} keyspace for a test that runs a real repair.
     *
     * <p>Tests that run real repairs must not share a table: one test's incremental repaired-state
     * (anticompaction / consistent sessions left on the shared Cassandra node) can otherwise leak into
     * another and cause a sporadic {@code repairSession(..., false)}. These tables are created at test
     * time rather than in {@code cassandra-test-image/src/main/docker/create_keyspaces.cql} on purpose:
     * that schema file is shared with the behave and topology test harnesses, which assert the exact set
     * of tables / schedule counts, so any table added there leaks into those unrelated suites.
     *
     * <p>The table schema matches the static {@code test.table*} tables so that
     * {@link #insertSomeDataAndFlush} works unchanged. The name is made unique per invocation to avoid
     * collisions between tests (and re-runs) that share the long-lived {@link SharedCassandraCluster}.
     *
     * @param prefix a short, test-specific prefix for readability in logs (e.g. {@code "hungrecovery"}).
     * @return the created unique table name.
     */
    protected static String createUniqueRepairTable(final CqlSession adminSession,
                                                     final CqlSession readingSession,
                                                     final String prefix)
    {
        String tableName = prefix + "_" + Long.toUnsignedString(System.nanoTime(), 36)
                + "_" + UNIQUE_TABLE_COUNTER.incrementAndGet();
        adminSession.execute(SimpleStatement.newInstance(
                "CREATE TABLE IF NOT EXISTS " + TEST_KEYSPACE + "." + tableName
                        + " (key1 text, key2 int, value int, PRIMARY KEY(key1, key2))")
                .setTimeout(DDL_TIMEOUT));
        // Block until all nodes agree on the new schema so the subsequent repair does not race ahead of
        // the table being visible cluster-wide. checkSchemaAgreement() is a point-in-time check, so poll
        // briefly until agreement is reached rather than trusting a single sample. Up to ~15s (30 x 500ms).
        for (int attempt = 0; attempt < 30 && !adminSession.checkSchemaAgreement(); attempt++)
        {
            sleepMillis(500);
        }
        // The TableReferenceFactory used by the tests reads schema from readingSession (a different
        // CqlSession than adminSession, with its own metadata cache). Wait until that session has the
        // new table in its metadata, otherwise forTable(...) returns null and the test NPEs.
        // Up to ~30s (60 x 500ms).
        for (int attempt = 0; attempt < 60 && !tableVisible(readingSession, tableName); attempt++)
        {
            sleepMillis(500);
        }
        return tableName;
    }

    private static boolean tableVisible(final CqlSession session, final String tableName)
    {
        return session.getMetadata()
                .getKeyspace(TEST_KEYSPACE)
                .flatMap(ks -> ks.getTable(tableName))
                .isPresent();
    }

    private static void sleepMillis(final long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for schema propagation", e);
        }
    }

    /**
     * Drop a table previously created by {@link #createUniqueRepairTable}. Safe to call in test teardown;
     * failures are swallowed so cleanup never masks the actual test result.
     */
    protected static void dropRepairTable(final CqlSession adminSession, final String tableName)
    {
        if (tableName == null)
        {
            return;
        }
        try
        {
            adminSession.execute(SimpleStatement.newInstance(
                    "DROP TABLE IF EXISTS " + TEST_KEYSPACE + "." + tableName)
                    .setTimeout(DDL_TIMEOUT));
        }
        catch (RuntimeException e)
        {
            LOG.warn("Failed to drop test table {}.{} during cleanup: {}", TEST_KEYSPACE, tableName, e.getMessage());
        }
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
