/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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

package cassandracluster;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorker;
import com.ericsson.bss.cassandra.ecchronos.core.impl.multithreads.NodeWorkerManager;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.testcontainers.containers.DockerComposeContainer;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class TestTableChanges extends AbstractCassandraCluster
{
    private static final Integer WAIT_TIME = 1000;
    private static final Logger LOG = LoggerFactory.getLogger(TestThreading.class);
    @Mock
    private DistributedNativeConnectionProvider nativeConnectionProvider;
    @Mock
    private ReplicatedTableProvider replicatedTableProvider;
    @Mock
    private RepairScheduler repairScheduler;
    @Mock
    private TableReferenceFactory tableReferenceFactory;
    private ThreadPoolTaskExecutor threadPool;
    @Mock
    private Function<TableReference, Set<RepairConfiguration>> repairConfigFunction;
    private CqlSession cqlSession;
    @Mock
    EccNodesSync eccNodesSync;
    @Mock
    DistributedJmxConnectionProvider jmxConnectionProvider;
    private NodeWorkerManager manager;
    private DefaultRepairConfigurationProvider defaultRepairConfigurationProvider;

    public static void setup()
    {
        String cassandraVersion = System.getProperty("it.cassandra.version", "4.0");
        String certificateDirectory = Paths.get(System.getProperty("project.build.directory", "target"))
                .resolve("certificates/cert")
                .toAbsolutePath()
                .toString();
        Path dockerComposePath = Paths.get("")
                .toAbsolutePath()
                .getParent()
                .resolve("cassandra-test-image/src/main/docker/docker-compose.yml");
        composeContainer = new DockerComposeContainer<>(dockerComposePath.toFile())
                .withEnv("JOLOKIA", "false")
                .withEnv("CASSANDRA_VERSION", cassandraVersion)
                .withEnv("CERTIFICATE_DIRECTORY", certificateDirectory);
        composeContainer.withScaledService("cassandra-node-dc1-rack1-node2", 0 );
        composeContainer.withScaledService("cassandra-node-dc2-rack1-node2", 0 );
        composeContainer.withScaledService("cassandra-seed-dc1-rack1-node1", 1 );
        composeContainer.withScaledService("cassandra-seed-dc2-rack1-node1", 0 );
        composeContainer.start();

        LOG.info("Waiting for the Cassandra cluster to finish starting up.");
        waitForNodesToBeUp("cassandra-seed-dc1-rack1-node1",1,DEFAULT_WAIT_TIME_IN_MS);

        containerIP = composeContainer.getContainerByServiceName(CASSANDRA_SEED_NODE_NAME).get()
                .getContainerInfo()
                .getNetworkSettings().getNetworks().values().stream().findFirst().get().getIpAddress();
    }

    public void testSetup()
    {
        threadPool = new TestThreading.NoopThreadPoolTaskExecutor();
        threadPool.setCorePoolSize(4);
        threadPool.setMaxPoolSize(10);
        threadPool.setQueueCapacity(20);
        threadPool.setKeepAliveSeconds(60);
        threadPool.setThreadNamePrefix("NodeWorker-");
        threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        NodeWorkerManager.Builder builder =  NodeWorkerManager.newBuilder()
                .withNativeConnection(nativeConnectionProvider)
                .withReplicatedTableProvider(replicatedTableProvider)
                .withRepairScheduler(repairScheduler)
                .withTableReferenceFactory(tableReferenceFactory)
                .withRepairConfiguration(repairConfigFunction)
                .withThreadPool(threadPool);

        manager = new MockNodeWorkManager(builder);

        defaultRepairConfigurationProvider = new DefaultRepairConfigurationProvider();

        cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(containerIP, 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials("cassandra", "cassandra")
                .withNodeStateListener(defaultRepairConfigurationProvider)
                .withSchemaChangeListener(defaultRepairConfigurationProvider)
                .build();

        when(nativeConnectionProvider.getCqlSession()).thenReturn(cqlSession);

        defaultRepairConfigurationProvider.fromBuilder(DefaultRepairConfigurationProvider.newBuilder()
                .withSession(cqlSession)
                .withEccNodesSync(eccNodesSync)
                .withJmxConnectionProvider(jmxConnectionProvider)
                .withNodeWorkerManager(manager)
                .withDistributedNativeConnectionProvider(nativeConnectionProvider));
    }

    public void testNewTableCluster()
    {
        composeContainer.withScaledService("cassandra-node-dc1-rack1-node2", 1 );
        composeContainer.withScaledService("cassandra-node-dc2-rack1-node2", 1 );
        composeContainer.withScaledService("cassandra-seed-dc2-rack1-node1", 1 );
        composeContainer.start();
        LOG.info("Waiting for the new nodes to finish starting up.");
        waitForNodesToBeUp("cassandra-seed-dc1-rack1-node1",4,DEFAULT_WAIT_TIME_IN_MS * 10 );

        cqlSession.execute(
                "CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {'class': 'NetworkTopologyStrategy', " +
                        "'datacenter1': 1}");
        try
        {
            Thread.sleep(WAIT_TIME);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        cqlSession.execute("CREATE TABLE IF NOT EXISTS test_keyspace.test_table (id UUID PRIMARY KEY, value text)");
        cqlSession.execute("CREATE TABLE IF NOT EXISTS test_keyspace.test_table2 (id UUID PRIMARY KEY, value text)");


        Collection<NodeWorker> workers = manager.getWorkers();

        try
        {
            Thread.sleep(WAIT_TIME);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        for ( NodeWorker nodeWorker: workers)
        {
            LOG.info( ((MockNodeWorker)nodeWorker).getKeyspaceCreateCount()+ " " + ((MockNodeWorker)nodeWorker).getTableCreateCount() );
            assertEquals(Optional.ofNullable(1), Optional.ofNullable(((MockNodeWorker) nodeWorker).getKeyspaceCreateCount())) ;;
            assertEquals(Optional.ofNullable(2), Optional.ofNullable(((MockNodeWorker) nodeWorker).getTableCreateCount())) ;;
        }
    }

    public void testRemoveTable()
    {

        composeContainer.withScaledService("cassandra-node-dc1-rack1-node2", 1 );
        composeContainer.withScaledService("cassandra-node-dc2-rack1-node2", 1 );
        composeContainer.withScaledService("cassandra-seed-dc2-rack1-node1", 1 );
        composeContainer.start();
        LOG.info("Waiting for the new nodes to finish starting up.");
        waitForNodesToBeUp("cassandra-seed-dc1-rack1-node1",4,DEFAULT_WAIT_TIME_IN_MS  );

        cqlSession.execute(
                "CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {'class': 'NetworkTopologyStrategy', " +
                        "'datacenter1': 1}");

        cqlSession.execute("CREATE TABLE IF NOT EXISTS test_keyspace.test_table (id UUID PRIMARY KEY, value text)");

        try
        {
            Thread.sleep(WAIT_TIME);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        cqlSession.execute("DROP TABLE test_keyspace.test_table");

        Collection<NodeWorker> workers = manager.getWorkers();

        try
        {
            Thread.sleep(WAIT_TIME);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        for ( NodeWorker nodeWorker: workers)
        {
            assertEquals(Optional.ofNullable(1), Optional.ofNullable(((MockNodeWorker) nodeWorker).getTableRemoveCount())) ;;
        }
    }

    public void testRemoveKeyspace()
    {
        composeContainer.withScaledService("cassandra-node-dc1-rack1-node2", 1 );
        composeContainer.withScaledService("cassandra-node-dc2-rack1-node2", 1 );
        composeContainer.withScaledService("cassandra-seed-dc2-rack1-node1", 1 );
        composeContainer.start();
        LOG.info("Waiting for the new nodes to finish starting up.");
        waitForNodesToBeUp("cassandra-seed-dc1-rack1-node1",4,DEFAULT_WAIT_TIME_IN_MS  );

        cqlSession.execute(
                "CREATE KEYSPACE IF NOT EXISTS test_keyspace2 WITH replication = {'class': 'NetworkTopologyStrategy', " +
                        "'datacenter1': 1}");

        cqlSession.execute("CREATE TABLE IF NOT EXISTS test_keyspace2.test_table1 (id UUID PRIMARY KEY, value text)");
        cqlSession.execute("CREATE TABLE IF NOT EXISTS test_keyspace2.test_table2 (id UUID PRIMARY KEY, value text)");
        cqlSession.execute("CREATE TABLE IF NOT EXISTS test_keyspace2.test_table3 (id UUID PRIMARY KEY, value text)");

        try
        {
            Thread.sleep(WAIT_TIME);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        try
        {
            cqlSession.execute("DROP KEYSPACE test_keyspace2");
        }
        catch ( DriverTimeoutException e) {
            // Due to the 4 docker nodes running on a local machine, this delete command can take longer than 2 seconds, and therefore timeout
            LOG.info("Drop KEYSPACE timed out");
        }

        Collection<NodeWorker> workers = manager.getWorkers();

        try
        {
            Thread.sleep(WAIT_TIME);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        for ( NodeWorker nodeWorker: workers)
        {
            assertEquals(Optional.ofNullable(3), Optional.ofNullable(((MockNodeWorker) nodeWorker).getTableRemoveCount())) ;;
        }
    }
}
