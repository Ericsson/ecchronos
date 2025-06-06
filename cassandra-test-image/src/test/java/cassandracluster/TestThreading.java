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
 package cassandracluster;

 import com.datastax.oss.driver.api.core.CqlSession;
 import com.datastax.oss.driver.api.core.cql.ResultSet;
 import com.datastax.oss.driver.api.core.metadata.Node;
 import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
 import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
 import com.ericsson.bss.cassandra.ecchronos.application.config.connection.ThreadPoolTaskConfig;
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
 import java.util.*;
 import java.util.concurrent.CompletableFuture;
 import java.util.concurrent.Future;
 import java.util.concurrent.ThreadPoolExecutor;
 import java.util.function.Function;

 import static org.junit.Assert.assertEquals;
 import static org.junit.Assert.assertTrue;
 import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestThreading  extends AbstractCassandraCluster
 {

     private static final Logger LOG = LoggerFactory.getLogger(TestThreading.class);


         @Mock
         private DistributedNativeConnectionProvider nativeConnectionProvider;

         @Mock
         private ReplicatedTableProvider replicatedTableProvider;

         @Mock
         private RepairScheduler repairScheduler;

         @Mock
         private TableReferenceFactory tableReferenceFactory;

//         private NoopThreadPoolTaskExecutor threadPool;
         private ThreadPoolTaskExecutor threadPool;

         @Mock
         private Function<TableReference, Set<RepairConfiguration>> repairConfigFunction;



         private CqlSession cqlSession;

         @Mock
         private KeyspaceMetadata keyspaceMetadata;

         @Mock
         private TableMetadata tableMetadata;

         @Mock
         EccNodesSync eccNodesSync;
         @Mock
         DistributedJmxConnectionProvider jmxConnectionProvider;

         private NodeWorkerManager manager;

         private DefaultRepairConfigurationProvider defaultRepairConfigurationProvider;

     @BeforeClass
     public static void setup()
     {
         Path dockerComposePath = Paths.get("")
                 .toAbsolutePath()
                 .getParent()
                 .resolve("cassandra-test-image/src/main/docker/docker-compose.yml");
         composeContainer = new DockerComposeContainer<>(dockerComposePath.toFile());
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





     @Before
     public void testSetup()
     {

         threadPool = new NoopThreadPoolTaskExecutor();
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
     @Test
     public void testCassandraCluster()
     {

         composeContainer.withScaledService("cassandra-node-dc1-rack1-node2", 1 );
         composeContainer.withScaledService("cassandra-node-dc2-rack1-node2", 1 );
         composeContainer.withScaledService("cassandra-seed-dc2-rack1-node1", 1 );
         composeContainer.start();
         LOG.info("Waiting for the new nodes to finish starting up.");
         waitForNodesToBeUp("cassandra-seed-dc1-rack1-node1",4,DEFAULT_WAIT_TIME_IN_MS );


         assertEquals(3, threadPool.getActiveCount());


     }


     public static class NoopThreadPoolTaskExecutor extends ThreadPoolTaskExecutor
         {

             private final List<Runnable> submittedTasks = new ArrayList<>();

             @Override
             public Future<?> submit(Runnable task)
             {
                 submittedTasks.add(task);
                 return super.submit(task);
             //    return CompletableFuture.completedFuture(null);
             }

             @Override
             public void stop(Runnable task)
             {
                 submittedTasks.remove(task);
             }
             public List<Runnable> getSubmittedTasks()
             {
                 return submittedTasks;
             }
         }

     }
