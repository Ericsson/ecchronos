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
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.DefaultRepairConfigurationProvider;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;

public class ITNodeAddition extends AbstractCassandraCluster
{
    private static final Logger LOG = LoggerFactory.getLogger(ITNodeAddition.class);
    @BeforeClass
    public static void setup() throws InterruptedException
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
        waitForNodesToBeUp("cassandra-seed-dc1-rack1-node1",1,50000);

    }
    @Test
    public void testAdditionalNodesAddedToCluster() throws InterruptedException, IOException
    {
        DefaultRepairConfigurationProvider listener = mock(DefaultRepairConfigurationProvider.class);
        containerIP = composeContainer.getContainerByServiceName("cassandra-seed-dc1-rack1-node1").get()
                .getContainerInfo()
                .getNetworkSettings().getNetworks().values().stream().findFirst().get().getIpAddress();
        CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(containerIP, 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials("cassandra", "cassandra")
                .withNodeStateListener(listener);
        mySession = builder.build();

        // scale up new nodes
        composeContainer.withScaledService("cassandra-node-dc1-rack1-node2", 1 );
        composeContainer.withScaledService("cassandra-node-dc2-rack1-node2", 1 );
        composeContainer.withScaledService("cassandra-seed-dc2-rack1-node1", 1 );
        composeContainer.start();
        LOG.info("Waiting for the new nodes to finish starting up.");
        waitForNodesToBeUp("cassandra-seed-dc1-rack1-node1",4,50000);

        assertEquals( 4, getNodeCountViaNodetool("cassandra-node-dc1-rack1-node2"));

        verify(listener, times(3)).onAdd(any());
        verify(listener, times(0)).onRemove(any());
    }
}
