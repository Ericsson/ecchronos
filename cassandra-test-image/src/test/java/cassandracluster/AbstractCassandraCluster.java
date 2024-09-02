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
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.DockerComposeContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractCassandraCluster
{
    private static DockerComposeContainer<?> composeContainer;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCassandraCluster.class);
    protected static String containerIP;
    protected static CqlSession mySession;

    @BeforeClass
    public static void setup() throws InterruptedException
    {
        Path dockerComposePath = Paths.get("")
                .toAbsolutePath()
                .getParent()
                .resolve("cassandra-test-image/src/main/docker/docker-compose.yml");
        composeContainer = new DockerComposeContainer<>(dockerComposePath.toFile());
        composeContainer.start();

        LOG.info("Waiting for the Cassandra cluster to finish starting up.");
        Thread.sleep(50000);

        containerIP = composeContainer.getContainerByServiceName("cassandra-seed-dc1-rack1-node1").get()
                .getContainerInfo()
                .getNetworkSettings().getNetworks().values().stream().findFirst().get().getIpAddress();
        CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(containerIP, 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials("cassandra", "cassandra");
        mySession = builder.build();
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (mySession != null)
        {
            mySession.close();
        }
        composeContainer.stop();
    }
}

