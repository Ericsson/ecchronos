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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.DockerComposeContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class AbstractCassandraCluster
{
    private static final String DOCKER_COMPOSE_FILE_PATH = "cassandra-test-image/src/main/docker/docker-compose.yml";
    private static final String CASSANDRA_SETUP_DB_SCRIPT_PATH = "/etc/cassandra/setup_db.sh";
    protected static final String CASSANDRA_SEED_NODE_NAME = "cassandra-seed-dc1-rack1-node1";
    protected static final long DEFAULT_WAIT_TIME_IN_MS = 90000;
    protected static DockerComposeContainer<?> composeContainer;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCassandraCluster.class);
    protected static String containerIP;
    protected static CqlSession mySession;
    private static final long DEFAULT_WAIT_TIME_IN_SECS= 10000;
    private static final String ALTER_SYSTEM_AUTH_CQL = "ALTER KEYSPACE system_auth WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2': 2};";

    @BeforeClass
    public static void setup() throws IOException, InterruptedException
    {
        if (composeContainer != null)
        {
            return;
        }
        String cassandraVersion = System.getProperty("it.cassandra.version", "4.0");
        String certificateDirectory = Paths.get(System.getProperty("project.build.directory", "target"))
                .resolve("certificates/cert")
                .toAbsolutePath()
                .toString();
        Path dockerComposePath = Paths.get("")
                .toAbsolutePath()
                .getParent()
                .resolve(DOCKER_COMPOSE_FILE_PATH);
        composeContainer = new DockerComposeContainer<>(dockerComposePath.toFile())
                .withEnv("JOLOKIA", "false")
                .withEnv("CASSANDRA_VERSION", cassandraVersion)
                .withEnv("CERTIFICATE_DIRECTORY", certificateDirectory)
                .withLogConsumer(CASSANDRA_SEED_NODE_NAME, new Slf4jLogConsumer(LOG));

        composeContainer.start();

        containerIP = composeContainer.getContainerByServiceName(CASSANDRA_SEED_NODE_NAME).get()
                .getContainerInfo()
                .getNetworkSettings().getNetworks().values().stream().findFirst().get().getIpAddress();
        LOG.info("Waiting for the Cassandra cluster to finish starting up.");
        waitForNodesToBeUp(CASSANDRA_SEED_NODE_NAME,4,DEFAULT_WAIT_TIME_IN_MS);
        modifySystemAuthKeyspace();
        runFullRepair();
        setupDb();
    }

    protected static void createDefaultSession()
    {
        mySession = defaultBuilder().build();
    }

    protected static CqlSessionBuilder defaultBuilder()
    {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(containerIP, 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials("cassandra", "cassandra");
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

    protected void decommissionNode ( String node) throws IOException, InterruptedException
    {
        composeContainer.getContainerByServiceName(node).get()
                .execInContainer("nodetool", "-u", "cassandra", "-pw", "cassandra", "decommission").getStdout();
    }

    protected static int getNodeCountViaNodetool( String node) throws IOException, InterruptedException
    {
        String stdout = composeContainer.getContainerByServiceName(node).get()
                .execInContainer("nodetool", "-u", "cassandra", "-pw", "cassandra", "status").getStdout();
        return stdout.split("UN",-1).length-1;
    }

    protected static void waitForNodesToBeUp( String node, int expectedNodes, long maxWaitTimeInMillis)
    {
        long startTime = System.currentTimeMillis();

        while ( startTime + maxWaitTimeInMillis > System.currentTimeMillis())
        {
            try
            {
                Thread.sleep(DEFAULT_WAIT_TIME_IN_SECS);
                if (getNodeCountViaNodetool(node) == expectedNodes)
                {
                    return;
                }
            }
            catch (IOException | InterruptedException e)
            {
                // ignore and retry
            }
        }
        LOG.info("Timed out waiting for the Cassandra cluster to finish starting up.");
    }

    private static void setupDb() throws IOException, InterruptedException
    {
        composeContainer.getContainerByServiceName(CASSANDRA_SEED_NODE_NAME).get()
                .execInContainer("bash", CASSANDRA_SETUP_DB_SCRIPT_PATH);
    }

    private static void modifySystemAuthKeyspace() throws IOException, InterruptedException
    {
        composeContainer.getContainerByServiceName(CASSANDRA_SEED_NODE_NAME).get()
                .execInContainer("cqlsh", "-e", ALTER_SYSTEM_AUTH_CQL);
    }

    private static void runFullRepair() throws IOException, InterruptedException
    {
        composeContainer.getContainerByServiceName(CASSANDRA_SEED_NODE_NAME).get()
                .execInContainer("nodetool", "-u", "cassandra", "-pw", "cassandra", "repair", "--full");
    }
}

