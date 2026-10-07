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
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.DockerComposeContainer.RemoveImages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class AbstractCassandraCluster
{
    private static final String DOCKER_COMPOSE_FILE_PATH = "cassandra-test-image/src/main/docker/docker-compose.yml";
    private static final String CASSANDRA_SETUP_DB_SCRIPT_PATH = "/etc/cassandra/setup_db.sh";
    protected static final String CASSANDRA_SEED_NODE_NAME = "cassandra-seed-dc1-rack1-node1";
    private static final String[] CASSANDRA_NODE_NAMES = {
        "cassandra-seed-dc1-rack1-node1",
        "cassandra-seed-dc2-rack1-node1",
        "cassandra-node-dc1-rack1-node2",
        "cassandra-node-dc2-rack1-node2",
    };
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
        String cassandraVersion = System.getProperty("it.cassandra.version", "4.1");
        String jolokiaEnabled = System.getProperty("it.jolokia.enabled", "false");
        // The Jolokia agent version must match the jolokia-client-jmx-adapter version from the pom
        // (property jolokia.adapter.version, forwarded here as the 'jolokia.version' system property).
        // There is intentionally no default: a missing value means the pom wiring is broken, and a
        // version mismatch between agent and client is a real problem, so fail fast instead.
        String jolokiaVersion = System.getProperty("jolokia.version");
        if (jolokiaVersion == null || jolokiaVersion.trim().isEmpty())
        {
            throw new IllegalStateException(
                    "System property 'jolokia.version' is not set. It must be forwarded from the pom "
                            + "property 'jolokia.adapter.version' via the failsafe/surefire "
                            + "systemPropertyVariables configuration.");
        }
        LOG.info("Using Jolokia agent version {} (from pom property jolokia.adapter.version).", jolokiaVersion);
        String certificateDirectory = Paths.get(System.getProperty("project.build.directory", "target"))
                .resolve("certificates/cert")
                .toAbsolutePath()
                .toString();
        Path dockerComposePath = Paths.get("")
                .toAbsolutePath()
                .getParent()
                .resolve(DOCKER_COMPOSE_FILE_PATH);
        composeContainer = new DockerComposeContainer<>(dockerComposePath.toFile())
                .withEnv("JOLOKIA", jolokiaEnabled)
                .withEnv("CASSANDRA_VERSION", cassandraVersion)
                .withEnv("JOLOKIA_VERSION", jolokiaVersion)
                .withEnv("CERTIFICATE_DIRECTORY", certificateDirectory)
                // Remove the locally-built compose images on teardown so each test run
                // does not leave behind a new set of '<project>_cassandra-*' images.
                .withRemoveImages(RemoveImages.LOCAL)
                .withLogConsumer(CASSANDRA_SEED_NODE_NAME, new Slf4jLogConsumer(LOG));

        composeContainer.start();

        containerIP = composeContainer.getContainerByServiceName(CASSANDRA_SEED_NODE_NAME).get()
                .getContainerInfo()
                .getNetworkSettings().getNetworks().values().stream().findFirst().get().getIpAddress();
        LOG.info("Waiting for the Cassandra cluster to finish starting up.");
        waitForNodesToBeUp(CASSANDRA_SEED_NODE_NAME,4,DEFAULT_WAIT_TIME_IN_MS);
        modifySystemAuthKeyspace();
        runFullRepair();
        waitForAuthReady();
        setupDb();
        verifyKeyspaceExists();
    }

    protected static void createDefaultSession()
    {
        mySession = defaultBuilder().build();
    }

    protected static CqlSessionBuilder defaultBuilder()
    {
        // The default driver request timeout (2s) is too tight for schema (DDL) and CAS operations against
        // this 4-node/2-DC container cluster, which can be slow or under load in CI, causing
        // DriverTimeoutException (PT2S). Raise the relevant timeouts so tests fail on real issues rather than
        // transient timing, mirroring the core.impl AbstractCassandraContainerTest configuration.
        DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(30))
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(30))
                .withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofSeconds(30))
                .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofSeconds(1))
                .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(5))
                .build();

        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(containerIP, 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials("cassandra", "cassandra")
                .withConfigLoader(configLoader);
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (mySession != null)
        {
            mySession.close();
        }
        if (composeContainer != null)
        {
            composeContainer.stop();
        }
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
        // After raising the system_auth replication factor, the default superuser row only exists on the
        // original replica. It is read at QUORUM during authentication, so it must be reconciled onto all
        // the newly required replicas (one in datacenter1, two in datacenter2) before any session is built.
        // Repair system_auth explicitly on every node rather than running an untargeted full repair on the
        // seed only, which does not reliably spread the row to quorum.
        for (String node : CASSANDRA_NODE_NAMES)
        {
            composeContainer.getContainerByServiceName(node).get()
                    .execInContainer("nodetool", "-u", "cassandra", "-pw", "cassandra",
                            "repair", "--full", "system_auth");
        }
    }

    /**
     * Wait until the 'cassandra' superuser can actually authenticate through the native driver.
     *
     * After raising the system_auth replication factor, the default superuser is read at QUORUM and its
     * credentials may not yet be present on the newly required replicas, causing transient
     * "Provided username cassandra and/or password are incorrect" failures when the tests build their
     * CqlSession. Verify readiness the same way the tests connect - a driver session against the cluster
     * IP in datacenter1 - rather than a local cqlsh on the seed, which can succeed against a single node
     * while a QUORUM read still fails. Fail fast if it never becomes ready, so the real cause is reported
     * here instead of cascading into every test's session setup.
     */
    private static void waitForAuthReady() throws InterruptedException
    {
        int attempts = 60;
        for (int i = 1; i <= attempts; i++)
        {
            try (CqlSession session = defaultBuilder().build())
            {
                session.execute("SELECT now() FROM system.local");
                LOG.info("Authentication ready on attempt {}", i);
                return;
            }
            catch (RuntimeException e)
            {
                LOG.warn("Attempt {} to verify authentication readiness failed (likely transient auth "
                        + "propagation): {}", i, e.getMessage());
                Thread.sleep(2000);
            }
        }
        throw new IllegalStateException("Authentication as user 'cassandra' was not ready after " + attempts
                + " attempts. The system_auth replication change may not have propagated (repair did not "
                + "reconcile the superuser credentials to quorum) before the integration test continued.");
    }

    private static void verifyKeyspaceExists() throws IOException, InterruptedException
    {
        for (int i = 1; i < 20; i++)
        {
            int stdout = composeContainer.getContainerByServiceName(CASSANDRA_SEED_NODE_NAME).get()
                    .execInContainer("cqlsh", "-e", "DESCRIBE KEYSPACE ecchronos;").getExitCode();
            if (stdout == 0)
            {
                LOG.info("Keyspace verified on attempt " + i);
                return;
            }
            Thread.sleep(1000);
        }
    }
}

