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
    // Budget for the initial cold bring-up of all nodes. Matches the python harness
    // (CLUSTER_STARTUP_WAIT_TIME_IN_SECS = 180s): the previous 90s was often too short for a
    // 4-node/2-DC cluster to fully converge on slower/loaded CI runners, and now that
    // waitForNodesToBeUp throws on timeout an under-budgeted wait would fail an otherwise
    // healthy but slow bring-up.
    protected static final long DEFAULT_WAIT_TIME_IN_MS = 180000;
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
        waitForAuthReady("cassandra", "cassandra");
        setupDb();
        // setupDb() creates the 'eccuser' application role (users.cql). Like the default
        // superuser, that role row is read at QUORUM during authentication, but it is written
        // only after the system_auth repair above, so it can be missing on the newly required
        // replicas when the tests build their eccuser session. Reconcile system_auth again and
        // wait until 'eccuser' can authenticate, so tests do not race the role's propagation
        // ("Provided username eccuser and/or password are incorrect" / "Cannot achieve
        // consistency level LOCAL_QUORUM").
        repairSystemAuthOnAllNodes();
        waitForAuthReady("eccuser", "eccpassword");
        verifyKeyspaceExists();
    }

    protected static void createDefaultSession()
    {
        mySession = defaultBuilder().build();
    }

    protected static CqlSessionBuilder defaultBuilder()
    {
        return builderForUser("cassandra", "cassandra");
    }

    protected static CqlSessionBuilder builderForUser(String username, String password)
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
                .withAuthCredentials(username, password)
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
        // Require the expected UN count to hold for a couple of consecutive polls before
        // proceeding: a just-joined node can briefly report UN and then flap to UJ/DN, and
        // returning on a single reading lets setup continue against a not-yet-converged
        // cluster. With system_auth RF {datacenter1:1, datacenter2:2}, authentication reads
        // at LOCAL_QUORUM in datacenter1 (RF=1) cannot be satisfied until that DC's replica
        // is actually up, so proceeding early surfaces as a persistent
        // "Cannot achieve consistency level LOCAL_QUORUM" during session/auth setup.
        //
        // Throw on timeout rather than logging and returning: previously this method only
        // logged, so a cluster that never reached the expected node count silently continued
        // into auth setup and failed much later with a misleading error. Failing fast here
        // reports the real cause (nodes not up) at the point it actually occurs.
        final int requiredStablePolls = 2;
        long startTime = System.currentTimeMillis();
        int consecutiveOk = 0;
        int lastCount = -1;

        while ( startTime + maxWaitTimeInMillis > System.currentTimeMillis())
        {
            try
            {
                Thread.sleep(DEFAULT_WAIT_TIME_IN_SECS);
                lastCount = getNodeCountViaNodetool(node);
                if (lastCount == expectedNodes)
                {
                    consecutiveOk++;
                    if (consecutiveOk >= requiredStablePolls)
                    {
                        return;
                    }
                }
                else
                {
                    consecutiveOk = 0;
                }
            }
            catch (IOException | InterruptedException e)
            {
                // nodetool not answering yet (node still starting); reset stability and retry
                consecutiveOk = 0;
            }
        }
        throw new IllegalStateException("Cassandra cluster did not reach " + expectedNodes
                + " Up/Normal (UN) nodes (stable over " + requiredStablePolls + " polls) within "
                + maxWaitTimeInMillis + "ms. Last observed UN count: " + lastCount
                + ". Proceeding would race auth reads at LOCAL_QUORUM (system_auth RF datacenter1:1) "
                + "and fail later with a misleading authentication error.");
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
        // Reconcile the default superuser row onto all system_auth replicas required by the raised
        // replication factor before any session is built. See repairSystemAuthOnAllNodes.
        repairSystemAuthOnAllNodes();
    }

    /**
     * Repair the system_auth keyspace on every node so that any role row (the default superuser, and the
     * application 'eccuser' role created later by setup_db.sh) is reconciled onto all replicas required by
     * the raised replication factor. A {@code nodetool repair} only reconciles ranges the invoked node is a
     * replica for, so repairing a single node can leave a role row short of QUORUM; authentication reads the
     * role at QUORUM, so an under-replicated row surfaces as a persistent "Provided username ... incorrect".
     */
    private static void repairSystemAuthOnAllNodes() throws IOException, InterruptedException
    {
        for (String node : CASSANDRA_NODE_NAMES)
        {
            composeContainer.getContainerByServiceName(node).get()
                    .execInContainer("nodetool", "-u", "cassandra", "-pw", "cassandra",
                            "repair", "--full", "system_auth");
        }
    }

    /**
     * Wait until the given role can actually authenticate through the native driver.
     *
     * After raising the system_auth replication factor, a role row is read at QUORUM and its credentials
     * may not yet be present on the newly required replicas, causing transient
     * "Provided username ... incorrect" / "Cannot achieve consistency level LOCAL_QUORUM" failures when the
     * tests build their CqlSession. Verify readiness the same way the tests connect - a driver session
     * against the cluster IP in datacenter1 - rather than a local cqlsh on the seed, which can succeed
     * against a single node while a QUORUM read still fails. Fail fast if it never becomes ready, so the
     * real cause is reported here instead of cascading into every test's session setup.
     */
    private static void waitForAuthReady(String username, String password) throws InterruptedException
    {
        int attempts = 60;
        for (int i = 1; i <= attempts; i++)
        {
            try (CqlSession session = builderForUser(username, password).build())
            {
                session.execute("SELECT now() FROM system.local");
                LOG.info("Authentication ready for user '{}' on attempt {}", username, i);
                return;
            }
            catch (RuntimeException e)
            {
                LOG.warn("Attempt {} to verify authentication readiness for user '{}' failed (likely "
                        + "transient auth propagation): {}", i, username, e.getMessage());
                Thread.sleep(2000);
            }
        }
        throw new IllegalStateException("Authentication as user '" + username + "' was not ready after "
                + attempts + " attempts. The system_auth replication change may not have propagated (repair "
                + "did not reconcile the role credentials to quorum) before the integration test continued.");
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

