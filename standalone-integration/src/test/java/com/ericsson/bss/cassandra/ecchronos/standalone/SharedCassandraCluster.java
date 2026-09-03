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
package com.ericsson.bss.cassandra.ecchronos.standalone;

import cassandracluster.AbstractCassandraCluster;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Image;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;

import java.io.IOException;
import java.util.regex.Pattern;

public class SharedCassandraCluster extends AbstractCassandraCluster
{
    private static final Logger LOG = LoggerFactory.getLogger(SharedCassandraCluster.class);
    // Matches the images docker compose builds for the cluster services, regardless of
    // the random testcontainers project prefix, e.g. 'awjhc7vxdswz_cassandra-seed-dc1-rack1-node1'.
    // The service-name part must stay in sync with the services declared in
    // cassandra-test-image/src/main/docker/docker-compose.yml; if those are renamed, this
    // pattern needs updating (worst case if it drifts: the built images are not removed and
    // accumulate again -- it never causes a test failure).
    private static final Pattern COMPOSE_IMAGE_PATTERN =
            Pattern.compile("^.*_cassandra-(seed|node)-dc\\d+-rack\\d+-node\\d+(:.*)?$");

    private static volatile boolean initialized = false;
    private static final Object lock = new Object();

    public static void ensureInitialized() throws IOException, InterruptedException
    {
        if (!initialized)
        {
            synchronized (lock)
            {
                if (!initialized)
                {
                    setup();
                    initialized = true;
                    // The shared cluster is started once and reused across all IT classes,
                    // so there is no @AfterClass that tears it down. Register a JVM shutdown
                    // hook (all ITs run in a single reused Failsafe fork) to tear the cluster
                    // down and remove the locally-built compose images exactly once at the
                    // end of the run. Image removal is by name (see removeComposeImages), so
                    // it works regardless of whether the containers have already been reaped.
                    Runtime.getRuntime().addShutdownHook(new Thread(
                            SharedCassandraCluster::tearDownIfInitialized,
                            "shared-cassandra-cluster-cleanup"));
                }
            }
        }
    }

    /**
     * Tears down the shared cluster if it was started, and removes the locally-built
     * compose images. Invoked once from the JVM shutdown hook registered in
     * {@link #ensureInitialized()}.
     *
     * <p>The images are removed explicitly by name rather than via
     * {@code docker compose down --rmi}, because the shared cluster is reused across all
     * IT classes and, by the time teardown runs, compose can no longer reliably associate
     * the built images with the (already torn-down) project. Matching on the stable
     * service-name pattern removes them deterministically without depending on the random
     * project prefix or compose project state. The pulled base image (cassandra:X.Y) does
     * not match the pattern and is kept.
     */
    public static void tearDownIfInitialized()
    {
        synchronized (lock)
        {
            if (!initialized)
            {
                return;
            }
            try
            {
                tearDownCluster();
            }
            finally
            {
                initialized = false;
                removeComposeImages();
            }
        }
    }

    private static void removeComposeImages()
    {
        try
        {
            DockerClient client = DockerClientFactory.lazyClient();
            for (Image image : client.listImagesCmd().exec())
            {
                String[] repoTags = image.getRepoTags();
                if (repoTags == null)
                {
                    continue;
                }
                for (String repoTag : repoTags)
                {
                    if (repoTag != null && COMPOSE_IMAGE_PATTERN.matcher(repoTag).matches())
                    {
                        try
                        {
                            client.removeImageCmd(repoTag).withForce(true).exec();
                            LOG.info("Removed leftover compose test image {}", repoTag);
                        }
                        catch (RuntimeException e)
                        {
                            LOG.warn("Failed to remove compose test image {}: {}", repoTag, e.getMessage());
                        }
                    }
                }
            }
        }
        catch (RuntimeException e)
        {
            LOG.warn("Failed to remove leftover compose test images", e);
        }
    }

    public static String getContainerIP()
    {
        return containerIP;
    }
}