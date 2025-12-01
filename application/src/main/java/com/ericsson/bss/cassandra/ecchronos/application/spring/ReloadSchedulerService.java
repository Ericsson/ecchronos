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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.application.config.connection.DistributedNativeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Service;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.RetryPolicyConfig;
import com.ericsson.bss.cassandra.ecchronos.application.providers.MountConnectionHelper;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.DistributedNativeBuilder;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.DefaultRepairConfigurationProvider;

import jakarta.annotation.PostConstruct;

@Service
public class ReloadSchedulerService implements DisposableBean
{
    private static final Logger LOG = LoggerFactory.getLogger(ReloadSchedulerService.class);
    private static final int DEFAULT_SCHEDULER_AWAIT_TERMINATION_IN_SECONDS = 60;
    private final DistributedNativeConnectionProvider myDistributedNativeConnectionProvider;
    private final RetryPolicyConfig.RetrySchedule myScheduleConfig;
    private final ScheduledExecutorService myScheduler = Executors.newScheduledThreadPool(1);
    private final Config myConfig;
    private final DefaultRepairConfigurationProvider myDefaultRepairConfigurationProvider;
    private final MountConnectionHelper myConnectionHelper = new MountConnectionHelper();

    public ReloadSchedulerService(
        final Config config,
        final DistributedNativeConnectionProvider distributedNativeConnectionProvider,
        final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider)
    {
        this.myDefaultRepairConfigurationProvider = defaultRepairConfigurationProvider;
        this.myConfig = config;
        this.myDistributedNativeConnectionProvider = distributedNativeConnectionProvider;
        this.myScheduleConfig = config.getConnectionConfig().getCqlConnection().getReloadSchedule();
    }

    private void reloadNodesMap()
    {
        try
        {
            LOG.info("Attempting to verify nodes map disagreements.");
            DistributedNativeConnection nativeConnectionConfig = myConfig.getConnectionConfig().getCqlConnection();

            DistributedNativeBuilder nativeBuilder = new DistributedNativeBuilder()
                    .withAgentType(nativeConnectionConfig.getType());

            // Use helper method to resolve agent provider configuration
            nativeBuilder = resolveAgentProviderBuilder(nativeBuilder, nativeConnectionConfig);

            Map<UUID, Node> nodes = nativeBuilder.createNodesMap(myDistributedNativeConnectionProvider.getCqlSession());
            compareNodesMap(nodes);
        }
        catch (Exception e)
        {
            LOG.error("Failed to reload nodes map", e);
        }
    }

    @SuppressWarnings("CPD-START")
    public final DistributedNativeBuilder resolveAgentProviderBuilder(
          final DistributedNativeBuilder builder,
          final DistributedNativeConnection nativeConnectionConfig)
    {
        return switch (nativeConnectionConfig.getType())
        {
            case datacenterAware ->
            {
                LOG.info("Using DatacenterAware as Agent Config");
                yield builder.withDatacenterAware(myConnectionHelper.resolveDatacenterAware(
                        nativeConnectionConfig.getDatacenterAware()));
            }
            case rackAware ->
            {
                LOG.info("Using RackAware as Agent Config");
                yield builder.withRackAware(myConnectionHelper.resolveRackAware(
                        nativeConnectionConfig.getRackAware()));
            }
            case hostAware ->
            {
                LOG.info("Using HostAware as Agent Config");
                yield builder.withHostAware(myConnectionHelper.resolveHostAware(
                        nativeConnectionConfig.getHostAware()));
            }
        };
    }

    private void compareNodesMap(final Map<UUID, Node> newMap)
    {
        // First add nodes
        for (Map.Entry<UUID, Node> entry : newMap.entrySet())
        {
            UUID nodeId = entry.getKey();
            Node node = entry.getValue();

            if (!myDistributedNativeConnectionProvider.getNodes().containsKey(nodeId))
            {
                LOG.info("Found disagreement, node {} is not in the nodes map, adding it", nodeId);
                myDefaultRepairConfigurationProvider.onAdd(node);
            }
        }

        // Check if there are nodes that are not in the new map
        for (Map.Entry<UUID, Node> entry : myDistributedNativeConnectionProvider.getNodes().entrySet())
        {
            UUID nodeId = entry.getKey();
            Node node = entry.getValue();

            if (!newMap.containsKey(nodeId))
            {
                LOG.info("Found disagreement, node {} is not in the session metadata, removing it", nodeId);
                myDefaultRepairConfigurationProvider.onRemove(node);
            }
        }
    }

    @PostConstruct
    public final void startScheduler()
    {
        long initialDelay = myScheduleConfig.getInitialDelay();
        long fixedDelay = myScheduleConfig.getFixedDelay();

        LOG.debug("Starting ReloadSchedulerService with initialDelay={} ms and fixedDelay={} ms", initialDelay, fixedDelay);

        myScheduler.scheduleWithFixedDelay(this::reloadNodesMap, initialDelay, fixedDelay, myScheduleConfig.getUnit());
    }

    @Override
    public final void destroy()
    {
        LOG.info("Shutting down ReloadSchedulerService...");
        RetryServiceShutdownManager.shutdownExecutorService(myScheduler, DEFAULT_SCHEDULER_AWAIT_TERMINATION_IN_SECONDS, TimeUnit.SECONDS);
        LOG.info("ReloadSchedulerService shut down complete.");
    }

}
