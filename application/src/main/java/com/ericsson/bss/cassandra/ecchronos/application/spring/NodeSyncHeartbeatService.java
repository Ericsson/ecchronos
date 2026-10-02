/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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

import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.NodesSyncHeartbeatConfig;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.sync.NodeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Service;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import jakarta.annotation.PostConstruct;

/**
 * Service that periodically renews (heartbeats) this instance's rows in the {@code nodes_sync}
 * table, applying a TTL to each write. While this instance is alive the rows are continuously
 * refreshed and survive; once the instance stops, the rows are no longer renewed and expire via
 * their TTL. This makes {@code nodes_sync} self-healing for permanent scale-in/decommission
 * (issue #1826), without requiring any cross-instance reconciliation.
 */
@Service
public class NodeSyncHeartbeatService implements DisposableBean
{
    private static final Logger LOG = LoggerFactory.getLogger(NodeSyncHeartbeatService.class);
    private static final int DEFAULT_SCHEDULER_AWAIT_TERMINATION_IN_SECONDS = 60;

    private static final String COLUMN_NODE_ID = "node_id";
    private static final String COLUMN_DATACENTER_NAME = "datacenter_name";
    private static final String COLUMN_NODE_ENDPOINT = "node_endpoint";
    private static final String COLUMN_NODE_STATUS = "node_status";

    private final EccNodesSync myEccNodesSync;
    private final NodesSyncHeartbeatConfig myConfig;
    private final DistributedNativeConnectionProvider myNativeConnectionProvider;
    private final ScheduledExecutorService myScheduler = Executors.newScheduledThreadPool(1);

    /**
     * Constructs a new NodeSyncHeartbeatService.
     *
     * @param config the application configuration providing heartbeat settings.
     * @param eccNodesSync the node sync data access used to renew rows.
     * @param nativeConnectionProvider the provider used to check current topology membership.
     */
    public NodeSyncHeartbeatService(final Config config,
                                    final EccNodesSync eccNodesSync,
                                    final DistributedNativeConnectionProvider nativeConnectionProvider)
    {
        this.myEccNodesSync = eccNodesSync;
        this.myNativeConnectionProvider = nativeConnectionProvider;
        this.myConfig = config.getConnectionConfig().getCqlConnection().getNodesSyncHeartbeat();
    }

    /**
     * Renews all rows owned by this instance, preserving each row's current status and endpoint
     * and applying the configured TTL.
     */
    final void heartbeat()
    {
        try
        {
            int ttlInSeconds = myConfig.getTtlInSeconds();
            ResultSet rs = myEccNodesSync.getAllByLocalInstance();
            int renewed = 0;
            int skipped = 0;
            for (Row row : rs)
            {
                UUID nodeId = row.getUuid(COLUMN_NODE_ID);

                // Avoid a race with node removal: a node may have been removed (and its row
                // deleted by NodeRemovedAction) after we read the result set. Re-check current
                // topology membership immediately before rewriting so we do not resurrect a
                // just-deleted row with a fresh TTL. getNodes() is updated when a node is removed.
                if (!myNativeConnectionProvider.getNodes().containsKey(nodeId))
                {
                    skipped++;
                    continue;
                }

                String datacenterName = row.getString(COLUMN_DATACENTER_NAME);
                String nodeEndpoint = row.getString(COLUMN_NODE_ENDPOINT);
                String statusStr = row.getString(COLUMN_NODE_STATUS);
                NodeStatus status = parseStatus(statusStr);

                myEccNodesSync.updateNodeHeartbeat(
                        status,
                        datacenterName,
                        nodeEndpoint,
                        nodeId,
                        ttlInSeconds);
                renewed++;
            }
            LOG.debug("Heartbeat renewed {} nodes_sync row(s) with TTL {}s (skipped {} no longer in topology)",
                    renewed, ttlInSeconds, skipped);
        }
        catch (Exception e)
        {
            LOG.error("Failed to write nodes_sync heartbeat", e);
        }
    }

    private NodeStatus parseStatus(final String statusStr)
    {
        if (statusStr == null)
        {
            return NodeStatus.UNAVAILABLE;
        }
        try
        {
            return NodeStatus.valueOf(statusStr.toUpperCase(Locale.ENGLISH));
        }
        catch (IllegalArgumentException e)
        {
            LOG.warn("Unknown node_status '{}' encountered during heartbeat, preserving as UNAVAILABLE", statusStr);
            return NodeStatus.UNAVAILABLE;
        }
    }

    /**
     * Starts the scheduled heartbeat task.
     */
    @PostConstruct
    public final void startScheduler()
    {
        long intervalMs = myConfig.getIntervalInMs();
        LOG.info("Starting NodeSyncHeartbeatService with interval={} ms and TTL={} s",
                intervalMs, myConfig.getTtlInSeconds());
        myScheduler.scheduleWithFixedDelay(this::heartbeat, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override
    public final void destroy()
    {
        LOG.info("Shutting down NodeSyncHeartbeatService...");
        RetryServiceShutdownManager.shutdownExecutorService(
                myScheduler, DEFAULT_SCHEDULER_AWAIT_TERMINATION_IN_SECONDS, TimeUnit.SECONDS);
        LOG.info("NodeSyncHeartbeatService shut down complete.");
    }
}
