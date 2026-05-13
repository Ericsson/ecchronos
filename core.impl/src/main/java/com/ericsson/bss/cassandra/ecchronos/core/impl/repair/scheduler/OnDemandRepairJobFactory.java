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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.IncrementalOnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OngoingJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.VnodeOnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;

import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Factory responsible for creating {@link OnDemandRepairJob} instances.
 */
public final class OnDemandRepairJobFactory
{
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final TableRepairMetrics myTableRepairMetrics;
    private final ReplicationState myReplicationState;
    private final RepairLockType myRepairLockType;
    private final RepairHistory myRepairHistory;
    private final RepairConfiguration myRepairConfiguration;
    private final OnDemandStatus myOnDemandStatus;
    private final BiConsumer<UUID, UUID> myOnFinishedHook;

    private OnDemandRepairJobFactory(final Builder builder)
    {
        myJmxProxyFactory = builder.myJmxProxyFactory;
        myTableRepairMetrics = builder.myTableRepairMetrics;
        myReplicationState = builder.myReplicationState;
        myRepairLockType = builder.myRepairLockType;
        myRepairHistory = builder.myRepairHistory;
        myRepairConfiguration = builder.myRepairConfiguration;
        myOnDemandStatus = builder.myOnDemandStatus;
        myOnFinishedHook = builder.myOnFinishedHook;
    }

    /**
     * Create an OnDemandRepairJob from an existing OngoingJob.
     *
     * @param ongoingJob The ongoing job to create a repair job for.
     * @return A new OnDemandRepairJob.
     */
    public OnDemandRepairJob createFromOngoingJob(final OngoingJob ongoingJob)
    {
        Node node = getNodeByHostId(ongoingJob.getHostId());
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder(myRepairConfiguration)
                .withRepairType(ongoingJob.getRepairType())
                .build();
        if (ongoingJob.getRepairType().equals(RepairType.INCREMENTAL))
        {
            return new IncrementalOnDemandRepairJob.Builder()
                    .withJmxProxyFactory(myJmxProxyFactory)
                    .withTableRepairMetrics(myTableRepairMetrics)
                    .withRepairLockType(myRepairLockType)
                    .withOnFinished(id -> myOnFinishedHook.accept(id, ongoingJob.getHostId()))
                    .withRepairConfiguration(repairConfiguration)
                    .withReplicationState(myReplicationState)
                    .withOngoingJob(ongoingJob)
                    .withNode(node)
                    .build();
        }
        return new VnodeOnDemandRepairJob.Builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairLockType(myRepairLockType)
                .withOnFinished(id -> myOnFinishedHook.accept(id, ongoingJob.getHostId()))
                .withRepairConfiguration(repairConfiguration)
                .withRepairHistory(myRepairHistory)
                .withOngoingJob(ongoingJob)
                .withNode(node)
                .build();
    }

    /**
     * Create a new OngoingJob and return the corresponding OnDemandRepairJob.
     *
     * @param tableReference The table to repair.
     * @param isClusterWide Whether this is a cluster-wide job.
     * @param repairType The repair type.
     * @param hostId The host to run on.
     * @return A new OnDemandRepairJob.
     */
    public OnDemandRepairJob createNewJob(final TableReference tableReference,
                                          final boolean isClusterWide,
                                          final RepairType repairType,
                                          final UUID hostId)
    {
        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withTableReference(tableReference)
                .withReplicationState(myReplicationState)
                .withHostId(hostId)
                .withRepairType(repairType)
                .build();
        if (isClusterWide)
        {
            ongoingJob.startClusterWideJob(repairType);
        }
        return createFromOngoingJob(ongoingJob);
    }

    private Node getNodeByHostId(final UUID hostId)
    {
        Node node = myOnDemandStatus.getNodes().get(hostId);
        if (node == null)
        {
            throw new NoSuchElementException("No node found with host ID: " + hostId);
        }
        return node;
    }

    /**
     * Create a new Builder instance.
     *
     * @return Builder
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder for constructing {@link OnDemandRepairJobFactory}.
     */
    public static final class Builder
    {
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private TableRepairMetrics myTableRepairMetrics;
        private ReplicationState myReplicationState;
        private RepairLockType myRepairLockType;
        private RepairHistory myRepairHistory;
        private RepairConfiguration myRepairConfiguration;
        private OnDemandStatus myOnDemandStatus;
        private BiConsumer<UUID, UUID> myOnFinishedHook;

        public Builder withJmxProxyFactory(final DistributedJmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public Builder withTableRepairMetrics(final TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public Builder withReplicationState(final ReplicationState replicationState)
        {
            myReplicationState = replicationState;
            return this;
        }

        public Builder withRepairLockType(final RepairLockType repairLockType)
        {
            myRepairLockType = repairLockType;
            return this;
        }

        public Builder withRepairHistory(final RepairHistory repairHistory)
        {
            myRepairHistory = repairHistory;
            return this;
        }

        public Builder withRepairConfiguration(final RepairConfiguration repairConfiguration)
        {
            myRepairConfiguration = repairConfiguration;
            return this;
        }

        public Builder withOnDemandStatus(final OnDemandStatus onDemandStatus)
        {
            myOnDemandStatus = onDemandStatus;
            return this;
        }

        public Builder withOnFinishedHook(final BiConsumer<UUID, UUID> onFinishedHook)
        {
            myOnFinishedHook = onFinishedHook;
            return this;
        }

        public OnDemandRepairJobFactory build()
        {
            return new OnDemandRepairJobFactory(this);
        }
    }
}
