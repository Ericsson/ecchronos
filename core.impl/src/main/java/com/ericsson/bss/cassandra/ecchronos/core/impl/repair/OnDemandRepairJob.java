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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import java.util.UUID;
import java.util.function.Consumer;
import com.google.common.base.Preconditions;

public abstract class OnDemandRepairJob extends ScheduledJob
{
    protected static final RepairLockFactory REPAIR_LOCK_FACTORY = new RepairLockFactoryImpl();
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairLockType myRepairLockType;
    private final Consumer<UUID> myOnFinishedHook;
    private final TableRepairMetrics myTableRepairMetrics;
    private final OngoingJob myOngoingJob;
    private final Node myCurrentNode;

    private boolean hasFailed;

    public OnDemandRepairJob(final Configuration configuration, final DistributedJmxProxyFactory jmxProxyFactory,
                             final RepairConfiguration repairConfiguration, final RepairLockType repairLockType,
                             final Consumer<UUID> onFinishedHook, final TableRepairMetrics tableRepairMetrics,
                             final OngoingJob ongoingJob,
                             final Node currentNode)
    {
        super(configuration, ongoingJob.getJobId());

        myOngoingJob = Preconditions.checkNotNull(ongoingJob,
                "Ongoing job must be set");
        myJmxProxyFactory = Preconditions.checkNotNull(jmxProxyFactory,
                "JMX Proxy Factory must be set");
        myTableRepairMetrics = Preconditions.checkNotNull(tableRepairMetrics,
                "Table repair metrics must be set");
        myRepairConfiguration = Preconditions.checkNotNull(repairConfiguration,
                "Repair configuration must be set");
        myRepairLockType = Preconditions.checkNotNull(repairLockType,
                "Repair lock type must be set");
        myOnFinishedHook = Preconditions.checkNotNull(onFinishedHook,
                "On finished hook must be set");
        myCurrentNode = Preconditions.checkNotNull(currentNode,
                "On current node must be set");
    }

    /**
     * Get the table reference for this job.
     * @return Table reference
     */
    public TableReference getTableReference()
    {
        return myOngoingJob.getTableReference();
    }

    protected final DistributedJmxProxyFactory getJmxProxyFactory()
    {
        return myJmxProxyFactory;
    }

    /**
     * Get the repair configuration for this job.
     * @return Repair configuration
     */
    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }

    protected final RepairLockType getRepairLockType()
    {
        return myRepairLockType;
    }

    protected final TableRepairMetrics getTableRepairMetrics()
    {
        return myTableRepairMetrics;
    }

    protected final Consumer<UUID> getOnFinishedHook()
    {
        return myOnFinishedHook;
    }

    public final OngoingJob getOngoingJob()
    {
        return myOngoingJob;
    }

    protected final Node getCurrentNode()
    {
        return myCurrentNode;
    }

    protected final void setFailed(final boolean failed)
    {
        hasFailed = failed;
    }

    protected final boolean hasFailed()
    {
        return hasFailed;
    }

    protected final OnDemandRepairJobView.Status getStatus()
    {
        if (hasFailed || getOngoingJob().getStatus() == OngoingJob.Status.failed)
        {
            return OnDemandRepairJobView.Status.ERROR;
        }
        else if (getOngoingJob().getStatus() == OngoingJob.Status.finished)
        {
            return OnDemandRepairJobView.Status.COMPLETED;
        }
        return OnDemandRepairJobView.Status.IN_QUEUE;
    }

    public abstract OnDemandRepairJobView getView();

    @Override
    public final long getLastSuccessfulRun()
    {
        return -1;
    }

    @Override
    public final boolean runnable()
    {
        return getState().equals(State.RUNNABLE);
    }
}

