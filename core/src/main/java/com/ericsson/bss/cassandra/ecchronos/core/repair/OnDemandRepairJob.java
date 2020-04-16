/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairGroupFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;

/**
 * A Job that will schedule and run repair on one table once. It creates {@link RepairTask RepairTasks} to fully repair
 * the table. Once all {@link RepairTask RepairTasks} are completed, the repair is finished and the job will be
 * descheduled.
 */
public class OnDemandRepairJob extends ScheduledJob
{
    private final TableReference myTableReference;
    private final JmxProxyFactory myJmxProxyFactory;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairLockType myRepairLockType;
    private final ReplicationState myReplicationState;
    private final Consumer<TableReference> myOnFinishedHook;

    private final TableRepairMetrics myTableRepairMetrics;

    private final List<ScheduledTask> myTasks;

    public OnDemandRepairJob(OnDemandRepairJob.Builder builder)
    {
        super(builder.configuration);

        myTableReference = builder.tableReference;
        myJmxProxyFactory = builder.jmxProxyFactory;
        myTableRepairMetrics = builder.tableRepairMetrics;
        myRepairConfiguration = builder.repairConfiguration;
        myRepairLockType = builder.repairLockType;
        myReplicationState = builder.replicationState;
        myOnFinishedHook = builder.onFinishedHook;
        myTasks = createRepairTasks();
    }

    private List<ScheduledTask> createRepairTasks()
    {
        List<ScheduledTask> taskList = new ArrayList<>();
        List<VnodeRepairState> vnodeRepairStates = new ArrayList<>();

        Map<LongTokenRange, ImmutableSet<Host>> tokenRanges = myReplicationState
                .getTokenRangeToReplicas(myTableReference);

        for (Map.Entry<LongTokenRange, ImmutableSet<Host>> entry : tokenRanges.entrySet())
        {
            LongTokenRange longTokenRange = entry.getKey();
            ImmutableSet<Host> replicas = entry.getValue();
            vnodeRepairStates.add(new VnodeRepairState(longTokenRange, replicas, -1));
        }

        List<ReplicaRepairGroup> repairGroups = VnodeRepairGroupFactory.INSTANCE
                .generateReplicaRepairGroups(vnodeRepairStates);

        for (ReplicaRepairGroup replicaRepairGroup : repairGroups)
        {
            taskList.add(new RepairGroup(getRealPriority(), myTableReference, myRepairConfiguration,
                    replicaRepairGroup, myJmxProxyFactory, myTableRepairMetrics,
                    myRepairLockType.getLockFactory(),
                    new RepairLockFactoryImpl()));
        }
        return taskList;
    }

    public TableReference getTableReference()
    {
        return myTableReference;
    }

    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }

    public RepairJobView getView()
    {
        return new RepairJobView(myTableReference, myRepairConfiguration, null);
    }

    @Override
    public Iterator<ScheduledTask> iterator()
    {
        return myTasks.iterator();
    }

    @Override
    public void postExecute(boolean successful, ScheduledTask task)
    {
        myTasks.remove(task);

        if (myTasks.isEmpty())
        {
            myOnFinishedHook.accept(myTableReference);
        }

        super.postExecute(successful, task);
    }

    @Override
    public long getLastSuccessfulRun()
    {
        return -1;
    }

    @Override
    public boolean runnable()
    {
        return true;
    }

    @Override
    public State getState()
    {
        if (myTasks.isEmpty())
        {
            return State.FINISHED;
        }
        else
        {
            return State.RUNNABLE;
        }
    }

    @Override
    public String toString()
    {
        return String.format("Repair job of %s", myTableReference);
    }

    public static class Builder
    {
        private final Configuration configuration = new ConfigurationBuilder()
                .withPriority(Priority.HIGHEST)
                .withRunInterval(0, TimeUnit.DAYS)
                .build();
        private TableReference tableReference;
        private JmxProxyFactory jmxProxyFactory;
        private TableRepairMetrics tableRepairMetrics = null;
        private final RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;
        private RepairLockType repairLockType;
        private ReplicationState replicationState;
        private Consumer<TableReference> onFinishedHook = table -> {
        };

        public Builder withTableReference(TableReference tableReference)
        {
            this.tableReference = tableReference;
            return this;
        }

        public Builder withJmxProxyFactory(JmxProxyFactory jmxProxyFactory)
        {
            this.jmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public Builder withTableRepairMetrics(TableRepairMetrics tableRepairMetrics)
        {
            this.tableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public Builder withRepairLockType(RepairLockType repairLockType)
        {
            this.repairLockType = repairLockType;
            return this;
        }

        public Builder withReplicationState(ReplicationState replicationState)
        {
            this.replicationState = replicationState;
            return this;
        }

        public Builder withOnFinished(Consumer<TableReference> onFinishedHook)
        {
            this.onFinishedHook = onFinishedHook;
            return this;
        }

        public OnDemandRepairJob build()
        {
            if (tableReference == null)
            {
                throw new IllegalArgumentException("Table reference cannot be null");
            }
            if (jmxProxyFactory == null)
            {
                throw new IllegalArgumentException("JMX Proxy factory cannot be null");
            }
            if (tableRepairMetrics == null)
            {
                throw new IllegalArgumentException("Metric interface not set");
            }
            if (replicationState == null)
            {
                throw new IllegalArgumentException("Replication State not set");
            }
            return new OnDemandRepairJob(this);
        }
    }
}
