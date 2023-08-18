/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.ericsson.bss.cassandra.ecchronos.core.Clock;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter.FaultCode;
import com.google.common.annotations.VisibleForTesting;

/**
 * A scheduled job that keeps track of the repair status of a single table. The table is considered repaired for this node if all the ranges this node
 * is responsible for is repaired within the minimum run interval.
 * <p>
 * When run this job will create {@link RepairTask RepairTasks} that repairs the table.
 */
public class TableRepairJob extends ScheduledJob
{
    private static final Logger LOG = LoggerFactory.getLogger(TableRepairJob.class);

    private final TableReference myTableReference;
    private final JmxProxyFactory myJmxProxyFactory;
    private final RepairState myRepairState;
    private final RepairFaultReporter myFaultReporter;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairLockType myRepairLockType;
    private final List<TableRepairPolicy> myRepairPolicies;

    private final TableRepairMetrics myTableRepairMetrics;

    private final AtomicReference<Clock> myClock = new AtomicReference<>(Clock.DEFAULT);

    TableRepairJob(Builder builder)
    {
        super(builder.configuration);

        myTableReference = builder.tableReference;
        myJmxProxyFactory = builder.jmxProxyFactory;
        myRepairState = builder.repairState;
        myFaultReporter = builder.faultReporter;
        myTableRepairMetrics = builder.tableRepairMetrics;
        myRepairConfiguration = builder.repairConfiguration;
        myRepairLockType = builder.repairLockType;
        myRepairPolicies = builder.repairPolicies;
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
        return new RepairJobView(myTableReference, myRepairConfiguration, myRepairState.getSnapshot());
    }

    @Override
    public Iterator<ScheduledTask> iterator()
    {
        RepairStateSnapshot repairStateSnapshot = myRepairState.getSnapshot();
        if (repairStateSnapshot.canRepair())
        {
            List<ScheduledTask> taskList = new ArrayList<>();

            for (ReplicaRepairGroup replicaRepairGroup : repairStateSnapshot.getRepairGroups())
            {
                taskList.add(new RepairGroup(getRealPriority(replicaRepairGroup.getLastCompletedAt()),
                        myTableReference, myRepairConfiguration,
                        replicaRepairGroup, myJmxProxyFactory, myTableRepairMetrics,
                        myRepairLockType.getLockFactory(),
                        new RepairLockFactoryImpl(), myRepairPolicies));
            }

            return taskList.iterator();
        }
        else
        {
            return Collections.emptyIterator();
        }
    }

    @Override
    public void postExecute(boolean successful)
    {
        try
        {
            myRepairState.update();

            RepairStateSnapshot repairStateSnapshot = myRepairState.getSnapshot();

            long lastRepaired = repairStateSnapshot.lastRepairedAt();

            if (lastRepaired != VnodeRepairState.UNREPAIRED)
            {
                sendOrCeaseAlarm(lastRepaired);
            }
        }
        catch (Exception e)
        {
            LOG.warn("Unable to check repair history, {}", this, e);
        }

        super.postExecute(successful);
    }

    @Override
    public long getLastSuccessfulRun()
    {
        return myRepairState.getSnapshot().lastRepairedAt();
    }

    private void raiseAlarm(FaultCode faultCode)
    {
        Map<String, Object> faultData = new HashMap<>();
        faultData.put(RepairFaultReporter.FAULT_KEYSPACE, myTableReference.getKeyspace());
        faultData.put(RepairFaultReporter.FAULT_TABLE, myTableReference.getTable());

        myFaultReporter.raise(faultCode, faultData);
    }

    private void ceaseWarningAlarm()
    {
        Map<String, Object> ceaseData = new HashMap<>();
        ceaseData.put(RepairFaultReporter.FAULT_KEYSPACE, myTableReference.getKeyspace());
        ceaseData.put(RepairFaultReporter.FAULT_TABLE, myTableReference.getTable());

        myFaultReporter.cease(FaultCode.REPAIR_WARNING, ceaseData);
    }

    private void sendOrCeaseAlarm(long lastRepaired)
    {
        long msSinceLastRepair = myClock.get().getTime() - lastRepaired;

        FaultCode faultCode = null;

        if (msSinceLastRepair >= myRepairConfiguration.getRepairErrorTimeInMs())
        {
            faultCode = FaultCode.REPAIR_ERROR;
        }
        else if (msSinceLastRepair >= myRepairConfiguration.getRepairWarningTimeInMs())
        {
            faultCode = FaultCode.REPAIR_WARNING;
        }

        if (faultCode != null)
        {
            raiseAlarm(faultCode);
        }
        else
        {
            ceaseWarningAlarm();
        }
    }

    @Override
    public boolean runnable()
    {
        RepairStateSnapshot repairStateSnapshot = myRepairState.getSnapshot();

        long lastRepaired = repairStateSnapshot.lastRepairedAt();

        if (lastRepaired != VnodeRepairState.UNREPAIRED)
        {
            sendOrCeaseAlarm(lastRepaired);
        }

        return repairStateSnapshot.canRepair() && super.runnable();
    }

    @Override
    public void refreshState()
    {
        try
        {
            myRepairState.update();
        }
        catch (Exception e)
        {
            LOG.warn("Unable to check repair history, {}", this, e);
        }
    }

    /**
     * Calculate real priority based on available tasks.
     */
    @Override
    public final int getRealPriority()
    {
        RepairStateSnapshot repairStateSnapshot = myRepairState.getSnapshot();
        int priority = -1;
        if (repairStateSnapshot.canRepair())
        {
            long minRepairedAt = System.currentTimeMillis();
            for (ReplicaRepairGroup replicaRepairGroup : repairStateSnapshot.getRepairGroups())
            {
                long replicaGroupCompletedAt = replicaRepairGroup.getLastCompletedAt();
                if (replicaGroupCompletedAt < minRepairedAt)
                {
                    minRepairedAt = replicaGroupCompletedAt;
                }
            }
            priority = getRealPriority(minRepairedAt);
        }
        return priority;
    }

    @Override
    public String toString()
    {
        return String.format("Repair job of %s", myTableReference);
    }

    public static class Builder
    {
        Configuration configuration = new ConfigurationBuilder()
                .withPriority(Priority.LOW)
                .withRunInterval(7, TimeUnit.DAYS)
                .build();
        private TableReference tableReference;
        private JmxProxyFactory jmxProxyFactory;
        private RepairState repairState;
        private RepairFaultReporter faultReporter;
        private TableRepairMetrics tableRepairMetrics = null;
        private RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;
        private RepairLockType repairLockType;
        private final List<TableRepairPolicy> repairPolicies = new ArrayList<>();

        public Builder withConfiguration(Configuration configuration)
        {
            this.configuration = configuration;
            return this;
        }

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

        public Builder withRepairState(RepairState repairState)
        {
            this.repairState = repairState;
            return this;
        }

        public Builder withFaultReporter(RepairFaultReporter faultReporter)
        {
            this.faultReporter = faultReporter;
            return this;
        }

        public Builder withTableRepairMetrics(TableRepairMetrics tableRepairMetrics)
        {
            this.tableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public Builder withRepairConfiguration(RepairConfiguration repairConfiguration)
        {
            this.repairConfiguration = repairConfiguration;
            return this;
        }

        public Builder withRepairLockType(RepairLockType repairLockType)
        {
            this.repairLockType = repairLockType;
            return this;
        }

        public Builder withRepairPolices(Collection<TableRepairPolicy> tableRepairPolicies)
        {
            this.repairPolicies.addAll(tableRepairPolicies);
            return this;
        }

        public TableRepairJob build()
        {
            if (tableReference == null)
            {
                throw new IllegalArgumentException("Table reference cannot be null");
            }
            if (faultReporter == null)
            {
                throw new IllegalArgumentException("Fault reporter cannot be null");
            }
            if (jmxProxyFactory == null)
            {
                throw new IllegalArgumentException("JMX Proxy factory cannot be null");
            }
            if (tableRepairMetrics == null)
            {
                throw new IllegalArgumentException("Metric interface not set");
            }
            return new TableRepairJob(this);
        }
    }

    @VisibleForTesting
    void setClock(Clock clock)
    {
        myClock.set(clock);
    }
}
