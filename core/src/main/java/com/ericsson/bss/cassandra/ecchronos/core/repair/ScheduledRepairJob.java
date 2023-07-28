/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

public abstract class ScheduledRepairJob extends ScheduledJob
{
    protected static final RepairLockFactory REPAIR_LOCK_FACTORY = new RepairLockFactoryImpl();
    private final TableReference myTableReference;
    private final JmxProxyFactory myJmxProxyFactory;
    private final RepairConfiguration myRepairConfiguration;
    private final RepairLockType myRepairLockType;
    private final List<TableRepairPolicy> myRepairPolicies;
    private final TableRepairMetrics myTableRepairMetrics;

    public ScheduledRepairJob(final Configuration configuration, final TableReference tableReference,
            final JmxProxyFactory jmxProxyFactory, final RepairConfiguration repairConfiguration,
            final RepairLockType repairLockType, final List<TableRepairPolicy> repairPolicies,
            final TableRepairMetrics tableRepairMetrics)
    {
        super(configuration);
        myTableReference = Preconditions.checkNotNull(tableReference, "Table reference must be set");
        myJmxProxyFactory = Preconditions.checkNotNull(jmxProxyFactory, "JMX proxy factory must be set");
        myRepairConfiguration = Preconditions.checkNotNull(repairConfiguration, "Repair configuration must be set");
        myRepairLockType = Preconditions.checkNotNull(repairLockType, "Repair lock type must be set");
        myRepairPolicies = Preconditions.checkNotNull(repairPolicies, "Repair policies must be set");
        myTableRepairMetrics = Preconditions.checkNotNull(tableRepairMetrics, "Table repair metrics must be set");
    }

    public ScheduledRepairJob(final Configuration configuration, final UUID id, final TableReference tableReference,
            final JmxProxyFactory jmxProxyFactory, final RepairConfiguration repairConfiguration,
            final RepairLockType repairLockType, final List<TableRepairPolicy> repairPolicies,
            final TableRepairMetrics tableRepairMetrics)
    {
        super(configuration, id);
        myTableReference = Preconditions.checkNotNull(tableReference, "Table reference must be set");
        myJmxProxyFactory = Preconditions.checkNotNull(jmxProxyFactory, "JMX proxy factory must be set");
        myRepairConfiguration = Preconditions.checkNotNull(repairConfiguration, "Repair configuration must be set");
        myRepairLockType = Preconditions.checkNotNull(repairLockType, "Repair lock type must be set");
        myRepairPolicies = Preconditions.checkNotNull(repairPolicies, "Repair policies must be set");
        myTableRepairMetrics = Preconditions.checkNotNull(tableRepairMetrics, "Table repair metrics must be set");
    }

    /**
     * Get the table reference for this job.
     * @return Table reference
     */
    public TableReference getTableReference()
    {
        return myTableReference;
    }

    protected final JmxProxyFactory getJmxProxyFactory()
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

    protected final List<TableRepairPolicy> getRepairPolicies()
    {
        return myRepairPolicies;
    }

    protected final TableRepairMetrics getTableRepairMetrics()
    {
        return myTableRepairMetrics;
    }

    public abstract ScheduledRepairJobView getView();

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }
        ScheduledRepairJob that = (ScheduledRepairJob) o;
        return Objects.equals(myTableReference, that.myTableReference) && Objects.equals(
                myJmxProxyFactory, that.myJmxProxyFactory) && Objects.equals(myRepairConfiguration,
                that.myRepairConfiguration) && myRepairLockType == that.myRepairLockType && Objects.equals(
                myRepairPolicies, that.myRepairPolicies) && Objects.equals(myTableRepairMetrics,
                that.myTableRepairMetrics);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), myTableReference, myJmxProxyFactory, myRepairConfiguration,
                myRepairLockType,
                myRepairPolicies, myTableRepairMetrics);
    }
}
