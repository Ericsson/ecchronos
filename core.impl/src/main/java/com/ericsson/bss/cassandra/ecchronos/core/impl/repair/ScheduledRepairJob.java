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

import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairPolicy;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Abstract base class for scheduled repair jobs, providing common fields such as table reference,
 * JMX proxy factory, repair configuration, policies, and metrics.
 */
public abstract class ScheduledRepairJob extends ScheduledJob
{
    /** The shared repair lock factory instance. */
    protected static final RepairLockFactory REPAIR_LOCK_FACTORY = new RepairLockFactoryImpl();
    private final TableReference myTableReference;
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final RepairConfiguration myRepairConfiguration;
    private final List<TableRepairPolicy> myRepairPolicies;
    private final TableRepairMetrics myTableRepairMetrics;
    private final RepairLockType myRepairLockType;

    /**
     * Constructs a scheduled repair job with a generated job ID.
     *
     * @param configuration the job configuration.
     * @param nodeID the node identifier.
     * @param tableReference the table to be repaired.
     * @param jmxProxyFactory the JMX proxy factory.
     * @param repairConfiguration the repair configuration.
     * @param repairPolicies the list of repair policies.
     * @param tableRepairMetrics the table repair metrics.
     * @param repairLockType the repair lock type.
     */
    public ScheduledRepairJob(
            final Configuration configuration,
            final UUID nodeID,
            final TableReference tableReference,
            final DistributedJmxProxyFactory jmxProxyFactory,
            final RepairConfiguration repairConfiguration,
            final List<TableRepairPolicy> repairPolicies,
            final TableRepairMetrics tableRepairMetrics,
            final RepairLockType repairLockType)
    {
        super(configuration, nodeID);
        myTableReference = Preconditions.checkNotNull(tableReference, "Table reference must be set");
        myJmxProxyFactory = Preconditions.checkNotNull(jmxProxyFactory, "JMX proxy factory must be set");
        myRepairConfiguration = Preconditions.checkNotNull(repairConfiguration, "Repair configuration must be set");
        myRepairPolicies = Preconditions.checkNotNull(repairPolicies, "Repair policies must be set");
        myTableRepairMetrics = Preconditions.checkNotNull(tableRepairMetrics, "Table repair metrics must be set");
        myRepairLockType = Preconditions.checkNotNull(repairLockType, "Repair lock type must be set");
    }

    /**
     * Constructs a scheduled repair job with a specified job ID.
     *
     * @param configuration the job configuration.
     * @param jobId the unique job identifier.
     * @param nodeID the node identifier.
     * @param tableReference the table to be repaired.
     * @param jmxProxyFactory the JMX proxy factory.
     * @param repairConfiguration the repair configuration.
     * @param repairPolicies the list of repair policies.
     * @param tableRepairMetrics the table repair metrics.
     * @param repairLockType the repair lock type.
     */
    public ScheduledRepairJob(
            final Configuration configuration,
            final UUID jobId,
            final UUID nodeID,
            final TableReference tableReference,
            final DistributedJmxProxyFactory jmxProxyFactory,
            final RepairConfiguration repairConfiguration,
            final List<TableRepairPolicy> repairPolicies,
            final TableRepairMetrics tableRepairMetrics,
            final RepairLockType repairLockType)
    {
        super(configuration, jobId, nodeID);
        myTableReference = Preconditions.checkNotNull(tableReference, "Table reference must be set");
        myJmxProxyFactory = Preconditions.checkNotNull(jmxProxyFactory, "JMX proxy factory must be set");
        myRepairConfiguration = Preconditions.checkNotNull(repairConfiguration, "Repair configuration must be set");
        myRepairPolicies = Preconditions.checkNotNull(repairPolicies, "Repair policies must be set");
        myTableRepairMetrics = Preconditions.checkNotNull(tableRepairMetrics, "Table repair metrics must be set");
        myRepairLockType = Preconditions.checkNotNull(repairLockType, "Repair lock type must be set");
    }

    /**
     * Get the repair lock type.
     *
     * @return the repair lock type.
     */
    protected final RepairLockType getRepairLockType()
    {
        return myRepairLockType;
    }

    /**
     * Get the table reference for this job.
     * @return Table reference
     */
    public TableReference getTableReference()
    {
        return myTableReference;
    }

    /**
     * Get the JMX proxy factory.
     *
     * @return the JMX proxy factory.
     */
    protected final DistributedJmxProxyFactory getJmxProxyFactory()
    {
        return myJmxProxyFactory;
    }

    /**
     * Gets a view representation of this scheduled repair job.
     *
     * @return the scheduled repair job view.
     */
    public abstract ScheduledRepairJobView getView();

    /**
     * Classify the scheduling status of this job from the time elapsed since its last completed repair.
     * <p>
     * The time-based lag ({@link ScheduledRepairJobView.Status#OVERDUE} / {@link ScheduledRepairJobView.Status#LATE})
     * takes precedence over {@link ScheduledRepairJobView.Status#BLOCKED} so an overdue/late table is not masked as
     * merely blocked; the lag is the more actionable signal for operators and alarms (issue #1822). A blocked job
     * that is not yet late still reports {@code BLOCKED}.
     * <p>
     * The BLOCKED determination uses the base {@link ScheduledJob#runnable()} check (via {@code super.runnable()})
     * rather than a subclass override (such as the vnode job's {@code canRepair()} gate), matching the behavior
     * that existed before this classification was hoisted into the base class.
     *
     * @param timestamp the current time in epoch milliseconds.
     * @param lastCompletedAt the timestamp of the last completed repair in epoch milliseconds.
     * @return the classified status.
     */
    protected final ScheduledRepairJobView.Status classifyStatus(final long timestamp, final long lastCompletedAt)
    {
        long msSinceLastRepair = timestamp - lastCompletedAt;
        RepairConfiguration config = getRepairConfiguration();
        if (msSinceLastRepair >= config.getRepairErrorTimeInMs())
        {
            return ScheduledRepairJobView.Status.OVERDUE;
        }
        if (msSinceLastRepair >= config.getRepairWarningTimeInMs())
        {
            return ScheduledRepairJobView.Status.LATE;
        }
        if (getRealPriority() != -1 && !super.runnable())
        {
            return ScheduledRepairJobView.Status.BLOCKED;
        }
        if (msSinceLastRepair >= (config.getRepairIntervalInMs() - getRunOffset()))
        {
            return ScheduledRepairJobView.Status.ON_TIME;
        }
        return ScheduledRepairJobView.Status.COMPLETED;
    }

    /**
     * Get the repair configuration for this job.
     * @return Repair configuration
     */
    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }

    /**
     * Get the list of repair policies.
     *
     * @return the repair policies.
     */
    protected final List<TableRepairPolicy> getRepairPolicies()
    {
        return myRepairPolicies;
    }

    /**
     * Get the table repair metrics.
     *
     * @return the table repair metrics.
     */
    protected final TableRepairMetrics getTableRepairMetrics()
    {
        return myTableRepairMetrics;
    }

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
                that.myRepairConfiguration) && Objects.equals(
                myRepairPolicies, that.myRepairPolicies) && Objects.equals(myTableRepairMetrics,
                that.myTableRepairMetrics)  && myRepairLockType == that.myRepairLockType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), myTableReference, myJmxProxyFactory, myRepairConfiguration,
                myRepairPolicies, myTableRepairMetrics, myRepairLockType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return String.format("Repair job of %s", myTableReference);
    }
}

