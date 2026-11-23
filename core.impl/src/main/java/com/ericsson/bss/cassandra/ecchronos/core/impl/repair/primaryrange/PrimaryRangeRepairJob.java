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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.primaryrange;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.PrimaryRangeRepairResourceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.TaskExecutionResult;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairPolicy;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A scheduled job that repairs the primary ranges of one node of a table in one repair session, like
 * {@code nodetool repair -pr}.
 * <p>
 * One job is created per managed node per table. The job is due when the repair interval has passed since the last
 * successful primary range repair of the node. That time comes from {@code ecchronos.repair_history}, where the
 * repair task records a row typed {@link RepairType#PRIMARY_RANGE} keyed by this node. The row is read with
 * {@link RepairHistoryProvider#lastSuccessfulRepair}, which does not map it to token ranges, so primary range
 * history is kept apart from the repair state that vnode repair uses.
 * <p>
 * A table with no such row is taken for a new table, as for vnode repair: its first repair waits for the initial
 * delay. A history that cannot be read is not taken for no history, because that says nothing about the table, so
 * the repair is not delayed.
 */
public class PrimaryRangeRepairJob extends ScheduledRepairJob // CPD-OFF: the builder is deliberately near-identical to IncrementalRepairJob.Builder; the two jobs share no state worth a common base class
{
    private static final Logger LOG = LoggerFactory.getLogger(PrimaryRangeRepairJob.class);
    private static final int DAYS_IN_A_WEEK = 7;
    private static final int NOT_DUE = -1;

    private final Node myNode;
    private final ReplicationState myReplicationState;
    private final RepairHistory myRepairHistory;
    private final RepairHistoryProvider myRepairHistoryProvider;
    private final PrimaryRangeRepairAlarm myAlarm;

    PrimaryRangeRepairJob(final Builder builder)
    {
        super(builder.myConfiguration, builder.myNodeId, builder.myTableReference, builder.myJmxProxyFactory,
                builder.myRepairConfiguration, builder.myRepairPolicies, builder.myTableRepairMetrics,
                builder.myRepairLockType);
        myNode = Preconditions.checkNotNull(builder.myNode, "Node must be set");
        myReplicationState = Preconditions.checkNotNull(builder.myReplicationState, "Replication state must be set");
        myRepairHistory = Preconditions.checkNotNull(builder.myRepairHistory, "Repair history must be set");
        myRepairHistoryProvider = builder.myRepairHistoryProvider;
        myAlarm = new PrimaryRangeRepairAlarm(builder.myFaultReporter, builder.myTableReference, builder.myNodeId,
                builder.myRepairConfiguration);
        HistoryLookup lookup = lookupLastSuccessfulRun(builder.myTableReference,
                builder.myRepairConfiguration.getColdStartLookbackInMs());
        myLastSuccessfulRun = initialLastSuccessfulRun(lookup, builder.myRepairConfiguration.getInitialDelayInMs());
        LOG.debug("{} - last successful run: {}", this, myLastSuccessfulRun);
    }

    /**
     * What the last successful run is taken to be when the job is created.
     * <ul>
     *     <li>A repair that was found is used as it is.</li>
     *     <li>With no repair in the history, the table is taken for a new table, as for vnode repair: it is placed
     *     one interval minus the initial delay in the past, so that the first repair waits for the initial delay.</li>
     *     <li>With a history that could not be read, which says nothing about the table, the repair is not delayed.
     *     The time is one interval in the past, so the job is due right away.</li>
     * </ul>
     *
     * @param lookup what was found in the history.
     * @param initialDelayInMs the configured initial delay.
     * @return the time to use as the last successful run.
     */
    private long initialLastSuccessfulRun(final HistoryLookup lookup, final long initialDelayInMs)
    {
        long now = System.currentTimeMillis();
        return switch (lookup.outcome())
        {
            case FOUND -> lookup.finishedAt();
            case NONE -> now - myRunIntervalInMs + initialDelayInMs;
            case UNAVAILABLE -> now - myRunIntervalInMs;
        };
    }

    /**
     * Get scheduled repair job view.
     *
     * @return ScheduledRepairJobView
     */
    @Override
    public ScheduledRepairJobView getView()
    {
        long now = System.currentTimeMillis();
        return new ScheduledRepairJobView(getNodeId(), getJobId(), getTableReference(), getRepairConfiguration(),
                classifyStatus(now, myLastSuccessfulRun), getProgress(now), getNextRunInMs(),
                getLastSuccessfulRun(), getRepairConfiguration().getRepairType());
    }

    private long getNextRunInMs()
    {
        return getLastSuccessfulRun() + getRepairConfiguration().getRepairIntervalInMs();
    }

    /**
     * A primary range repair has no partial progress: the table is either repaired within the interval or not.
     */
    private double getProgress(final long now)
    {
        return now - myLastSuccessfulRun < getRepairConfiguration().getRepairIntervalInMs() ? 1.0d : 0.0d;
    }

    /**
     * Iterator for scheduled tasks built up by repair groups. There is always one group with one task, that repairs
     * all the primary ranges of the node.
     *
     * @return Scheduled task iterator
     */
    @Override
    public Iterator<ScheduledTask> iterator()
    {
        ImmutableSet<DriverNode> replicas = myReplicationState.getReplicas(getTableReference(), myNode);
        ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(replicas, ImmutableList.of(),
                myLastSuccessfulRun);
        RepairGroup.Builder builder = RepairGroup.newBuilder()
                .withTableReference(getTableReference())
                .withRepairConfiguration(getRepairConfiguration())
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(getTableRepairMetrics())
                .withReplicaRepairGroup(replicaRepairGroup)
                .withRepairLockFactory(REPAIR_LOCK_FACTORY)
                .withRepairResourceFactory(new PrimaryRangeRepairResourceFactory(getTableReference()))
                .withRepairPolicies(getRepairPolicies())
                .withJobId(getJobId())
                .withRepairHistory(myRepairHistory)
                .withNode(myNode);

        List<ScheduledTask> taskList = new ArrayList<>();
        taskList.add(builder.build(getRealPriority()));
        return taskList.iterator();
    }

    /**
     * Pick up a primary range repair that finished since the last refresh, for example one run on demand, so the job
     * does not repeat it within the interval. The history is only read when the job is due, so a job that is not due
     * costs nothing.
     */
    @Override
    public void refreshState()
    {
        // Evaluated first and also when the job is not due: it only compares times, and a table that is late must
        // alarm whether or not the history is read below.
        myAlarm.evaluate(myLastSuccessfulRun, System.currentTimeMillis());
        if (getRealPriority() == NOT_DUE)
        {
            return;
        }
        // Only a repair that was found moves the last run. Finding nothing must not apply the initial delay again,
        // that is done once, when the job is created.
        HistoryLookup lookup = lookupLastSuccessfulRun(getTableReference(),
                getRepairConfiguration().getColdStartLookbackInMs());
        if (lookup.outcome() == LookupOutcome.FOUND && lookup.finishedAt() > myLastSuccessfulRun)
        {
            LOG.info("{} - primary range repair of {} already completed at {}, skipping.",
                    this, getTableReference(), lookup.finishedAt());
            myLastSuccessfulRun = lookup.finishedAt();
            myAlarm.evaluate(myLastSuccessfulRun, System.currentTimeMillis());
        }
    }

    /**
     * Records the result, and ceases the alarm if the repair succeeded.
     *
     * @param result the result of the task.
     * @param task the task that was executed.
     */
    @Override
    public void postExecute(final TaskExecutionResult result, final ScheduledTask task)
    {
        super.postExecute(result, task);
        myAlarm.evaluate(myLastSuccessfulRun, System.currentTimeMillis());
    }

    /**
     * Looks up the last successful primary range repair of this node in {@code ecchronos.repair_history}.
     *
     * @param tableReference the table to look up.
     * @param coldStartLookbackMs how far back to look, in milliseconds. {@code 0} disables reading the history, which
     *        is the same as there being no history.
     * @return what was found. It is {@link LookupOutcome#NONE} when there is nothing to find, and
     *         {@link LookupOutcome#UNAVAILABLE} only when the history could not be read.
     */
    private HistoryLookup lookupLastSuccessfulRun(final TableReference tableReference, final long coldStartLookbackMs)
    {
        if (myRepairHistoryProvider == null || coldStartLookbackMs <= 0)
        {
            return new HistoryLookup(LookupOutcome.NONE, 0L);
        }
        try
        {
            long now = System.currentTimeMillis();
            OptionalLong latest = myRepairHistoryProvider.lastSuccessfulRepair(myNode, tableReference,
                    RepairType.PRIMARY_RANGE, Math.max(0L, now - coldStartLookbackMs), now);
            return latest.isPresent()
                    ? new HistoryLookup(LookupOutcome.FOUND, latest.getAsLong())
                    : new HistoryLookup(LookupOutcome.NONE, 0L);
        }
        catch (Exception e)
        {
            LOG.warn("Unable to read repair history of {}, the primary range repair is not delayed",
                    tableReference, e);
            return new HistoryLookup(LookupOutcome.UNAVAILABLE, 0L);
        }
    }

    /** The result of looking in the repair history. */
    private enum LookupOutcome
    {
        /** A successful repair was found. */
        FOUND,
        /** The history was read, or there is none to read, and there is no successful repair in it. */
        NONE,
        /** The history could not be read. */
        UNAVAILABLE
    }

    private record HistoryLookup(LookupOutcome outcome, long finishedAt)
    {
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("Primary range repair job of %s (coordinator node %s)", getTableReference(),
                myNode.getHostId());
    }

    @Override
    public final boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        else if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        if (!super.equals(o))
        {
            return false;
        }
        PrimaryRangeRepairJob that = (PrimaryRangeRepairJob) o;
        return Objects.equals(myReplicationState, that.myReplicationState)
                && Objects.equals(myNode, that.myNode)
                && Objects.equals(myRepairHistory, that.myRepairHistory)
                && Objects.equals(myRepairHistoryProvider, that.myRepairHistoryProvider);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(super.hashCode(), myReplicationState, myNode, myRepairHistory, myRepairHistoryProvider);
    }

    /**
     * Builder class to construct PrimaryRangeRepairJob.
     */
    @SuppressWarnings("VisibilityModifier")
    public static class Builder
    {
        ScheduledJob.Configuration myConfiguration = new ScheduledJob.ConfigurationBuilder().withPriority(
                        ScheduledJob.Priority.LOW)
                .withRunInterval(DAYS_IN_A_WEEK, TimeUnit.DAYS).build();
        private TableReference myTableReference;
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private Node myNode;
        private UUID myNodeId;
        private TableRepairMetrics myTableRepairMetrics = null;
        private ReplicationState myReplicationState;
        private RepairConfiguration myRepairConfiguration = RepairConfiguration.DEFAULT;
        private final List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();
        private RepairLockType myRepairLockType;
        private RepairHistory myRepairHistory;
        private RepairHistoryProvider myRepairHistoryProvider;
        private RepairFaultReporter myFaultReporter;

        /**
         * Default constructor.
         */
        public Builder()
        {
            // Default constructor
        }

        /**
         * Build with repair lock type. It is required by the base class and not used for primary range repair,
         * which has its own fixed locks.
         *
         * @param repairLockType Repair lock type.
         * @return Builder
         */
        public Builder withRepairLockType(final RepairLockType repairLockType)
        {
            myRepairLockType = repairLockType;
            return this;
        }

        /**
         * Build with configuration.
         *
         * @param configuration Configuration.
         * @return Builder
         */
        public Builder withConfiguration(final ScheduledJob.Configuration configuration)
        {
            myConfiguration = configuration;
            return this;
        }

        /**
         * Build with the coordinator node. The repair is started on this node, and the node is the {@code node_id}
         * of the repair history rows.
         *
         * @param node Node.
         * @return Builder
         */
        public Builder withNode(final Node node)
        {
            myNode = node;
            myNodeId = myNode.getHostId();
            return this;
        }

        /**
         * Build with table reference.
         *
         * @param tableReference Table reference.
         * @return Builder
         */
        public Builder withTableReference(final TableReference tableReference)
        {
            myTableReference = tableReference;
            return this;
        }

        /**
         * Build with JMX proxy factory.
         *
         * @param jmxProxyFactory JMX proxy factory.
         * @return Builder
         */
        public Builder withJmxProxyFactory(final DistributedJmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        /**
         * Build with table repair metrics.
         *
         * @param tableRepairMetrics Table repair metrics.
         * @return Builder
         */
        public Builder withTableRepairMetrics(final TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        /**
         * Build with repair configuration.
         *
         * @param repairConfiguration The repair configuration.
         * @return Builder
         */
        public Builder withRepairConfiguration(final RepairConfiguration repairConfiguration)
        {
            myRepairConfiguration = repairConfiguration;
            return this;
        }

        /**
         * Build with replication state.
         *
         * @param replicationState Replication state.
         * @return Builder
         */
        public Builder withReplicationState(final ReplicationState replicationState)
        {
            myReplicationState = replicationState;
            return this;
        }

        /**
         * Build with repair policies.
         *
         * @param repairPolicies The table repair policies.
         * @return Builder
         */
        public Builder withRepairPolices(final Collection<TableRepairPolicy> repairPolicies)
        {
            myRepairPolicies.addAll(repairPolicies);
            return this;
        }

        /**
         * Build with the repair history used to record executions in {@code ecchronos.repair_history}.
         *
         * @param repairHistory The repair history.
         * @return Builder
         */
        public Builder withRepairHistory(final RepairHistory repairHistory)
        {
            myRepairHistory = repairHistory;
            return this;
        }

        /**
         * Build with the repair history provider used to read the last successful primary range repair.
         *
         * @param repairHistoryProvider The repair history provider.
         * @return Builder
         */
        public Builder withRepairHistoryProvider(final RepairHistoryProvider repairHistoryProvider)
        {
            myRepairHistoryProvider = repairHistoryProvider;
            return this;
        }

        /**
         * Build with a fault reporter, that the repair alarms are raised and ceased in. Without one there are no
         * alarms.
         *
         * @param faultReporter The fault reporter, or null.
         * @return Builder
         */
        public Builder withFaultReporter(final RepairFaultReporter faultReporter)
        {
            myFaultReporter = faultReporter;
            return this;
        }

        /**
         * Build primary range repair job.
         *
         * @return PrimaryRangeRepairJob
         */
        public PrimaryRangeRepairJob build()
        {
            Preconditions.checkNotNull(myTableReference, "Table reference must be set");

            return new PrimaryRangeRepairJob(this);
        }
    }
}
