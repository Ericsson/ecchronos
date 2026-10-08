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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.unified;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@link RepairScheduler} for the consolidated {@code UNIFIED_VNODE} path.
 * <p>
 * Unlike the legacy per-node scheduler, configuration events for a table are consolidated by a
 * {@link UnifiedTableScheduleRegistry} into a single multi-node {@link UnifiedTableRepairJob} spanning all
 * reporting nodes. Per-node visibility is preserved via {@link UnifiedTableRepairJob#getViews()} and
 * {@link UnifiedTableRepairJob#getViewForNode(UUID)}.
 */
public final class UnifiedRepairScheduler implements RepairScheduler, Closeable
{
    private static final int DEFAULT_TERMINATION_WAIT_IN_SECONDS = 10;

    private static final Logger LOG = LoggerFactory.getLogger(UnifiedRepairScheduler.class);

    private final ReadWriteLock myLock = new ReentrantReadWriteLock();
    private final ExecutorService myExecutor;
    private final ScheduleManager myScheduleManager;
    private final UnifiedTableScheduleRegistry myRegistry;

    private UnifiedRepairScheduler(final Builder builder)
    {
        myExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("UnifiedRepairScheduler-%d").build());
        myScheduleManager = builder.myScheduleManager;
        UnifiedRepairJobFactory jobFactory = UnifiedRepairJobFactory.builder()
                .withTableRepairMetrics(builder.myTableRepairMetrics)
                .withRepairHistoryService(builder.myRepairHistoryService)
                .withFaultReporter(builder.myFaultReporter)
                .withJmxProxyFactory(builder.myJmxProxyFactory)
                .withRepairStateFactory(builder.myRepairStateFactory)
                .withRepairPolicies(builder.myRepairPolicies)
                .withTableStorageStates(builder.myTableStorageStates)
                .withRepairLockType(builder.myRepairLockType)
                .withTimeBasedRunPolicy(builder.myTimeBasedRunPolicy)
                .build();
        myRegistry = new UnifiedTableScheduleRegistry(jobFactory, myScheduleManager);
    }

    @Override
    public void putConfigurations(
            final Node node,
            final TableReference tableReference,
            final Set<RepairConfiguration> repairConfiguration)
    {
        myExecutor.execute(() -> runLocked(() ->
                myRegistry.applyConfiguration(node, tableReference, repairConfiguration)));
    }

    @Override
    public void removeConfiguration(final Node node, final TableReference tableReference)
    {
        myExecutor.execute(() -> runLocked(() -> myRegistry.removeConfiguration(node, tableReference)));
    }

    @Override
    public void removeAllConfigurationsForNode(final UUID nodeId)
    {
        myExecutor.execute(() -> runLocked(() -> myRegistry.removeNode(nodeId)));
    }

    @Override
    @SuppressWarnings("CPD-START")
    public List<ScheduledRepairJobView> getCurrentRepairJobs()
    {
        myLock.readLock().lock();
        try
        {
            return myRegistry.allJobs().stream()
                    .flatMap(job -> job.getViews().stream())
                    .collect(Collectors.toList());
        }
        finally
        {
            myLock.readLock().unlock();
        }
    }

    @Override
    public List<ScheduledRepairJobView> getCurrentRepairJobsByNode(final UUID nodeId)
    {
        myLock.readLock().lock();
        try
        {
            return myRegistry.allJobs().stream()
                    .map(job -> job.getViewForNode(nodeId))
                    .flatMap(Stream::ofNullable)
                    .collect(Collectors.toList());
        }
        finally
        {
            myLock.readLock().unlock();
        }
    }

    @Override
    public String getCurrentJobStatus()
    {
        return myScheduleManager.getCurrentJobStatus();
    }

    @Override
    public void close()
    {
        myExecutor.shutdown();
        try
        {
            if (!myExecutor.awaitTermination(DEFAULT_TERMINATION_WAIT_IN_SECONDS, TimeUnit.SECONDS))
            {
                LOG.warn("Waited {}s for unified scheduler executor to shutdown, still not shut down",
                        DEFAULT_TERMINATION_WAIT_IN_SECONDS);
            }
        }
        catch (InterruptedException e)
        {
            LOG.error("Interrupted while waiting for unified scheduler executor to shutdown", e);
            Thread.currentThread().interrupt();
        }
        runLocked(myRegistry::clear);
    }

    private void runLocked(final Runnable action)
    {
        myLock.writeLock().lock();
        try
        {
            action.run();
        }
        catch (Exception e)
        {
            LOG.error("Unexpected error during unified schedule change", e);
        }
        finally
        {
            myLock.writeLock().unlock();
        }
    }

    /**
     * Create instance of Builder to construct UnifiedRepairScheduler.
     *
     * @return Builder
     */
    @SuppressWarnings("CPD-END")
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder used to construct {@link UnifiedRepairScheduler}.
     */
    public static final class Builder
    {
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private RepairFaultReporter myFaultReporter;
        private RepairStateFactory myRepairStateFactory;
        private ScheduleManager myScheduleManager;
        // CPD-OFF: builder field declarations are intentionally near-identical to RepairSchedulerImpl.Builder
        private final List<TableRepairPolicy> myRepairPolicies = new ArrayList<>();
        private TableRepairMetrics myTableRepairMetrics;
        private RepairHistoryService myRepairHistoryService;
        private TableStorageStates myTableStorageStates;
        private RepairLockType myRepairLockType;
        private TimeBasedRunPolicy myTimeBasedRunPolicy;
        // CPD-ON

        /**
         * Default constructor.
         */
        @SuppressWarnings("CPD-START")
        public Builder()
        {
            // Default constructor
        }

        /**
         * Build with repair lock type.
         * @return Builder
         */
        public Builder withRepairLockType(final RepairLockType repairLockType)
        {
            myRepairLockType = repairLockType;
            return this;
        }

        /**
         * Build with fault reporter.
         *
         * @param repairFaultReporter Repair fault reporter.
         * @return Builder
         */
        public Builder withFaultReporter(final RepairFaultReporter repairFaultReporter)
        {
            myFaultReporter = repairFaultReporter;
            return this;
        }

        /**
         * Build with repair history.
         *
         * @param repairHistory Repair history.
         * @return Builder
         */
        public Builder withRepairHistory(final RepairHistoryService repairHistory)
        {
            myRepairHistoryService = repairHistory;
            return this;
        }

        /**
         * Build with table storage states.
         *
         * @param tableStorageStates Table storage states.
         * @return Builder
         */
        public Builder withTableStorageStates(final TableStorageStates tableStorageStates)
        {
            myTableStorageStates = tableStorageStates;
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
         * Build with schedule manager.
         *
         * @param scheduleManager Schedule manager.
         * @return Builder
         */
        public Builder withScheduleManager(final ScheduleManager scheduleManager)
        {
            myScheduleManager = scheduleManager;
            return this;
        }

        /**
         * Build with repair state factory.
         *
         * @param repairStateFactory Repair state factory.
         * @return Builder
         */
        public Builder withRepairStateFactory(final RepairStateFactory repairStateFactory)
        {
            myRepairStateFactory = repairStateFactory;
            return this;
        }

        /**
         * Build with repair policies.
         *
         * @param tableRepairPolicies Table repair policies.
         * @return Builder
         */
        public Builder withRepairPolicies(final Collection<TableRepairPolicy> tableRepairPolicies)
        {
            myRepairPolicies.addAll(tableRepairPolicies);
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
         * Build with TimeBasedRunPolicy.
         *
         * @param timeBasedRunPolicy TimeBasedRunPolicy.
         * @return Builder
         */
        @SuppressWarnings("CPD-END")
        public Builder withTimeBasedRunPolicy(final TimeBasedRunPolicy timeBasedRunPolicy)
        {
            myTimeBasedRunPolicy = timeBasedRunPolicy;
            return this;
        }

        /**
         * Build.
         *
         * @return UnifiedRepairScheduler
         */
        public UnifiedRepairScheduler build()
        {
            return new UnifiedRepairScheduler(this);
        }
    }
}
