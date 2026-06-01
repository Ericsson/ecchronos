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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Monitors a running repair for hangs by periodically checking node status
 * and repair activity. Terminates the repair if the node is down, the repair
 * is no longer active, or the max wait time is exceeded.
 *
 * Uses a shared static executor to avoid per-task thread creation overhead
 * and prevent native memory leaks from orphaned executor threads.
 */
public class RepairHangMonitor
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairHangMonitor.class);
    private static final String NORMAL_STATUS = "NORMAL";
    private static final int DEFAULT_HEALTH_CHECK_INTERVAL_MINUTES = 1;
    private static final int SHARED_POOL_SIZE = 2;

    private static final ScheduledExecutorService SHARED_EXECUTOR;
    static
    {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(SHARED_POOL_SIZE,
                new ThreadFactoryBuilder().setNameFormat("HangPreventingTask-%d").setDaemon(true).build());
        executor.setRemoveOnCancelPolicy(true);
        SHARED_EXECUTOR = executor;
    }

    private final ScheduledExecutorService myExecutor;
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final UUID myNodeID;
    private final TableReference myTableReference;
    private final int myMaxWaitTimeInMinutes;
    private final RepairNotificationHandler myNotificationHandler;
    private final long myHealthCheckInterval;
    private final TimeUnit myHealthCheckTimeUnit;

    private volatile ScheduledFuture<?> myHangPreventFuture;

    public RepairHangMonitor(final DistributedJmxProxyFactory jmxProxyFactory,
            final UUID nodeID,
            final TableReference tableReference,
            final int maxWaitTimeInMinutes,
            final RepairNotificationHandler notificationHandler)
    {
        this(jmxProxyFactory, nodeID, tableReference, maxWaitTimeInMinutes, notificationHandler,
                DEFAULT_HEALTH_CHECK_INTERVAL_MINUTES, TimeUnit.MINUTES, SHARED_EXECUTOR);
    }

    @VisibleForTesting
    RepairHangMonitor(final DistributedJmxProxyFactory jmxProxyFactory,
            final UUID nodeID,
            final TableReference tableReference,
            final int maxWaitTimeInMinutes,
            final RepairNotificationHandler notificationHandler,
            final long healthCheckInterval,
            final TimeUnit healthCheckTimeUnit)
    {
        this(jmxProxyFactory, nodeID, tableReference, maxWaitTimeInMinutes, notificationHandler,
                healthCheckInterval, healthCheckTimeUnit, SHARED_EXECUTOR);
    }

    @VisibleForTesting
    RepairHangMonitor(final DistributedJmxProxyFactory jmxProxyFactory,
            final UUID nodeID,
            final TableReference tableReference,
            final int maxWaitTimeInMinutes,
            final RepairNotificationHandler notificationHandler,
            final long healthCheckInterval,
            final TimeUnit healthCheckTimeUnit,
            final ScheduledExecutorService executor)
    {
        myJmxProxyFactory = jmxProxyFactory;
        myNodeID = nodeID;
        myTableReference = tableReference;
        myMaxWaitTimeInMinutes = maxWaitTimeInMinutes;
        myNotificationHandler = notificationHandler;
        myHealthCheckInterval = healthCheckInterval;
        myHealthCheckTimeUnit = healthCheckTimeUnit;
        myExecutor = executor;
    }

    /**
     * Reschedule the hang prevention check. Should be called whenever progress is made.
     */
    public void reschedule()
    {
        if (myHangPreventFuture != null)
        {
            myHangPreventFuture.cancel(false);
        }
        myHangPreventFuture = myExecutor.schedule(new HangPreventingTask(),
                myHealthCheckInterval, myHealthCheckTimeUnit);
    }

    /**
     * Cancel any pending checks. Since the executor is shared, only the future is cancelled.
     */
    public void cancel()
    {
        if (myHangPreventFuture != null)
        {
            myHangPreventFuture.cancel(false);
        }
    }

    private final class HangPreventingTask implements Runnable
    {
        private final long startTime = System.currentTimeMillis();

        @Override
        public void run()
        {
            int command = myNotificationHandler.getCommand();
            try (DistributedJmxProxy proxy = myJmxProxyFactory.connect())
            {
                String nodeStatus = proxy.getNodeStatus(myNodeID);
                if (!NORMAL_STATUS.equals(nodeStatus))
                {
                    LOG.error("Cassandra node {} is down, aborting repair task.", myNodeID);
                    myNotificationHandler.setError(
                            new ScheduledJobException("Cassandra node " + myNodeID + " is down"));
                    proxy.forceTerminateAllRepairSessionsInSpecificNode(myNodeID);
                    myNotificationHandler.countDown();
                }
                else if (!proxy.isRepairActive(myNodeID, command))
                {
                    LOG.warn("Repair-{} of {} is no longer active on node {}, notification may have been lost",
                            command, myTableReference, myNodeID);
                    myNotificationHandler.countDown();
                }
                else if (System.currentTimeMillis() - startTime
                        >= TimeUnit.MINUTES.toMillis(myMaxWaitTimeInMinutes))
                {
                    LOG.error("Repair-{} of {} still active after {} minutes on node {}, forcing termination",
                            command, myTableReference, myMaxWaitTimeInMinutes, myNodeID);
                    proxy.forceTerminateAllRepairSessionsInSpecificNode(myNodeID);
                    myNotificationHandler.countDown();
                }
                else
                {
                    LOG.debug("Repair-{} still in status {}. Will recheck in {} {}",
                            command, nodeStatus, myHealthCheckInterval, myHealthCheckTimeUnit);
                    myHangPreventFuture = myExecutor.schedule(this,
                            myHealthCheckInterval, myHealthCheckTimeUnit);
                }
            }
            catch (IOException e)
            {
                LOG.error("Unable to check node status or prevent hanging repair task for Repair-{} of {}",
                        command, myTableReference, e);
            }
        }
    }
}
