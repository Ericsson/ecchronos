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

import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.RepairAsyncTransportException;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.TransientRepairException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class used to represent repair tasks. Orchestrates repair execution
 * by delegating to {@link RepairRangeTracker}, {@link RepairNotificationHandler},
 * and {@link RepairHangMonitor}.
 */
public abstract class RepairTask
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairTask.class);

    private final UUID nodeID;
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final TableReference myTableReference;
    private final TableRepairMetrics myTableRepairMetrics;
    private final RepairConfiguration myRepairConfiguration;

    private final RepairRangeTracker myRangeTracker = new RepairRangeTracker();
    private final RepairNotificationHandler myNotificationHandler;
    private final RepairHangMonitor myHangMonitor;

    /**
     * Constructs a RepairTask for the specified node and table with the given repair configuration and metrics.
     *
     * @param currentNodeID the UUID of the current node where the repair task is running. Must not be {@code null}.
     * @param jmxProxyFactory the factory to create connections to distributed JMX proxies. Must not be {@code null}.
     * @param tableReference the reference to the table that is being repaired. Must not be {@code null}.
     * @param repairConfiguration the configuration specifying how the repair task should be executed. Must not be {@code null}.
     * @param tableRepairMetrics the metrics associated with table repairs for monitoring and tracking purposes. May be {@code null}.
     * @param maxWaitTime the maximum number of minutes to wait for a repair to complete before forcing termination.
     */
    protected RepairTask(
            final UUID currentNodeID,
            final DistributedJmxProxyFactory jmxProxyFactory,
            final TableReference tableReference,
            final RepairConfiguration repairConfiguration,
            final TableRepairMetrics tableRepairMetrics,
            final Integer maxWaitTime
    )
    {
        nodeID = currentNodeID;
        myJmxProxyFactory = Preconditions.checkNotNull(jmxProxyFactory, "Jmx proxy factory must be set");
        myTableReference = Preconditions.checkNotNull(tableReference, "Table reference must be set");
        myRepairConfiguration = Preconditions.checkNotNull(repairConfiguration, "Repair configuration must be set");
        myTableRepairMetrics = tableRepairMetrics;

        myNotificationHandler = new RepairNotificationHandler(
                this::onRangeFinished,
                this::rescheduleHangPrevention
        );
        myHangMonitor = new RepairHangMonitor(
                myJmxProxyFactory, nodeID, myTableReference, maxWaitTime, myNotificationHandler
        );
    }

    /**
     * Execute the repair task.
     *
     * @throws ScheduledJobException
     *         Scheduled job exception if the repair fails.
     */
    public void execute() throws ScheduledJobException
    {
        long start = System.nanoTime();
        long end;
        long total;
        boolean successful = true;
        onExecute();
        try (DistributedJmxProxy proxy = myJmxProxyFactory.connect())
        {
            myHangMonitor.reschedule();
            repair(proxy);
            onFinish(RepairStatus.SUCCESS);
        }
        catch (TransientRepairException e)
        {
            onFinish(RepairStatus.FAILED);
            successful = false;
            throw e;
        }
        catch (IOException e)
        {
            // Failing to obtain the JMX connection is transient (node restarting, connection stale); retryable.
            onFinish(RepairStatus.FAILED);
            successful = false;
            throw new TransientRepairException("Unable to connect for repair of '" + this + "'", e);
        }
        catch (Exception e)
        {
            onFinish(RepairStatus.FAILED);
            successful = false;
            throw new ScheduledJobException("Unable to repair '" + this + "'", e);
        }
        finally
        {
            myHangMonitor.cancel();
            end = System.nanoTime();
            total = end - start;
            myTableRepairMetrics.repairSession(myTableReference, total, TimeUnit.NANOSECONDS, successful);
        }
        lazySleep(total);
    }

    /**
     * Method called before the task is executed, default implementation is NOOP.
     */
    protected void onExecute()
    {
        // NOOP
    }

    private void repair(final DistributedJmxProxy proxy) throws ScheduledJobException
    {
        LOG.debug("repair starting for table {} on node {}", myTableReference, nodeID);
        if (!proxy.addStorageServiceListener(nodeID, myNotificationHandler))
        {
            String msg = String.format("Repair of %s has no jmx connection", myTableReference);
            LOG.warn(msg);
            throw new TransientRepairException(msg);
        }
        try
        {
            myNotificationHandler.prepareForCommand();
            int command;
            // Captured just before the call so the lost-response handler can exclude older (e.g. retained
            // FINALIZED) sessions for the same table and only adopt one started by this request. See issue #1892.
            long requestEpochSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            try
            {
                command = proxy.repairAsync(nodeID, myTableReference.getKeyspace(), getOptions());
            }
            catch (RepairAsyncTransportException e)
            {
                handleLostRepairResponse(proxy, e, requestEpochSeconds);
                return;
            }
            if (command > 0)
            {
                myNotificationHandler.setCommand(command);
                awaitAndVerifyRepair(proxy, command);
            }
            else
            {
                // A non-positive command means Cassandra did not start a repair session (for example an
                // incremental repair aborting in the prepare phase because another session holds the SSTables).
                // Nothing was repaired. This is typically transient (the blocking session will finish), so it is
                // retryable rather than a terminal failure.
                String msg = String.format("Repair of %s did not start (command=%d), no data was repaired",
                        myTableReference, command);
                LOG.warn(msg);
                throw new TransientRepairException(msg);
            }
        }
        finally
        {
            proxy.removeStorageServiceListener(nodeID, myNotificationHandler);
        }
    }

    /**
     * Handle a lost {@code repairAsync} response: the request reached Cassandra and may have started a repair, but
     * the response (and command id) was lost in transit, so the JMX notification handler cannot correlate the
     * running repair (notifications are keyed by the command id we never received). See issue #1892.
     * <p>
     * Subclasses that can discover the already-started session (incremental repairs, via {@code getRepairSessions})
     * may override {@link #onLostRepairResponse(DistributedJmxProxy, long)} to adopt and await it. When the session cannot
     * be discovered or adopted (vnode repairs are not listed by {@code getSessions}), the task is classified as a
     * transient, retryable failure rather than a terminal one — the ranges are re-attempted on a later cycle, and no
     * misleading repair-failure error is logged.
     *
     * @param proxy the JMX proxy.
     * @param cause the transport failure.
     * @param requestEpochSeconds epoch-seconds captured just before the repairAsync call, used to exclude older
     *        sessions when discovering the one this request started.
     * @throws ScheduledJobException {@link TransientRepairException} when not adopted, or a terminal
     *         {@link ScheduledJobException} when an adopted session ended in failure.
     */
    private void handleLostRepairResponse(final DistributedJmxProxy proxy, final RepairAsyncTransportException cause,
            final long requestEpochSeconds)
            throws ScheduledJobException
    {
        LostResponseOutcome outcome = onLostRepairResponse(proxy, requestEpochSeconds);
        if (outcome == LostResponseOutcome.ADOPTED_SUCCESS)
        {
            LOG.info("Adopted and awaited the already-started repair of {} on node {} after a lost repairAsync "
                    + "response", myTableReference, nodeID);
            return;
        }
        if (outcome == LostResponseOutcome.ADOPTED_FAILED)
        {
            throw new ScheduledJobException("Adopted repair session of " + myTableReference + " on node " + nodeID
                    + " ended in failure after a lost repairAsync response");
        }
        // NOT_ADOPTED: the repair may have started on Cassandra but could not be adopted (for example a vnode
        // repair, which getSessions does not list). Treat it as transient and retryable rather than a terminal
        // failure, and log it distinctly so it is not mistaken for a real repair failure.
        LOG.warn("Lost the repairAsync response for {} on node {}; the repair may have started on Cassandra but "
                + "could not be adopted. Treating as transient and retryable.",
                myTableReference, nodeID, cause);
        throw new TransientRepairException(
                "Lost repairAsync response for " + myTableReference + " on node " + nodeID, cause);
    }

    /**
     * Outcome of attempting to adopt an already-started repair after a lost {@code repairAsync} response.
     */
    protected enum LostResponseOutcome
    {
        /** A matching session was found and awaited to successful completion. */
        ADOPTED_SUCCESS,
        /** A matching session was found and awaited, but it ended in failure. */
        ADOPTED_FAILED,
        /** No matching session could be found/adopted (for example vnode repairs, not listed by getSessions). */
        NOT_ADOPTED
    }

    /**
     * Attempt to discover and await an already-started repair session after a lost {@code repairAsync} response. The
     * default implementation cannot discover a session (vnode repairs are not listed by {@code getSessions}) and
     * returns {@link LostResponseOutcome#NOT_ADOPTED}. Incremental repairs override this.
     *
     * @param proxy the JMX proxy.
     * @param requestEpochSeconds epoch-seconds captured just before the repairAsync call; implementations must only
     *        adopt a session started at/after this time (minus a small clock-skew tolerance) so a retained older
     *        session for the same table is not mistaken for the one this request started.
     * @return the adoption outcome.
     */
    protected LostResponseOutcome onLostRepairResponse(final DistributedJmxProxy proxy, final long requestEpochSeconds)
    {
        return LostResponseOutcome.NOT_ADOPTED;
    }

    private void awaitAndVerifyRepair(final DistributedJmxProxy proxy, final int command)
            throws ScheduledJobException
    {
        try
        {
            LOG.debug("waiting for latch for table {} on node {}", myTableReference, nodeID);
            myNotificationHandler.await();
            LOG.debug("finished waiting for latch for table {} on node {}", myTableReference, nodeID);
            verifyRepair(proxy);
            if (myNotificationHandler.getLastError() != null)
            {
                throw myNotificationHandler.getLastError();
            }
            if (myNotificationHandler.hasLostNotification())
            {
                String msg = String.format("Repair-%d of %s had lost notifications", command, myTableReference);
                LOG.warn(msg);
                throw new ScheduledJobException(msg);
            }
            LOG.debug("{} completed successfully on node {}", this, nodeID);
        }
        catch (InterruptedException e)
        {
            String msg = this + " was interrupted";
            LOG.warn(msg, e);
            Thread.currentThread().interrupt();
            throw new ScheduledJobException(msg, e);
        }
    }

    private void rescheduleHangPrevention()
    {
        myHangMonitor.reschedule();
    }

    /**
     * Method used to construct options for the repair.
     *
     * @return Options
     */
    protected abstract Map<String, String> getOptions();

    /**
     * Method is called once a repair is completed.
     *
     * @param proxy
     *         The jmx proxy
     * @throws ScheduledJobException
     *         In case when repair is deemed as failed.
     */
    protected void verifyRepair(final DistributedJmxProxy proxy) throws ScheduledJobException
    {
        if (myRangeTracker.hasFailedRanges())
        {
            // Do not force-terminate here: the command is terminal (the latch is counted down on COMPLETE) so the
            // failed session has already ended on Cassandra's side. forceTerminateAllRepairSessions() is
            // cluster-wide across every managed node and would abort unrelated in-flight repairs; failing the task
            // is sufficient.
            throw new ScheduledJobException("Repair has failed ranges '" + myRangeTracker.getFailedRanges() + "'");
        }
    }

    /**
     * Method called when the task is finished.
     *
     * @param repairStatus
     *         The status of the finished task.
     */
    protected abstract void onFinish(RepairStatus repairStatus);

    private void lazySleep(final long executionInNanos) throws ScheduledJobException
    {
        if (myRepairConfiguration.getRepairUnwindRatio() != RepairConfiguration.NO_UNWIND)
        {
            double sleepDurationInNanos = executionInNanos * myRepairConfiguration.getRepairUnwindRatio();
            long sleepDurationInMs = TimeUnit.NANOSECONDS.toMillis((long) sleepDurationInNanos);
            sleepDurationInMs = Math.max(sleepDurationInMs, 1);
            try
            {
                Thread.sleep(sleepDurationInMs);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new ScheduledJobException(e);
            }
        }
    }

    /**
     * Clean up the repair task.
     */
    public void cleanup()
    {
        myHangMonitor.cancel();
    }

    /**
     * Method called once a range is finished. In case of multiple ranges being repaired this will be
     * called once per range. If this method is overridden make sure to call the super method.
     *
     * @param range The range
     * @param repairStatus The status of the range
     */
    protected void onRangeFinished(final LongTokenRange range, final RepairStatus repairStatus)
    {
        myRangeTracker.onRangeFinished(range, repairStatus);
    }

    /**
     * Enum used to provide Event Progress for RepairTask.
     */
    public enum ProgressEventType
    {
        /**
         * Fired first when progress starts. Happens only once.
         */
        START,

        /**
         * Fire when progress happens. This can be zero or more time after START.
         */
        PROGRESS,

        /**
         * When observing process completes with error, this is sent once before COMPLETE.
         */
        ERROR,

        /**
         * When observing process is aborted by user, this is sent once before COMPLETE.
         */
        ABORT,

        /**
         * When observing process completes successfully, this is sent once before COMPLETE.
         */
        SUCCESS,

        /**
         * Fire when progress complete. This is fired once, after ERROR/ABORT/SUCCESS is fired. After this, no more
         * ProgressEvent should be fired for the same event.
         */
        COMPLETE,

        /**
         * Used when sending message without progress.
         */
        NOTIFICATION
    }

    /**
     * Returns the set of token ranges that have failed during the repair task.
     *
     * @return a set of {@link LongTokenRange} representing the failed token ranges.
     */
    @VisibleForTesting
    protected final Set<LongTokenRange> getFailedRanges()
    {
        return myRangeTracker.getFailedRanges();
    }

    /**
     * Returns the set of token ranges that have been successfully repaired during the repair task.
     *
     * @return a set of {@link LongTokenRange} representing the successful token ranges.
     */
    @VisibleForTesting
    protected final Set<LongTokenRange> getSuccessfulRanges()
    {
        return myRangeTracker.getSuccessfulRanges();
    }

    /**
     * Get table reference.
     *
     * @return TableReference
     */
    public TableReference getTableReference()
    {
        return myTableReference;
    }

    /**
     * Get the identifier of the node this repair task runs on.
     *
     * @return the node UUID.
     */
    protected final UUID getNodeID()
    {
        return nodeID;
    }

    /**
     * Get the repair configuration.
     *
     * @return RepairConfiguration
     */
    public RepairConfiguration getRepairConfiguration()
    {
        return myRepairConfiguration;
    }
}
