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

import com.ericsson.bss.cassandra.ecchronos.core.impl.logging.ThrottlingLogger;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduler saturation observability (see #1854). Exposes metrics that let an operator see the scheduler falling
 * behind before repairs silently stall:
 * <ul>
 *   <li>{@code ecc.scheduler.refresh} — timer of per-pass repair-state refresh duration (count gives refresh rate);</li>
 *   <li>{@code ecc.scheduler.refreshed.jobs} — number of jobs refreshed per pass (summary);</li>
 *   <li>{@code ecc.scheduler.pass.within_window} / {@code ecc.scheduler.pass.exceeded_window} — pass completion
 *       relative to the session window (ratio = within / (within + exceeded));</li>
 *   <li>{@code ecc.scheduler.lock.success} / {@code ecc.scheduler.lock.failure} — lock-attempt success rate;</li>
 *   <li>{@code ecc.scheduler.lock.latency} — timer of lock-acquisition latency (the CAS LWT round trip).</li>
 * </ul>
 * It also logs a throttled WARN when the scheduler repeatedly finds runnable work it cannot make progress on,
 * which indicates the schedule is falling behind.
 */
final class SchedulerMetrics
{
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerMetrics.class);
    private static final ThrottlingLogger THROTTLED_LOGGER = new ThrottlingLogger(LOG, 5, TimeUnit.MINUTES);

    static final String METRIC_REFRESH = "ecc.scheduler.refresh";
    static final String METRIC_REFRESHED_JOBS = "ecc.scheduler.refreshed.jobs";
    static final String METRIC_PASS_WITHIN_WINDOW = "ecc.scheduler.pass.within_window";
    static final String METRIC_PASS_EXCEEDED_WINDOW = "ecc.scheduler.pass.exceeded_window";
    static final String METRIC_LOCK_SUCCESS = "ecc.scheduler.lock.success";
    static final String METRIC_LOCK_FAILURE = "ecc.scheduler.lock.failure";
    static final String METRIC_LOCK_LATENCY = "ecc.scheduler.lock.latency";

    /** Consecutive no-progress passes (runnable work present but nothing executed) before warning. */
    static final int FALLING_BEHIND_PASS_THRESHOLD = 10;

    private final MeterRegistry myMeterRegistry;
    private final AtomicInteger myConsecutiveNoProgressPasses = new AtomicInteger(0);

    SchedulerMetrics(final MeterRegistry meterRegistry)
    {
        myMeterRegistry = meterRegistry;
    }

    /**
     * Record a repair-state refresh for a scheduler pass.
     *
     * @param durationNanos the time spent refreshing/selecting jobs this pass.
     * @param refreshedJobs the number of jobs considered for refresh this pass.
     */
    void recordRefresh(final long durationNanos, final int refreshedJobs)
    {
        if (myMeterRegistry == null)
        {
            return;
        }
        myMeterRegistry.timer(METRIC_REFRESH).record(durationNanos, TimeUnit.NANOSECONDS);
        myMeterRegistry.summary(METRIC_REFRESHED_JOBS).record(refreshedJobs);
    }

    /**
     * Record whether a scheduler pass/session completed within the configured session window.
     *
     * @param withinWindow true if the pass finished within the window.
     */
    void recordPass(final boolean withinWindow)
    {
        if (myMeterRegistry == null)
        {
            return;
        }
        myMeterRegistry.counter(withinWindow ? METRIC_PASS_WITHIN_WINDOW : METRIC_PASS_EXCEEDED_WINDOW).increment();
    }

    /**
     * Record a lock acquisition attempt and its latency.
     *
     * @param success whether the lock was acquired.
     * @param durationNanos the time the attempt took.
     */
    void recordLockAttempt(final boolean success, final long durationNanos)
    {
        if (myMeterRegistry == null)
        {
            return;
        }
        myMeterRegistry.counter(success ? METRIC_LOCK_SUCCESS : METRIC_LOCK_FAILURE).increment();
        myMeterRegistry.timer(METRIC_LOCK_LATENCY).record(durationNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Record the outcome of a scheduler pass to detect the schedule falling behind. A pass that had runnable work
     * but made no progress is counted; once this happens for {@link #FALLING_BEHIND_PASS_THRESHOLD} consecutive
     * passes a throttled warning is logged. Any pass that makes progress resets the counter.
     *
     * @param madeProgress whether the pass executed at least one task.
     * @param hadRunnableWork whether the pass had at least one runnable (eligible) job.
     * @param nodeID the node, for logging context.
     */
    void recordPassOutcome(final boolean madeProgress, final boolean hadRunnableWork, final Object nodeID)
    {
        if (madeProgress || !hadRunnableWork)
        {
            myConsecutiveNoProgressPasses.set(0);
            return;
        }
        int consecutive = myConsecutiveNoProgressPasses.incrementAndGet();
        if (consecutive >= FALLING_BEHIND_PASS_THRESHOLD)
        {
            THROTTLED_LOGGER.warn("Scheduler for node {} has had runnable repair work but made no progress for {} "
                    + "consecutive passes; the schedule may be falling behind (lock contention or saturation).",
                    nodeID, consecutive);
        }
    }
}
