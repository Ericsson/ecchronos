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

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockClientSaturationException;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;

import javax.management.RuntimeMBeanException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks per-job lock-acquisition backoff for the scheduler and distinguishes client-side saturation (CQL
 * timeouts) from ordinary contention / cluster unavailability (see #1852).
 * <p>
 * Ordinary contention gets a flat, single-step backoff. Repeated client-side saturation timeouts for the same job
 * get an escalating, capped backoff so the agent reduces its lock-acquisition rate instead of hammering the CAS
 * slots at full rate while saturated. Counters distinguish the two causes so operators can tell "ecChronos is
 * saturated" from "Cassandra is unavailable".
 */
final class LockFailureBackoff
{
    private static final Logger LOG = LoggerFactory.getLogger(LockFailureBackoff.class);

    /** Maximum multiplier applied to the base backoff when client saturation timeouts repeat, this must be a power-of-two. */
    static final int MAX_SATURATION_BACKOFF_MULTIPLIER = 32;
    /** Metric counting lock acquisition failures caused by client-side saturation (CQL timeouts). */
    static final String METRIC_LOCK_SATURATION_TIMEOUTS = "ecc.lock.saturation.timeouts";
    /** Metric counting lock acquisition failures caused by cluster unavailability / contention. */
    static final String METRIC_LOCK_UNAVAILABLE = "ecc.lock.unavailable";

    private final ConcurrentHashMap<ScheduledJob, Long> myContentionBackoff = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ScheduledJob, AtomicInteger> mySaturationTimeouts = new ConcurrentHashMap<>();
    private final long myRunIntervalInMs;
    private final MeterRegistry myMeterRegistry;

    LockFailureBackoff(final long runIntervalInMs, final MeterRegistry meterRegistry)
    {
        myRunIntervalInMs = runIntervalInMs;
        myMeterRegistry = meterRegistry;
    }

    /**
     * Whether the given throwable represents a client-side saturation lock failure (a CQL/driver timeout) rather
     * than cluster unavailability. The cause chain is walked because the saturation exception may be re-wrapped in
     * a plain lock exception before it reaches the scheduler.
     *
     * @param throwable the throwable to inspect.
     * @return true if a {@link LockClientSaturationException} is found anywhere in the cause chain.
     */
    @VisibleForTesting
    static boolean isClientSaturation(final Throwable throwable)
    {
        Throwable current = throwable;
        while (current != null)
        {
            if (current instanceof LockClientSaturationException)
            {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Record a lock failure for a job and set the appropriate backoff.
     *
     * @param job the job whose lock acquisition failed.
     * @param throwable the failure.
     * @param nodeID the node the job runs on (for logging context).
     */
    void recordFailure(final ScheduledJob job, final Throwable throwable, final UUID nodeID)
    {
        if (isClientSaturation(throwable))
        {
            recordSaturation(job, nodeID);
        }
        else
        {
            logContentionFailure(job, nodeID, throwable);
            incrementMetric(METRIC_LOCK_UNAVAILABLE);
            myContentionBackoff.put(job, System.currentTimeMillis() + flatBackoff());
        }
    }

    private static void logContentionFailure(final ScheduledJob job, final UUID nodeID, final Throwable e)
    {
        if (e instanceof RuntimeMBeanException rme)
        {
            if (rme.getCause() instanceof IllegalStateException
                    && rme.getCause().getMessage() != null
                    && rme.getCause().getMessage().contains("More than one key found"))
            {
                LOG.debug("Unable to get schedule lock on job {} in node {}, probably Jolokia 2.3.0 or older",
                        job, nodeID, e);
            }
            else
            {
                LOG.warn("Unable to get schedule lock on job {} in node {}", job, nodeID, e);
            }
        }
        else
        {
            LOG.debug("Lock contention for job {} in node {}: {}", job, nodeID, e.getMessage());
        }
    }

    private void recordSaturation(final ScheduledJob job, final UUID nodeID)
    {
        int consecutive = mySaturationTimeouts.computeIfAbsent(job, k -> new AtomicInteger(0)).incrementAndGet();
        incrementMetric(METRIC_LOCK_SATURATION_TIMEOUTS);
        int maxExponent = Integer.numberOfTrailingZeros(MAX_SATURATION_BACKOFF_MULTIPLIER);
        int exponent = Math.min(consecutive - 1, maxExponent);
        long multiplier = 1L << exponent;
        long backoff = flatBackoff() * multiplier;
        myContentionBackoff.put(job, System.currentTimeMillis() + backoff);
        LOG.warn("ecChronos appears saturated: lock acquisition for job {} on node {} timed out {} time(s) in a row "
                + "(client-side). Backing off {} ms before retrying. Cassandra is not necessarily unavailable.",
                job, nodeID, consecutive, backoff);
    }

    private long flatBackoff()
    {
        return ThreadLocalRandom.current().nextLong(myRunIntervalInMs / 2, myRunIntervalInMs);
    }

    private void incrementMetric(final String name)
    {
        if (myMeterRegistry != null)
        {
            myMeterRegistry.counter(name).increment();
        }
    }

    /**
     * Whether the given job is currently within a backoff window.
     *
     * @param job the job.
     * @return true if backing off.
     */
    boolean isInBackoff(final ScheduledJob job)
    {
        Long backoffUntil = myContentionBackoff.get(job);
        return backoffUntil != null && System.currentTimeMillis() < backoffUntil;
    }

    /**
     * Clear the backoff window for a job (for example once it has elapsed on selection).
     *
     * @param job the job.
     */
    void clearBackoff(final ScheduledJob job)
    {
        myContentionBackoff.remove(job);
    }

    /**
     * Reset all backoff state for a job (on success or deschedule), including the consecutive saturation count.
     *
     * @param job the job.
     */
    void reset(final ScheduledJob job)
    {
        myContentionBackoff.remove(job);
        mySaturationTimeouts.remove(job);
    }

    /**
     * Clear all tracked state.
     */
    void clear()
    {
        myContentionBackoff.clear();
        mySaturationTimeouts.clear();
    }
}
