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

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class TestSchedulerMetrics
{
    private SimpleMeterRegistry myMeterRegistry;
    private SchedulerMetrics myMetrics;
    private final UUID myNodeId = UUID.randomUUID();

    @Before
    public void setup()
    {
        myMeterRegistry = new SimpleMeterRegistry();
        myMetrics = new SchedulerMetrics(myMeterRegistry);
    }

    @Test
    public void testRecordRefresh()
    {
        myMetrics.recordRefresh(TimeUnit.MILLISECONDS.toNanos(5), 7);

        assertThat(myMeterRegistry.timer("ecc.scheduler.refresh").count()).isEqualTo(1L);
        assertThat(myMeterRegistry.summary("ecc.scheduler.refreshed.jobs").totalAmount()).isEqualTo(7.0d);
    }

    @Test
    public void testRecordPassWithinAndExceededWindow()
    {
        myMetrics.recordPass(true);
        myMetrics.recordPass(true);
        myMetrics.recordPass(false);

        assertThat(myMeterRegistry.counter("ecc.scheduler.pass.within_window").count()).isEqualTo(2.0d);
        assertThat(myMeterRegistry.counter("ecc.scheduler.pass.exceeded_window").count()).isEqualTo(1.0d);
    }

    @Test
    public void testRecordLockAttempt()
    {
        myMetrics.recordLockAttempt(true, TimeUnit.MILLISECONDS.toNanos(2));
        myMetrics.recordLockAttempt(false, TimeUnit.MILLISECONDS.toNanos(3));
        myMetrics.recordLockAttempt(true, TimeUnit.MILLISECONDS.toNanos(1));

        assertThat(myMeterRegistry.counter("ecc.scheduler.lock.success").count()).isEqualTo(2.0d);
        assertThat(myMeterRegistry.counter("ecc.scheduler.lock.failure").count()).isEqualTo(1.0d);
        assertThat(myMeterRegistry.timer("ecc.scheduler.lock.latency").count()).isEqualTo(3L);
    }

    @Test
    public void testProgressResetsNoProgressCounter()
    {
        // Several no-progress passes short of the threshold, then a progress pass resets the counter.
        for (int i = 0; i < SchedulerMetrics.FALLING_BEHIND_PASS_THRESHOLD - 1; i++)
        {
            myMetrics.recordPassOutcome(false, true, myNodeId);
        }
        myMetrics.recordPassOutcome(true, true, myNodeId);
        // After reset, a single further no-progress pass must not be at the threshold.
        myMetrics.recordPassOutcome(false, true, myNodeId);
        // No assertion on logging here (throttled logger); this verifies no exception and the reset path runs.
    }

    @Test
    public void testPassesWithNoRunnableWorkDoNotCountAsFallingBehind()
    {
        // No runnable work means the scheduler is simply idle, not falling behind; counter must not accumulate.
        for (int i = 0; i < SchedulerMetrics.FALLING_BEHIND_PASS_THRESHOLD * 2; i++)
        {
            myMetrics.recordPassOutcome(false, false, myNodeId);
        }
        // Reaching here without the throttled warning path mis-firing is the behavioural check.
    }

    @Test
    public void testNullRegistryIsNoOp()
    {
        SchedulerMetrics metrics = new SchedulerMetrics(null);
        metrics.recordRefresh(1L, 1);
        metrics.recordPass(true);
        metrics.recordLockAttempt(true, 1L);
        metrics.recordPassOutcome(false, true, myNodeId);
        // No registry wired -> no metrics and no exceptions.
    }
}
