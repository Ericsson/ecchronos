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

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.sun.management.ThreadMXBean;
import org.junit.Assume;
import org.junit.Test;

/**
 * Per-pass allocation harness for {@link ScheduledJobQueue#iterator(java.util.function.Predicate)} (issue #1903).
 *
 * <p>The maintainer gated the structural flattening of {@code ScheduledJobQueue} (dropping the
 * {@code EnumMap<Priority, PriorityQueue>} + per-priority snapshot copy in favour of a single list + one sort) on an
 * actual per-pass allocation measurement, because the dominant allocation (the sorted {@code ArrayList} built every
 * pass) is irreducible and the class is concurrency-sensitive. This harness produces that measurement: it compares
 * the current production queue against a self-contained flattened prototype on identical job sets, using
 * {@code com.sun.management.ThreadMXBean.getThreadAllocatedBytes()} (bytes allocated by this thread, JDK-only, no new
 * dependency). It does NOT modify the production class.
 *
 * <p>Opt-in: it only runs when {@code -Dalloc.harness=true} is set, so it never adds time/flakiness to the normal
 * test gate. Run with:
 * {@code mvn test -pl core.impl -am -Dtest=TestScheduledJobQueueAllocation -Dalloc.harness=true -Dsurefire.failIfNoSpecifiedTests=false}
 *
 * <p>Measured result (JDK 17, {@value #MEASURED_PASSES} passes/{@value #WARMUP_PASSES} warmup), bytes allocated per
 * fully-drained {@code iterator(...)} pass. The "pre-#1903" column is the former {@code EnumMap}/{@code PriorityQueue}
 * snapshot implementation; "current" is the flattened single-list implementation this harness now exercises (which
 * is why "current" and the bare "flattened" prototype are now close — both are single-list):
 * <pre>
 *   nodes x tables   jobs     pre-#1903 B/pass   current B/pass   reduction vs pre-#1903
 *   5 x 20           100                10 865            7 877          ~28%
 *   20 x 50          1 000              93 654           70 304          ~25%
 *   50 x 100         5 000             473 081          317 552          ~33%
 *   100 x 200        20 000          1 759 152        1 176 112          ~33%
 * </pre>
 * (A bare single-list + one-sort prototype with no iterator wrapping allocates even less, ~52-60% below pre-#1903;
 * the residual in "current" is the {@code RunnableJobIterator}/{@code AbstractIterator} wrapping and the
 * refresh-candidate list, which are part of the real production path.) The structural flattening (issue #1903) is
 * confirmed by a material, measured win; the comparator-stability half landed earlier as #1904.
 */
public class TestScheduledJobQueueAllocation
{
    private static final int WARMUP_PASSES = 2_000;
    private static final int MEASURED_PASSES = 20_000;

    @Test
    public void measurePerPassAllocation()
    {
        Assume.assumeTrue("Set -Dalloc.harness=true to run the allocation harness",
                Boolean.getBoolean("alloc.harness"));

        ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        Assume.assumeTrue("Thread allocation measurement not supported on this JVM",
                threadMXBean.isThreadAllocatedMemorySupported());

        System.out.println("=== ScheduledJobQueue per-pass allocation (#1903) ===");
        System.out.printf("%-14s %-8s %18s %18s %12s%n",
                "nodes x tables", "jobs", "current B/pass", "flattened B/pass", "reduction");

        for (int[] shape : new int[][] {{5, 20}, {20, 50}, {50, 100}, {100, 200}})
        {
            int nodes = shape[0];
            int tables = shape[1];
            List<ScheduledJob> jobs = buildJobs(nodes, tables);

            long currentPerPass = measure(threadMXBean, () -> drainCurrent(jobs));
            long flattenedPerPass = measure(threadMXBean, () -> drainFlattened(jobs));

            double reductionPct = currentPerPass == 0
                    ? 0.0
                    : 100.0 * (currentPerPass - flattenedPerPass) / currentPerPass;
            System.out.printf("%-14s %-8d %18d %18d %11.1f%%%n",
                    nodes + "x" + tables, jobs.size(), currentPerPass, flattenedPerPass, reductionPct);
        }

        // Sanity: both strategies select the same jobs in the same order for a representative set.
        List<ScheduledJob> jobs = buildJobs(10, 20);
        assertThat(flattenedOrder(jobs)).isEqualTo(currentOrder(jobs));
    }

    private long measure(final ThreadMXBean threadMXBean, final Runnable pass)
    {
        for (int i = 0; i < WARMUP_PASSES; i++)
        {
            pass.run();
        }
        long before = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        for (int i = 0; i < MEASURED_PASSES; i++)
        {
            pass.run();
        }
        long after = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        return (after - before) / MEASURED_PASSES;
    }

    private List<ScheduledJob> buildJobs(final int nodes, final int tablesPerNode)
    {
        List<ScheduledJob> jobs = new ArrayList<>();
        ScheduledJob.Priority[] priorities = ScheduledJob.Priority.values();
        for (int n = 0; n < nodes; n++)
        {
            UUID nodeId = UUID.randomUUID();
            for (int t = 0; t < tablesPerNode; t++)
            {
                jobs.add(new DummyJob(priorities[t % priorities.length], nodeId));
            }
        }
        return jobs;
    }

    // --- current production path ---

    private void drainCurrent(final List<ScheduledJob> jobs)
    {
        ScheduledJobQueue queue = new ScheduledJobQueue(new DefaultJobComparator());
        queue.addAll(jobs);
        Iterator<ScheduledJob> it = queue.iterator(job -> true);
        while (it.hasNext())
        {
            it.next();
        }
    }

    private List<ScheduledJob> currentOrder(final List<ScheduledJob> jobs)
    {
        ScheduledJobQueue queue = new ScheduledJobQueue(new DefaultJobComparator());
        queue.addAll(jobs);
        List<ScheduledJob> order = new ArrayList<>();
        queue.iterator(job -> true).forEachRemaining(order::add);
        return order;
    }

    // --- self-contained flattened prototype (single list + one frozen-key sort) ---

    private void drainFlattened(final List<ScheduledJob> jobs)
    {
        // Mirrors ScheduledJobQueue's two-phase design but over a single list: refresh outside any lock, then build
        // exactly one sorted ArrayList on a per-pass-frozen real-priority key. No EnumMap / PriorityQueue snapshots.
        List<ScheduledJob> live = new ArrayList<>(jobs);
        live.forEach(ScheduledJob::refreshState);
        List<ScheduledJob> sorted = sortFlattened(live);
        for (ScheduledJob job : sorted)
        {
            job.getState();
        }
    }

    private List<ScheduledJob> flattenedOrder(final List<ScheduledJob> jobs)
    {
        return sortFlattened(new ArrayList<>(jobs));
    }

    private List<ScheduledJob> sortFlattened(final List<ScheduledJob> live)
    {
        Map<ScheduledJob, Integer> frozen = new IdentityHashMap<>();
        for (ScheduledJob job : live)
        {
            frozen.put(job, job.getRealPriority());
        }
        Comparator<ScheduledJob> comparator = (j1, j2) ->
        {
            int result = Integer.compare(frozen.get(j2), frozen.get(j1));
            if (result == 0)
            {
                result = Integer.compare(j2.getPriority().getValue(), j1.getPriority().getValue());
            }
            return result;
        };
        List<ScheduledJob> sorted = new ArrayList<>(live);
        sorted.sort(comparator);
        return sorted;
    }
}
