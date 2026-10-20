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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Focused tests for {@link JobRunExecutor}'s lazy node resolution.
 * <p>
 * The executor is created with a {@code Supplier<Node>} rather than a captured {@link Node} so that it always
 * reads the current node from the native connection provider when it needs it, instead of relying on whatever
 * value happened to be present at construction time. This guards against the race in issue #1754, where a
 * schedule (and therefore its executor) could be created before the node was registered in the provider.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestJobRunExecutor
{
    @Mock
    private LockFailureBackoff mockLockFailureBackoff;
    @Mock
    private SchedulerMetrics mockSchedulerMetrics;
    @Mock
    private ScheduledJob mockJob;
    @Mock
    private Node mockNode;

    private final UUID nodeId = UUID.randomUUID();

    // Backing store that models the native connection provider's node map.
    private final Map<UUID, Node> nodes = new HashMap<>();

    // Records the node the validator was last invoked with, so we can assert what the executor resolved.
    private final AtomicReference<Node> lastValidatedNode = new AtomicReference<>();

    private JobRunExecutor executor;

    @Before
    public void setup()
    {
        // Validator records the node argument and parks the job (returns a non -1 reject delay) so that
        // handleUnsuccessfulTask takes the run-policy path, which is the single place the node is read.
        BiFunction<ScheduledJob, Node, Long> recordingValidator = (job, node) ->
        {
            lastValidatedNode.set(node);
            return 1000L;
        };

        executor = new JobRunExecutor(
                nodeId,
                () -> nodes.get(nodeId),
                mockLockFailureBackoff,
                mockSchedulerMetrics,
                new ConcurrentHashMap<>(),
                recordingValidator,
                new AtomicReference<>(Clock.systemUTC()));
    }

    @Test
    public void testNodeResolvedLazilyWhenAbsentAtConstruction()
    {
        // Node is NOT registered yet (as during the issue #1754 race window).
        assertThat(nodes).doesNotContainKey(nodeId);

        executor.handleUnsuccessfulTask(mockJob, new AtomicInteger(0));

        assertThat(lastValidatedNode.get())
                .as("with no node registered, the supplier resolves to null (not a cached stale value)")
                .isNull();
    }

    @Test
    public void testNodeResolvedFreshOnEachUseAfterItAppears()
    {
        // Executor created while the node is absent, then the node appears later (async NodeAddedAction, or a
        // later registration). The executor must pick it up on the next use rather than staying stuck with null.
        executor.handleUnsuccessfulTask(mockJob, new AtomicInteger(0));
        assertThat(lastValidatedNode.get()).isNull();

        nodes.put(nodeId, mockNode);

        executor.handleUnsuccessfulTask(mockJob, new AtomicInteger(0));
        assertThat(lastValidatedNode.get())
                .as("once the node is registered, the executor resolves the current node on its next use")
                .isSameAs(mockNode);
    }
}
