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
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairJob;
import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Bundles a single Cassandra {@link Node} with the {@link IncrementalRepairJob} delegate that tracks that
 * node's incremental repair progress for one table, together with the per-node refresh-throttle bookkeeping.
 * <p>
 * This is the per-node unit held by a {@link UnifiedIncrementalRepairJob}: the consolidated multi-node job
 * holds one {@code IncrementalNodeState} per managed node. Each node's {@link IncrementalRepairJob} delegate
 * remains fully independent so that repair history, the Cassandra {@code percentRepaired}/{@code repaired_at}
 * signal, the cross-cycle stall guard, and the single incremental session all stay keyed to exactly one node
 * (the delegate implementation is unchanged and still coordinates exactly one node).
 * <p>
 * Each node's state is refreshed on its own throttle rather than sharing a single throttle across all nodes,
 * mirroring {@link NodeRepairState}.
 */
public final class IncrementalNodeState
{
    private static final long MIN_REFRESH_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);

    private final Node myNode;
    private final IncrementalRepairJob myDelegate;
    private volatile long myLastRefreshTime;

    /**
     * Construct a per-node incremental state holder.
     *
     * @param node the managed Cassandra node. Must not be {@code null}.
     * @param delegate the incremental repair job tracking this node's progress for the table. Must not be
     *            {@code null}.
     */
    public IncrementalNodeState(final Node node, final IncrementalRepairJob delegate)
    {
        myNode = Preconditions.checkNotNull(node, "Node must be set");
        myDelegate = Preconditions.checkNotNull(delegate, "Incremental repair job delegate must be set");
    }

    /**
     * Get the managed node.
     *
     * @return the node.
     */
    public Node getNode()
    {
        return myNode;
    }

    /**
     * Get the host id of the managed node.
     *
     * @return the node host id.
     */
    public UUID getHostId()
    {
        return myNode.getHostId();
    }

    /**
     * Get the incremental repair job delegate for this node.
     *
     * @return the delegate.
     */
    public IncrementalRepairJob getDelegate()
    {
        return myDelegate;
    }

    /**
     * Reset the refresh throttle so the next {@link #tryClaimRefresh(long)} is allowed to proceed. Called
     * after a task has executed so that the node's state is recomputed promptly on the next scheduler pass.
     */
    public void resetRefreshThrottle()
    {
        myLastRefreshTime = 0;
    }

    /**
     * Attempt to claim a refresh for this node, respecting the minimum refresh interval.
     *
     * @param now the current time in epoch milliseconds.
     * @return {@code true} if a refresh may proceed now; {@code false} if throttled.
     */
    public boolean tryClaimRefresh(final long now)
    {
        if (now - myLastRefreshTime < MIN_REFRESH_INTERVAL_MS)
        {
            return false;
        }
        myLastRefreshTime = now;
        return true;
    }

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
        IncrementalNodeState that = (IncrementalNodeState) o;
        return Objects.equals(myNode, that.myNode) && Objects.equals(myDelegate, that.myDelegate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myNode, myDelegate);
    }

    @Override
    public String toString()
    {
        return String.format("IncrementalNodeState{node=%s}", myNode.getHostId());
    }
}
