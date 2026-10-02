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
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Bundles a single Cassandra {@link Node} with the {@link RepairState} that tracks that node's repair
 * progress for one table, together with the per-node refresh-throttle bookkeeping.
 * <p>
 * This is the per-node unit held by a {@link UnifiedTableRepairJob}: the consolidated multi-node job holds
 * one {@code NodeRepairState} per managed node. Each node's {@link RepairState} remains fully independent so
 * that repair progress, replica sets, and vnode state stay distinguishable per node (the {@link RepairState}
 * implementation is unchanged and still tracks exactly one node).
 * <p>
 * Each node's state is refreshed on its own throttle rather than sharing a single throttle across all nodes.
 * <p>
 * Instances are thread-safe with respect to the refresh throttle ({@code lastRefreshTime} is volatile and
 * mutated through {@link #tryClaimRefresh(long)}); the referenced {@link Node} and {@link RepairState} are
 * effectively immutable references. The {@code RepairState} implementation itself already performs lock-free
 * snapshot updates internally.
 */
public final class NodeRepairState
{
    private static final long MIN_REFRESH_INTERVAL_MS = TimeUnit.SECONDS.toMillis(30);

    private final Node myNode;
    private final RepairState myRepairState;
    private volatile long myLastRefreshTime;

    /**
     * Construct a per-node repair state holder.
     *
     * @param node the managed Cassandra node. Must not be {@code null}.
     * @param repairState the repair state tracking this node's progress for the table. Must not be {@code null}.
     */
    public NodeRepairState(final Node node, final RepairState repairState)
    {
        myNode = Preconditions.checkNotNull(node, "Node must be set");
        myRepairState = Preconditions.checkNotNull(repairState, "Repair state must be set");
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
     * Get the repair state for this node.
     *
     * @return the repair state.
     */
    public RepairState getRepairState()
    {
        return myRepairState;
    }

    /**
     * Get the current immutable repair state snapshot for this node.
     *
     * @return the current snapshot.
     */
    public RepairStateSnapshot getSnapshot()
    {
        return myRepairState.getSnapshot();
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
     * <p>
     * If at least {@value #MIN_REFRESH_INTERVAL_MS} ms have elapsed since the last claimed refresh, the
     * throttle timestamp is advanced to {@code now} and {@code true} is returned, indicating the caller
     * should perform the (potentially expensive) {@link RepairState#update()}. Otherwise {@code false} is
     * returned and the caller should skip the update.
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
        NodeRepairState that = (NodeRepairState) o;
        return Objects.equals(myNode, that.myNode) && Objects.equals(myRepairState, that.myRepairState);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myNode, myRepairState);
    }

    @Override
    public String toString()
    {
        return String.format("NodeRepairState{node=%s}", myNode.getHostId());
    }
}
