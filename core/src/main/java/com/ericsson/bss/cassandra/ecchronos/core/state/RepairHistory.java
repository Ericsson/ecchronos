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
package com.ericsson.bss.cassandra.ecchronos.core.state;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

/**
 * Interface for tracking repair history sessions.
 */
public interface RepairHistory
{
    /** A no-op implementation that performs no tracking. */
    RepairHistory NO_OP = new NoOpRepairHistory();

    /**
     * Create a new repair session for the given table and token range.
     *
     * @param node The node performing the repair.
     * @param tableReference The table being repaired.
     * @param jobId The unique identifier of the repair job.
     * @param range The token range being repaired.
     * @param participants The set of nodes participating in the repair.
     * @param repairType The type of repair being executed.
     * @return A new repair session.
     */
    RepairSession newSession(
            Node node,
            TableReference tableReference,
            UUID jobId,
            LongTokenRange range,
            Set<DriverNode> participants,
            RepairType repairType);

    /**
     * Record a completed repair in {@code ecchronos.repair_history} for each of the provided replica nodes.
     * <p>
     * Unlike {@link #newSession} (which records the execution keyed by a single node), this method writes one row per
     * replica node id, keyed by that replica's {@code node_id}. It is used by incremental repairs to reflect a
     * completed logical repair for all replicas this ecChronos instance is responsible for — both when this instance
     * performs the repair and when it observes that another instance already completed it.
     * <p>
     * The default implementation is a no-op.
     *
     * @param tableReference The table that was repaired.
     * @param jobId The unique identifier of the repair job.
     * @param replicaNodeIds The replica node ids to write a history row for (the {@code node_id} of each row).
     * @param participants The set of nodes participating in the repair (recorded on each row).
     * @param repairType The type of repair that was executed.
     * @param startedAt The repair start timestamp (epoch millis).
     * @param finishedAt The repair finish timestamp (epoch millis).
     * @param repairStatus The final status of the repair.
     */
    default void recordCompletedRepair(
            final TableReference tableReference,
            final UUID jobId,
            final Collection<UUID> replicaNodeIds,
            final Set<DriverNode> participants,
            final RepairType repairType,
            final long startedAt,
            final long finishedAt,
            final RepairStatus repairStatus)
    {
        // No-op by default.
    }

    /**
     * Represents a single repair session that can be started and finished.
     */
    interface RepairSession
    {
        /**
         * Mark the repair session as started.
         */
        void start();

        /**
         * Mark the repair session as finished with the given status.
         *
         * @param repairStatus The final status of the repair.
         */
        void finish(RepairStatus repairStatus);
    }

    /**
     * A no-op implementation of {@link RepairHistory} that does not record any repair history.
     */
    class NoOpRepairHistory implements RepairHistory
    {
        private static final RepairSession NO_OP = new NoOpRepairSession();

        /**
         * Default constructor.
         */
        NoOpRepairHistory()
        {
            // Default constructor
        }

        /**
         * New session.
         */
        @Override
        public RepairSession newSession(
                final Node node,
                final TableReference tableReference,
                final UUID jobId,
                final LongTokenRange range,
                final Set<DriverNode> participants,
                final RepairType repairType)
        {
            return NO_OP;
        }
    }

    /**
     * A no-op implementation of {@link RepairSession} that does nothing on start or finish.
     */
    class NoOpRepairSession implements RepairSession
    {
        /**
         * Default constructor.
         */
        NoOpRepairSession()
        {
            // Default constructor
        }

        /**
         * Start.
         */
        @Override
        public void start()
        {
            // Do nothing
        }

        /**
         * End.
         */
        @Override
        public void finish(final RepairStatus repairStatus)
        {
            // Do nothing
        }
    }
}
