/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import java.util.Set;
import java.util.UUID;

import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * Repair history interface.
 */
@FunctionalInterface
public interface RepairHistory
{
    /**
     * Returns a no-op repair history instance.
     */
    RepairHistory NO_OP = new NoOpRepairHistory();

    /**
     * Creates a new repair session.
     * @param tableReference the table reference
     * @param jobId the job id
     * @param range the token range
     * @param participants the nodes participating in repair
     * @return the repair session
     */
    RepairSession newSession(TableReference tableReference,
                             UUID jobId,
                             LongTokenRange range,
                             Set<DriverNode> participants);

    /** Represents an active repair session. */
    interface RepairSession
    {
        /** Starts this component. */
        void start();

        /**
         * Marks the operation as finished.
         * @param repairStatus the repair status
         */
        void finish(RepairStatus repairStatus);
    }

    /** A no-op repair history implementation that discards all entries. */
    class NoOpRepairHistory implements RepairHistory
    {
        private static final RepairSession NO_OP = new NoOpRepairSession();

        /** Constructs a new NoOpRepairHistory. */
        NoOpRepairHistory()
        {
            // Default constructor
        }

        /**
         * New session.
         */
        @Override
        public RepairSession newSession(final TableReference tableReference,
                                        final UUID jobId,
                                        final LongTokenRange range,
                                        final Set<DriverNode> participants)
        {
            return NO_OP;
        }
    }

    /** A no-op repair session that performs no operations. */
    class NoOpRepairSession implements RepairSession
    {
        /** Constructs a new NoOpRepairSession. */
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
