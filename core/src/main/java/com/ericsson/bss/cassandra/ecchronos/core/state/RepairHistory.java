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
import java.util.Set;
import java.util.UUID;

public interface RepairHistory
{
    RepairHistory NO_OP = new NoOpRepairHistory();

    RepairSession newSession(
            Node node,
            TableReference tableReference,
            UUID jobId,
            LongTokenRange range,
            Set<DriverNode> participants);

    interface RepairSession
    {
        void start();

        void finish(RepairStatus repairStatus);
    }

    class NoOpRepairHistory implements RepairHistory
    {
        private static final RepairSession NO_OP = new NoOpRepairSession();

        /**
         * New session.
         */
        @Override
        public RepairSession newSession(
                final Node node,
                final TableReference tableReference,
                final UUID jobId,
                final LongTokenRange range,
                final Set<DriverNode> participants)
        {
            return NO_OP;
        }
    }

    class NoOpRepairSession implements RepairSession
    {
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
