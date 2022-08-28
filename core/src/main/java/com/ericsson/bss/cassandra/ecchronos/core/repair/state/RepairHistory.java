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

public interface RepairHistory
{
    RepairHistory NO_OP = new NoOpRepairHistory();

    RepairSession newSession(TableReference tableReference, UUID jobId, LongTokenRange range, Set<DriverNode> participants);

    interface RepairSession
    {
        void start();

        void finish(RepairStatus repairStatus);
    }

    class NoOpRepairHistory implements RepairHistory
    {
        private static final RepairSession NO_OP = new NoOpRepairSession();

        @Override
        public RepairSession newSession(TableReference tableReference, UUID jobId, LongTokenRange range,
                Set<DriverNode> participants)
        {
            return NO_OP;
        }
    }

    class NoOpRepairSession implements RepairSession
    {
        @Override
        public void start()
        {
            // Do nothing
        }

        @Override
        public void finish(RepairStatus repairStatus)
        {
            // Do nothing
        }
    }
}
