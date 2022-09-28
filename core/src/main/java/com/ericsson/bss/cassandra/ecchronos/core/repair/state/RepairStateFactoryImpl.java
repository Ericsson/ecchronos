/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

public final class RepairStateFactoryImpl implements RepairStateFactory
{
    private final HostStates myHostStates;
    private final TableRepairMetrics myTableRepairMetrics;

    private final VnodeRepairStateFactoryImpl myVnodeRepairStateFactory;
    private final VnodeRepairStateFactoryImpl mySubRangeRepairStateFactory;

    private RepairStateFactoryImpl(final Builder builder)
    {
        myHostStates = builder.myHostStates;
        myTableRepairMetrics = builder.myTableRepairMetrics;

        myVnodeRepairStateFactory = new VnodeRepairStateFactoryImpl(builder.myReplicationState,
                builder.myRepairHistoryProvider, false);
        mySubRangeRepairStateFactory = new VnodeRepairStateFactoryImpl(builder.myReplicationState,
                builder.myRepairHistoryProvider, true);
    }

    @Override
    public RepairState create(final TableReference tableReference,
                              final RepairConfiguration repairConfiguration,
                              final PostUpdateHook postUpdateHook)
    {
        ReplicaRepairGroupFactory replicaRepairGroupFactory = VnodeRepairGroupFactory.INSTANCE;

        VnodeRepairStateFactory vnodeRepairStateFactory = myVnodeRepairStateFactory;
        if (repairConfiguration.getTargetRepairSizeInBytes() != RepairConfiguration.FULL_REPAIR_SIZE)
        {
            vnodeRepairStateFactory = mySubRangeRepairStateFactory;
        }

        return new RepairStateImpl(tableReference, repairConfiguration, vnodeRepairStateFactory, myHostStates,
                myTableRepairMetrics, replicaRepairGroupFactory, postUpdateHook);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ReplicationState myReplicationState;
        private HostStates myHostStates;
        private RepairHistoryProvider myRepairHistoryProvider;
        private TableRepairMetrics myTableRepairMetrics;

        /**
         * Build repair state factory with replication state.
         *
         * @param replicationState Replication state.
         * @return Builder
         */
        public Builder withReplicationState(final ReplicationState replicationState)
        {
            myReplicationState = replicationState;
            return this;
        }

        /**
         * Build repair state factory with host states.
         *
         * @param hostStates The host states.
         * @return Builder
         */
        public Builder withHostStates(final HostStates hostStates)
        {
            myHostStates = hostStates;
            return this;
        }

        /**
         * Build repair state factory with repair history provider.
         *
         * @param repairHistoryProvider The repair history provider.
         * @return Builder
         */
        public Builder withRepairHistoryProvider(final RepairHistoryProvider repairHistoryProvider)
        {
            myRepairHistoryProvider = repairHistoryProvider;
            return this;
        }

        /**
         * Build repair state factory with table repair metrics.
         *
         * @param tableRepairMetrics  The table repair metrics.
         * @return Builder
         */
        public Builder withTableRepairMetrics(final TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        /**
         * Build repair state factory.
         *
         * @return RepairStateFactoryImpl
         */
        public RepairStateFactoryImpl build()
        {
            return new RepairStateFactoryImpl(this);
        }
    }
}
