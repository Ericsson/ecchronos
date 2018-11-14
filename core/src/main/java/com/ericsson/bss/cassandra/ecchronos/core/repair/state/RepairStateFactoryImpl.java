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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import java.util.concurrent.TimeUnit;

public class RepairStateFactoryImpl implements RepairStateFactory
{
    private final Metadata myMetadata;
    private final Host myHost;
    private final HostStates myHostStates;
    private final RepairHistoryProvider myRepairHistoryProvider;
    private final TableRepairMetrics myTableRepairMetrics;

    private RepairStateFactoryImpl(Builder builder)
    {
        myMetadata = builder.myMetadata;
        myHost = builder.myHost;
        myHostStates = builder.myHostStates;
        myRepairHistoryProvider = builder.myRepairHistoryProvider;
        myTableRepairMetrics = builder.myTableRepairMetrics;
    }

    @Override
    public RepairState create(TableReference tableReference, RepairConfiguration repairConfiguration)
    {
        return new RepairStateImpl.Builder()
                .withTableReference(tableReference)
                .withMetadata(myMetadata)
                .withHost(myHost)
                .withHostStates(myHostStates)
                .withRepairHistoryProvider(myRepairHistoryProvider)
                .withRunInterval(repairConfiguration.getRepairIntervalInMs(), TimeUnit.MILLISECONDS)
                .withRepairMetrics(myTableRepairMetrics)
                .build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Metadata myMetadata;
        private Host myHost;
        private HostStates myHostStates;
        private RepairHistoryProvider myRepairHistoryProvider;
        private TableRepairMetrics myTableRepairMetrics;

        public Builder withMetadata(Metadata metadata)
        {
            myMetadata = metadata;
            return this;
        }

        public Builder withHost(Host host)
        {
            myHost = host;
            return this;
        }

        public Builder withHostStates(HostStates hostStates)
        {
            myHostStates = hostStates;
            return this;
        }

        public Builder withRepairHistoryProvider(RepairHistoryProvider repairHistoryProvider)
        {
            myRepairHistoryProvider = repairHistoryProvider;
            return this;
        }

        public Builder withTableRepairMetrics(TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public RepairStateFactoryImpl build()
        {
            return new RepairStateFactoryImpl(this);
        }
    }
}
