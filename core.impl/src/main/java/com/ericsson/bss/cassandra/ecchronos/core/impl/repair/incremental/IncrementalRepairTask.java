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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental;

import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class IncrementalRepairTask extends RepairTask
{
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalRepairTask.class);

    public IncrementalRepairTask(
            final UUID currentNode,
            final DistributedJmxProxyFactory jmxProxyFactory,
            final TableReference tableReference,
            final RepairConfiguration repairConfiguration,
            final TableRepairMetrics tableRepairMetrics)
    {
        super(currentNode, jmxProxyFactory, tableReference, repairConfiguration, tableRepairMetrics);
    }

    @Override
    protected final Map<String, String> getOptions()
    {
        Map<String, String> options = new HashMap<>();
        options.put(RepairOptions.PARALLELISM_KEY, getRepairConfiguration().getRepairParallelism().getName());
        options.put(RepairOptions.PRIMARY_RANGE_KEY, Boolean.toString(false));
        options.put(RepairOptions.COLUMNFAMILIES_KEY, getTableReference().getTable());
        options.put(RepairOptions.INCREMENTAL_KEY, Boolean.toString(true));
        return options;
    }

    @Override
    protected final void onFinish(final RepairStatus repairStatus)
    {
        if (repairStatus.equals(RepairStatus.FAILED))
        {
            LOG.warn("Unable to repair '{}', affected ranges: '{}'", this, getFailedRanges());
        }
    }

    @Override
    protected final void onRangeFinished(final LongTokenRange range, final RepairStatus repairStatus)
    {
        super.onRangeFinished(range, repairStatus);
        LOG.debug("{} for range {}", repairStatus, range);
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("Incremental repairTask of %s", getTableReference());
    }
}

