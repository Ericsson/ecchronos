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

import java.util.Collection;
import java.util.Map;

import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.google.common.base.Predicate;

public class FullyRepairedRepairEntryPredicate implements Predicate<RepairEntry>
{
    private final Map<LongTokenRange, Collection<Node>> myTokenToNodeMap;

    public FullyRepairedRepairEntryPredicate(Map<LongTokenRange, Collection<Node>> tokenToNodeMap)
    {
        myTokenToNodeMap = tokenToNodeMap;
    }

    @Override
    public boolean apply(RepairEntry repairEntry)
    {
        if (repairEntry.getStatus() == RepairStatus.SUCCESS)
        {
            Collection<Node> replicas = myTokenToNodeMap.get(repairEntry.getRange());
            if (replicas != null)
            {
                return repairEntry.getParticipants().containsAll(replicas);
            }
        }
        return false;
    }
}
