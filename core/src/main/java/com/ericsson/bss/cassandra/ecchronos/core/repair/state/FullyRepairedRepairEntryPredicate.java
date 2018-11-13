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
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.google.common.base.Predicate;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FullyRepairedRepairEntryPredicate implements Predicate<RepairEntry>
{
    private final Map<LongTokenRange, Collection<Host>> myTokenToHostMap;

    public FullyRepairedRepairEntryPredicate(Map<LongTokenRange, Collection<Host>> tokenToHostMap)
    {
        myTokenToHostMap = tokenToHostMap;
    }

    @Override
    public boolean apply(RepairEntry repairEntry)
    {
        if (repairEntry.getStatus() == RepairStatus.SUCCESS)
        {
            Collection<Host> allReplicasHosts = myTokenToHostMap.get(repairEntry.getRange());
            if (allReplicasHosts != null)
            {
                Set<InetAddress> allReplicasAddress = new HashSet<>();
                for (Host host : allReplicasHosts)
                {
                    allReplicasAddress.add(host.getAddress());
                }

                return repairEntry.getParticipants().containsAll(allReplicasAddress);
            }
        }
        return false;
    }
}
