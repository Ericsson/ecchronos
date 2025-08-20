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

import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A group of replicas and ranges that should be repaired together.
 */
public record ReplicaRepairGroup(Set<DriverNode> replicas, List<LongTokenRange> vnodes,
                                 long lastCompletedAt) implements Iterable<LongTokenRange>
{
    /**
     * Get datacenters.
     *
     * @return Datacenters
     */
    public Set<String> getDataCenters()
    {
        return replicas.stream().map(DriverNode::getDatacenter).collect(Collectors.toSet());
    }

    /**
     * Iterate.
     *
     * @return Token range iterator
     */
    @Override
    public Iterator<LongTokenRange> iterator()
    {
        return vnodes.iterator();
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("(replicas=%s,vnodes=%s)", replicas, vnodes);
    }
}
