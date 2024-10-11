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

import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A group of replicas and ranges that should be repaired together.
 */
public class ReplicaRepairGroup implements Iterable<LongTokenRange>
{
    private final ImmutableSet<DriverNode> myReplicas;
    private final ImmutableList<LongTokenRange> myVnodes;
    private final long myLastCompletedAt;

    /**
     * Constructor.
     *
     * @param replicas The nodes.
     * @param vnodes The token ranges.
     * @param lastCompletedAt last repair completed
     */
    public ReplicaRepairGroup(final ImmutableSet<DriverNode> replicas, final ImmutableList<LongTokenRange> vnodes,
            final long lastCompletedAt)
    {
        myReplicas = replicas;
        myVnodes = vnodes;
        myLastCompletedAt = lastCompletedAt;
    }

    /**
     * Get replicas.
     *
     * @return Replicas
     */
    public Set<DriverNode> getReplicas()
    {
        return myReplicas;
    }

    /**
     * Get datacenters.
     *
     * @return Datacenters
     */
    public Set<String> getDataCenters()
    {
        return myReplicas.stream().map(DriverNode::getDatacenter).collect(Collectors.toSet());
    }

    /**
     * Get last completed at.
     *
     * @return Last completed at for this repair group.
     */
    public long getLastCompletedAt()
    {
        return myLastCompletedAt;
    }

    /**
     * Iterate.
     *
     * @return Token range iterator
     */
    @Override
    public Iterator<LongTokenRange> iterator()
    {
        return myVnodes.iterator();
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("(replicas=%s,vnodes=%s)", myReplicas, myVnodes);
    }
}


