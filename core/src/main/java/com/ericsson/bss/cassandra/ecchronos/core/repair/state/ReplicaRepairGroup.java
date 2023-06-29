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
    private final ImmutableSet<Host> myReplicas;
    private final ImmutableList<LongTokenRange> myVnodes;
    private final long myLastCompletedAt;

    public ReplicaRepairGroup(ImmutableSet<Host> replicas, ImmutableList<LongTokenRange> vnodes,
            final long lastCompletedAt)
    {
        myReplicas = replicas;
        myVnodes = vnodes;
        myLastCompletedAt = lastCompletedAt;
    }

    public Set<Host> getReplicas()
    {
        return myReplicas;
    }

    public Set<String> getDataCenters()
    {
        return myReplicas.stream().map(Host::getDatacenter).collect(Collectors.toSet());
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

    @Override
    public Iterator<LongTokenRange> iterator()
    {
        return myVnodes.iterator();
    }

    @Override
    public String toString()
    {
        return String.format("(replicas=%s,vnodes=%s)", myReplicas, myVnodes);
    }
}
