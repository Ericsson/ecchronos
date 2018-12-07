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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A group of replicas and ranges that should be repaired together.
 */
public class ReplicaRepairGroup implements Iterable<LongTokenRange>
{
    private final ImmutableSet<Host> myReplicas;
    private final ImmutableList<LongTokenRange> myVnodes;
    private final ImmutableSet<String> myDataCenters;

    public ReplicaRepairGroup(Set<Host> replicas, List<LongTokenRange> vnodes)
    {
        myReplicas = ImmutableSet.copyOf(replicas);
        myVnodes = ImmutableList.copyOf(vnodes);
        myDataCenters = ImmutableSet.copyOf(myReplicas.stream().map(Host::getDatacenter).collect(Collectors.toSet()));
    }

    public Set<Host> getReplicas()
    {
        return myReplicas;
    }

    public Set<String> getDataCenters()
    {
        return myDataCenters;
    }

    public List<LongTokenRange> getVnodes()
    {
        return myVnodes;
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaRepairGroup that = (ReplicaRepairGroup) o;
        return Objects.equals(myReplicas, that.myReplicas) &&
                Objects.equals(myVnodes, that.myVnodes) &&
                Objects.equals(myDataCenters, that.myDataCenters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myReplicas, myVnodes, myDataCenters);
    }
}
