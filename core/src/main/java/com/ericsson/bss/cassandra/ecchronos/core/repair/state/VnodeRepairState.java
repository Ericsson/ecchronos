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
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

public class VnodeRepairState
{
    public static final long UNREPAIRED = -1L;

    private final LongTokenRange myTokenRange;
    private final ImmutableSet<Host> myReplicas;
    private final long myLastRepairedAt;

    public VnodeRepairState(LongTokenRange tokenRange, Collection<Host> replicas, long lastRepairedAt)
    {
        myTokenRange = tokenRange;
        myReplicas = ImmutableSet.copyOf(replicas);
        myLastRepairedAt = lastRepairedAt;
    }

    public LongTokenRange getTokenRange()
    {
        return myTokenRange;
    }

    public Set<Host> getReplicas()
    {
        return myReplicas;
    }

    public long lastRepairedAt()
    {
        return myLastRepairedAt;
    }

    public boolean isSameVnode(VnodeRepairState other)
    {
        return getTokenRange().equals(other.getTokenRange()) && getReplicas().equals(other.getReplicas());
    }

    @Override
    public String toString()
    {
        return String.format("(tokenRange=%s,replicas=%s,repairedAt=%d)", myTokenRange, myReplicas, myLastRepairedAt);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VnodeRepairState that = (VnodeRepairState) o;
        return myLastRepairedAt == that.myLastRepairedAt &&
                Objects.equals(myTokenRange, that.myTokenRange) &&
                Objects.equals(myReplicas, that.myReplicas);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myTokenRange, myReplicas, myLastRepairedAt);
    }
}
