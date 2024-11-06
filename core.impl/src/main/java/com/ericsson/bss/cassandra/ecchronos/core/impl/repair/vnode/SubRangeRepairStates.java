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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode;

import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStates;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class SubRangeRepairStates implements VnodeRepairStates // CPD-OFF
{
    private final ImmutableList<VnodeRepairState> myVnodeRepairStatuses;

    private SubRangeRepairStates(final SubRangeRepairStates.Builder builder)
    {
        List<VnodeRepairState> baseVnodes = builder.myVnodeRepairStatesBase;
        Collection<VnodeRepairState> partialVnodes = builder.myActualVnodeRepairStates.values();

        List<VnodeRepairState> summarizedVnodes = VnodeRepairStateSummarizer.summarizePartialVnodes(baseVnodes,
                partialVnodes);

        myVnodeRepairStatuses = ImmutableList.copyOf(summarizedVnodes);
    }

    @Override
    public Collection<VnodeRepairState> getVnodeRepairStates()
    {
        return myVnodeRepairStatuses;
    }

    @Override
    public SubRangeRepairStates combineWithRepairedAt(final long repairedAt)
    {
        Builder builder = newBuilder(getVnodeRepairStates());

        for (VnodeRepairState vnodeRepairState : getVnodeRepairStates())
        {
            VnodeRepairState vnodeRepairStateWithRepairedAt = new VnodeRepairState(vnodeRepairState.getTokenRange(),
                    vnodeRepairState.getReplicas(), repairedAt);
            builder.updateVnodeRepairState(vnodeRepairStateWithRepairedAt);
        }

        return builder.build();
    }

    @Override
    public String toString()
    {
        return myVnodeRepairStatuses.toString();
    }

    public static Builder newBuilder(final Collection<VnodeRepairState> vnodeRepairStates)
    {
        return new Builder(vnodeRepairStates);
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        SubRangeRepairStates that = (SubRangeRepairStates) o;
        return Objects.equals(myVnodeRepairStatuses, that.myVnodeRepairStatuses);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myVnodeRepairStatuses);
    }

    public static class Builder implements VnodeRepairStates.Builder
    {
        private final ImmutableList<VnodeRepairState> myVnodeRepairStatesBase;
        private final Map<LongTokenRange, VnodeRepairState> myActualVnodeRepairStates = new HashMap<>();

        public Builder(final Collection<VnodeRepairState> vnodeRepairStates)
        {
            ImmutableList.Builder<VnodeRepairState> builder = ImmutableList.builder();
            for (VnodeRepairState vnodeRepairState : vnodeRepairStates)
            {
                builder.add(vnodeRepairState);
            }
            myVnodeRepairStatesBase = builder.build();
        }

        /**
         * Update vNode repair state.
         *
         * @param vnodeRepairState The vnode repair status to update.
         * @return VnodeRepairStates.Builder
         */
        @Override
        public VnodeRepairStates.Builder updateVnodeRepairState(final VnodeRepairState vnodeRepairState)
        {
            for (VnodeRepairState baseVnode : myVnodeRepairStatesBase)
            {
                if (baseVnode.getTokenRange().isCovering(vnodeRepairState.getTokenRange()))
                {
                    replaceIfNewer(baseVnode, vnodeRepairState);
                    break;
                }
            }

            return this;
        }

        /**
         * Build subrange repair states.
         *
         * @return SubRangeRepairStates
         */
        @Override
        public SubRangeRepairStates build()
        {
            return new SubRangeRepairStates(this);
        }

        private void replaceIfNewer(final VnodeRepairState baseVnode, final VnodeRepairState newVnode)
        {
            if (baseVnode.getTokenRange().equals(newVnode.getTokenRange())) // Original vnode
            {
                if (shouldReplace(baseVnode, newVnode))
                {
                    myActualVnodeRepairStates.put(newVnode.getTokenRange(), newVnode);
                }
            }
            else if (partialVnodeIsNewer(baseVnode, newVnode)) // Partial vnode
            {
                myActualVnodeRepairStates.put(newVnode.getTokenRange(), newVnode);
            }
        }

        private boolean shouldReplace(final VnodeRepairState baseVnode, final VnodeRepairState newVnode)
        {
            if (!baseVnode.isSameVnode(newVnode))
            {
                return false;
            }

            return isNewer(baseVnode, newVnode);
        }

        private boolean partialVnodeIsNewer(final VnodeRepairState baseVnode, final VnodeRepairState newVnode)
        {
            if (!baseVnode.getReplicas().equals(newVnode.getReplicas()))
            {
                return false;
            }

            return isNewer(baseVnode, newVnode);
        }

        private boolean isNewer(final VnodeRepairState baseVnode, final VnodeRepairState newVnode)
        {
            VnodeRepairState oldVnode = myActualVnodeRepairStates.getOrDefault(baseVnode.getTokenRange(), baseVnode);

            return oldVnode.lastRepairedAt() < newVnode.lastRepairedAt()
                    || oldVnode.getFinishedAt() < newVnode.getFinishedAt();
        }
    }
}

