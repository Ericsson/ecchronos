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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class VnodeRepairStatesImpl implements VnodeRepairStates // CPD-OFF
{
    private final ImmutableList<VnodeRepairState> myVnodeRepairStatuses;

    private VnodeRepairStatesImpl(final Builder builder)
    {
        myVnodeRepairStatuses = ImmutableList.copyOf(builder.myVnodeRepairStates.values());
    }

    @Override
    public Collection<VnodeRepairState> getVnodeRepairStates()
    {
        return myVnodeRepairStatuses;
    }

    @Override
    public VnodeRepairStatesImpl combineWithRepairedAt(final long repairedAt)
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
        VnodeRepairStatesImpl that = (VnodeRepairStatesImpl) o;
        return Objects.equals(myVnodeRepairStatuses, that.myVnodeRepairStatuses);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myVnodeRepairStatuses);
    }

    public static class Builder implements VnodeRepairStates.Builder
    {
        private final Map<LongTokenRange, VnodeRepairState> myVnodeRepairStates = new LinkedHashMap<>();

        public Builder(final Collection<VnodeRepairState> vnodeRepairStates)
        {
            for (VnodeRepairState vnodeRepairState : vnodeRepairStates)
            {
                myVnodeRepairStates.put(vnodeRepairState.getTokenRange(), vnodeRepairState);
            }
        }

        /**
         * Update vNode repair states.
         *
         * @return Builder
         */
        @Override
        public Builder updateVnodeRepairStates(final Collection<VnodeRepairState> vnodeRepairStates)
        {
            for (VnodeRepairState vnodeRepairState : vnodeRepairStates)
            {
                updateVnodeRepairState(vnodeRepairState);
            }
            return this;
        }

        /**
         * Update vNode repair state.
         *
         * @return Builder
         */
        @Override
        public Builder updateVnodeRepairState(final VnodeRepairState vnodeRepairState)
        {
            VnodeRepairState oldVnode = myVnodeRepairStates.get(vnodeRepairState.getTokenRange());
            if (shouldReplace(oldVnode, vnodeRepairState))
            {
                myVnodeRepairStates.put(vnodeRepairState.getTokenRange(), vnodeRepairState);
            }
            return this;
        }

        /**
         * Build vNode repair state.
         *
         * @return VnodeRepairStatesImpl
         */
        @Override
        public VnodeRepairStatesImpl build()
        {
            return new VnodeRepairStatesImpl(this);
        }

        private boolean shouldReplace(final VnodeRepairState oldVnode, final VnodeRepairState newVnode)
        {
            if (oldVnode == null)
            {
                return false;
            }

            if (!oldVnode.isSameVnode(newVnode))
            {
                return false;
            }

            return oldVnode.lastRepairedAt() < newVnode.lastRepairedAt()
                    || oldVnode.getFinishedAt() < newVnode.getFinishedAt();
        }
    }
}

