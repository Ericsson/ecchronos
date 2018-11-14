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
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class VnodeRepairStates
{
    private final ImmutableMap<LongTokenRange, VnodeRepairState> myVnodeRepairStatuses;

    private VnodeRepairStates(Builder builder)
    {
        myVnodeRepairStatuses = ImmutableMap.copyOf(builder.myVnodeRepairStatuses);
    }

    public Collection<VnodeRepairState> getVnodeRepairStates()
    {
        return myVnodeRepairStatuses.values();
    }

    public VnodeRepairStates combineWithRepairedAt(long repairedAt)
    {
        Builder builder = newBuilder().combineVnodeRepairStates(getVnodeRepairStates());

        for (VnodeRepairState vnodeRepairState : getVnodeRepairStates())
        {
            VnodeRepairState vnodeRepairStateWithRepairedAt = new VnodeRepairState(vnodeRepairState.getTokenRange(), vnodeRepairState.getReplicas(), repairedAt);
            builder.combineVnodeRepairState(vnodeRepairStateWithRepairedAt);
        }

        return builder.build();
    }

    @Override
    public String toString()
    {
        return myVnodeRepairStatuses.toString();
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VnodeRepairStates that = (VnodeRepairStates) o;
        return Objects.equals(myVnodeRepairStatuses, that.myVnodeRepairStatuses);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myVnodeRepairStatuses);
    }

    public static class Builder
    {
        private final Map<LongTokenRange, VnodeRepairState> myVnodeRepairStatuses = new HashMap<>();

        /**
         *
         * @param vnodeRepairStates The vnode repair statuses to add.
         * @return This builder
         * @see #combineVnodeRepairState(VnodeRepairState)
         */
        public Builder combineVnodeRepairStates(Collection<VnodeRepairState> vnodeRepairStates)
        {
            for (VnodeRepairState vnodeRepairState : vnodeRepairStates)
            {
                combineVnodeRepairState(vnodeRepairState);
            }
            return this;
        }

        /**
         * Combine the provided {@link VnodeRepairState} with the current representation.
         * If there already was a higher timestamp recorded for the vnode, no change will be made.
         *
         * @param vnodeRepairState The vnode repair status to add.
         * @return This builder
         */
        public Builder combineVnodeRepairState(VnodeRepairState vnodeRepairState)
        {
            VnodeRepairState oldVnode = myVnodeRepairStatuses.get(vnodeRepairState.getTokenRange());
            if (shouldReplace(oldVnode, vnodeRepairState))
            {
                myVnodeRepairStatuses.put(vnodeRepairState.getTokenRange(), vnodeRepairState);
            }
            return this;
        }

        private boolean shouldReplace(VnodeRepairState oldVnode, VnodeRepairState newVnode)
        {
            if (oldVnode == null)
            {
                return true;
            }

            if (!oldVnode.isSameVnode(newVnode))
            {
                return true;
            }

            return oldVnode.lastRepairedAt() < newVnode.lastRepairedAt();
        }

        public VnodeRepairStates build()
        {
            return new VnodeRepairStates(this);
        }
    }
}
