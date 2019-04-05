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
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A collection of {@link VnodeRepairState VnodeRepairStates} that contains utilities to easily combine with new entries.
 */
public class VnodeRepairStates
{
    private final ImmutableList<VnodeRepairState> myVnodeRepairStatuses;

    private VnodeRepairStates(Builder builder)
    {
        myVnodeRepairStatuses = ImmutableList.copyOf(builder.myVnodeRepairStates.values());
    }

    public Collection<VnodeRepairState> getVnodeRepairStates()
    {
        return myVnodeRepairStatuses;
    }

    /**
     * Create a new vnode repair states object with the minimum repaired at set to the provided value.
     *
     * Entries which contain a higher repaired at will keep that value.
     *
     * @param repairedAt The minimum repaired at to use.
     * @return The created state.
     */
    public VnodeRepairStates combineWithRepairedAt(long repairedAt)
    {
        Builder builder = newBuilder(getVnodeRepairStates());

        for (VnodeRepairState vnodeRepairState : getVnodeRepairStates())
        {
            VnodeRepairState vnodeRepairStateWithRepairedAt = new VnodeRepairState(vnodeRepairState.getTokenRange(), vnodeRepairState.getReplicas(), repairedAt);
            builder.updateVnodeRepairState(vnodeRepairStateWithRepairedAt);
        }

        return builder.build();
    }

    @Override
    public String toString()
    {
        return myVnodeRepairStatuses.toString();
    }

    public static Builder newBuilder(Collection<VnodeRepairState> vnodeRepairStates)
    {
        return new Builder(vnodeRepairStates);
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
        private final Map<LongTokenRange, VnodeRepairState> myVnodeRepairStates = new HashMap<>();

        public Builder(Collection<VnodeRepairState> vnodeRepairStates)
        {
            for (VnodeRepairState vnodeRepairState : vnodeRepairStates)
            {
                myVnodeRepairStates.put(vnodeRepairState.getTokenRange(), vnodeRepairState);
            }
        }

        /**
         * Combine a collection of vnode repair states into this collection.
         *
         * @param vnodeRepairStates The vnode repair statuses to update.
         * @return This builder
         * @see #updateVnodeRepairState(VnodeRepairState)
         */
        public Builder updateVnodeRepairStates(Collection<VnodeRepairState> vnodeRepairStates)
        {
            for (VnodeRepairState vnodeRepairState : vnodeRepairStates)
            {
                updateVnodeRepairState(vnodeRepairState);
            }
            return this;
        }

        /**
         * Combine the provided {@link VnodeRepairState} with the current representation.
         * If there already was a higher timestamp recorded for the vnode, no change will be made.
         *
         * An entry will be replaced if it has a higher timestamp.
         * No new entries will be added.
         *
         * @param vnodeRepairState The vnode repair status to update.
         * @return This builder
         */
        public Builder updateVnodeRepairState(VnodeRepairState vnodeRepairState)
        {
            VnodeRepairState oldVnode = myVnodeRepairStates.get(vnodeRepairState.getTokenRange());
            if (shouldReplace(oldVnode, vnodeRepairState))
            {
                myVnodeRepairStates.put(vnodeRepairState.getTokenRange(), vnodeRepairState);
            }
            return this;
        }

        private boolean shouldReplace(VnodeRepairState oldVnode, VnodeRepairState newVnode)
        {
            if (oldVnode == null)
            {
                return false;
            }

            if (!oldVnode.isSameVnode(newVnode))
            {
                return false;
            }

            return oldVnode.lastRepairedAt() < newVnode.lastRepairedAt();
        }

        public VnodeRepairStates build()
        {
            return new VnodeRepairStates(this);
        }
    }
}
