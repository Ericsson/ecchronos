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

import java.util.Collection;

/**
 * A collection of {@link VnodeRepairState VnodeRepairStates} that contains utilities to easily combine
 * with new entries.
 */
public interface VnodeRepairStates
{

    Collection<VnodeRepairState> getVnodeRepairStates();

    /**
     * Create a new vnode repair states object with the minimum repaired at set to the provided value.
     *
     * Entries which contain a higher repaired at will keep that value.
     *
     * @param repairedAt The minimum repaired at to use.
     * @return The created state.
     */
    VnodeRepairStates combineWithRepairedAt(long repairedAt);

    interface Builder
    {
        /**
         * Combine a collection of vnode repair states into this collection.
         *
         * @param vnodeRepairStates The vnode repair statuses to update.
         * @return This builder
         * @see #updateVnodeRepairState(VnodeRepairState)
         */
        default Builder updateVnodeRepairStates(Collection<VnodeRepairState> vnodeRepairStates)
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
        Builder updateVnodeRepairState(VnodeRepairState vnodeRepairState);

        VnodeRepairStates build();
    }
}

