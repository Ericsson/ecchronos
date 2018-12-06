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

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * A factory to create {@link VnodeRepairStates} for a specific table.
 */
public interface VnodeRepairStateFactory
{
    /**
     * Calculate the current repair state based on the previous.
     *
     * If the previous repair state is unknown it should be calculated from start.
     *
     * @param tableReference The table to calculate the new repair state for vnodes.
     * @param previous The previous repair state or null if non exists.
     * @return The calculated repair state.
     */
    VnodeRepairStates calculateNewState(TableReference tableReference, RepairStateSnapshot previous);
}
