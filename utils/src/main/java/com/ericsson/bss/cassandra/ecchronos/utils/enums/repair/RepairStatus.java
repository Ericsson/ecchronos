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
package com.ericsson.bss.cassandra.ecchronos.utils.enums.repair;

public enum RepairStatus
{
    STARTED,
    SUCCESS,
    FAILED,
    UNKNOWN;

    /**
     * Get RepairStatus from value.
     *
     * @param status The status value.
     * @return RepairStatus
     */
    public static RepairStatus getFromStatus(final String status)
    {
        RepairStatus repairStatus;

        try
        {
            repairStatus = RepairStatus.valueOf(status);
        }
        catch (IllegalArgumentException e)
        {
            repairStatus = RepairStatus.UNKNOWN;
        }

        return repairStatus;
    }
}
