/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

package com.ericsson.bss.cassandra.ecchronos.core.repair.types;

import java.util.List;
import java.util.Objects;

public record RepairInfo(long sinceInMs, long toInMs, List<RepairStats> repairStats)
{
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
        RepairInfo that = (RepairInfo) o;
        return sinceInMs == that.sinceInMs
                && toInMs == that.toInMs
                && repairStats.size() == that.repairStats.size()
                && repairStats.containsAll(that.repairStats);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sinceInMs, toInMs, repairStats);
    }
}
