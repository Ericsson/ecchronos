/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;

import java.util.Set;

/**
 * Utility class to determine if repair configurations have changed compared to currently scheduled jobs.
 */
public final class RepairConfigurationComparator
{
    private RepairConfigurationComparator()
    {
        // Utility class
    }

    /**
     * Determine if the given repair configurations differ from the currently scheduled jobs.
     *
     * @param currentJobs The currently scheduled jobs (may be null or empty).
     * @param newConfigurations The new repair configurations.
     * @return true if the configurations have changed and jobs need to be recreated.
     */
    public static boolean hasChanged(
            final Set<ScheduledRepairJob> currentJobs,
            final Set<RepairConfiguration> newConfigurations)
    {
        if (newConfigurations == null || newConfigurations.isEmpty())
        {
            return false;
        }
        if (currentJobs == null || currentJobs.isEmpty())
        {
            return true;
        }
        int matching = 0;
        for (ScheduledRepairJob job : currentJobs)
        {
            for (RepairConfiguration repairConfiguration : newConfigurations)
            {
                if (job.getRepairConfiguration().equals(repairConfiguration))
                {
                    matching++;
                }
            }
        }
        return matching != newConfigurations.size();
    }
}
