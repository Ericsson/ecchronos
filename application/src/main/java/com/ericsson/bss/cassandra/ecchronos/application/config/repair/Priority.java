/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config.repair;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Configuration for repair job priority granularity. */
public class Priority
{

    private static final Set<TimeUnit> ALLOWED_UNITS = EnumSet.of(TimeUnit.HOURS, TimeUnit.MINUTES, TimeUnit.SECONDS);
    private static final String ALLOWED_VALUES_STR = String.join(", ", ALLOWED_UNITS.stream()
            .map(TimeUnit::name)
            .toList());

    private TimeUnit myGranularityUnit = TimeUnit.HOURS;

    /** Constructs a new Priority. */
    public Priority()
    {
        // Default constructor for jackson
    }

    /**
     * Constructs a new Priority.
     * @param granularityUnit the granularity unit
     */
    @JsonCreator
    public Priority(@JsonProperty("granularity_unit") final TimeUnit granularityUnit)
    {
        myGranularityUnit = granularityUnit;
    }

    /**
     * Returns the priority granularity unit.
     * @return the priority granularity unit
     */
    public final TimeUnit getPriorityGranularityUnit()
    {
        return myGranularityUnit;
    }

    /**
     * Sets the priority granularity unit.
     * @param granularityUnit the granularity unit
     */
    @JsonProperty ("granularity_unit")
    public final void setPriorityGranularityUnit(final TimeUnit granularityUnit)
    {
        Optional.ofNullable(granularityUnit)
                .orElseThrow(() -> new IllegalArgumentException(String.format(
                        "Granularity unit cannot be null. Allowed values are: %s.", ALLOWED_VALUES_STR)));

        if (!ALLOWED_UNITS.contains(granularityUnit))
        {
            throw new IllegalArgumentException(String.format(
                    "Invalid granularity unit '%s'. Allowed values are: %s.",
                    granularityUnit.name(), ALLOWED_VALUES_STR));
        }

        myGranularityUnit = granularityUnit;
    }
}
