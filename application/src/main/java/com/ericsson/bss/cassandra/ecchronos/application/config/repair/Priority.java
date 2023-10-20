package com.ericsson.bss.cassandra.ecchronos.application.config.repair;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.TimeUnit;

public class Priority
{
    private static final String DEFAULT_PRIORITY_GRANULARITY_UNIT = "HOURS";
    private TimeUnit myGranularityUnit = TimeUnit.HOURS;

    public Priority()
    {
        // Default constructor for jackson
    }

    public Priority(final TimeUnit granularityUnit)
    {
        myGranularityUnit = granularityUnit;
    }

    public final TimeUnit getPriorityGranularityUnit()
    {
        return myGranularityUnit;
    }

    @JsonProperty ("granularity_unit")
    public final void setPriorityGranularityUnit(final TimeUnit granularityUnit)
    {
        myGranularityUnit = granularityUnit;
    }
}
