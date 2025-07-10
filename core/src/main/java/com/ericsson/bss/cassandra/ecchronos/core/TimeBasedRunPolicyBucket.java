/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core;

import java.util.Set;

public class TimeBasedRunPolicyBucket
{
    private final String keyspaceName;
    private final String tableName;
    private final Integer startHour;
    private final Integer startMinute;
    private final Integer endHour;
    private final Integer endMinute;
    private final Set<String> dcExclusions;

    public TimeBasedRunPolicyBucket(
            final String myKeyspaceName,
            final String myTableName,
            final Integer myStartHour,
            final Integer myStartMinute,
            final Integer myEndHour,
            final Integer myEndMinute,
            final Set<String> myDcExclusions)
    {
        this.keyspaceName = myKeyspaceName;
        this.tableName = myTableName;
        this.startHour = myStartHour;
        this.startMinute = myStartMinute;
        this.endHour = myEndHour;
        this.endMinute = myEndMinute;
        this.dcExclusions = myDcExclusions;
    }

    public final String getKeyspaceName()
    {
        return keyspaceName;
    }

    public final String getTableName()
    {
        return tableName;
    }

    public final Integer getStartHour()
    {
        return startHour;
    }

    public final Integer getStartMinute()
    {
        return startMinute;
    }

    public final Integer getEndHour()
    {
        return endHour;
    }

    public final Integer getEndMinute()
    {
        return endMinute;
    }

    public final Set<String> getDcExclusions()
    {
        return dcExclusions;
    }
}
