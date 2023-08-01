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
package com.ericsson.bss.cassandra.ecchronos.application.config.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ExcludedMetric
{
    private String myMetricName;
    private Map<String, String> myMetricTags = new HashMap<>();

    @JsonProperty("name")
    public final String getMetricName()
    {
        return myMetricName;
    }

    @JsonProperty("name")
    public final void setMetricName(final String name)
    {
        myMetricName = name;
    }

    @JsonProperty("tags")
    public final Map<String, String> getMetricTags()
    {
        return myMetricTags;
    }

    @JsonProperty("tags")
    public final void setMetricTags(final Map<String, String> tags)
    {
        myMetricTags = tags;
    }

    @Override
    public final boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        ExcludedMetric that = (ExcludedMetric) o;
        return Objects.equals(myMetricName, that.myMetricName) && Objects.equals(myMetricTags, that.myMetricTags);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(myMetricName, myMetricTags);
    }
}
