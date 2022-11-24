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

package com.ericsson.bss.cassandra.ecchronos.application;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class MeterFilterImpl implements MeterFilter
{
    private static final Logger LOG = LoggerFactory.getLogger(MeterFilterImpl.class);
    private final String myPrefix;
    private final Set<String> myExcludedMetrics;

    public MeterFilterImpl(final String prefix, final Set<String> excludedMetrics)
    {
        myPrefix = prefix;
        myExcludedMetrics = excludedMetrics;
    }

    /**
     * Checks if metric with provided name should be reported or not.
     *
     * @param id The meter id
     * @return MeterFilterReply.DENY if metric should be excluded, MeterFilterReply.NEUTRAL if it should be included.
     */
    @Override
    public MeterFilterReply accept(final Meter.Id id)
    {
        String name = id.getName();
        if (myExcludedMetrics == null || name == null)
        {
            return MeterFilterReply.NEUTRAL;
        }
        if (myExcludedMetrics.contains(id.getName()))
        {
            LOG.trace("Metric with name '{}' matches excluded metrics, will filter this metric out.", name);
            return MeterFilterReply.DENY;
        }
        for (String excludeMetricRegex : myExcludedMetrics)
        {
            if (name.matches(excludeMetricRegex))
            {
                LOG.trace("Metric with name '{}' matches excluded metric regex '{}', will filter this metric out.",
                        name, excludeMetricRegex);
                return MeterFilterReply.DENY;
            }
        }
        return MeterFilterReply.NEUTRAL;
    }

    /**
     * Applies a prefix to a metric if prefix is configured and not already present in id name.
     * @param id The meter id
     * @return The meter id with prefix applied
     */
    @Override
    public Meter.Id map(final Meter.Id id)
    {
        if (myPrefix == null || myPrefix.isEmpty() || id.getName().startsWith(myPrefix))
        {
            return id;
        }
        return id.withName(myPrefix + "." + id.getName());
    }
}
