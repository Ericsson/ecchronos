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

package com.ericsson.bss.cassandra.ecchronos.application;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.application.config.metrics.ExcludedMetric;

public class MeterFilterImpl implements MeterFilter
{
    private static final Logger LOG = LoggerFactory.getLogger(MeterFilterImpl.class);
    private final String myPrefix;
    private final Set<ExcludedMetric> myExcludedMetrics;

    public MeterFilterImpl(final String prefix, final Set<ExcludedMetric> excludedMetrics)
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
        String metricName = id.getName();
        if (myExcludedMetrics == null || metricName == null)
        {
            return MeterFilterReply.NEUTRAL;
        }
        metricName = removePrefixIfPresent(metricName);
        List<Tag> tags = id.getTags();
        for (ExcludedMetric excludedMetric : myExcludedMetrics)
        {
            if (shouldExclude(metricName, excludedMetric.getMetricName(), tags, excludedMetric.getMetricTags()))
            {
                return MeterFilterReply.DENY;
            }
        }
        return MeterFilterReply.NEUTRAL;
    }

    private String removePrefixIfPresent(final String metricName)
    {
        if (myPrefix != null && !myPrefix.isEmpty())
        {
            return metricName.replaceFirst(myPrefix + ".", "");
        }
        return metricName;
    }

    private boolean shouldExclude(final String metricName, final String excludeOnNameRegexp, final List<Tag> tags,
            final Map<String, String> excludeOnTags)
    {
        if (metricName.matches(excludeOnNameRegexp))
        {
            // If no tags, then exclude on name
            if (excludeOnTags == null || excludeOnTags.isEmpty())
            {
                LOG.trace("Excluding metric '{}' with tags '{}' it matches name regex '{}'",
                        metricName, tags, excludeOnNameRegexp);
                return true;
            }
            // If tags, then we must consider them together.
            // I.e, metric matching only name but not tags should not be excluded.
            if (shouldExcludeOnTags(tags, excludeOnTags))
            {
                LOG.trace("Excluding metric '{}' with tags '{}' it matches exclude [name regex '{}', tags '{}']",
                        metricName, tags, excludeOnNameRegexp, excludeOnTags);
                return true;
            }
        }
        return false;
    }

    private boolean shouldExcludeOnTags(final List<Tag> tags, final Map<String, String> excludedTags)
    {
        int matches = 0;
        for (Tag tag : tags)
        {
            for (Map.Entry<String, String> excludedTag : excludedTags.entrySet())
            {
                if (tag.getKey().equals(excludedTag.getKey()) && tag.getValue().matches(excludedTag.getValue()))
                {
                    matches++;
                }
            }
        }
        return matches >= excludedTags.size();
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
