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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public final class MetricFilterImpl implements MetricFilter
{
    private static final Logger LOG = LoggerFactory.getLogger(MetricFilterImpl.class);
    private final Set<String> myExcludedMetrics;

    public MetricFilterImpl(final Set<String> excludedMetrics)
    {
        myExcludedMetrics = excludedMetrics;
    }

    /**
     * Checks if metric with provided name should be reported or not.
     *
     * @param name   The metric name
     * @param metric The metric
     * @return false if metric should be excluded, true if it should be included.
     */
    @Override
    public boolean matches(final String name, final Metric metric)
    {
        if (myExcludedMetrics == null)
        {
            return true;
        }
        if (myExcludedMetrics.contains(name))
        {
            LOG.trace("Metric with name '{}' matches excluded metrics, will filter this metric out.", name);
            return false;
        }
        for (String excludeMetricRegex : myExcludedMetrics)
        {
            if (name.matches(excludeMetricRegex))
            {
                LOG.trace("Metric with name '{}' matches excluded metric regex '{}', will filter this metric out.",
                        name, excludeMetricRegex);
                return false;
            }
        }
        return true;
    }
}
