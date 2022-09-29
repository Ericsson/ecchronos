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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class TestMetricFilterImpl
{
    private static final String FIRST_METRIC_NAME = "fooMetricName";
    private static final String SECOND_METRIC_NAME = "barMetricName";
    @Test
    public void testMatchesNullNotExcluded()
    {
        MetricFilter metricFilter = new MetricFilterImpl(null);
        assertThat(metricFilter.matches(FIRST_METRIC_NAME, mock(Metric.class))).isTrue();
    }

    @Test
    public void testMatchesEmptySetNotExcluded()
    {
        MetricFilter metricFilter = new MetricFilterImpl(new HashSet<>());
        assertThat(metricFilter.matches(FIRST_METRIC_NAME, mock(Metric.class))).isTrue();
    }

    @Test
    public void testMatchesExcludedExact()
    {
        Set<String> excluded = new HashSet<>();
        excluded.add(FIRST_METRIC_NAME);
        MetricFilter metricFilter = new MetricFilterImpl(excluded);
        assertThat(metricFilter.matches(FIRST_METRIC_NAME, mock(Metric.class))).isFalse();
    }

    @Test
    public void testMatchesExcludedRegex()
    {
        Set<String> excluded = new HashSet<>();
        excluded.add("foo.*");
        MetricFilter metricFilter = new MetricFilterImpl(excluded);
        assertThat(metricFilter.matches(FIRST_METRIC_NAME, mock(Metric.class))).isFalse();
        assertThat(metricFilter.matches(SECOND_METRIC_NAME, mock(Metric.class))).isTrue();
    }

    @Test
    public void testMatchesMultipleMetricsExcludedWildcardRegex()
    {
        Set<String> excluded = new HashSet<>();
        excluded.add(".*");
        MetricFilter metricFilter = new MetricFilterImpl(excluded);
        assertThat(metricFilter.matches(FIRST_METRIC_NAME, mock(Metric.class))).isFalse();
        assertThat(metricFilter.matches(SECOND_METRIC_NAME, mock(Metric.class))).isFalse();
    }
}
