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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMeterFilterImpl
{
    private static final String FIRST_METRIC_NAME = "fooMetricName";
    private static final String SECOND_METRIC_NAME = "barMetricName";

    @Test
    public void testMatchesNullNotExcluded()
    {
        MeterFilter meterFilter = new MeterFilterImpl(null);

        Meter.Id firstMeterId = mockMeterId(FIRST_METRIC_NAME);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
    }

    @Test
    public void testMatchesEmptySetNotExcluded()
    {
        MeterFilter meterFilter = new MeterFilterImpl(new HashSet<>());
        Meter.Id firstMeterId = mockMeterId(FIRST_METRIC_NAME);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
    }

    @Test
    public void testMatchesExcludedExact()
    {
        Set<String> excluded = new HashSet<>();
        excluded.add(FIRST_METRIC_NAME);
        MeterFilter meterFilter = new MeterFilterImpl(excluded);

        Meter.Id firstMeterId = mockMeterId(FIRST_METRIC_NAME);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);
    }

    @Test
    public void testMatchesExcludedRegex()
    {
        Set<String> excluded = new HashSet<>();
        excluded.add("foo.*");
        MeterFilter meterFilter = new MeterFilterImpl(excluded);

        Meter.Id firstMeterId = mockMeterId(FIRST_METRIC_NAME);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);

        Meter.Id secondMeterId = mockMeterId(SECOND_METRIC_NAME);
        assertThat(meterFilter.accept(secondMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
    }

    @Test
    public void testMatchesMultipleMetricsExcludedWildcardRegex()
    {
        Set<String> excluded = new HashSet<>();
        excluded.add(".*");
        MeterFilter meterFilter = new MeterFilterImpl(excluded);

        Meter.Id firstMeterId = mockMeterId(FIRST_METRIC_NAME);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);

        Meter.Id secondMeterId = mockMeterId(SECOND_METRIC_NAME);
        assertThat(meterFilter.accept(secondMeterId)).isEqualTo(MeterFilterReply.DENY);
    }

    private Meter.Id mockMeterId(String name)
    {
        Meter.Id meterId = mock(Meter.Id.class);
        when(meterId.getName()).thenReturn(name);
        return meterId;
    }
}
