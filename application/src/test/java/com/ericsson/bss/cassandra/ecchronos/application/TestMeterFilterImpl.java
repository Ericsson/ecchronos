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
import io.micrometer.core.instrument.Tags;
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
    private static final String PREFIX = "ecc";
    private static final String FIRST_METRIC_NAME = "fooMetricName";
    private static final String SECOND_METRIC_NAME = "barMetricName";
    private static final Tags FIRST_METRIC_TAGS = Tags.of("fooTag", "fooTagValue");
    private static final Tags SECOND_METRIC_TAGS = Tags.of("barTag", "barTagValue");

    @Test
    public void testMatchesNullNotExcluded()
    {
        MeterFilter meterFilter = new MeterFilterImpl(null, null);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
    }

    @Test
    public void testMatchesEmptySetNotExcluded()
    {
        MeterFilter meterFilter = new MeterFilterImpl(null, new HashSet<>());
        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
    }

    @Test
    public void testMatchesExcludedExact()
    {
        Set<String> excluded = new HashSet<>();
        excluded.add(FIRST_METRIC_NAME);
        MeterFilter meterFilter = new MeterFilterImpl(null, excluded);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);
    }

    @Test
    public void testMatchesExcludedRegex()
    {
        Set<String> excluded = new HashSet<>();
        excluded.add("foo.*");
        MeterFilter meterFilter = new MeterFilterImpl(null, excluded);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);

        Meter.Id secondMeterId = createMeterId(SECOND_METRIC_NAME, SECOND_METRIC_TAGS);
        assertThat(meterFilter.accept(secondMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
    }

    @Test
    public void testMatchesMultipleMetricsExcludedWildcardRegex()
    {
        Set<String> excluded = new HashSet<>();
        excluded.add(".*");
        MeterFilter meterFilter = new MeterFilterImpl(null, excluded);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);

        Meter.Id secondMeterId = createMeterId(SECOND_METRIC_NAME, SECOND_METRIC_TAGS);
        assertThat(meterFilter.accept(secondMeterId)).isEqualTo(MeterFilterReply.DENY);
    }

    @Test
    public void testMapPrefixNull()
    {
        MeterFilter meterFilter = new MeterFilterImpl(null, null);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        Meter.Id mappedMeterId = meterFilter.map(firstMeterId);
        assertThat(mappedMeterId.getName()).isEqualTo(FIRST_METRIC_NAME);
    }

    @Test
    public void testMapPrefixEmpty()
    {
        MeterFilter meterFilter = new MeterFilterImpl("", null);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        Meter.Id mappedMeterId = meterFilter.map(firstMeterId);
        assertThat(mappedMeterId.getName()).isEqualTo(FIRST_METRIC_NAME);
    }

    @Test
    public void testMapPrefix()
    {
        MeterFilter meterFilter = new MeterFilterImpl(PREFIX, null);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        Meter.Id secondMeterId = createMeterId(SECOND_METRIC_NAME, SECOND_METRIC_TAGS);

        Meter.Id firstMappedMeterId = meterFilter.map(firstMeterId);
        assertThat(firstMappedMeterId.getName()).isEqualTo(PREFIX + "." + FIRST_METRIC_NAME);
        assertThat(firstMappedMeterId.getTags()).isEqualTo(firstMappedMeterId.getTags());

        Meter.Id secondMappedMeterId = meterFilter.map(secondMeterId);
        assertThat(secondMappedMeterId.getName()).isEqualTo(PREFIX + "." + SECOND_METRIC_NAME);
        assertThat(secondMappedMeterId.getTags()).isEqualTo(secondMeterId.getTags());
    }

    @Test
    public void testMapPrefixAlreadyPrefixed()
    {
        MeterFilter meterFilter = new MeterFilterImpl(PREFIX, null);

        Meter.Id firstMeterId = createMeterId(PREFIX + "." + FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        Meter.Id secondMeterId = createMeterId(SECOND_METRIC_NAME, SECOND_METRIC_TAGS);

        Meter.Id firstMappedMeterId = meterFilter.map(firstMeterId);
        assertThat(firstMappedMeterId.getName()).isEqualTo(PREFIX + "." + FIRST_METRIC_NAME);
        assertThat(firstMappedMeterId.getTags()).isEqualTo(firstMappedMeterId.getTags());

        Meter.Id secondMappedMeterId = meterFilter.map(secondMeterId);
        assertThat(secondMappedMeterId.getName()).isEqualTo(PREFIX + "." + SECOND_METRIC_NAME);
        assertThat(secondMappedMeterId.getTags()).isEqualTo(secondMeterId.getTags());
    }

    private Meter.Id createMeterId(String name, Tags tags)
    {
        return new Meter.Id(name, tags, null, null, Meter.Type.GAUGE);
    }
}
