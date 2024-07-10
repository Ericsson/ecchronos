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

import com.ericsson.bss.cassandra.ecchronos.application.config.metrics.ExcludedMetric;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TestMeterFilterImpl
{
    private static final String PREFIX = "ecc";
    private static final String FIRST_METRIC_NAME = "fooMetricName";
    private static final String SECOND_METRIC_NAME = "barMetricName";
    private static final String FIRST_METRIC_NAME_WITH_PREFIX = PREFIX + ".fooMetricName";
    private static final String SECOND_METRIC_NAME_WITH_PREFIX = PREFIX + ".barMetricName";
    private static final String COMMON_TAG_KEY = "commonTag";
    private static final String COMMON_TAG_VALUE = "commonTagValue";
    private static final String TAG_KEY = "keyspace";
    private static final String FIRST_METRIC_TAG_VALUE = "fooKeyspace";
    private static final String SECOND_METRIC_TAG_VALUE = "barKeyspace";
    private static final Tags FIRST_METRIC_TAGS = Tags.of(TAG_KEY, FIRST_METRIC_TAG_VALUE,
            COMMON_TAG_KEY, COMMON_TAG_VALUE);
    private static final Tags SECOND_METRIC_TAGS = Tags.of(TAG_KEY, SECOND_METRIC_TAG_VALUE,
            COMMON_TAG_KEY, COMMON_TAG_VALUE);

    @Test
    public void testAcceptNullExcluded()
    {
        MeterFilter meterFilter = new MeterFilterImpl(null, null);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
    }

    @Test
    public void testAcceptEmptySet()
    {
        MeterFilter meterFilter = new MeterFilterImpl(null, new HashSet<>());
        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
    }

    @Test
    public void testAcceptExcludedExactNameNullTags()
    {
        Set<ExcludedMetric> excluded = new HashSet<>();
        excluded.add(createExcludedMetric(FIRST_METRIC_NAME, null));
        MeterFilter meterFilter = new MeterFilterImpl(null, excluded);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);
    }

    @Test
    public void testAcceptExcludedExactNameNullTagsPrefixed()
    {
        Set<ExcludedMetric> excluded = new HashSet<>();
        excluded.add(createExcludedMetric(FIRST_METRIC_NAME, null));
        MeterFilter meterFilter = new MeterFilterImpl(PREFIX, excluded);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME_WITH_PREFIX, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);
    }

    @Test
    public void testAcceptExcludedRegexNameNullTags()
    {
        Set<ExcludedMetric> excluded = new HashSet<>();
        excluded.add(createExcludedMetric("foo.*", null));
        MeterFilter meterFilter = new MeterFilterImpl(null, excluded);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);

        Meter.Id secondMeterId = createMeterId(SECOND_METRIC_NAME, SECOND_METRIC_TAGS);
        assertThat(meterFilter.accept(secondMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
    }

    @Test
    public void testAcceptExcludedRegexNameNullTagsPrefixed()
    {
        Set<ExcludedMetric> excluded = new HashSet<>();
        excluded.add(createExcludedMetric("foo.*", null));
        MeterFilter meterFilter = new MeterFilterImpl(PREFIX, excluded);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME_WITH_PREFIX, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);

        Meter.Id secondMeterId = createMeterId(SECOND_METRIC_NAME_WITH_PREFIX, SECOND_METRIC_TAGS);
        assertThat(meterFilter.accept(secondMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
    }

    @Test
    public void testAcceptMultipleExcludedWildcardRegexNameNullTags()
    {
        Set<ExcludedMetric> excluded = new HashSet<>();
        excluded.add(createExcludedMetric(".*", null));
        MeterFilter meterFilter = new MeterFilterImpl(null, excluded);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);

        Meter.Id secondMeterId = createMeterId(SECOND_METRIC_NAME, SECOND_METRIC_TAGS);
        assertThat(meterFilter.accept(secondMeterId)).isEqualTo(MeterFilterReply.DENY);
    }

    @Test
    public void testAcceptMultipleExcludedWildcardRegexNameNullTagsPrefixed()
    {
        Set<ExcludedMetric> excluded = new HashSet<>();
        excluded.add(createExcludedMetric(".*", null));
        MeterFilter meterFilter = new MeterFilterImpl(PREFIX, excluded);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME_WITH_PREFIX, FIRST_METRIC_TAGS);
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);

        Meter.Id secondMeterId = createMeterId(SECOND_METRIC_NAME_WITH_PREFIX, SECOND_METRIC_TAGS);
        assertThat(meterFilter.accept(secondMeterId)).isEqualTo(MeterFilterReply.DENY);
    }

    @Test
    public void testAcceptWithTags()
    {
        Set<ExcludedMetric> excluded = new HashSet<>();
        Map<String, String> commonExcludedTags = new HashMap<>();
        commonExcludedTags.put(COMMON_TAG_KEY, COMMON_TAG_VALUE);
        excluded.add(createExcludedMetric(".*", commonExcludedTags));
        Map<String, String> firstMetricExcludedTags = new HashMap<>();
        firstMetricExcludedTags.put(TAG_KEY, FIRST_METRIC_TAG_VALUE);
        excluded.add(createExcludedMetric(FIRST_METRIC_NAME, firstMetricExcludedTags));
        MeterFilter meterFilter = new MeterFilterImpl(null, excluded);

        // Test all having excluded tags are excluded
        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME, Tags.of(COMMON_TAG_KEY, COMMON_TAG_VALUE));
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);
        Meter.Id secondMeterId = createMeterId(SECOND_METRIC_NAME, Tags.of(COMMON_TAG_KEY, COMMON_TAG_VALUE));
        assertThat(meterFilter.accept(secondMeterId)).isEqualTo(MeterFilterReply.DENY);

        // Even though the metric name is excluded, the tags are not, so the metric should be accepted
        Meter.Id thirdMeterId = createMeterId(SECOND_METRIC_NAME, Tags.of("foo", "bar"));
        assertThat(meterFilter.accept(thirdMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);

        // test only some having excluded tags are excluded
        Meter.Id fourthMeterId = createMeterId(FIRST_METRIC_NAME, Tags.of(TAG_KEY, FIRST_METRIC_TAG_VALUE));
        assertThat(meterFilter.accept(fourthMeterId)).isEqualTo(MeterFilterReply.DENY);
        Meter.Id fifthMeterId = createMeterId(FIRST_METRIC_NAME, Tags.of(TAG_KEY, SECOND_METRIC_TAG_VALUE));
        assertThat(meterFilter.accept(fifthMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
        Meter.Id sixthMeterId = createMeterId(SECOND_METRIC_NAME, Tags.of(TAG_KEY, FIRST_METRIC_TAG_VALUE));
        assertThat(meterFilter.accept(sixthMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
        Meter.Id seventhMeterId = createMeterId(SECOND_METRIC_NAME, Tags.of(TAG_KEY, FIRST_METRIC_TAG_VALUE, COMMON_TAG_KEY, COMMON_TAG_VALUE));
        assertThat(meterFilter.accept(seventhMeterId)).isEqualTo(MeterFilterReply.DENY);
    }

    @Test
    public void testAcceptWithTagsPrefixed()
    {
        Set<ExcludedMetric> excluded = new HashSet<>();
        Map<String, String> commonExcludedTags = new HashMap<>();
        commonExcludedTags.put(COMMON_TAG_KEY, COMMON_TAG_VALUE);
        excluded.add(createExcludedMetric(".*", commonExcludedTags));
        Map<String, String> firstMetricExcludedTags = new HashMap<>();
        firstMetricExcludedTags.put(TAG_KEY, FIRST_METRIC_TAG_VALUE);
        excluded.add(createExcludedMetric(FIRST_METRIC_NAME, firstMetricExcludedTags));
        MeterFilter meterFilter = new MeterFilterImpl(PREFIX, excluded);

        // Test all having excluded tags are excluded
        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME_WITH_PREFIX, Tags.of(COMMON_TAG_KEY, COMMON_TAG_VALUE));
        assertThat(meterFilter.accept(firstMeterId)).isEqualTo(MeterFilterReply.DENY);
        Meter.Id secondMeterId = createMeterId(SECOND_METRIC_NAME_WITH_PREFIX, Tags.of(COMMON_TAG_KEY, COMMON_TAG_VALUE));
        assertThat(meterFilter.accept(secondMeterId)).isEqualTo(MeterFilterReply.DENY);

        // Even though the metric name is excluded, the tags are not, so the metric should be accepted
        Meter.Id thirdMeterId = createMeterId(SECOND_METRIC_NAME_WITH_PREFIX, Tags.of("foo", "bar"));
        assertThat(meterFilter.accept(thirdMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);

        // test only some having excluded tags are excluded
        Meter.Id fourthMeterId = createMeterId(FIRST_METRIC_NAME_WITH_PREFIX, Tags.of(TAG_KEY, FIRST_METRIC_TAG_VALUE));
        assertThat(meterFilter.accept(fourthMeterId)).isEqualTo(MeterFilterReply.DENY);
        Meter.Id fifthMeterId = createMeterId(FIRST_METRIC_NAME_WITH_PREFIX, Tags.of(TAG_KEY, SECOND_METRIC_TAG_VALUE));
        assertThat(meterFilter.accept(fifthMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
        Meter.Id sixthMeterId = createMeterId(SECOND_METRIC_NAME_WITH_PREFIX, Tags.of(TAG_KEY, FIRST_METRIC_TAG_VALUE));
        assertThat(meterFilter.accept(sixthMeterId)).isEqualTo(MeterFilterReply.NEUTRAL);
        Meter.Id seventhMeterId = createMeterId(SECOND_METRIC_NAME_WITH_PREFIX,
                Tags.of(TAG_KEY, FIRST_METRIC_TAG_VALUE, COMMON_TAG_KEY, COMMON_TAG_VALUE));
        assertThat(meterFilter.accept(seventhMeterId)).isEqualTo(MeterFilterReply.DENY);
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
        assertThat(firstMappedMeterId.getName()).isEqualTo(FIRST_METRIC_NAME_WITH_PREFIX);
        assertThat(firstMappedMeterId.getTags()).isEqualTo(firstMappedMeterId.getTags());

        Meter.Id secondMappedMeterId = meterFilter.map(secondMeterId);
        assertThat(secondMappedMeterId.getName()).isEqualTo(SECOND_METRIC_NAME_WITH_PREFIX);
        assertThat(secondMappedMeterId.getTags()).isEqualTo(secondMeterId.getTags());
    }

    @Test
    public void testMapPrefixAlreadyPrefixed()
    {
        MeterFilter meterFilter = new MeterFilterImpl(PREFIX, null);

        Meter.Id firstMeterId = createMeterId(FIRST_METRIC_NAME_WITH_PREFIX, FIRST_METRIC_TAGS);
        Meter.Id secondMeterId = createMeterId(SECOND_METRIC_NAME, SECOND_METRIC_TAGS);

        Meter.Id firstMappedMeterId = meterFilter.map(firstMeterId);
        assertThat(firstMappedMeterId.getName()).isEqualTo(FIRST_METRIC_NAME_WITH_PREFIX);
        assertThat(firstMappedMeterId.getTags()).isEqualTo(firstMappedMeterId.getTags());

        Meter.Id secondMappedMeterId = meterFilter.map(secondMeterId);
        assertThat(secondMappedMeterId.getName()).isEqualTo(SECOND_METRIC_NAME_WITH_PREFIX);
        assertThat(secondMappedMeterId.getTags()).isEqualTo(secondMeterId.getTags());
    }

    private ExcludedMetric createExcludedMetric(String name, Map<String, String> tags)
    {
        ExcludedMetric excludedMetric = new ExcludedMetric();
        excludedMetric.setMetricName(name);
        if (tags != null)
        {
            excludedMetric.setMetricTags(tags);
        }
        return excludedMetric;
    }

    private Meter.Id createMeterId(String name, Tags tags)
    {
        return new Meter.Id(name, tags, null, null, Meter.Type.GAUGE);
    }
}
