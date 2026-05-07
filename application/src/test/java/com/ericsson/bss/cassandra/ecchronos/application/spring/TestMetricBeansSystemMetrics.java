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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class TestMetricBeansSystemMetrics
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    @Test
    void testSystemMetricsRegisteredWhenStatisticsEnabled() throws IOException
    {
        Config config = loadConfig("nothing_set.yml");
        MetricBeans metricBeans = new MetricBeans(config);
        CompositeMeterRegistry registry = metricBeans.eccCompositeMeterRegistry();

        assertThat(registry.find("jvm.memory.used").gauge()).isNotNull();
        assertThat(registry.find("jvm.threads.live").gauge()).isNotNull();
        assertThat(registry.find("system.cpu.usage").gauge()).isNotNull();
    }

    @Test
    void testSystemMetricsNotRegisteredWhenStatisticsDisabled() throws IOException
    {
        Config config = loadConfig("all_set.yml");
        MetricBeans metricBeans = new MetricBeans(config);
        CompositeMeterRegistry registry = metricBeans.eccCompositeMeterRegistry();

        assertThat(registry.find("jvm.memory.used").gauge()).isNull();
        assertThat(registry.find("jvm.threads.live").gauge()).isNull();
        assertThat(registry.find("system.cpu.usage").gauge()).isNull();
    }

    private Config loadConfig(final String fileName) throws IOException
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());
        return OBJECT_MAPPER.readValue(file, Config.class);
    }
}
