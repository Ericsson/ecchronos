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
package com.ericsson.bss.cassandra.ecchronos.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.http.HttpStatus.NOT_FOUND;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.expositionformats.OpenMetricsTextFormatWriter;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;

import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

public class TestMetricsRESTImpl
{
    private static final String TEST_COUNTER = "test_counter";
    private static final String TEST_GAUGE = "test_gauge";

    @Test
    public void testGetMetricsMeterRegistryNull()
    {
        MetricsRESTImpl metricsREST = new MetricsRESTImpl(null);
        
        assertThatThrownBy(() -> metricsREST.getMetrics("", Collections.emptySet()))
                .isInstanceOf(ResponseStatusException.class)
                .hasFieldOrPropertyWithValue("status", NOT_FOUND);
    }

    @Test
    public void testGetMetricsNoParams()
    {
        MetricsRESTImpl metricsREST = new MetricsRESTImpl(createRegistryWithMetrics());
        
        ResponseEntity<String> response = metricsREST.getMetrics("", Collections.emptySet());
        assertThat(response.getBody()).isNotEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).contains(TEST_COUNTER);
        assertThat(response.getBody()).contains(TEST_GAUGE);
        List<String> contentTypeHeaders = response.getHeaders().get(HttpHeaders.CONTENT_TYPE);
        assertThat(contentTypeHeaders).hasSize(1);
        assertThat(contentTypeHeaders.get(0)).isEqualTo(PrometheusTextFormatWriter.CONTENT_TYPE);
    }

    @Test
    public void testGetMetricsAcceptHeader()
    {
        MetricsRESTImpl metricsREST = new MetricsRESTImpl(createRegistryWithMetrics());
        
        ResponseEntity<String> response = metricsREST.getMetrics("application/openmetrics-text", Collections.emptySet());
        assertThat(response.getBody()).isNotEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).contains(TEST_COUNTER);
        assertThat(response.getBody()).contains(TEST_GAUGE);
        List<String> contentTypeHeaders = response.getHeaders().get(HttpHeaders.CONTENT_TYPE);
        assertThat(contentTypeHeaders).hasSize(1);
        assertThat(contentTypeHeaders.get(0)).isEqualTo(OpenMetricsTextFormatWriter.CONTENT_TYPE);
    }

    @Test
    public void testGetMetricsIncludedNames()
    {
        MetricsRESTImpl metricsREST = new MetricsRESTImpl(createRegistryWithMetrics());
        
        ResponseEntity<String> response = metricsREST.getMetrics("", Collections.singleton(TEST_COUNTER));
        assertThat(response.getBody()).isNotEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).contains(TEST_COUNTER);
        assertThat(response.getBody()).doesNotContain(TEST_GAUGE);
        List<String> contentTypeHeaders = response.getHeaders().get(HttpHeaders.CONTENT_TYPE);
        assertThat(contentTypeHeaders).hasSize(1);
        assertThat(contentTypeHeaders.get(0)).isEqualTo(PrometheusTextFormatWriter.CONTENT_TYPE);
    }

    @Test
    public void testGetMetricsWithNullIncludedMetricsRealRegistry()
    {
        MetricsRESTImpl metricsREST = new MetricsRESTImpl(createRegistryWithMetrics());
        
        ResponseEntity<String> response = metricsREST.getMetrics("", null);
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).isNotEmpty();
        assertThat(response.getBody()).contains(TEST_COUNTER);
        assertThat(response.getBody()).contains(TEST_GAUGE);
    }

    @Test
    public void testGetMetricsWithEmptyIncludedMetricsRealRegistry()
    {
        MetricsRESTImpl metricsREST = new MetricsRESTImpl(createRegistryWithMetrics());
        
        ResponseEntity<String> response = metricsREST.getMetrics("", Collections.emptySet());
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).isNotEmpty();
        assertThat(response.getBody()).contains(TEST_COUNTER);
        assertThat(response.getBody()).contains(TEST_GAUGE);
    }

    private PrometheusMeterRegistry createRegistryWithMetrics()
    {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT);
        registry.counter(TEST_COUNTER).increment();
        registry.gauge(TEST_GAUGE, 42.0);
        return registry;
    }
}
