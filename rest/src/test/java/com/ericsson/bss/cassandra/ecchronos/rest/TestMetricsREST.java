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
package com.ericsson.bss.cassandra.ecchronos.rest;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.prometheus.metrics.expositionformats.OpenMetricsTextFormatWriter;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestMetricsREST
{
    @Mock
    private PrometheusMeterRegistry myPrometheusMeterRegistryMock;

    private MetricsREST myMetricsREST;

    @Before
    public void init()
    {
        myMetricsREST = new MetricsREST(myPrometheusMeterRegistryMock);
        when(myPrometheusMeterRegistryMock.scrape(any(String.class), any(Set.class))).thenReturn("fooMetrics");
    }

    @Test
    public void testGetMetricsMeterRegistryNull()
    {
        MetricsREST metricsREST = new MetricsREST(null);
        ResponseEntity<String> response = null;
        try
        {
            response = metricsREST.getMetrics("", Collections.emptySet());
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getStatusCode().value()).isEqualTo(NOT_FOUND.value());
        }
        assertThat(response).isNull();
        verify(myPrometheusMeterRegistryMock, never()).scrape(any(String.class), any(Set.class));
    }

    @Test
    public void testGetMetricsNoParams()
    {
        ResponseEntity<String> response = myMetricsREST.getMetrics("", Collections.emptySet());
        assertThat(response.getBody()).isNotEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        List<String> contentTypeHeaders = response.getHeaders().get(HttpHeaders.CONTENT_TYPE);
        assertThat(contentTypeHeaders).hasSize(1);
        assertThat(contentTypeHeaders.get(0)).isEqualTo(PrometheusTextFormatWriter.CONTENT_TYPE);
        verify(myPrometheusMeterRegistryMock).scrape(PrometheusTextFormatWriter.CONTENT_TYPE, Collections.emptySet());
    }

    @Test
    public void testGetMetricsAcceptHeader()
    {
        ResponseEntity<String> response = myMetricsREST.getMetrics("application/openmetrics-text", Collections.emptySet());
        assertThat(response.getBody()).isNotEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        List<String> contentTypeHeaders = response.getHeaders().get(HttpHeaders.CONTENT_TYPE);
        assertThat(contentTypeHeaders).hasSize(1);
        assertThat(contentTypeHeaders.get(0)).isEqualTo(OpenMetricsTextFormatWriter.CONTENT_TYPE);
        verify(myPrometheusMeterRegistryMock).scrape(OpenMetricsTextFormatWriter.CONTENT_TYPE, Collections.emptySet());
    }

    @Test
    public void testGetMetricsIncludedNames()
    {
        ResponseEntity<String> response = myMetricsREST.getMetrics("", Collections.singleton("fooMetrics"));
        assertThat(response.getBody()).isNotEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        List<String> contentTypeHeaders = response.getHeaders().get(HttpHeaders.CONTENT_TYPE);
        assertThat(contentTypeHeaders).hasSize(1);
        assertThat(contentTypeHeaders.get(0)).isEqualTo(PrometheusTextFormatWriter.CONTENT_TYPE);
        verify(myPrometheusMeterRegistryMock).scrape(PrometheusTextFormatWriter.CONTENT_TYPE, Collections.singleton("fooMetrics"));
    }
}
