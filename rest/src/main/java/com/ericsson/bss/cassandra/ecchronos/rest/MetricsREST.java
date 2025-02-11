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
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.expositionformats.OpenMetricsTextFormatWriter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Set;

import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Tag(name = "Metrics", description = "Retrieve metrics about ecChronos")
@RestController
public class MetricsREST
{
    private final PrometheusMeterRegistry myPrometheusMeterRegistry;

    public MetricsREST(@Autowired(required = false) final PrometheusMeterRegistry prometheusMeterRegistry)
    {
        myPrometheusMeterRegistry = prometheusMeterRegistry;
    }

    @GetMapping(value = "/metrics", produces = { PrometheusTextFormatWriter.CONTENT_TYPE,
            OpenMetricsTextFormatWriter.CONTENT_TYPE })
    @Operation(operationId = "metrics", description = "Get metrics in the specified format", summary = "Get metrics")
    public final ResponseEntity<String> getMetrics(@RequestHeader(value = HttpHeaders.ACCEPT, required = false,
            defaultValue = PrometheusTextFormatWriter.CONTENT_TYPE)
            final String acceptHeader,
            @RequestParam(value = "name[]", required = false, defaultValue = "")
            @Parameter(description = "Filter metrics based on these names.")
            final Set<String> includedMetrics)
    {
        if (myPrometheusMeterRegistry == null)
        {
            throw new ResponseStatusException(NOT_FOUND);
        }

        // Check the content type, but default to PrometheusTextFormat
        String contentType = PrometheusTextFormatWriter.CONTENT_TYPE;
        if (acceptHeader != null && acceptHeader.contains("application/openmetrics-text"))
        {
            contentType = OpenMetricsTextFormatWriter.CONTENT_TYPE;
        }

        return ResponseEntity.ok()
                .header(CONTENT_TYPE, contentType)
                .body(myPrometheusMeterRegistry.scrape(contentType, includedMetrics));
    }
}
