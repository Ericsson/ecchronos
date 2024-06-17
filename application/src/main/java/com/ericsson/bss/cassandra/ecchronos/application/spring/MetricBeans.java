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

package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.ericsson.bss.cassandra.ecchronos.application.CsvConfig;
import com.ericsson.bss.cassandra.ecchronos.application.CsvMeterRegistry;
import com.ericsson.bss.cassandra.ecchronos.application.MeterFilterImpl;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.metrics.StatisticsConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Configuration
public class MetricBeans
{
    private static final String METRICS_ENDPOINT = "/metrics";
    private final CompositeMeterRegistry myCompositeMeterRegistry;
    private PrometheusMeterRegistry myPrometheusMeterRegistry;

    public MetricBeans(final Config config)
    {
        myCompositeMeterRegistry = new CompositeMeterRegistry(Clock.SYSTEM);
        StatisticsConfig metricConfig = config.getStatisticsConfig();
        if (metricConfig.isEnabled())
        {
            if (metricConfig.getReportingConfigs().isJmxReportingEnabled())
            {
                createJmxMeterRegistry(metricConfig);
            }
            if (metricConfig.getReportingConfigs().isFileReportingEnabled())
            {
                createCsvMeterRegistry(metricConfig);
            }
            if (metricConfig.getReportingConfigs().isHttpReportingEnabled())
            {
                createPrometheusMeterRegistry(metricConfig);
            }
        }
        createStatusLoggerMeterRegistry();
    }

    private void createJmxMeterRegistry(final StatisticsConfig metricConfig)
    {
        MeterFilter meterFilter = new MeterFilterImpl(metricConfig.getMetricsPrefix(), metricConfig
                .getReportingConfigs()
                .getJmxReportingConfig()
                .getExcludedMetrics());
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        jmxMeterRegistry.config().meterFilter(meterFilter);
        myCompositeMeterRegistry.add(jmxMeterRegistry);
    }

    private void createCsvMeterRegistry(final StatisticsConfig metricConfig)
    {
        MeterFilter meterFilter = new MeterFilterImpl(metricConfig.getMetricsPrefix(), metricConfig
                .getReportingConfigs()
                .getFileReportingConfig()
                .getExcludedMetrics());
        CsvMeterRegistry csvMeterRegistry = new CsvMeterRegistry(CsvConfig.DEFAULT, Clock.SYSTEM,
                metricConfig.getOutputDirectory());
        csvMeterRegistry.config().meterFilter(meterFilter);
        csvMeterRegistry.start();
        myCompositeMeterRegistry.add(csvMeterRegistry);
    }

    private void createPrometheusMeterRegistry(final StatisticsConfig metricConfig)
    {
        MeterFilter meterFilter = new MeterFilterImpl(metricConfig.getMetricsPrefix(), metricConfig
                .getReportingConfigs()
                .getHttpReportingConfig()
                .getExcludedMetrics());
        myPrometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        myPrometheusMeterRegistry.config().meterFilter(meterFilter);
        myCompositeMeterRegistry.add(myPrometheusMeterRegistry);
    }

    private void createStatusLoggerMeterRegistry()
    {
        SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();
        myCompositeMeterRegistry.add(simpleMeterRegistry);
    }

    @Bean
    public PrometheusMeterRegistry prometheusMeterRegistry()
    {
        return myPrometheusMeterRegistry;
    }

    @Bean
    public CompositeMeterRegistry eccCompositeMeterRegistry()
    {
        return myCompositeMeterRegistry;
    }

    @Bean
    public MetricsServerProperties metricsServerProperties()
    {
        return new MetricsServerProperties();
    }

    /**
     * Creates a bean that filters requests based on URI and port.
     *
     * @return the filter that disallows /metric on any other port than metricsServer.port
     * and disallows any endpoint besides /metric on metricsServer.port.
     */
    @Bean
    public FilterRegistrationBean requestFilter(final MetricsServerProperties metricsServerProperties)
    {
        Filter filter = new OncePerRequestFilter()
        {
            @Override
            protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response,
                    final FilterChain filterChain) throws ServletException, IOException
            {
                int requestPort = request.getLocalPort();
                String requestURI = request.getRequestURI();
                if (metricsServerProperties.isEnabled() && (requestURI.startsWith(METRICS_ENDPOINT)
                        && requestPort != metricsServerProperties.getPort()))
                {
                    response.sendError(HttpStatus.NOT_FOUND.value());
                }
                else if (metricsServerProperties.isEnabled() && (!requestURI.startsWith(METRICS_ENDPOINT)
                        && requestPort == metricsServerProperties.getPort()))
                {
                    response.sendError(HttpStatus.NOT_FOUND.value());
                }
                else
                {
                    filterChain.doFilter(request, response);
                }
            }
        };
        FilterRegistrationBean filterRegistrationBean = new FilterRegistrationBean();
        filterRegistrationBean.setFilter(filter);
        filterRegistrationBean.addUrlPatterns("/*");
        return filterRegistrationBean;
    }
}
