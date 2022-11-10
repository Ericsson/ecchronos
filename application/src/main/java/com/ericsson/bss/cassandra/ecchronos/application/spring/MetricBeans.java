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
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
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
import java.util.Arrays;

@Configuration
public class MetricBeans
{
    private static final String METRICS_ENDPOINT = "/metrics";
    private final CompositeMeterRegistry myCompositeMeterRegistry;
    private PrometheusMeterRegistry myPrometheusMeterRegistry;

    public MetricBeans(final Config config)
    {
        myCompositeMeterRegistry = new CompositeMeterRegistry(Clock.SYSTEM);
        Config.StatisticsConfig metricConfig = config.getStatistics();
        if (metricConfig.isEnabled())
        {
            if (metricConfig.getReporting().isJmxReportingEnabled())
            {
                createJmxMeterRegistry(metricConfig);
            }
            if (metricConfig.getReporting().isFileReportingEnabled())
            {
                createCsvMeterRegistry(metricConfig);
            }
            if (metricConfig.getReporting().isHttpReportingEnabled())
            {
                createPrometheusMeterRegistry(metricConfig);
            }
        }
    }

    private void createJmxMeterRegistry(final Config.StatisticsConfig metricConfig)
    {
        MeterFilter meterFilter = new MeterFilterImpl(metricConfig
                .getReporting()
                .getJmx()
                .getExcludedMetrics());
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        jmxMeterRegistry.config().meterFilter(meterFilter);
        myCompositeMeterRegistry.add(jmxMeterRegistry);
    }

    private void createCsvMeterRegistry(final Config.StatisticsConfig metricConfig)
    {
        MeterFilter meterFilter = new MeterFilterImpl(metricConfig
                .getReporting()
                .getFile()
                .getExcludedMetrics());
        CsvMeterRegistry csvMeterRegistry = new CsvMeterRegistry(CsvConfig.DEFAULT, Clock.SYSTEM,
                metricConfig.getDirectory());
        csvMeterRegistry.config().meterFilter(meterFilter);
        csvMeterRegistry.start();
        myCompositeMeterRegistry.add(csvMeterRegistry);
    }

    private void createPrometheusMeterRegistry(final Config.StatisticsConfig metricConfig)
    {
        MeterFilter meterFilter = new MeterFilterImpl(metricConfig
                .getReporting()
                .getHttp()
                .getExcludedMetrics());
        myPrometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        myPrometheusMeterRegistry.config().meterFilter(meterFilter);
        myCompositeMeterRegistry.add(myPrometheusMeterRegistry);
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

    @Bean
    public ServletRegistrationBean registerMetricsServlet()
    {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        ServletRegistrationBean servletRegistrationBean = new ServletRegistrationBean();
        if (myPrometheusMeterRegistry != null)
        {
            collectorRegistry = myPrometheusMeterRegistry.getPrometheusRegistry();
        }
        servletRegistrationBean.setServlet(new MetricsServlet(collectorRegistry));
        servletRegistrationBean.setUrlMappings(Arrays.asList(METRICS_ENDPOINT + "/*"));
        return servletRegistrationBean;
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
