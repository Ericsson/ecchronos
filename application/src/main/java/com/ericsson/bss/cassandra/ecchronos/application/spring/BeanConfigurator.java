/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.codahale.metrics.MetricFilter;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.application.MetricFilterImpl;
import com.ericsson.bss.cassandra.ecchronos.application.ReloadingCertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.codahale.metrics.MetricRegistry;
import com.ericsson.bss.cassandra.ecchronos.application.ConfigurationException;
import com.ericsson.bss.cassandra.ecchronos.application.ReflectionUtils;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigRefresher;
import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigurationHelper;
import com.ericsson.bss.cassandra.ecchronos.application.config.Security;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import org.springframework.format.FormatterRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Configuration
public class BeanConfigurator
{
    private static final Logger LOG = LoggerFactory.getLogger(BeanConfigurator.class);

    private static final String CONFIGURATION_FILE = "ecc.yml";
    private static final String SECURITY_FILE = "security.yml";
    private static final String METRICS_ENDPOINT = "/metrics";

    private final AtomicReference<Security.CqlSecurity> cqlSecurity = new AtomicReference<>();
    private final AtomicReference<Security.JmxSecurity> jmxSecurity = new AtomicReference<>();

    private final ConfigRefresher configRefresher;

    public BeanConfigurator() throws ConfigurationException
    {
        if (ConfigurationHelper.DEFAULT_INSTANCE.usePath())
        {
            configRefresher = new ConfigRefresher(ConfigurationHelper.DEFAULT_INSTANCE.getConfigPath());
            configRefresher.watch(ConfigurationHelper.DEFAULT_INSTANCE.configFile(SECURITY_FILE).toPath(),
                    () -> refreshSecurityConfig(cqlSecurity::set, jmxSecurity::set));
        }
        else
        {
            configRefresher = null;
        }

        Security security = getSecurityConfig();
        cqlSecurity.set(security.getCql());
        jmxSecurity.set(security.getJmx());
    }

    public final void close()
    {
        if (configRefresher != null)
        {
            configRefresher.close();
        }
    }

    @Bean
    public Config config() throws ConfigurationException
    {
        return getConfiguration();
    }

    private Security getSecurityConfig() throws ConfigurationException
    {
        return ConfigurationHelper.DEFAULT_INSTANCE.getConfiguration(SECURITY_FILE, Security.class);
    }

    private Config getConfiguration() throws ConfigurationException
    {
        return ConfigurationHelper.DEFAULT_INSTANCE.getConfiguration(CONFIGURATION_FILE, Config.class);
    }

    @Bean
    public ConfigurableServletWebServerFactory webServerFactory(final Config configuration) throws UnknownHostException
    {
        TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
        factory.setAddress(InetAddress.getByName(configuration.getRestServer().getHost()));
        factory.setPort(configuration.getRestServer().getPort());
        return factory;
    }

    @Bean
    public RepairFaultReporter repairFaultReporter(final Config config) throws ConfigurationException
    {
        return ReflectionUtils.construct(config.getRepair().getAlarm().getFaultReporter());
    }

    @Bean
    public DefaultRepairConfigurationProvider defaultRepairConfigurationProvider()
    {
        return new DefaultRepairConfigurationProvider();
    }

    @Bean
    public MetricsServerProperties metricsServerProperties()
    {
        return new MetricsServerProperties();
    }

    @Bean
    public NativeConnectionProvider nativeConnectionProvider(final Config config,
            final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
            final MetricRegistry metricRegistry) throws ConfigurationException
    {
        return getNativeConnectionProvider(config, cqlSecurity::get, defaultRepairConfigurationProvider,
                metricRegistry);
    }

    private static NativeConnectionProvider getNativeConnectionProvider(
            final Config configuration,
            final Supplier<Security.CqlSecurity> securitySupplier,
            final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
            final MetricRegistry metricRegistry) throws ConfigurationException
    {
        Supplier tlsSupplier = () -> securitySupplier.get().getTls();

        CertificateHandler certificateHandler =
                ReflectionUtils.construct(configuration.getConnectionConfig().getCql().getCertificateHandlerClass(),
                new Class<?>[] {
                        Supplier.class
                }, tlsSupplier);
        try
        {
            return ReflectionUtils
                    .construct(configuration.getConnectionConfig().getCql().getProviderClass(),
                            new Class<?>[] {
                                    Config.class,
                                    Supplier.class,
                                    CertificateHandler.class,
                                    DefaultRepairConfigurationProvider.class,
                                    MetricRegistry.class
                            },
                            configuration, securitySupplier, certificateHandler, defaultRepairConfigurationProvider,
                            metricRegistry);
        }
        catch (ConfigurationException e)
        {
            if (!ReloadingCertificateHandler.class.equals(certificateHandler.getClass())
                    && e.getCause() instanceof NoSuchMethodException)
            {
                throw new ConfigurationException("Invalid configuration, connection provider does not support"
                        + " configured certificate handler", e);
            }
        }

        // Check for old versions of DefaultNativeConnectionProvider
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getCql().getProviderClass(),
                        new Class<?>[]{
                                Config.class, Supplier.class, DefaultRepairConfigurationProvider.class,
                                MetricRegistry.class
                        },
                        configuration, securitySupplier, defaultRepairConfigurationProvider, metricRegistry);
    }

    @Bean
    public JmxConnectionProvider jmxConnectionProvider(final Config config) throws ConfigurationException
    {
        return getJmxConnectionProvider(config, jmxSecurity::get);
    }

    private static JmxConnectionProvider getJmxConnectionProvider(final Config configuration,
                                                                  final Supplier<Security.JmxSecurity> securitySupplier)
            throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getJmx().getProviderClass(),
                        new Class<?>[] {
                                Config.class, Supplier.class
                        },
                        configuration, securitySupplier);
    }

    @Bean
    public StatementDecorator statementDecorator(final Config config) throws ConfigurationException
    {
        return getStatementDecorator(config);
    }

    @Bean
    public MetricRegistry metricRegistry()
    {
        return new MetricRegistry();
    }

    @Bean
    public MetricFilter jmxMetricFilter(final Config config)
    {
        return new MetricFilterImpl(config.getStatistics().getReporting().getJmx().getExcludedMetrics());
    }

    @Bean
    public MetricFilter fileMetricFilter(final Config config)
    {
        return new MetricFilterImpl(config.getStatistics().getReporting().getFile().getExcludedMetrics());
    }

    @Bean
    public MetricFilter httpMetricFilter(final Config config)
    {
        return new MetricFilterImpl(config.getStatistics().getReporting().getHttp().getExcludedMetrics());
    }

    @Bean
    ServletRegistrationBean registerMetricsServlet(final MetricRegistry metricRegistry,
            final MetricFilter httpMetricFilter, final Config config)
    {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        ServletRegistrationBean servletRegistrationBean = new ServletRegistrationBean();
        if (config.getStatistics().getReporting().isHttpReportingEnabled())
        {
            collectorRegistry.register(new DropwizardExports(metricRegistry, httpMetricFilter));
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

    private static StatementDecorator getStatementDecorator(final Config configuration) throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getCql().getDecoratorClass(), configuration);
    }

    private void refreshSecurityConfig(final Consumer<Security.CqlSecurity> cqlSetter,
                                       final Consumer<Security.JmxSecurity> jmxSetter)
    {
        try
        {
            Security security = getSecurityConfig();

            cqlSetter.accept(security.getCql());
            jmxSetter.accept(security.getJmx());
        }
        catch (ConfigurationException e)
        {
            LOG.warn("Unable to refresh security config");
        }
    }

    @Bean
    public NodeResolver nodeResolver(final NativeConnectionProvider nativeConnectionProvider)
    {
        CqlSession session = nativeConnectionProvider.getSession();

        return new NodeResolverImpl(session);
    }

    @Bean
    public ReplicationState replicationState(final NativeConnectionProvider nativeConnectionProvider,
                                             final NodeResolver nodeResolver)
    {
        Node node = nativeConnectionProvider.getLocalNode();
        CqlSession session = nativeConnectionProvider.getSession();

        return new ReplicationStateImpl(nodeResolver, session, node);
    }

    @Bean
    public WebMvcConfigurer conversionConfigurer() //Add application converters to web so springboot can convert in REST
    {
        return new WebMvcConfigurer()
        {
            @Override
            public void addFormatters(final FormatterRegistry registry)
            {
                ApplicationConversionService.configure(registry);
            }
        };
    }
}
