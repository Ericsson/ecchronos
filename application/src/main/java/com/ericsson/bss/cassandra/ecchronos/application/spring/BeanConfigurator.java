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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.ericsson.bss.cassandra.ecchronos.application.ConfigurationException;
import com.ericsson.bss.cassandra.ecchronos.application.ReflectionUtils;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigRefresher;
import com.ericsson.bss.cassandra.ecchronos.application.config.Security;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

@Configuration
public class BeanConfigurator
{
    private static final Logger LOG = LoggerFactory.getLogger(BeanConfigurator.class);

    private static final String CONFIGURATION_DIRECTORY_PATH = "ecchronos.config";
    private static final String CONFIGURATION_FILE = "ecc.yml";
    private static final String SECURITY_FILE = "security.yml";

    private final AtomicReference<Security.CqlSecurity> cqlSecurity = new AtomicReference<>();
    private final AtomicReference<Security.JmxSecurity> jmxSecurity = new AtomicReference<>();

    private final ConfigRefresher configRefresher;

    private boolean usePath = false;

    public BeanConfigurator() throws ConfigurationException
    {
        if (System.getProperty(CONFIGURATION_DIRECTORY_PATH) != null)
        {
            configRefresher = new ConfigRefresher(getConfigPath());
            configRefresher.watch(configFile(SECURITY_FILE).toPath(),
                    () -> refreshSecurityConfig(cqlSecurity::set, jmxSecurity::set));
            usePath = true;
        }
        else
        {
            configRefresher = null;
        }

        Security security = getSecurityConfig();
        cqlSecurity.set(security.getCql());
        jmxSecurity.set(security.getJmx());
    }

    public void close()
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
        if (usePath)
        {
            return getConfiguration(configFile(SECURITY_FILE), Security.class);
        }
        else
        {
            return getFileFromClassPath(SECURITY_FILE, Security.class);
        }
    }

    private <T> T getFileFromClassPath(String file, Class<T> clazz) throws ConfigurationException
    {
        ClassLoader loader = ClassLoader.getSystemClassLoader();
        return getConfiguration(loader.getResourceAsStream(file), clazz);
    }

    private Config getConfiguration() throws ConfigurationException
    {
        if (usePath)
        {
            return getConfiguration(configFile(CONFIGURATION_FILE), Config.class);
        }
        else
        {
            return getFileFromClassPath(CONFIGURATION_FILE, Config.class);
        }
    }

    private static <T> T getConfiguration(File configurationFile, Class<T> clazz) throws ConfigurationException
    {
        try
        {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

            return objectMapper.readValue(configurationFile, clazz);
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Unable to load configuration file " + configurationFile, e);
        }
    }

    private static <T> T getConfiguration(InputStream configurationFile, Class<T> clazz) throws ConfigurationException
    {
        try
        {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

            return objectMapper.readValue(configurationFile, clazz);
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Unable to load configuration file from classpath", e);
        }
    }

    private static File configFile(String configFile)
    {
        return new File(getConfigPath().toFile(), configFile);
    }

    @Bean
    public ConfigurableServletWebServerFactory webServerFactory(Config configuration) throws UnknownHostException
    {
        TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
        factory.setAddress(InetAddress.getByName(configuration.getRestServer().getHost()));
        factory.setPort(configuration.getRestServer().getPort());
        return factory;
    }

    @Bean
    public RepairFaultReporter repairFaultReporter()
    {
        return new LoggingFaultReporter();
    }

    @Bean
    public NativeConnectionProvider nativeConnectionProvider(Config config) throws ConfigurationException
    {
        return getNativeConnectionProvider(config, cqlSecurity::get);
    }

    private static NativeConnectionProvider getNativeConnectionProvider(Config configuration,
            Supplier<Security.CqlSecurity> securitySupplier)
            throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getCql().getProviderClass(),
                        new Class<?>[] { Config.class, Supplier.class },
                        configuration, securitySupplier);
    }

    @Bean
    public JmxConnectionProvider jmxConnectionProvider(Config config) throws ConfigurationException
    {
        return getJmxConnectionProvider(config, jmxSecurity::get);
    }

    private static JmxConnectionProvider getJmxConnectionProvider(Config configuration,
            Supplier<Security.JmxSecurity> securitySupplier) throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getJmx().getProviderClass(),
                        new Class<?>[] { Config.class, Supplier.class },
                        configuration, securitySupplier);
    }

    @Bean
    public StatementDecorator statementDecorator(Config config) throws ConfigurationException
    {
        return getStatementDecorator(config);
    }

    @Bean
    public MetricRegistry metricRegistry()
    {
        return new MetricRegistry();
    }

    @Bean
    ServletRegistrationBean registerMetricsServlet(MetricRegistry metricRegistry)
    {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        ServletRegistrationBean servletRegistrationBean = new ServletRegistrationBean();

        collectorRegistry.register(new DropwizardExports(metricRegistry));
        servletRegistrationBean.setServlet(new MetricsServlet(collectorRegistry));
        servletRegistrationBean.setUrlMappings(Arrays.asList("/metrics/*"));
        return servletRegistrationBean;
    }

    private static StatementDecorator getStatementDecorator(Config configuration) throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getCql().getDecoratorClass(), configuration);
    }

    private static Path getConfigPath()
    {
        return FileSystems.getDefault().getPath(System.getProperty(CONFIGURATION_DIRECTORY_PATH));
    }

    private void refreshSecurityConfig(Consumer<Security.CqlSecurity> cqlSetter,
            Consumer<Security.JmxSecurity> jmxSetter)
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
    public NodeResolver nodeResolver(NativeConnectionProvider nativeConnectionProvider)
    {
        Metadata metadata = nativeConnectionProvider.getSession().getCluster().getMetadata();

        return new NodeResolverImpl(metadata);
    }

    @Bean
    public ReplicationState replicationState(NativeConnectionProvider nativeConnectionProvider,
            NodeResolver nodeResolver)
    {
        Host host = nativeConnectionProvider.getLocalHost();
        Metadata metadata = nativeConnectionProvider.getSession().getCluster().getMetadata();

        return new ReplicationStateImpl(nodeResolver, metadata, host);
    }
}
