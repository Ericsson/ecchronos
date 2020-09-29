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
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.ericsson.bss.cassandra.ecchronos.application.ConfigurationException;
import com.ericsson.bss.cassandra.ecchronos.application.ReflectionUtils;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

@Configuration
public class BeanConfigurator
{
    private static final String CONFIGURATION_FILE_PATH = "ecchronos.config";
    private static final String DEFAULT_CONFIGURATION_FILE = "./ecc.yml";

    @Bean
    public Config config() throws ConfigurationException
    {
        return getConfiguration();
    }

    private static Config getConfiguration() throws ConfigurationException
    {
        String configurationFile = System.getProperty(CONFIGURATION_FILE_PATH, DEFAULT_CONFIGURATION_FILE);
        File file = new File(configurationFile);

        try
        {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

            return objectMapper.readValue(file, Config.class);
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Unable to load configuration file " + file, e);
        }
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
        return getNativeConnectionProvider(config);
    }

    private static NativeConnectionProvider getNativeConnectionProvider(Config configuration)
            throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getCql().getProviderClass(), configuration);
    }

    @Bean
    public JmxConnectionProvider jmxConnectionProvider(Config config) throws ConfigurationException
    {
        return getJmxConnectionProvider(config);
    }

    private static JmxConnectionProvider getJmxConnectionProvider(Config configuration) throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getJmx().getProviderClass(), configuration);
    }

    @Bean
    public StatementDecorator statementDecorator(Config config) throws ConfigurationException
    {
        return getStatementDecorator(config);
    }

    private static StatementDecorator getStatementDecorator(Config configuration) throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getCql().getDecoratorClass(), configuration);
    }
}
