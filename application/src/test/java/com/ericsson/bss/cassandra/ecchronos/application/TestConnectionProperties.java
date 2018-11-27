/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import org.junit.Test;

import javax.management.remote.JMXConnector;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestConnectionProperties
{
    @Test
    public void testDefaultValues() throws ConfigurationException
    {
        Properties properties = new Properties();

        ConnectionProperties connectionProperties = ConnectionProperties.from(properties);

        assertThat(connectionProperties.getNativeConnectionProviderClass()).isEqualTo(DefaultNativeConnectionProvider.class);
        assertThat(connectionProperties.getJmxConnectionProviderClass()).isEqualTo(DefaultJmxConnectionProvider.class);
        assertThat(connectionProperties.getStatementDecoratorClass()).isEqualTo(DefaultStatementDecorator.class);
    }

    @Test
    public void testSetNativeConnectionProviderClass() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("connection.native.class", WorkingNativeConnectionProvider.class.getName());

        ConnectionProperties connectionProperties = ConnectionProperties.from(properties);

        assertThat(connectionProperties.getNativeConnectionProviderClass()).isEqualTo(WorkingNativeConnectionProvider.class);
    }

    @Test
    public void testSetNativeConnectionProviderClassWithInvalidConstructor()
    {
        Properties properties = new Properties();
        properties.put("connection.native.class", NativeConnectionProviderWithInvalidConstructor.class.getName());

        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> ConnectionProperties.from(properties));
    }

    @Test
    public void testSetNativeConnectionProviderClassWithInvalidClass()
    {
        Properties properties = new Properties();
        properties.put("connection.native.class", NonOverridingProviderClass.class.getName());

        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> ConnectionProperties.from(properties));
    }

    @Test
    public void testSetJmxConnectionProviderClass() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("connection.jmx.class", WorkingJmxConnectionProvider.class.getName());

        ConnectionProperties connectionProperties = ConnectionProperties.from(properties);

        assertThat(connectionProperties.getJmxConnectionProviderClass()).isEqualTo(WorkingJmxConnectionProvider.class);
    }

    @Test
    public void testSetJmxConnectionProviderClassWithInvalidConstructor()
    {
        Properties properties = new Properties();
        properties.put("connection.jmx.class", JmxConnectionProviderWithInvalidConstructor.class.getName());

        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> ConnectionProperties.from(properties));
    }

    @Test
    public void testSetJmxConnectionProviderClassWithInvalidClass()
    {
        Properties properties = new Properties();
        properties.put("connection.jmx.class", NonOverridingProviderClass.class.getName());

        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> ConnectionProperties.from(properties));
    }

    @Test
    public void testSetStatementDecoratorClass() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("connection.native.decorator.class", WorkingStatementDecorator.class.getName());

        ConnectionProperties connectionProperties = ConnectionProperties.from(properties);

        assertThat(connectionProperties.getStatementDecoratorClass()).isEqualTo(WorkingStatementDecorator.class);
    }

    @Test
    public void testSetStatementDecoratorClassWithInvalidConstructor()
    {
        Properties properties = new Properties();
        properties.put("connection.native.decorator.class", StatementDecoratorWithInvalidConstructor.class.getName());

        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> ConnectionProperties.from(properties));
    }

    @Test
    public void testSetStatementDecoratorClassWithInvalidClass()
    {
        Properties properties = new Properties();
        properties.put("connection.native.decorator.class", NonOverridingProviderClass.class.getName());

        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(() -> ConnectionProperties.from(properties));
    }

    static class WorkingNativeConnectionProvider implements NativeConnectionProvider
    {
        WorkingNativeConnectionProvider(Properties properties)
        {
        }

        @Override
        public Session getSession()
        {
            return null;
        }

        @Override
        public Host getLocalHost()
        {
            return null;
        }
    }

    static class NativeConnectionProviderWithInvalidConstructor implements NativeConnectionProvider
    {
        @Override
        public Session getSession()
        {
            return null;
        }

        @Override
        public Host getLocalHost()
        {
            return null;
        }
    }

    static class WorkingJmxConnectionProvider implements JmxConnectionProvider
    {
        WorkingJmxConnectionProvider(Properties properties)
        {
        }

        @Override
        public JMXConnector getJmxConnector()
        {
            return null;
        }
    }

    static class JmxConnectionProviderWithInvalidConstructor implements JmxConnectionProvider
    {
        @Override
        public JMXConnector getJmxConnector()
        {
            return null;
        }
    }

    static class WorkingStatementDecorator implements StatementDecorator
    {
        WorkingStatementDecorator(Properties properties)
        {
        }

        @Override
        public Statement apply(Statement statement)
        {
            return null;
        }
    }

    static class StatementDecoratorWithInvalidConstructor implements StatementDecorator
    {
        @Override
        public Statement apply(Statement statement)
        {
            return null;
        }
    }

    static class NonOverridingProviderClass
    {
        NonOverridingProviderClass(Properties properties)
        {
        }
    }
}
