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

import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;

import java.util.Properties;

public final class ConnectionProperties
{
    private static final String CONFIG_CONNECTION_NATIVE_CLASS = "connection.native.class";
    private static final String CONFIG_STATEMENT_DECORATOR_CLASS = "connection.native.decorator.class";
    private static final String CONFIG_CONNECTION_JMX_CLASS = "connection.jmx.class";

    private static final String DEFAULT_CONNECTION_NATIVE_CLASS = DefaultNativeConnectionProvider.class.getName();
    private static final String DEFAULT_STATEMENT_DECORATOR_CLASS = DefaultJmxConnectionProvider.class.getName();
    private static final String DEFAULT_CONNECTION_JMX_CLASS = DefaultStatementDecorator.class.getName();

    private final Class<? extends NativeConnectionProvider> myNativeConnectionProviderClass;
    private final Class<? extends JmxConnectionProvider> myJmxConnectionProviderClass;
    private final Class<? extends StatementDecorator> myStatementDecoratorClass;

    private ConnectionProperties(Class<? extends NativeConnectionProvider> nativeConnectionProviderClass,
                                 Class<? extends JmxConnectionProvider> jmxConnectionProviderClass,
                                 Class<? extends StatementDecorator> statementDecoratorClass)
    {
        myNativeConnectionProviderClass = nativeConnectionProviderClass;
        myJmxConnectionProviderClass = jmxConnectionProviderClass;
        myStatementDecoratorClass = statementDecoratorClass;
    }

    public Class<? extends NativeConnectionProvider> getNativeConnectionProviderClass()
    {
        return myNativeConnectionProviderClass;
    }

    public Class<? extends JmxConnectionProvider> getJmxConnectionProviderClass()
    {
        return myJmxConnectionProviderClass;
    }

    public Class<? extends StatementDecorator> getStatementDecoratorClass()
    {
        return myStatementDecoratorClass;
    }

    @Override
    public String toString()
    {
        return String.format("Connection(native=%s, jmx=%s, statementDecorator=%s)", myNativeConnectionProviderClass,
                myJmxConnectionProviderClass, myStatementDecoratorClass);
    }

    public static ConnectionProperties from(Properties properties) throws ConfigurationException
    {
        Class<? extends NativeConnectionProvider> nativeConnectionProviderClass = getClassOfType(properties, CONFIG_CONNECTION_NATIVE_CLASS, DEFAULT_CONNECTION_NATIVE_CLASS, NativeConnectionProvider.class);
        Class<? extends JmxConnectionProvider> jmxConnectionProviderClass = getClassOfType(properties, CONFIG_CONNECTION_JMX_CLASS, DEFAULT_STATEMENT_DECORATOR_CLASS, JmxConnectionProvider.class);
        Class<? extends StatementDecorator> statementDecoratorClass = getClassOfType(properties, CONFIG_STATEMENT_DECORATOR_CLASS, DEFAULT_CONNECTION_JMX_CLASS, StatementDecorator.class);

        return new ConnectionProperties(nativeConnectionProviderClass, jmxConnectionProviderClass, statementDecoratorClass);
    }

    private static <T> Class<? extends T> getClassOfType(Properties properties, String property, String defaultClass, Class<T> wantedType) throws ConfigurationException
    {
        String className = properties.getProperty(property, defaultClass);

        return ReflectionUtils.resolveClassOfType(className, wantedType, Properties.class);
    }
}
