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
package com.ericsson.bss.cassandra.ecchronos.connection.impl;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@Component(service = NativeConnectionProvider.class, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class OSGiLocalNativeConnectionProvider implements NativeConnectionProvider
{
    private volatile LocalNativeConnectionProvider myDelegateNativeConnectionProvider;

    @Activate
    public synchronized void activate(Configuration configuration)
    {
        String localhost = configuration.localHost();
        int port = configuration.nativePort();

        myDelegateNativeConnectionProvider = LocalNativeConnectionProvider.builder()
                .withLocalhost(localhost)
                .withPort(port)
                .build();
    }

    @Deactivate
    public synchronized void deactivate()
    {
        myDelegateNativeConnectionProvider.close();
    }

    @Override
    public Session getSession()
    {
        return myDelegateNativeConnectionProvider.getSession();
    }

    @Override
    public Host getLocalHost()
    {
        return myDelegateNativeConnectionProvider.getLocalHost();
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition(name = "Cassandra native port", description = "The port to use for native communication with Cassandra")
        int nativePort() default LocalNativeConnectionProvider.DEFAULT_NATIVE_PORT;

        @AttributeDefinition(name = "Cassandra host", description = "The host to use for native communication with Cassandra")
        String localHost() default LocalNativeConnectionProvider.DEFAULT_LOCAL_HOST;
    }
}
