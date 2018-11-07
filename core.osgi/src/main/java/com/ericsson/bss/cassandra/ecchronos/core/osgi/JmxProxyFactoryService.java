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
package com.ericsson.bss.cassandra.ecchronos.core.osgi;

import java.io.IOException;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactoryImpl;

import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

@Component(service = JmxProxyFactory.class)
public class JmxProxyFactoryService implements JmxProxyFactory
{
    @Reference(service = JmxConnectionProvider.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile JmxConnectionProvider myJmxConnectionProvider;

    private volatile JmxProxyFactoryImpl myDelegateJmxProxyFactory;

    @Activate
    public void activate()
    {
        myDelegateJmxProxyFactory = JmxProxyFactoryImpl.builder()
                .withJmxConnectionProvider(myJmxConnectionProvider)
                .build();
    }

    @Deactivate
    public void deactivate()
    {
        myDelegateJmxProxyFactory = null;
    }

    @Override
    public JmxProxy connect() throws IOException
    {
        return myDelegateJmxProxyFactory.connect();
    }
}
