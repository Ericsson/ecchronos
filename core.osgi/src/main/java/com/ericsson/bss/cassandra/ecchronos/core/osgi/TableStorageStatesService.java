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

import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStatesImpl;

import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@Component(service = TableStorageStates.class)
@Designate(ocd = TableStorageStatesService.Configuration.class)
public class TableStorageStatesService implements TableStorageStates
{
    private static final short DEFAULT_UPDATE_DELAY_IN_SECONDS = 60;

    @Reference(service = ReplicatedTableProvider.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile ReplicatedTableProvider myReplicatedTableProvider;

    @Reference (service = JmxProxyFactory.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile JmxProxyFactory myJmxProxyFactory;

    private volatile TableStorageStatesImpl myDelegateTableStorageStates;

    @Activate
    public final synchronized void activate(final Configuration configuration)
    {
        long updateDelayInSeconds = configuration.updateStartupDelayInSeconds();

        myDelegateTableStorageStates = TableStorageStatesImpl.builder()
                .withReplicatedTableProvider(myReplicatedTableProvider)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withUpdateDelay(updateDelayInSeconds, TimeUnit.SECONDS)
                .build();
    }

    @Deactivate
    public final synchronized void deactivate()
    {
        myDelegateTableStorageStates.close();
    }

    @Override
    public final long getDataSize(final TableReference tableReference)
    {
        return myDelegateTableStorageStates.getDataSize(tableReference);
    }

    @Override
    public final long getDataSize()
    {
        return myDelegateTableStorageStates.getDataSize();
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition(name = "Startup delay of fetching storage state",
                description = "The interval in seconds between updates of the storage states of tables")
        long updateStartupDelayInSeconds() default DEFAULT_UPDATE_DELAY_IN_SECONDS;
    }
}
