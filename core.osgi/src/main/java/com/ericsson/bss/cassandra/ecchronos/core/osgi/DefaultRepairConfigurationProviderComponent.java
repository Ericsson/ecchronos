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

import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;

/**
 * OSGi component wrapping {@link DefaultRepairConfigurationProvider} bound with OSGi services.
 */
@Component
@Designate(ocd = DefaultRepairConfigurationProviderComponent.Configuration.class)
public class DefaultRepairConfigurationProviderComponent
{
    private static final long DEFAULT_REPAIR_INTERVAL_SECONDS = 7L * 24L * 60L * 60L;
    private static final long DEFAULT_REPAIR_WARNING_SECONDS = 8L * 24L * 60L * 60L;
    private static final long DEFAULT_REPAIR_ERROR_SECONDS = 10L * 24L * 60L * 60L;

    @Reference (service = NativeConnectionProvider.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile NativeConnectionProvider myNativeConnectionProvider;

    @Reference (service = ReplicatedTableProvider.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile ReplicatedTableProvider myReplicatedTableProvider;

    @Reference (service = RepairScheduler.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile RepairScheduler myRepairScheduler;

    private volatile DefaultRepairConfigurationProvider myDelegateRepairConfigurationProvider;

    @Activate
    public synchronized void activate(Configuration configuration)
    {
        if (configuration.enabled())
        {
            long repairInterval = configuration.repairIntervalSeconds();

            RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                    .withParallelism(configuration.repairParallelism())
                    .withRepairInterval(repairInterval, TimeUnit.SECONDS)
                    .withRepairWarningTime(configuration.repairWarningSeconds(), TimeUnit.SECONDS)
                    .withRepairErrorTime(configuration.repairErrorSeconds(), TimeUnit.SECONDS)
                    .withRepairUnwindRatio(configuration.repairUnwindRatio())
                    .build();

            myDelegateRepairConfigurationProvider = DefaultRepairConfigurationProvider.newBuilder()
                    .withReplicatedTableProvider(myReplicatedTableProvider)
                    .withRepairScheduler(myRepairScheduler)
                    .withCluster(myNativeConnectionProvider.getSession().getCluster())
                    .withDefaultRepairConfiguration(repairConfiguration)
                    .build();
        }
    }

    @Deactivate
    public synchronized void deactivate()
    {
        if (myDelegateRepairConfigurationProvider != null)
        {
            myDelegateRepairConfigurationProvider.close();
        }
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition (name = "Enable or disable", description = "If the default repair configuration provider should be enabled or disabled")
        boolean enabled() default true;

        @AttributeDefinition (name = "Repair interval in seconds", description = "The wanted interval between successful repairs in seconds")
        long repairIntervalSeconds() default DEFAULT_REPAIR_INTERVAL_SECONDS;

        @AttributeDefinition (name = "Repair warning time", description = "After a table has not been repaired for the specified")
        long repairWarningSeconds() default DEFAULT_REPAIR_WARNING_SECONDS;

        @AttributeDefinition (name = "Repair error time", description = "The wanted interval between successful repairs in seconds")
        long repairErrorSeconds() default DEFAULT_REPAIR_ERROR_SECONDS;

        @AttributeDefinition (name = "Repair paralleism", description = "The repair parallelism to use")
        RepairOptions.RepairParallelism repairParallelism() default RepairOptions.RepairParallelism.PARALLEL;

        @AttributeDefinition (name = "Repair unwind ratio", description = "The ratio of time to wait before starting the next repair. The amount of time to wait is based on the time it took to perform the repair. A value of 1.0 gives 100% of the repair time as wait time.")
        double repairUnwindRatio() default RepairConfiguration.NO_UNWIND;
    }
}
