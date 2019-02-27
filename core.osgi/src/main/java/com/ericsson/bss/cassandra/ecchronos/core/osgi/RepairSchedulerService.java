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

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TableRepairJob;

import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

/**
 * A factory creating {@link TableRepairJob}'s for tables that replicates data over multiple nodes.
 * <p>
 * This factory will schedule new jobs automatically when new tables are added.
 */
@Component(service = RepairScheduler.class)
@Designate(ocd = RepairSchedulerService.Configuration.class)
public class RepairSchedulerService implements RepairScheduler
{
    @Reference (service = JmxProxyFactory.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile JmxProxyFactory myJmxProxyFactory;

    @Reference(service = TableRepairMetrics.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile TableRepairMetrics myTableRepairMetrics;

    @Reference(service = ScheduleManager.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile ScheduleManager myScheduleManager;

    @Reference(service = RepairStateFactory.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile RepairStateFactory myRepairStateFactory;

    private volatile RepairSchedulerImpl myDelegateRepairSchedulerImpl;

    @Activate
    public synchronized void activate(Configuration configuration)
    {
        myDelegateRepairSchedulerImpl = RepairSchedulerImpl.builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withScheduleManager(myScheduleManager)
                .withRepairStateFactory(myRepairStateFactory)
                .withRepairLockType(configuration.repairLockType())
                .build();
    }

    @Deactivate
    public synchronized void deactivate()
    {
        myDelegateRepairSchedulerImpl.close();
        myDelegateRepairSchedulerImpl = null;
    }

    @Override
    public void putConfiguration(TableReference tableReference, RepairConfiguration repairConfiguration)
    {
        myDelegateRepairSchedulerImpl.putConfiguration(tableReference, repairConfiguration);
    }

    @Override
    public void removeConfiguration(TableReference tableReference)
    {
        myDelegateRepairSchedulerImpl.removeConfiguration(tableReference);
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition(name = "Type of repair lock", description = "The type of locks to take for repair jobs")
        RepairLockType repairLockType() default RepairLockType.VNODE;
    }
}
