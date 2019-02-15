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
package com.ericsson.bss.cassandra.ecchronos.osgi;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.osgi.DefaultRepairConfigurationProviderComponent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.osgi.ScheduleManagerService;
import com.ericsson.bss.cassandra.ecchronos.core.osgi.TimeBasedRunPolicyService;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.runtime.ServiceComponentRuntime;
import org.osgi.service.component.runtime.dto.ComponentDescriptionDTO;
import org.osgi.util.tracker.ServiceTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class ITTestECChronos extends TestBase
{
    private static final String SCHEDULE_MANAGER_PID = "com.ericsson.bss.cassandra.ecchronos.core.osgi.ScheduleManagerService";

    private static final String TIME_BASED_RUN_POLICY_PID = "com.ericsson.bss.cassandra.ecchronos.core.osgi.TimeBasedRunPolicyService";
    private static final String REPAIR_CONFIGURATION_PID = "com.ericsson.bss.cassandra.ecchronos.core.osgi.DefaultRepairConfigurationProviderComponent";

    @Inject
    BundleContext myBundleContext;

    @Inject
    ServiceComponentRuntime myScrService;

    @Configuration
    public Option[] configure() throws IOException
    {
        return basicOptions();
    }

    @Before
    public void verifyConfiguration() throws InterruptedException
    {
        checkNativeConnection();
        checkJmxConnection();
    }

    @Test
    public void testGetLockFactoryService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, LockFactory.class, null);
        serviceTracker.open();

        LockFactory lockFactory = (LockFactory) serviceTracker.waitForService(TimeUnit.SECONDS.toMillis(10));
        serviceTracker.close();

        assertThat(lockFactory).isNotNull();
    }

    @Test
    public void testGetHostStatesService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, HostStates.class, null);
        serviceTracker.open();

        HostStates hostStates = (HostStates) serviceTracker.waitForService(TimeUnit.SECONDS.toMillis(10));
        serviceTracker.close();

        assertThat(hostStates).isNotNull();
    }

    @Test
    public void testGetJmxProxyFactoryService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, JmxProxyFactory.class, null);
        serviceTracker.open();
        JmxProxyFactory jmxProxyFactory = (JmxProxyFactory) serviceTracker.waitForService(10000);
        serviceTracker.close();

        assertThat(jmxProxyFactory).isNotNull();
    }

    @Test
    public void testGetReplicatedTableProviderService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, ReplicatedTableProvider.class, null);
        serviceTracker.open();
        ReplicatedTableProvider replicatedTableProvider = (ReplicatedTableProvider) serviceTracker.waitForService(10000);
        serviceTracker.close();

        assertThat(replicatedTableProvider).isNotNull();
    }

    @Test
    public void testGetTableStorageStatesService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, TableStorageStates.class, null);
        serviceTracker.open();

        TableStorageStates tableStorageStates = (TableStorageStates) serviceTracker.waitForService(TimeUnit.SECONDS.toMillis(10));
        serviceTracker.close();

        assertThat(tableStorageStates).isNotNull();
    }

    @Test
    public void testGetTableRepairMetricsService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, TableRepairMetrics.class, null);
        serviceTracker.open();

        TableRepairMetrics tableRepairMetrics = (TableRepairMetrics) serviceTracker.waitForService(TimeUnit.SECONDS.toMillis(10));
        serviceTracker.close();

        assertThat(tableRepairMetrics).isNotNull();
    }

    @Test
    public void testGetRepairStateFactoryService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, RepairStateFactory.class, null);
        serviceTracker.open();

        RepairStateFactory repairStateFactory = (RepairStateFactory) serviceTracker.waitForService(TimeUnit.SECONDS.toMillis(10));
        serviceTracker.close();

        assertThat(repairStateFactory).isNotNull();
    }

    @Test
    public void testGetRepairSchedulerService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, RepairScheduler.class, null);
        serviceTracker.open();

        RepairScheduler repairScheduler = (RepairScheduler) serviceTracker.waitForService(TimeUnit.SECONDS.toMillis(10));
        serviceTracker.close();

        assertThat(repairScheduler).isNotNull();
    }

    @Test
    public void testGetScheduleManagerComponent()
    {
        assertComponentIsActive(SCHEDULE_MANAGER_PID, ScheduleManagerService.class);
    }

    @Test
    public void testGetTimeBasedRunPolicyComponent()
    {
        assertComponentIsActive(TIME_BASED_RUN_POLICY_PID, TimeBasedRunPolicyService.class);
    }

    @Test
    public void testGetRepairConfigurationComponent()
    {
        assertComponentIsActive(REPAIR_CONFIGURATION_PID, DefaultRepairConfigurationProviderComponent.class);
    }

    private void assertComponentIsActive(String pid, Class clazz)
    {
        for (ComponentDescriptionDTO desc : myScrService.getComponentDescriptionDTOs())
        {
            if (clazz.getCanonicalName().equals(desc.implementationClass))
            {
                assertThat(desc.configurationPid).contains(pid);
                assertThat(myScrService.isComponentEnabled(desc)).isTrue();

                return;
            }
        }
        fail();
    }

    private void checkNativeConnection() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, NativeConnectionProvider.class, null);
        serviceTracker.open();

        NativeConnectionProvider nativeConnectionProvider = (NativeConnectionProvider) serviceTracker.waitForService(10000);
        if (nativeConnectionProvider == null)
        {
            throw new IllegalStateException("Native Connection provider not started");
        }
        serviceTracker.close();
    }

    private void checkJmxConnection() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, JmxConnectionProvider.class, null);
        serviceTracker.open();
        JmxConnectionProvider jmxConnectionProvider = (JmxConnectionProvider) serviceTracker.waitForService(10000);
        if (jmxConnectionProvider == null)
        {
            throw new IllegalStateException("JMX Connection provider not started");
        }
        serviceTracker.close();
    }
}
