/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.FileBasedRepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ConfigurationException;
import java.io.Closeable;

import com.datastax.oss.driver.api.core.CqlSession;
import java.util.Collections;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ECChronos implements Closeable
{
    private final ECChronosInternals myECChronosInternals;
    private final RepairSchedulerImpl myRepairSchedulerImpl;
    private final TimeBasedRunPolicy myTimeBasedRunPolicy;

    public ECChronos(
            final Config configuration,
            final ApplicationContext applicationContext,
            final DistributedNativeConnectionProvider nativeConnectionProvider,
            final DistributedJmxConnectionProvider jmxConnectionProvider,
            final ReplicationState replicationState,
            final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
            final EccNodesSync eccNodesSync
            )
            throws ConfigurationException
    {
        myECChronosInternals = new ECChronosInternals(
                configuration, nativeConnectionProvider, jmxConnectionProvider, eccNodesSync);

        CqlSession session = nativeConnectionProvider.getCqlSession();

        myTimeBasedRunPolicy = TimeBasedRunPolicy.builder()
                .withSession(session)
                .withKeyspaceName(configuration.getRunPolicy().getTimeBasedConfig().getKeyspaceName())
                .build();

        myRepairSchedulerImpl = RepairSchedulerImpl.builder()
                .withJmxProxyFactory(myECChronosInternals.getJmxProxyFactory())
                .withScheduleManager(myECChronosInternals.getScheduleManager())
                .withTableRepairMetrics(myECChronosInternals.getTableRepairMetrics())
                .withCassandraMetrics(myECChronosInternals.getCassandraMetrics())
                .withReplicationState(replicationState)
                .withRepairPolicies(Collections.singletonList(myTimeBasedRunPolicy))
                .withCassandraMetrics(myECChronosInternals.getCassandraMetrics())
                .build();

        AbstractRepairConfigurationProvider repairConfigurationProvider = new FileBasedRepairConfiguration(applicationContext);

        defaultRepairConfigurationProvider.fromBuilder(DefaultRepairConfigurationProvider.newBuilder()
                .withRepairScheduler(myRepairSchedulerImpl)
                .withSession(session)
                .withNodesList(nativeConnectionProvider.getNodes())
                .withReplicatedTableProvider(myECChronosInternals.getReplicatedTableProvider())
                .withRepairConfiguration(repairConfigurationProvider::get)
                .withEccNodesSync(eccNodesSync)
                .withJmxConnectionProvider(jmxConnectionProvider)
                .withDistributedNativeConnectionProvider(nativeConnectionProvider)
                .withTableReferenceFactory(myECChronosInternals.getTableReferenceFactory()));

        myECChronosInternals.addRunPolicy(myTimeBasedRunPolicy);
    }

    @Bean
    public TableReferenceFactory tableReferenceFactory()
    {
        return myECChronosInternals.getTableReferenceFactory();
    }

    @Bean(destroyMethod = "")
    public RepairScheduler repairScheduler()
    {
        return myRepairSchedulerImpl;
    }

    @Bean
    public ReplicatedTableProvider replicatedTableProvider()
    {
        return myECChronosInternals.getReplicatedTableProvider();
    }

    @Override
    public final void close()
    {
        myECChronosInternals.removeRunPolicy(myTimeBasedRunPolicy);
        myTimeBasedRunPolicy.close();
        myRepairSchedulerImpl.close();
        myECChronosInternals.close();
    }
}


