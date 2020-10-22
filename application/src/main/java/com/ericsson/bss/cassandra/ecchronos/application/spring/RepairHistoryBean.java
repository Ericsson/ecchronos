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

import java.util.concurrent.TimeUnit;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.*;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;

@Configuration
public class RepairHistoryBean
{
    private final RepairHistory repairHistory;
    private final RepairHistoryProvider repairHistoryProvider;

    public RepairHistoryBean(Config configuration, NativeConnectionProvider nativeConnectionProvider,
            NodeResolver nodeResolver, StatementDecorator statementDecorator, ReplicationState replicationState)
    {
        Host host = nativeConnectionProvider.getLocalHost();
        Session session = nativeConnectionProvider.getSession();

        Node localNode = nodeResolver.fromUUID(host.getHostId()).orElseThrow(IllegalStateException::new);

        Config.RepairConfig repairConfig = configuration.getRepair();

        if (repairConfig.getHistory().getProvider() == Config.RepairHistory.Provider.CASSANDRA)
        {
            repairHistoryProvider = createCassandraHistoryProvider(repairConfig, session, nodeResolver,
                    statementDecorator);
            repairHistory = RepairHistory.NO_OP;
        }
        else
        {
            EccRepairHistory eccRepairHistory = EccRepairHistory.newBuilder()
                    .withSession(session)
                    .withReplicationState(replicationState)
                    .withLocalNode(localNode)
                    .withStatementDecorator(statementDecorator)
                    .withLookbackTime(repairConfig.getHistoryLookback().getInterval(TimeUnit.MILLISECONDS),
                            TimeUnit.MILLISECONDS)
                    .withKeyspace(repairConfig.getHistory().getKeyspace())
                    .build();

            if (repairConfig.getHistory().getProvider() == Config.RepairHistory.Provider.UPGRADE)
            {
                repairHistoryProvider = createCassandraHistoryProvider(repairConfig, session, nodeResolver,
                        statementDecorator);
            }
            else
            {
                repairHistoryProvider = eccRepairHistory;
            }

            repairHistory = eccRepairHistory;
        }
    }

    @Bean
    public RepairHistory repairHistory()
    {
        return repairHistory;
    }

    @Bean
    public RepairHistoryProvider repairHistoryProvider()
    {
        return repairHistoryProvider;
    }

    private RepairHistoryProvider createCassandraHistoryProvider(Config.RepairConfig repairConfig, Session session,
            NodeResolver nodeResolver, StatementDecorator statementDecorator)
    {
        return new RepairHistoryProviderImpl(nodeResolver, session, statementDecorator,
                repairConfig.getHistoryLookback().getInterval(TimeUnit.MILLISECONDS));
    }
}
