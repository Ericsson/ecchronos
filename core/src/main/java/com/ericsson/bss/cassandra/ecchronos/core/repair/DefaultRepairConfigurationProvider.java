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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import java.io.Closeable;
import java.util.function.Consumer;
import java.util.function.Function;

import com.datastax.driver.core.*;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.google.common.base.Preconditions;

/**
 * A repair configuration provider that adds configuration to {@link RepairScheduler} based on if the table is
 * replicated locally using the default repair configuration provided during construction of this object.
 */
public class DefaultRepairConfigurationProvider implements SchemaChangeListener, Closeable
{
    private final Cluster myCluster;
    private final ReplicatedTableProvider myReplicatedTableProvider;
    private final RepairScheduler myRepairScheduler;
    private final Function<TableReference, RepairConfiguration> myRepairConfigurationFunction;
    private final TableReferenceFactory myTableReferenceFactory;

    private DefaultRepairConfigurationProvider(Builder builder)
    {
        myCluster = builder.myCluster;
        myReplicatedTableProvider = builder.myReplicatedTableProvider;
        myRepairScheduler = builder.myRepairScheduler;
        myRepairConfigurationFunction = builder.myRepairConfigurationFunction;
        myTableReferenceFactory = Preconditions.checkNotNull(builder.myTableReferenceFactory,
                "Table reference factory must be set");

        for (KeyspaceMetadata keyspaceMetadata : myCluster.getMetadata().getKeyspaces())
        {
            String keyspaceName = keyspaceMetadata.getName();
            if (myReplicatedTableProvider.accept(keyspaceName))
            {
                allTableOperation(keyspaceName, this::putConfiguration);
            }
        }
    }

    @Override
    public void onKeyspaceChanged(KeyspaceMetadata current, KeyspaceMetadata previous)
    {
        String keyspaceName = current.getName();
        if (myReplicatedTableProvider.accept(keyspaceName))
        {
            allTableOperation(keyspaceName, this::putConfiguration);
        }
        else
        {
            allTableOperation(keyspaceName, myRepairScheduler::removeConfiguration);
        }
    }

    @Override
    public void onTableAdded(TableMetadata table)
    {
        if (myReplicatedTableProvider.accept(table.getKeyspace().getName()))
        {
            TableReference tableReference = myTableReferenceFactory.forTable(table.getKeyspace().getName(),
                    table.getName());
            putConfiguration(tableReference);
        }
    }

    @Override
    public void onTableRemoved(TableMetadata table)
    {
        if (myReplicatedTableProvider.accept(table.getKeyspace().getName()))
        {
            TableReference tableReference = myTableReferenceFactory.forTable(table.getKeyspace().getName(),
                    table.getName());
            myRepairScheduler.removeConfiguration(tableReference);
        }
    }

    @Override
    public void close()
    {
        myCluster.unregister(this);

        for (KeyspaceMetadata keyspaceMetadata : myCluster.getMetadata().getKeyspaces())
        {
            allTableOperation(keyspaceMetadata.getName(), myRepairScheduler::removeConfiguration);
        }
    }

    private void allTableOperation(String keyspaceName, Consumer<TableReference> consumer)
    {
        for (TableMetadata tableMetadata : myCluster.getMetadata().getKeyspace(keyspaceName).getTables())
        {
            String tableName = tableMetadata.getName();
            TableReference tableReference = myTableReferenceFactory.forTable(keyspaceName, tableName);

            consumer.accept(tableReference);
        }
    }

    private void putConfiguration(TableReference tableReference)
    {
        myRepairScheduler.putConfiguration(tableReference, myRepairConfigurationFunction.apply(tableReference));
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Cluster myCluster;
        private ReplicatedTableProvider myReplicatedTableProvider;
        private RepairScheduler myRepairScheduler;
        private Function<TableReference, RepairConfiguration> myRepairConfigurationFunction;
        private TableReferenceFactory myTableReferenceFactory;

        public Builder withCluster(Cluster cluster)
        {
            myCluster = cluster;
            return this;
        }

        public Builder withDefaultRepairConfiguration(RepairConfiguration defaultRepairConfiguration)
        {
            myRepairConfigurationFunction = (tableReference) -> defaultRepairConfiguration;
            return this;
        }

        public Builder withRepairConfiguration(Function<TableReference, RepairConfiguration> defaultRepairConfiguration)
        {
            myRepairConfigurationFunction = defaultRepairConfiguration;
            return this;
        }

        public Builder withReplicatedTableProvider(ReplicatedTableProvider replicatedTableProvider)
        {
            myReplicatedTableProvider = replicatedTableProvider;
            return this;
        }

        public Builder withRepairScheduler(RepairScheduler repairScheduler)
        {
            myRepairScheduler = repairScheduler;
            return this;
        }

        public Builder withTableReferenceFactory(TableReferenceFactory tableReferenceFactory)
        {
            myTableReferenceFactory = tableReferenceFactory;
            return this;
        }

        public DefaultRepairConfigurationProvider build()
        {
            DefaultRepairConfigurationProvider configurationProvider = new DefaultRepairConfigurationProvider(this);
            myCluster.register(configurationProvider);

            return configurationProvider;
        }
    }

    @Override
    public void onKeyspaceAdded(KeyspaceMetadata keyspace)
    {
        // NOOP
    }

    @Override
    public void onKeyspaceRemoved(KeyspaceMetadata keyspace)
    {
        // NOOP
    }

    @Override
    public void onTableChanged(TableMetadata current, TableMetadata previous)
    {
        // NOOP
    }

    @Override
    public void onUserTypeAdded(UserType type)
    {
        // NOOP
    }

    @Override
    public void onUserTypeRemoved(UserType type)
    {
        // NOOP
    }

    @Override
    public void onUserTypeChanged(UserType current, UserType previous)
    {
        // NOOP
    }

    @Override
    public void onFunctionAdded(FunctionMetadata function)
    {
        // NOOP
    }

    @Override
    public void onFunctionRemoved(FunctionMetadata function)
    {
        // NOOP
    }

    @Override
    public void onFunctionChanged(FunctionMetadata current, FunctionMetadata previous)
    {
        // NOOP
    }

    @Override
    public void onAggregateAdded(AggregateMetadata aggregate)
    {
        // NOOP
    }

    @Override
    public void onAggregateRemoved(AggregateMetadata aggregate)
    {
        // NOOP
    }

    @Override
    public void onAggregateChanged(AggregateMetadata current, AggregateMetadata previous)
    {
        // NOOP
    }

    @Override
    public void onMaterializedViewAdded(MaterializedViewMetadata view)
    {
        // NOOP
    }

    @Override
    public void onMaterializedViewRemoved(MaterializedViewMetadata view)
    {
        // NOOP
    }

    @Override
    public void onMaterializedViewChanged(MaterializedViewMetadata current, MaterializedViewMetadata previous)
    {
        // NOOP
    }

    @Override
    public void onRegister(Cluster cluster)
    {
        // NOOP
    }

    @Override
    public void onUnregister(Cluster cluster)
    {
        // NOOP
    }
}