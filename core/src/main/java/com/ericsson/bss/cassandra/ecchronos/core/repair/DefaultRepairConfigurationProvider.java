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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.google.common.base.Preconditions;

/**
 * A repair configuration provider that adds configuration to {@link RepairScheduler} based on whether or not the table
 * is replicated locally using the default repair configuration provided during construction of this object.
 */
//TODO TEST
public class DefaultRepairConfigurationProvider implements SchemaChangeListener, Closeable
{
    private final CqlSession mySession;
    private final ReplicatedTableProvider myReplicatedTableProvider;
    private final RepairScheduler myRepairScheduler;
    private final Function<TableReference, RepairConfiguration> myRepairConfigurationFunction;
    private final TableReferenceFactory myTableReferenceFactory;

    private DefaultRepairConfigurationProvider(Builder builder)
    {
        mySession = builder.mySession;
        myReplicatedTableProvider = builder.myReplicatedTableProvider;
        myRepairScheduler = builder.myRepairScheduler;
        myRepairConfigurationFunction = builder.myRepairConfigurationFunction;
        myTableReferenceFactory = Preconditions.checkNotNull(builder.myTableReferenceFactory,
                "Table reference factory must be set");

        for (KeyspaceMetadata keyspaceMetadata : mySession.getMetadata().getKeyspaces().values())
        {
            String keyspaceName = keyspaceMetadata.getName().asInternal();
            if (myReplicatedTableProvider.accept(keyspaceName))
            {
                allTableOperation(keyspaceName, this::updateConfiguration);
            }
        }
    }

    @Override
    public void onKeyspaceUpdated(KeyspaceMetadata current, KeyspaceMetadata previous)
    {
        String keyspaceName = current.getName().asInternal();
        if (myReplicatedTableProvider.accept(keyspaceName))
        {
            allTableOperation(keyspaceName, this::updateConfiguration);
        }
        else
        {
            allTableOperation(keyspaceName, myRepairScheduler::removeConfiguration);
        }
    }

    @Override
    public void onTableCreated(TableMetadata table)
    {
        if (myReplicatedTableProvider.accept(table.getKeyspace().asInternal()))
        {
            TableReference tableReference = myTableReferenceFactory.forTable(table.getKeyspace().asInternal(),
                    table.getName().asInternal());
            updateConfiguration(tableReference);
        }
    }

    @Override
    public void onTableDropped(TableMetadata table)
    {
        if (myReplicatedTableProvider.accept(table.getKeyspace().asInternal()))
        {
            TableReference tableReference = myTableReferenceFactory.forTable(table);
            myRepairScheduler.removeConfiguration(tableReference);
        }
    }

    @Override
    public void close()
    {
        for (KeyspaceMetadata keyspaceMetadata : mySession.getMetadata().getKeyspaces().values())
        {
            allTableOperation(keyspaceMetadata.getName().asInternal(), myRepairScheduler::removeConfiguration);
        }
    }

    private void allTableOperation(String keyspaceName, Consumer<TableReference> consumer)
    {
        for (TableMetadata tableMetadata : mySession.getMetadata().getKeyspace(keyspaceName).get().getTables().values())
        {
            String tableName = tableMetadata.getName().asInternal();
            TableReference tableReference = myTableReferenceFactory.forTable(keyspaceName, tableName);

            consumer.accept(tableReference);
        }
    }

    private void updateConfiguration(TableReference tableReference)
    {
        RepairConfiguration repairConfiguration = myRepairConfigurationFunction.apply(tableReference);

        if (RepairConfiguration.DISABLED.equals(repairConfiguration))
        {
            myRepairScheduler.removeConfiguration(tableReference);
        }
        else
        {
            myRepairScheduler.putConfiguration(tableReference, myRepairConfigurationFunction.apply(tableReference));
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    @Override
    public void onKeyspaceCreated(KeyspaceMetadata keyspace)
    {
        // NOOP
    }

    @Override
    public void onKeyspaceDropped(KeyspaceMetadata keyspace)
    {
        // NOOP
    }

    @Override
    public void onTableUpdated(TableMetadata current, TableMetadata previous)
    {
        // NOOP
    }

    @Override
    public void onUserDefinedTypeCreated(UserDefinedType type)
    {
        // NOOP
    }

    @Override
    public void onUserDefinedTypeDropped(UserDefinedType type)
    {
        // NOOP
    }

    @Override
    public void onUserDefinedTypeUpdated(UserDefinedType current, UserDefinedType previous)
    {
        // NOOP
    }

    @Override
    public void onFunctionCreated(FunctionMetadata function)
    {
        // NOOP
    }

    @Override
    public void onFunctionDropped(FunctionMetadata function)
    {
        // NOOP
    }

    @Override
    public void onFunctionUpdated(FunctionMetadata current, FunctionMetadata previous)
    {
        // NOOP
    }

    @Override
    public void onAggregateCreated(AggregateMetadata aggregate)
    {
        // NOOP
    }

    @Override
    public void onAggregateDropped(AggregateMetadata aggregate)
    {
        // NOOP
    }

    @Override
    public void onAggregateUpdated(AggregateMetadata current, AggregateMetadata previous)
    {
        // NOOP
    }

    @Override
    public void onViewCreated(ViewMetadata view)
    {
        // NOOP
    }

    @Override
    public void onViewDropped(ViewMetadata view)
    {
        // NOOP
    }

    @Override
    public void onViewUpdated(ViewMetadata current, ViewMetadata previous)
    {
        // NOOP
    }

    public static class Builder
    {
        private CqlSession mySession;
        private ReplicatedTableProvider myReplicatedTableProvider;
        private RepairScheduler myRepairScheduler;
        private Function<TableReference, RepairConfiguration> myRepairConfigurationFunction;
        private TableReferenceFactory myTableReferenceFactory;

        public Builder withSession(CqlSession session)
        {
            mySession = session;
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
            return configurationProvider;
        }
    }
}