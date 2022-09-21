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

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Metadata;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.google.common.base.Preconditions;

/**
 * A repair configuration provider that adds configuration to {@link RepairScheduler} based on whether or not the table
 * is replicated locally using the default repair configuration provided during construction of this object.
 */
public class DefaultRepairConfigurationProvider implements SchemaChangeListener
{
    private CqlSession mySession;
    private ReplicatedTableProvider myReplicatedTableProvider;
    private RepairScheduler myRepairScheduler;
    private Function<TableReference, RepairConfiguration> myRepairConfigurationFunction;
    private TableReferenceFactory myTableReferenceFactory;

    public DefaultRepairConfigurationProvider()
    {
        //NOOP
    }

    /**
     * From builder.
     * @param builder
     */
    public void fromBuilder(final Builder builder)
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

    private DefaultRepairConfigurationProvider(final Builder builder)
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

    /**
     * On keyspace created.
     *
     * @param keyspace
     */
    @Override
    public void onKeyspaceCreated(final KeyspaceMetadata keyspace)
    {
        String keyspaceName = keyspace.getName().asInternal();
        if (myReplicatedTableProvider.accept(keyspaceName))
        {
            allTableOperation(keyspaceName, this::updateConfiguration);
        }
        else
        {
            allTableOperation(keyspaceName, myRepairScheduler::removeConfiguration);
        }
    }

    /**
     * On keyspace updated.
     *
     * @param current
     * @param previous
     */
    @Override
    public void onKeyspaceUpdated(final KeyspaceMetadata current,
                                  final KeyspaceMetadata previous)
    {
        onKeyspaceCreated(current);
    }

    /**
     * On table created.
     *
     * @param table
     */
    @Override
    public void onTableCreated(final TableMetadata table)
    {
        if (myReplicatedTableProvider.accept(table.getKeyspace().asInternal()))
        {
            TableReference tableReference = myTableReferenceFactory.forTable(table.getKeyspace().asInternal(),
                    table.getName().asInternal());
            updateConfiguration(tableReference, table);
        }
    }

    /**
     * On table dropped.
     *
     * @param table
     */
    @Override
    public void onTableDropped(final TableMetadata table)
    {
        if (myReplicatedTableProvider.accept(table.getKeyspace().asInternal()))
        {
            TableReference tableReference = myTableReferenceFactory.forTable(table);
            myRepairScheduler.removeConfiguration(tableReference);
        }
    }

    /**
     * On table updated.
     *
     * @param current
     * @param previous
     */
    @Override
    public void onTableUpdated(final TableMetadata current, final TableMetadata previous)
    {
        onTableCreated(current);
    }

    /**
     * Close.
     */
    @Override
    public void close()
    {
        for (KeyspaceMetadata keyspaceMetadata : mySession.getMetadata().getKeyspaces().values())
        {
            allTableOperation(keyspaceMetadata.getName().asInternal(), myRepairScheduler::removeConfiguration);
        }
    }

    private void allTableOperation(final String keyspaceName, final BiConsumer<TableReference, TableMetadata> consumer)
    {
        for (TableMetadata tableMetadata : Metadata.getKeyspace(mySession, keyspaceName).get().getTables().values())
        {
            String tableName = tableMetadata.getName().asInternal();
            TableReference tableReference = myTableReferenceFactory.forTable(keyspaceName, tableName);

            consumer.accept(tableReference, tableMetadata);
        }
    }

    private void allTableOperation(final String keyspaceName, final Consumer<TableReference> consumer)
    {
        for (TableMetadata tableMetadata : Metadata.getKeyspace(mySession, keyspaceName).get().getTables().values())
        {
            String tableName = tableMetadata.getName().asInternal();
            TableReference tableReference = myTableReferenceFactory.forTable(keyspaceName, tableName);

            consumer.accept(tableReference);
        }
    }

    private void updateConfiguration(final TableReference tableReference, final TableMetadata table)
    {
        RepairConfiguration repairConfiguration = myRepairConfigurationFunction.apply(tableReference);

        if (RepairConfiguration.DISABLED.equals(repairConfiguration)
                || isTableIgnored(table, repairConfiguration.getIgnoreTWCSTables()))
        {
            myRepairScheduler.removeConfiguration(tableReference);
        }
        else
        {
            myRepairScheduler.putConfiguration(tableReference, myRepairConfigurationFunction.apply(tableReference));
        }
    }

    private boolean isTableIgnored(final TableMetadata table, final boolean ignore)
    {
        Map<CqlIdentifier, Object> tableOptions = table.getOptions();
        if (tableOptions == null)
        {
            return false;
        }
        Map<String, String> compaction
                = (Map<String, String>) tableOptions.get(CqlIdentifier.fromInternal("compaction"));
        if (compaction == null)
        {
            return false;
        }
        return ignore
                && "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy".equals(compaction.get("class"));
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    /**
     * On keyspace dropped.
     *
     * @param keyspace
     */
    @Override
    public void onKeyspaceDropped(final KeyspaceMetadata keyspace)
    {
        // NOOP
    }

    /**
     * On user defined type created.
     * @param type
     */
    @Override
    public void onUserDefinedTypeCreated(final UserDefinedType type)
    {
        // NOOP
    }

    /**
     * On user defined type dropped.
     *
     * @param type
     */
    @Override
    public void onUserDefinedTypeDropped(final UserDefinedType type)
    {
        // NOOP
    }

    /**
     * On user defined type updated.
     *
     * @param current
     * @param previous
     */
    @Override
    public void onUserDefinedTypeUpdated(final UserDefinedType current, final UserDefinedType previous)
    {
        // NOOP
    }

    /**
     * On function created.
     *
     * @param function
     */
    @Override
    public void onFunctionCreated(final FunctionMetadata function)
    {
        // NOOP
    }

    /**
     * On function dropped.
     *
     * @param function
     */
    @Override
    public void onFunctionDropped(final FunctionMetadata function)
    {
        // NOOP
    }

    /**
     * On function updated.
     *
     * @param current
     * @param previous
     */
    @Override
    public void onFunctionUpdated(final FunctionMetadata current, final FunctionMetadata previous)
    {
        // NOOP
    }

    /**
     * On aggregate created.
     *
     * @param aggregate
     */
    @Override
    public void onAggregateCreated(final AggregateMetadata aggregate)
    {
        // NOOP
    }

    /**
     * On aggregate updated.
     *
     * @param aggregate
     */
    @Override
    public void onAggregateDropped(final AggregateMetadata aggregate)
    {
        // NOOP
    }

    /**
     * On aggregate updated.
     *
     * @param current
     * @param previous
     */
    @Override
    public void onAggregateUpdated(final AggregateMetadata current, final AggregateMetadata previous)
    {
        // NOOP
    }

    /**
     * On view created.
     *
     * @param view
     */
    @Override
    public void onViewCreated(final ViewMetadata view)
    {
        // NOOP
    }

    /**
     * On view dropped.
     *
     * @param view
     */
    @Override
    public void onViewDropped(final ViewMetadata view)
    {
        // NOOP
    }

    /**
     * On view updated.
     *
     * @param current
     * @param previous
     */
    @Override
    public void onViewUpdated(final ViewMetadata current, final ViewMetadata previous)
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

        /**
         * Build with session.
         *
         * @param session
         * @return Builder
         */
        public Builder withSession(final CqlSession session)
        {
            mySession = session;
            return this;
        }

        /**
         * Buiild with default repair configuration.
         *
         * @param defaultRepairConfiguration
         * @return Builder
         */
        public Builder withDefaultRepairConfiguration(final RepairConfiguration defaultRepairConfiguration)
        {
            myRepairConfigurationFunction = (tableReference) -> defaultRepairConfiguration;
            return this;
        }

        /**
         * Build with repair configuration.
         *
         * @param defaultRepairConfiguration
         * @return Builder
         */
        public Builder withRepairConfiguration(final Function<TableReference, RepairConfiguration>
                                                       defaultRepairConfiguration)
        {
            myRepairConfigurationFunction = defaultRepairConfiguration;
            return this;
        }

        /**
         * Build with replicated table provider.
         *
         * @param replicatedTableProvider
         * @return Builder
         */
        public Builder withReplicatedTableProvider(final ReplicatedTableProvider replicatedTableProvider)
        {
            myReplicatedTableProvider = replicatedTableProvider;
            return this;
        }

        /**
         * Build with table repair scheduler.
         *
         * @param repairScheduler
         * @return Builder
         */
        public Builder withRepairScheduler(final RepairScheduler repairScheduler)
        {
            myRepairScheduler = repairScheduler;
            return this;
        }

        /**
         * Build with table reference factory.
         *
         * @param tableReferenceFactory
         * @return Builder
         */
        public Builder withTableReferenceFactory(final TableReferenceFactory tableReferenceFactory)
        {
            myTableReferenceFactory = tableReferenceFactory;
            return this;
        }

        /**
         * Build.
         *
         * @return DefaultRepairConfigurationProvider
         */
        public DefaultRepairConfigurationProvider build()
        {
            DefaultRepairConfigurationProvider configurationProvider = new DefaultRepairConfigurationProvider(this);
            return configurationProvider;
        }
    }
}
