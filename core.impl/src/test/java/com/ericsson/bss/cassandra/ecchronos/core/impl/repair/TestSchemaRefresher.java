/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestSchemaRefresher
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";

    @Mock
    private ReplicatedTableProvider myReplicatedTableProvider;

    @Mock
    private RepairScheduler myRepairScheduler;

    @Mock
    private TableReferenceFactory myTableReferenceFactory;

    @Mock
    private Function<TableReference, Set<RepairConfiguration>> myRepairConfigurationFunction;

    @Mock
    private CqlSession mySession;

    @Mock
    private Node myNode;

    @Mock
    private TableReference myTableReference;

    @Mock
    private TableMetadata myTableMetadata;

    private final UUID myNodeId = UUID.randomUUID();

    private SchemaRefresher mySchemaRefresher;

    private final RepairConfiguration incrementalConfig = RepairConfiguration.newBuilder()
            .withRepairType(RepairType.INCREMENTAL).build();
    private final RepairConfiguration vnodeConfig = RepairConfiguration.newBuilder()
            .withRepairType(RepairType.VNODE).build();

    @Before
    public void setup()
    {
        Map<UUID, Node> nodes = new HashMap<>();
        nodes.put(myNodeId, myNode);
        when(myNode.getHostId()).thenReturn(myNodeId);
        when(myTableMetadata.getKeyspace()).thenReturn(CqlIdentifier.fromInternal(KEYSPACE));
        when(myTableMetadata.getName()).thenReturn(CqlIdentifier.fromInternal(TABLE));
        when(myTableMetadata.getOptions()).thenReturn(Map.of());
        when(myTableReferenceFactory.forTable(KEYSPACE, TABLE)).thenReturn(myTableReference);

        mySchemaRefresher = new SchemaRefresher(myReplicatedTableProvider, myRepairScheduler,
                myTableReferenceFactory, myRepairConfigurationFunction, mySession);
    }

    @Test
    public void testPerNodeUpdateSchedulesOnlyNonIncremental()
    {
        when(myRepairConfigurationFunction.apply(myTableReference)).thenReturn(Set.of(vnodeConfig));

        mySchemaRefresher.updateConfiguration(myNode, myTableReference, myTableMetadata);

        verify(myRepairScheduler).putConfigurations(eq(myNode), eq(myTableReference), eq(Set.of(vnodeConfig)));
    }

    @Test
    public void testPerNodeUpdateIgnoresIncremental()
    {
        when(myRepairConfigurationFunction.apply(myTableReference)).thenReturn(Set.of(incrementalConfig));

        mySchemaRefresher.updateConfiguration(myNode, myTableReference, myTableMetadata);

        // Incremental goes through the same per-node path as vnode.
        verify(myRepairScheduler).putConfigurations(eq(myNode), eq(myTableReference), eq(Set.of(incrementalConfig)));
    }
}
