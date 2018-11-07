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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.ericsson.bss.cassandra.ecchronos.core.TokenUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class TestReplicatedTableProviderImpl
{
    @Mock
    private Metadata myMetadata;

    @Mock
    private Host myLocalhost;

    @Mock
    private Host myRemoteHost;

    private List<KeyspaceMetadata> myKeyspaces = new ArrayList<>();

    private Set<Host> myHosts = new HashSet<>();

    private Set<TokenRange> tokenRanges = new HashSet<>();

    private ReplicatedTableProviderImpl myReplicatedTableProviderImpl;

    @Before
    public void init() throws Exception
    {
        tokenRanges.add(TokenUtil.getRange(1, 2));

        myHosts.add(myLocalhost);
        myHosts.add(myRemoteHost);

        doReturn(myKeyspaces).when(myMetadata).getKeyspaces();

        myReplicatedTableProviderImpl = new ReplicatedTableProviderImpl(myLocalhost, myMetadata);
    }

    @Test
    public void testAcceptTableForLocalDC()
    {
        String keyspace = "keyspace";

        doReturn(myHosts).when(myMetadata).getReplicas(eq(keyspace), any(TokenRange.class));
        doReturn(tokenRanges).when(myMetadata).getTokenRanges(eq(keyspace), eq(myLocalhost));

        assertThat(myReplicatedTableProviderImpl.accept(keyspace)).isTrue();
    }

    @Test
    public void testDoesNotAcceptTableForOtherDC()
    {
        String keyspace = "keyspace";

        // No local ranges for the keyspace and verify it isn't retrieved for scheduling
        doReturn(myHosts).when(myMetadata).getReplicas(eq(keyspace), any(TokenRange.class));
        doReturn(Sets.newHashSet()).when(myMetadata).getTokenRanges(eq(keyspace), eq(myLocalhost));

        assertThat(myReplicatedTableProviderImpl.accept(keyspace)).isFalse();

        // Add local ranges for the keyspace and verify it is retrieved for scheduling
        doReturn(tokenRanges).when(myMetadata).getTokenRanges(eq(keyspace), eq(myLocalhost));

        assertThat(myReplicatedTableProviderImpl.accept(keyspace)).isTrue();
    }

    @Test
    public void testAcceptSystemAuth()
    {
        String keyspace = "system_auth";

        doReturn(myHosts).when(myMetadata).getReplicas(eq(keyspace), any(TokenRange.class));
        doReturn(tokenRanges).when(myMetadata).getTokenRanges(eq(keyspace), eq(myLocalhost));

        assertThat(myReplicatedTableProviderImpl.accept(keyspace)).isTrue();
    }

    @Test
    public void testDoesNotAcceptSystemDistributed()
    {
        String keyspace = "system_distributed";

        doReturn(myHosts).when(myMetadata).getReplicas(eq(keyspace), any(TokenRange.class));
        doReturn(tokenRanges).when(myMetadata).getTokenRanges(eq(keyspace), eq(myLocalhost));

        assertThat(myReplicatedTableProviderImpl.accept(keyspace)).isFalse();
    }

    @Test
    public void testGetAllJobs()
    {
        mockReplicatedKeyspace("system_auth", "roles", "role_members", "role_permissions");
        mockReplicatedKeyspace("system_distributed", "parent_repair_history", "repair_history");
        mockReplicatedKeyspace("user_keyspace", "table1", "table2");

        TableReference[] expectedTableReferences = new TableReference[] {
                new TableReference("system_auth", "roles"),
                new TableReference("system_auth", "role_members"),
                new TableReference("system_auth", "role_permissions"),
                new TableReference("user_keyspace", "table1"),
                new TableReference("user_keyspace", "table2")
        };

        assertThat(myReplicatedTableProviderImpl.getAll()).containsExactlyInAnyOrder(expectedTableReferences);
    }

    private void mockReplicatedKeyspace(String keyspace, String... tables)
    {
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);

        doReturn(keyspace).when(keyspaceMetadata).getName();

        List<TableMetadata> tableMetadatas = new ArrayList<>();

        for (String table : tables)
        {
            TableMetadata tableMetadata = mock(TableMetadata.class);

            doReturn(table).when(tableMetadata).getName();
            doReturn(keyspaceMetadata).when(tableMetadata).getKeyspace();

            tableMetadatas.add(tableMetadata);
        }

        doReturn(tableMetadatas).when(keyspaceMetadata).getTables();

        doReturn(myHosts).when(myMetadata).getReplicas(eq(keyspace), any(TokenRange.class));
        doReturn(tokenRanges).when(myMetadata).getTokenRanges(eq(keyspace), eq(myLocalhost));

        myKeyspaces.add(keyspaceMetadata);
    }
}
