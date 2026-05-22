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
package com.ericsson.bss.cassandra.ecchronos.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestOnDemandRepairRequestValidator
{
    @Mock
    private OnDemandRepairScheduler myOnDemandRepairScheduler;

    @Mock
    private ReplicatedTableProvider myReplicatedTableProvider;

    @Mock
    private DistributedNativeConnectionProvider myDistributedNativeConnectionProvider;

    @Mock
    private TableReference myTableReference;

    @Mock
    private RepairConfiguration myRepairConfiguration;

    @Mock
    private Node myNode;

    private OnDemandRepairRequestValidator validator;

    @Before
    public void setup()
    {
        validator = new OnDemandRepairRequestValidator(
                myOnDemandRepairScheduler, myReplicatedTableProvider, myDistributedNativeConnectionProvider);
        when(myOnDemandRepairScheduler.getRepairConfiguration()).thenReturn(myRepairConfiguration);
    }

    @Test
    public void testCheckValidClusterRunThrowsWhenNoNodeAndNotAll()
    {
        assertThatThrownBy(() -> validator.checkValidClusterRun(null, false, "ks", null))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("parameter all should be true");
    }

    @Test
    public void testCheckValidClusterRunThrowsWhenTableWithoutKeyspace()
    {
        assertThatThrownBy(() -> validator.checkValidClusterRun("node1", false, null, "tbl"))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("Keyspace must be provided");
    }

    @Test
    public void testCheckValidClusterRunPassesWithValidInput()
    {
        validator.checkValidClusterRun(null, true, "ks", "tbl");
        validator.checkValidClusterRun("node1", false, "ks", null);
    }

    @Test
    public void testValidateNodeExistsThrowsForUnknownNode()
    {
        UUID unknownNode = UUID.randomUUID();
        Map<UUID, Node> nodes = new HashMap<>();
        when(myDistributedNativeConnectionProvider.getNodes()).thenReturn(nodes);

        assertThatThrownBy(() -> validator.validateNodeExists(unknownNode))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("not a valid node");
    }

    @Test
    public void testValidateNodeExistsPassesForNull()
    {
        validator.validateNodeExists(null);
    }

    @Test
    public void testRejectForTWCSReturnsTrueWhenTWCSAndNotForced()
    {
        when(myTableReference.getTwcs()).thenReturn(true);
        when(myRepairConfiguration.getIgnoreTWCSTables()).thenReturn(true);

        assertThat(validator.rejectForTWCS(myTableReference, false)).isTrue();
    }

    @Test
    public void testRejectForTWCSReturnsFalseWhenForced()
    {
        when(myTableReference.getTwcs()).thenReturn(true);
        when(myRepairConfiguration.getIgnoreTWCSTables()).thenReturn(true);

        assertThat(validator.rejectForTWCS(myTableReference, true)).isFalse();
    }

    @Test
    public void testGetRepairTypeOrDefaultReturnsVnodeWhenNull()
    {
        assertThat(validator.getRepairTypeOrDefault(null)).isEqualTo(RepairType.VNODE);
    }

    @Test
    public void testGetRepairTypeOrDefaultReturnsProvidedType()
    {
        assertThat(validator.getRepairTypeOrDefault(RepairType.INCREMENTAL)).isEqualTo(RepairType.INCREMENTAL);
    }
}
