/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair.types;

import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairJobView.Status;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOnDemandRepair
{
    @Test
    public void testCompleted()
    {
        long completedAt = System.currentTimeMillis();
        UUID id = UUID.randomUUID();
        UUID hostId = UUID.randomUUID();

        OnDemandRepairJobView repairJobView = new TestUtils.OnDemandRepairJobBuilder()
                .withId(id)
                .withHostId(hostId)
                .withKeyspace("ks")
                .withTable("tb")
                .withCompletedAt(completedAt)
                .withStatus(Status.COMPLETED)
                .withProgress(1.0d)
                .build();

        OnDemandRepair onDemandRepair = new OnDemandRepair(repairJobView);

        assertThat(onDemandRepair.id).isEqualTo(id);
        assertThat(onDemandRepair.hostId).isEqualTo(hostId);
        assertThat(onDemandRepair.keyspace).isEqualTo("ks");
        assertThat(onDemandRepair.table).isEqualTo("tb");
        assertThat(onDemandRepair.repairedRatio).isEqualTo(1.0d);
        assertThat(onDemandRepair.status).isEqualTo(Status.COMPLETED);
        assertThat(onDemandRepair.completedAt).isEqualTo(completedAt);
    }

    @Test
    public void testFailed()
    {
        long completedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(11);
        UUID id = UUID.randomUUID();
        UUID hostId = UUID.randomUUID();

        OnDemandRepairJobView repairJobView = new TestUtils.OnDemandRepairJobBuilder()
                .withId(id)
                .withHostId(hostId)
                .withKeyspace("ks")
                .withTable("tb")
                .withCompletedAt(completedAt)
                .withStatus(Status.ERROR)
                .withProgress(0.5d)
                .build();
        OnDemandRepair onDemandRepair = new OnDemandRepair(repairJobView);

        assertThat(onDemandRepair.id).isEqualTo(id);
        assertThat(onDemandRepair.hostId).isEqualTo(hostId);
        assertThat(onDemandRepair.keyspace).isEqualTo("ks");
        assertThat(onDemandRepair.table).isEqualTo("tb");
        assertThat(onDemandRepair.repairedRatio).isEqualTo(0.5d);
        assertThat(onDemandRepair.completedAt).isEqualTo(completedAt);
        assertThat(onDemandRepair.status).isEqualTo(Status.ERROR);
    }

    @Test
    public void testInProgress()
    {
        long completedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(6);
        OnDemandRepairJobView repairJobView = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withCompletedAt(completedAt)
                .build();

        OnDemandRepair onDemandRepair = new OnDemandRepair(repairJobView);

        assertThat(onDemandRepair.completedAt).isEqualTo(completedAt);
        assertThat(onDemandRepair.status).isEqualTo(Status.IN_QUEUE);
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.simple().forClass(OnDemandRepair.class).usingGetClass()
                .withNonnullFields("id", "hostId", "keyspace", "table")
                .verify();
    }
}
