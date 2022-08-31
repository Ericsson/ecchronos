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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRepairStats
{
    @Test
    public void testRepairStats()
    {
        String keyspace = "foo";
        String table = "bar";
        Double ratio = 1.0;
        long repairTimeTaken = 123L;
        RepairStats repairStats = new RepairStats(keyspace, table, ratio, repairTimeTaken);
        assertThat(repairStats.keyspace).isEqualTo(keyspace);
        assertThat(repairStats.table).isEqualTo(table);
        assertThat(repairStats.repairedRatio).isEqualTo(ratio);
        assertThat(repairStats.repairTimeTakenMs).isEqualTo(repairTimeTaken);
    }

    @Test
    public void testEquals()
    {
        EqualsVerifier.simple().forClass(RepairStats.class).usingGetClass()
                .withNonnullFields("keyspace", "table", "repairedRatio", "repairTimeTakenMs")
                .verify();
    }
}
