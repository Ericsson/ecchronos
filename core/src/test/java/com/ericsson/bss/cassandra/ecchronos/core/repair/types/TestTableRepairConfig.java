/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import java.util.UUID;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTableRepairConfig
{
    @Test
    public void testFullyRepairedJob()
    {
        // Given
        UUID id = UUID.randomUUID();
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(id, tableReference("ks", "tbl"), repairConfig,
                null, RepairJobView.Status.COMPLETED, 0);

        // When
        TableRepairConfig tableRepairConfig = new TableRepairConfig(repairJobView);
        // Then
        assertThat(tableRepairConfig.id).isEqualTo(id);
        assertThat(tableRepairConfig.keyspace).isEqualTo("ks");
        assertThat(tableRepairConfig.table).isEqualTo("tbl");
        assertThat(tableRepairConfig.repairIntervalInMs).isEqualTo(11);
        assertThat(tableRepairConfig.repairParallelism).isEqualTo(RepairParallelism.PARALLEL);
        assertThat(tableRepairConfig.repairUnwindRatio).isEqualTo(2.2);
        assertThat(tableRepairConfig.repairWarningTimeInMs).isEqualTo(33);
        assertThat(tableRepairConfig.repairErrorTimeInMs).isEqualTo(44);
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.forClass(TableRepairConfig.class).usingGetClass().verify();
    }
}
