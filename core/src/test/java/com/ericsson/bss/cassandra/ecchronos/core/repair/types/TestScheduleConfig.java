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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import java.util.UUID;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;

public class TestScheduleConfig
{
    @Test
    public void testFullyRepairedConfig()
    {
        UUID id = UUID.randomUUID();
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new ScheduledRepairJobView(id, tableReference("ks", "tbl"), repairConfig,
                null, RepairJobView.Status.COMPLETED, 0, 0);

        ScheduleConfig config = new ScheduleConfig(repairJobView);

        assertThat(config.intervalInMs).isEqualTo(11);
        assertThat(config.parallelism).isEqualTo(RepairParallelism.PARALLEL);
        assertThat(config.unwindRatio).isEqualTo(2.2);
        assertThat(config.warningTimeInMs).isEqualTo(33);
        assertThat(config.errorTimeInMs).isEqualTo(44);
        assertThat(config).isEqualTo(new ScheduleConfig(repairJobView));
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.simple().forClass(ScheduleConfig.class).usingGetClass().verify();
    }
}
