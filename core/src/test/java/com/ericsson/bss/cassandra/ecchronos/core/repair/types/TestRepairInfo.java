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

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class TestRepairInfo
{
    @Test
    public void testRepairInfo()
    {
        long since = 1234L;
        long to = 12345L;
        List<RepairStats> repairStatsList = Collections.singletonList(mock(RepairStats.class));
        RepairInfo repairInfo = new RepairInfo(since, to, repairStatsList);
        assertThat(repairInfo.sinceInMs).isEqualTo(since);
        assertThat(repairInfo.toInMs).isEqualTo(to);
        assertThat(repairInfo.repairStats).isEqualTo(repairStatsList);
    }

    @Test
    public void testEquals()
    {
        EqualsVerifier.simple().forClass(RepairInfo.class).usingGetClass()
                .withNonnullFields("sinceInMs", "toInMs", "repairStats")
                .verify();
    }
}
