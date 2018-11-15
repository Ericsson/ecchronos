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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRepairResource
{
    @Test
    public void testRepairResource()
    {
        RepairResource repairResource = new RepairResource("dc1", "my-resource");

        assertThat(repairResource.getDataCenter()).isEqualTo("dc1");
        assertThat(repairResource.getResourceName(1)).isEqualTo("RepairResource-my-resource-1");
        assertThat(repairResource.getResourceName(2)).isEqualTo("RepairResource-my-resource-2");
    }

    @Test
    public void testRepairResourceEquality()
    {
        RepairResource repairResource = new RepairResource("dc1", "my-resource");
        RepairResource equalRepairResource = new RepairResource("dc1", "my-resource");
        RepairResource repairResourceWithDifferentDc = new RepairResource("dc2", "my-resource");
        RepairResource repairResourceWithDifferentResource = new RepairResource("dc1", "not-my-resource");

        assertThat(repairResource).isEqualTo(equalRepairResource);
        assertThat(repairResource).isNotEqualTo(repairResourceWithDifferentDc);
        assertThat(repairResource).isNotEqualTo(repairResourceWithDifferentResource);
    }
}
