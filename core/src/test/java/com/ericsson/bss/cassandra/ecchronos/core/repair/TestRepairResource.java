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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TestRepairResource
{
    @Test
    public void testDefaultConstructorUsesGlobalSlots()
    {
        RepairResource resource = new RepairResource("DC1", "my-resource");
        assertThat(resource.getMaxSlots()).isEqualTo(RepairResource.USE_GLOBAL_SLOTS);
    }

    @Test
    public void testExplicitMaxSlots()
    {
        RepairResource resource = new RepairResource("DC1", "ks.tbl", 1);
        assertThat(resource.getMaxSlots()).isEqualTo(1);
    }

    @Test
    public void testGetDataCenterAndResourceName()
    {
        RepairResource resource = new RepairResource("DC1", "my-resource", 5);
        assertThat(resource.getDataCenter()).isEqualTo("DC1");
        assertThat(resource.getResourceName(2)).isEqualTo("RepairResource-my-resource-2");
    }

    @Test
    public void testEqualsIgnoresMaxSlots()
    {
        RepairResource global = new RepairResource("DC1", "my-resource");
        RepairResource oneSlot = new RepairResource("DC1", "my-resource", 1);
        RepairResource manySlots = new RepairResource("DC1", "my-resource", 10);

        // Identity is datacenter + name only; slot count must not affect equality/hashCode,
        // otherwise a table resource with maxSlots=1 could fail to collide with itself.
        assertThat(oneSlot).isEqualTo(global);
        assertThat(oneSlot).isEqualTo(manySlots);
        assertThat(oneSlot.hashCode()).isEqualTo(global.hashCode());
        assertThat(oneSlot.hashCode()).isEqualTo(manySlots.hashCode());
    }

    @Test
    public void testNotEqualWhenNameOrDataCenterDiffers()
    {
        RepairResource base = new RepairResource("DC1", "my-resource", 1);
        assertThat(base).isNotEqualTo(new RepairResource("DC1", "other-resource", 1));
        assertThat(base).isNotEqualTo(new RepairResource("DC2", "my-resource", 1));
    }
}
