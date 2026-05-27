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
package com.ericsson.bss.cassandra.ecchronos.core.impl.jmx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.jolokia.json.JSONObject;
import org.junit.Test;

import javax.management.openmbean.CompositeData;
import java.util.Arrays;
import java.util.Collections;

public class TestRepairStatsParser
{
    @Test
    public void testNullInputReturnsZero()
    {
        assertThat(RepairStatsParser.extractMaxRepairedValue(null, false)).isZero();
        assertThat(RepairStatsParser.extractMaxRepairedValue(null, true)).isZero();
    }

    @Test
    public void testNonListInputReturnsZero()
    {
        assertThat(RepairStatsParser.extractMaxRepairedValue("not a list", false)).isZero();
        assertThat(RepairStatsParser.extractMaxRepairedValue(42, true)).isZero();
    }

    @Test
    public void testEmptyListReturnsZero()
    {
        assertThat(RepairStatsParser.extractMaxRepairedValue(Collections.emptyList(), false)).isZero();
        assertThat(RepairStatsParser.extractMaxRepairedValue(Collections.emptyList(), true)).isZero();
    }

    @Test
    public void testJolokiaJsonObjectExtraction()
    {
        JSONObject json = new JSONObject();
        json.put("maxRepaired", 12345L);

        long result = RepairStatsParser.extractMaxRepairedValue(Arrays.asList(json), true);
        assertThat(result).isEqualTo(12345L);
    }

    @Test
    public void testJolokiaJsonObjectZeroValueSkipped()
    {
        JSONObject zero = new JSONObject();
        zero.put("maxRepaired", 0L);
        JSONObject nonZero = new JSONObject();
        nonZero.put("maxRepaired", 999L);

        long result = RepairStatsParser.extractMaxRepairedValue(Arrays.asList(zero, nonZero), true);
        assertThat(result).isEqualTo(999L);
    }

    @Test
    public void testCompositeDataExtraction()
    {
        CompositeData data = mock(CompositeData.class);
        when(data.getAll(new String[]{"maxRepaired"})).thenReturn(new Object[]{54321L});

        long result = RepairStatsParser.extractMaxRepairedValue(Arrays.asList(data), false);
        assertThat(result).isEqualTo(54321L);
    }

    @Test
    public void testJolokiaCompositeDataFallback()
    {
        CompositeData data = mock(CompositeData.class);
        when(data.getAll(new String[]{"maxRepaired"})).thenReturn(new Object[]{777L});

        long result = RepairStatsParser.extractMaxRepairedValue(Arrays.asList(data), true);
        assertThat(result).isEqualTo(777L);
    }
}
