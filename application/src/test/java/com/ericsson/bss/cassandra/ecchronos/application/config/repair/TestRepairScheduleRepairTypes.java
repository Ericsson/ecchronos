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
package com.ericsson.bss.cassandra.ecchronos.application.config.repair;

import static org.assertj.core.api.Assertions.assertThat;

import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLMapper;

public class TestRepairScheduleRepairTypes
{
    @Test
    public void testGetConfiguredRepairTypesReportsEachTableType() throws Exception
    {
        RepairSchedule schedule = load("repair/mixed_repair_types_schedule.yml");

        List<RepairSchedule.TableRepairType> types = schedule.getConfiguredRepairTypes();

        assertThat(types).hasSize(2);
        assertThat(types).extracting(RepairSchedule.TableRepairType::getTable,
                        RepairSchedule.TableRepairType::getRepairType)
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple("tb_unified", RepairType.UNIFIED_VNODE),
                        org.assertj.core.groups.Tuple.tuple("tb_legacy", RepairType.INCREMENTAL));
        assertThat(types).allSatisfy(t -> assertThat(t.getKeyspace()).isEqualTo("ks1"));
    }

    @Test
    public void testMixedFamiliesAreDetectable() throws Exception
    {
        RepairSchedule schedule = load("repair/mixed_repair_types_schedule.yml");

        List<Boolean> unifiedFlags = schedule.getConfiguredRepairTypes().stream()
                .map(t -> t.getRepairType().isUnified())
                .collect(Collectors.toList());

        // One unified, one legacy -> the two families are present, which is what the startup validation rejects.
        assertThat(unifiedFlags).containsExactlyInAnyOrder(true, false);
    }

    @Test
    public void testRepairTypeFamilyClassification()
    {
        assertThat(RepairType.UNIFIED_VNODE.isUnified()).isTrue();
        assertThat(RepairType.VNODE.isUnified()).isFalse();
        assertThat(RepairType.PARALLEL_VNODE.isUnified()).isFalse();
        assertThat(RepairType.INCREMENTAL.isUnified()).isFalse();
    }

    private static RepairSchedule load(final String resource) throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource(resource).getFile());
        ObjectMapper objectMapper = new YAMLMapper();
        return objectMapper.readValue(file, RepairSchedule.class);
    }
}
