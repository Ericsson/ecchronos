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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairConfigurationComparator
{
    @Test
    public void testNullNewConfigurationsReturnsFalse()
    {
        Set<ScheduledRepairJob> jobs = new HashSet<>();
        assertThat(RepairConfigurationComparator.hasChanged(jobs, null)).isFalse();
    }

    @Test
    public void testEmptyNewConfigurationsReturnsFalse()
    {
        Set<ScheduledRepairJob> jobs = new HashSet<>();
        assertThat(RepairConfigurationComparator.hasChanged(jobs, Collections.emptySet())).isFalse();
    }

    @Test
    public void testNullCurrentJobsReturnsTrue()
    {
        Set<RepairConfiguration> configs = Collections.singleton(RepairConfiguration.DEFAULT);
        assertThat(RepairConfigurationComparator.hasChanged(null, configs)).isTrue();
    }

    @Test
    public void testEmptyCurrentJobsReturnsTrue()
    {
        Set<RepairConfiguration> configs = Collections.singleton(RepairConfiguration.DEFAULT);
        assertThat(RepairConfigurationComparator.hasChanged(new HashSet<>(), configs)).isTrue();
    }

    @Test
    public void testMatchingConfigurationReturnsFalse()
    {
        ScheduledRepairJob job = mock(ScheduledRepairJob.class);
        when(job.getRepairConfiguration()).thenReturn(RepairConfiguration.DEFAULT);

        Set<ScheduledRepairJob> jobs = Collections.singleton(job);
        Set<RepairConfiguration> configs = Collections.singleton(RepairConfiguration.DEFAULT);

        assertThat(RepairConfigurationComparator.hasChanged(jobs, configs)).isFalse();
    }

    @Test
    public void testDifferentConfigurationReturnsTrue()
    {
        ScheduledRepairJob job = mock(ScheduledRepairJob.class);
        when(job.getRepairConfiguration()).thenReturn(RepairConfiguration.DEFAULT);

        RepairConfiguration updatedConfig = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.DAYS)
                .build();

        Set<ScheduledRepairJob> jobs = Collections.singleton(job);
        Set<RepairConfiguration> configs = Collections.singleton(updatedConfig);

        assertThat(RepairConfigurationComparator.hasChanged(jobs, configs)).isTrue();
    }

    @Test
    public void testMultipleConfigsAllMatchReturnsFalse()
    {
        RepairConfiguration config1 = RepairConfiguration.DEFAULT;
        RepairConfiguration config2 = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.DAYS)
                .build();

        ScheduledRepairJob job1 = mock(ScheduledRepairJob.class);
        when(job1.getRepairConfiguration()).thenReturn(config1);
        ScheduledRepairJob job2 = mock(ScheduledRepairJob.class);
        when(job2.getRepairConfiguration()).thenReturn(config2);

        Set<ScheduledRepairJob> jobs = new HashSet<>();
        jobs.add(job1);
        jobs.add(job2);

        Set<RepairConfiguration> configs = new HashSet<>();
        configs.add(config1);
        configs.add(config2);

        assertThat(RepairConfigurationComparator.hasChanged(jobs, configs)).isFalse();
    }

    @Test
    public void testMultipleConfigsPartialMatchReturnsTrue()
    {
        RepairConfiguration config1 = RepairConfiguration.DEFAULT;
        RepairConfiguration config2 = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.DAYS)
                .build();
        RepairConfiguration config3 = RepairConfiguration.newBuilder()
                .withRepairInterval(2, TimeUnit.DAYS)
                .build();

        ScheduledRepairJob job1 = mock(ScheduledRepairJob.class);
        when(job1.getRepairConfiguration()).thenReturn(config1);

        Set<ScheduledRepairJob> jobs = Collections.singleton(job1);

        Set<RepairConfiguration> configs = new HashSet<>();
        configs.add(config1);
        configs.add(config3);

        assertThat(RepairConfigurationComparator.hasChanged(jobs, configs)).isTrue();
    }
}
