/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair.config;

import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRepairConfiguration
{
    private static final long DEFAULT_REPAIR_INTERVAL_IN_MS = TimeUnit.DAYS.toMillis(7);
    private static final long DEFAULT_REPAIR_WARNING_TIME_IN_MS = TimeUnit.DAYS.toMillis(8);
    private static final long DEFAULT_REPAIR_ERROR_TIME_IN_MS = TimeUnit.DAYS.toMillis(10);
    private static final RepairParallelism DEFAULT_REPAIR_PARALLELISM = RepairParallelism.PARALLEL;
    private static final double DEFAULT_REPAIR_UNWIND_RATIO = 0.0d;
    private static final long DEFAULT_TARGET_REPAIR_SIZE_IN_BYTES = Long.MAX_VALUE;
    private static final long DEFAULT_BACKOFF_IN_MS = TimeUnit.MINUTES.toMillis(30);
    private static final RepairType DEFAULT_REPAIR_TYPE = RepairType.VNODE;

    @Test
    public void testDefaultValues()
    {
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder().build();

        assertThat(RepairConfiguration.DEFAULT).isEqualTo(repairConfiguration);

        assertThat(repairConfiguration.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairConfiguration.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairConfiguration.getRepairWarningTimeInMs()).isEqualTo(DEFAULT_REPAIR_WARNING_TIME_IN_MS);
        assertThat(repairConfiguration.getRepairErrorTimeInMs()).isEqualTo(DEFAULT_REPAIR_ERROR_TIME_IN_MS);
        assertThat(repairConfiguration.getRepairUnwindRatio()).isEqualTo(DEFAULT_REPAIR_UNWIND_RATIO);
        assertThat(repairConfiguration.getTargetRepairSizeInBytes()).isEqualTo(DEFAULT_TARGET_REPAIR_SIZE_IN_BYTES);
        assertThat(repairConfiguration.getBackoffInMs()).isEqualTo(DEFAULT_BACKOFF_IN_MS);
        assertThat(repairConfiguration.getRepairType()).isEqualTo(DEFAULT_REPAIR_TYPE);
    }

    @Test
    public void testSetInterval()
    {
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.SECONDS)
                .build();

        assertThat(repairConfiguration.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairConfiguration.getRepairIntervalInMs()).isEqualTo(1000L);
        assertThat(repairConfiguration.getRepairWarningTimeInMs()).isEqualTo(DEFAULT_REPAIR_WARNING_TIME_IN_MS);
        assertThat(repairConfiguration.getRepairErrorTimeInMs()).isEqualTo(DEFAULT_REPAIR_ERROR_TIME_IN_MS);
        assertThat(repairConfiguration.getRepairUnwindRatio()).isEqualTo(DEFAULT_REPAIR_UNWIND_RATIO);
        assertThat(repairConfiguration.getTargetRepairSizeInBytes()).isEqualTo(DEFAULT_TARGET_REPAIR_SIZE_IN_BYTES);
        assertThat(repairConfiguration.getBackoffInMs()).isEqualTo(DEFAULT_BACKOFF_IN_MS);
        assertThat(repairConfiguration.getRepairType()).isEqualTo(DEFAULT_REPAIR_TYPE);
    }

    @Test
    public void testSetRepairUnwindRatio()
    {
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                .withRepairUnwindRatio(1.0d)
                .build();

        assertThat(repairConfiguration.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairConfiguration.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairConfiguration.getRepairWarningTimeInMs()).isEqualTo(DEFAULT_REPAIR_WARNING_TIME_IN_MS);
        assertThat(repairConfiguration.getRepairErrorTimeInMs()).isEqualTo(DEFAULT_REPAIR_ERROR_TIME_IN_MS);
        assertThat(repairConfiguration.getRepairUnwindRatio()).isEqualTo(1.0d);
        assertThat(repairConfiguration.getTargetRepairSizeInBytes()).isEqualTo(DEFAULT_TARGET_REPAIR_SIZE_IN_BYTES);
        assertThat(repairConfiguration.getBackoffInMs()).isEqualTo(DEFAULT_BACKOFF_IN_MS);
        assertThat(repairConfiguration.getRepairType()).isEqualTo(DEFAULT_REPAIR_TYPE);
    }

    @Test
    public void testSetTargetRepairSizeInBytes()
    {
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                .withTargetRepairSizeInBytes(1024L)
                .build();

        assertThat(repairConfiguration.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairConfiguration.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairConfiguration.getRepairWarningTimeInMs()).isEqualTo(DEFAULT_REPAIR_WARNING_TIME_IN_MS);
        assertThat(repairConfiguration.getRepairErrorTimeInMs()).isEqualTo(DEFAULT_REPAIR_ERROR_TIME_IN_MS);
        assertThat(repairConfiguration.getRepairUnwindRatio()).isEqualTo(DEFAULT_REPAIR_UNWIND_RATIO);
        assertThat(repairConfiguration.getTargetRepairSizeInBytes()).isEqualTo(1024L);
        assertThat(repairConfiguration.getBackoffInMs()).isEqualTo(DEFAULT_BACKOFF_IN_MS);
        assertThat(repairConfiguration.getRepairType()).isEqualTo(DEFAULT_REPAIR_TYPE);
    }

    @Test
    public void testSetBackoff()
    {
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                .withBackoff(1, TimeUnit.SECONDS)
                .build();

        assertThat(repairConfiguration.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairConfiguration.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairConfiguration.getRepairWarningTimeInMs()).isEqualTo(DEFAULT_REPAIR_WARNING_TIME_IN_MS);
        assertThat(repairConfiguration.getRepairErrorTimeInMs()).isEqualTo(DEFAULT_REPAIR_ERROR_TIME_IN_MS);
        assertThat(repairConfiguration.getRepairUnwindRatio()).isEqualTo(DEFAULT_REPAIR_UNWIND_RATIO);
        assertThat(repairConfiguration.getTargetRepairSizeInBytes()).isEqualTo(DEFAULT_TARGET_REPAIR_SIZE_IN_BYTES);
        assertThat(repairConfiguration.getBackoffInMs()).isEqualTo(1000L);
        assertThat(repairConfiguration.getRepairType()).isEqualTo(DEFAULT_REPAIR_TYPE);
    }

    @Test
    public void testSetRepairType()
    {
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder()
                .withRepairType(RepairType.INCREMENTAL)
                .build();

        assertThat(repairConfiguration.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairConfiguration.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairConfiguration.getRepairWarningTimeInMs()).isEqualTo(DEFAULT_REPAIR_WARNING_TIME_IN_MS);
        assertThat(repairConfiguration.getRepairErrorTimeInMs()).isEqualTo(DEFAULT_REPAIR_ERROR_TIME_IN_MS);
        assertThat(repairConfiguration.getRepairUnwindRatio()).isEqualTo(DEFAULT_REPAIR_UNWIND_RATIO);
        assertThat(repairConfiguration.getTargetRepairSizeInBytes()).isEqualTo(DEFAULT_TARGET_REPAIR_SIZE_IN_BYTES);
        assertThat(repairConfiguration.getBackoffInMs()).isEqualTo(DEFAULT_BACKOFF_IN_MS);
        assertThat(repairConfiguration.getRepairType()).isEqualTo(RepairType.INCREMENTAL);
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.forClass(RepairConfiguration.class).usingGetClass().verify();
    }
}
