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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.GlobalRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.Interval;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * Regression tests for the repair-history lookback resolution. A previous double unit conversion turned the
 * already-in-milliseconds interval into an astronomically large value, which underflowed the history query window
 * to a negative start time and returned no rows (so every table looked never-repaired on startup). See issue #1950.
 */
public class TestBeanConfiguratorRepairHistoryLookback
{
    @Test
    public void testDefaultThirtyDaysResolvesToMilliseconds()
    {
        assertThat(resolve(new Interval(30, TimeUnit.DAYS)))
                .isEqualTo(TimeUnit.DAYS.toMillis(30))
                .isEqualTo(2_592_000_000L);
    }

    @Test
    public void testOneHourResolvesToMilliseconds()
    {
        assertThat(resolve(new Interval(1, TimeUnit.HOURS)))
                .isEqualTo(3_600_000L);
    }

    @Test
    public void testThirtyMinutesResolvesToMilliseconds()
    {
        assertThat(resolve(new Interval(30, TimeUnit.MINUTES)))
                .isEqualTo(1_800_000L);
    }

    @Test
    public void testAlreadyMillisecondsIsNotConvertedAgain()
    {
        // Guards against the double conversion specifically: an interval configured in MILLISECONDS must pass
        // through unchanged.
        assertThat(resolve(new Interval(5_000, TimeUnit.MILLISECONDS)))
                .isEqualTo(5_000L);
    }

    private static long resolve(final Interval lookback)
    {
        GlobalRepairConfig repairConfig = new GlobalRepairConfig();
        repairConfig.setRepairHistoryLookback(lookback);
        Config config = new Config();
        config.setRepairConfig(repairConfig);
        return BeanConfigurator.resolveRepairHistoryLookbackMs(config);
    }
}
