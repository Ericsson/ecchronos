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

import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;

import java.util.concurrent.TimeUnit;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Verifies the global-only history_lookback_intervals config and the initial_delay &lt; repair_interval startup
 * warning (issue #1952).
 */
public class TestGlobalRepairConfig
{
    private final ListAppender<ILoggingEvent> myLogAppender = new ListAppender<>();
    private final Logger myLogger = (Logger) LoggerFactory.getLogger(GlobalRepairConfig.class);

    @Before
    public void setUp()
    {
        myLogAppender.start();
        myLogger.addAppender(myLogAppender);
    }

    @After
    public void tearDown()
    {
        myLogger.detachAppender(myLogAppender);
        myLogAppender.stop();
    }

    @Test
    public void testDefaultHistoryLookbackIntervalsIsTwo()
    {
        GlobalRepairConfig config = new GlobalRepairConfig();
        assertThat(config.getHistoryLookbackIntervals()).isEqualTo(2);
        assertThat(config.asRepairConfiguration().getHistoryLookbackIntervals()).isEqualTo(2);
    }

    @Test
    public void testHistoryLookbackIntervalsPropagatesToRepairConfiguration()
    {
        GlobalRepairConfig config = new GlobalRepairConfig();
        config.setHistoryLookbackIntervals(5);
        RepairConfiguration repairConfiguration = config.asRepairConfiguration();
        assertThat(repairConfiguration.getHistoryLookbackIntervals()).isEqualTo(5);
    }

    @Test
    public void testWarnsWhenInitialDelayBelowRepairInterval()
    {
        GlobalRepairConfig config = new GlobalRepairConfig();
        config.setRepairInterval(new Interval(7, TimeUnit.DAYS));
        config.setInitialDelay(new Interval(1, TimeUnit.HOURS)); // < interval

        config.asRepairConfiguration();

        assertThat(myLogAppender.list)
                .anyMatch(e -> e.getFormattedMessage().contains("initial_delay")
                        && e.getFormattedMessage().contains("repair_interval"));
    }

    @Test
    public void testDoesNotWarnWhenInitialDelayAtLeastRepairInterval()
    {
        GlobalRepairConfig config = new GlobalRepairConfig();
        config.setRepairInterval(new Interval(1, TimeUnit.HOURS));
        config.setInitialDelay(new Interval(1, TimeUnit.DAYS)); // >= interval

        config.asRepairConfiguration();

        assertThat(myLogAppender.list)
                .noneMatch(e -> e.getFormattedMessage().contains("initial_delay")
                        && e.getFormattedMessage().contains("repair_interval"));
    }
}
