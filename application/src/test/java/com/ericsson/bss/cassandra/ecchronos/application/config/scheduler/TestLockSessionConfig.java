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
package com.ericsson.bss.cassandra.ecchronos.application.config.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.dataformat.yaml.YAMLMapper;

/**
 * Verifies the {@code scheduler.session_window} configuration, including backward compatibility with the legacy
 * flat form ({@code session_window: {time, unit}}) used before {@link LockSessionConfig} introduced the
 * {@code enabled}/{@code interval} structure.
 */
public class TestLockSessionConfig
{
    private final ObjectMapper myMapper = new YAMLMapper();

    @Test
    public void testDefaultsWhenNotConfigured()
    {
        SchedulerConfig config = myMapper.readValue("frequency:\n  time: 30\n  unit: SECONDS\n",
                SchedulerConfig.class);

        LockSessionConfig lockSession = config.getLockSession();
        assertThat(lockSession.isEnabled()).isTrue();
        assertThat(lockSession.getSessionWindow().getInterval(TimeUnit.MINUTES)).isEqualTo(5);
    }

    @Test
    public void testNewNestedForm()
    {
        String yaml = "session_window:\n"
                + "  enabled: false\n"
                + "  interval:\n"
                + "    time: 2\n"
                + "    unit: MINUTES\n";

        SchedulerConfig config = myMapper.readValue(yaml, SchedulerConfig.class);

        LockSessionConfig lockSession = config.getLockSession();
        assertThat(lockSession.isEnabled()).isFalse();
        assertThat(lockSession.getSessionWindow().getInterval(TimeUnit.MINUTES)).isEqualTo(2);
    }

    /**
     * A configuration file written for an earlier version, where {@code session_window} was a bare interval, must
     * keep its configured window instead of silently reverting to the default.
     */
    @Test
    public void testLegacyFlatFormIsHonoured()
    {
        String legacyYaml = "session_window:\n"
                + "  time: 10\n"
                + "  unit: MINUTES\n";

        SchedulerConfig config = myMapper.readValue(legacyYaml, SchedulerConfig.class);

        LockSessionConfig lockSession = config.getLockSession();
        // Legacy files never set enabled, so it defaults to the historical batched behaviour.
        assertThat(lockSession.isEnabled()).isTrue();
        // The configured 10 minutes must be preserved, not reset to the 5-minute default.
        assertThat(lockSession.getSessionWindow().getInterval(TimeUnit.MINUTES)).isEqualTo(10);
    }

    @Test
    public void testEnabledWithLegacyFlatWindow()
    {
        String yaml = "session_window:\n"
                + "  enabled: false\n"
                + "  time: 90\n"
                + "  unit: SECONDS\n";

        SchedulerConfig config = myMapper.readValue(yaml, SchedulerConfig.class);

        LockSessionConfig lockSession = config.getLockSession();
        assertThat(lockSession.isEnabled()).isFalse();
        assertThat(lockSession.getSessionWindow().getInterval(TimeUnit.SECONDS)).isEqualTo(90);
    }

    @Test
    public void testNestedIntervalWinsOverLegacyFlat()
    {
        // If both forms are present the new nested interval takes precedence.
        String yaml = "session_window:\n"
                + "  time: 99\n"
                + "  unit: SECONDS\n"
                + "  interval:\n"
                + "    time: 7\n"
                + "    unit: MINUTES\n";

        SchedulerConfig config = myMapper.readValue(yaml, SchedulerConfig.class);

        assertThat(config.getLockSession().getSessionWindow().getInterval(TimeUnit.MINUTES)).isEqualTo(7);
    }
}
