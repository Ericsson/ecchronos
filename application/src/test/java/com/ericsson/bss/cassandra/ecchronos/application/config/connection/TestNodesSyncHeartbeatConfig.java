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
package com.ericsson.bss.cassandra.ecchronos.application.config.connection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestNodesSyncHeartbeatConfig
{
    @Test
    void defaultsAreOneHourIntervalAndThreeHourTtl()
    {
        NodesSyncHeartbeatConfig config = new NodesSyncHeartbeatConfig();
        assertThat(config.getIntervalInMs()).isEqualTo(60 * 60 * 1000L);
        // TTL unset -> derived as 3x interval = 3 hours = 10800 seconds.
        assertThat(config.getTtlInSeconds()).isEqualTo(3 * 60 * 60);
    }

    @Test
    void ttlDefaultsToThreeTimesIntervalWhenUnset()
    {
        NodesSyncHeartbeatConfig config = new NodesSyncHeartbeatConfig();
        config.setUnit("seconds");
        config.setInterval(40L);
        // No ttl set -> 3x interval = 120 seconds.
        assertThat(config.getTtlInSeconds()).isEqualTo(120);
    }

    @Test
    void unitAppliesToBothIntervalAndTtl()
    {
        NodesSyncHeartbeatConfig config = new NodesSyncHeartbeatConfig();
        config.setUnit("seconds");
        config.setInterval(30L);
        config.setTtl(90L);
        assertThat(config.getIntervalInMs()).isEqualTo(30_000L);
        assertThat(config.getTtlInSeconds()).isEqualTo(90);
    }

    @Test
    void ttlIsConvertedToSecondsFromTheSharedUnit()
    {
        NodesSyncHeartbeatConfig config = new NodesSyncHeartbeatConfig();
        config.setUnit("minutes");
        config.setInterval(2L);
        config.setTtl(6L);
        assertThat(config.getIntervalInMs()).isEqualTo(120_000L);
        // 6 minutes -> 360 seconds.
        assertThat(config.getTtlInSeconds()).isEqualTo(360);
    }

    @Test
    void ttlNotGreaterThanIntervalIsRejected()
    {
        NodesSyncHeartbeatConfig config = new NodesSyncHeartbeatConfig();
        config.setUnit("seconds");
        config.setInterval(60L);
        config.setTtl(60L); // equal -> invalid
        assertThatThrownBy(config::getTtlInSeconds)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be greater than");
    }

    @Test
    void ttlSmallerThanIntervalIsRejected()
    {
        NodesSyncHeartbeatConfig config = new NodesSyncHeartbeatConfig();
        config.setUnit("seconds");
        config.setInterval(60L);
        config.setTtl(30L);
        assertThatThrownBy(config::getIntervalInMs)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be greater than");
    }
}
