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

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockClientSaturationException;

import java.util.UUID;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class TestLockFailureBackoff
{
    @Test
    public void testSaturationBackoffRemainsActiveAfterManyConsecutiveTimeouts()
    {
        ScheduledJob job = mock(ScheduledJob.class);
        LockFailureBackoff backoff = new LockFailureBackoff(1000L, null);
        UUID nodeID = UUID.randomUUID();

        for (int i = 0; i < 63; i++)
        {
            backoff.recordFailure(
                    job,
                    new LockClientSaturationException("client timeout"),
                    nodeID);
        }

        assertThat(backoff.isInBackoff(job)).isTrue();

        backoff.recordFailure(
                job,
                new LockClientSaturationException("client timeout"),
                nodeID);

        assertThat(backoff.isInBackoff(job)).isTrue();
    }
}
