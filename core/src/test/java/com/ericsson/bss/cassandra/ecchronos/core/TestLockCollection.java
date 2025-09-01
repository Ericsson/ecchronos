/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import com.ericsson.bss.cassandra.ecchronos.core.scheduling.DummyLock;
import org.junit.Test;

import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory.DistributedLock;

public class TestLockCollection
{

    @Test
    public void testCloseAllLocks()
    {
        List<DummyLock> locks = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            locks.add(new DummyLock());
        }

        new LockCollection(locks).close();

        for (DummyLock lock : locks)
        {
            assertThat(lock.closed).isTrue();
        }
    }

    @Test
    public void testCloseAllLocksOneThrowing()
    {
        List<DistributedLock> locks = new ArrayList<>();
        for (int i = 0; i < 4; i++)
        {
            locks.add(new DummyLock());
        }

        locks.add(new ThrowingLock());

        for (int i = 0; i < 5; i++)
        {
            locks.add(new DummyLock());
        }

        new LockCollection(locks).close();

        for (DistributedLock lock : locks)
        {
            if (lock instanceof DummyLock dummyLock)
            {
                assertThat(dummyLock.closed).isTrue();
            }
        }
    }

    private class ThrowingLock implements DistributedLock
    {
        @Override
        public void close()
        {
            throw new IllegalStateException();
        }
    }
}
