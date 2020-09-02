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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lock implementation covering multiple distributed locks.
 * <p>
 * Closes all underlying locks when closed.
 */
public class LockCollection implements LockFactory.DistributedLock
{
    private static final Logger LOG = LoggerFactory.getLogger(LockCollection.class);

    private final List<LockFactory.DistributedLock> myLocks;

    public LockCollection(Collection<? extends LockFactory.DistributedLock> locks)
    {
        myLocks = new ArrayList<>(locks);
    }

    @Override
    public void close()
    {
        for (LockFactory.DistributedLock lock : myLocks) // NOPMD
        {
            try
            {
                lock.close();
            }
            catch (Exception e)
            {
                LOG.warn("Unable to release lock {} ", lock, e);
            }
        }
    }
}
