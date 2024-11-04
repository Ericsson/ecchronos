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
package com.ericsson.bss.cassandra.ecchronos.core.impl.locks;

import static com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory.DistributedLock;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public final class LockCache
{
    private static final Logger LOG = LoggerFactory.getLogger(LockCache.class);

    private final Cache<LockKey, LockException> myFailureCache;
    private final LockSupplier myLockSupplier;

    public LockCache(final LockSupplier lockSupplier, final long expireTimeInSeconds)
    {
        this(lockSupplier, expireTimeInSeconds, TimeUnit.SECONDS);
    }

    LockCache(final LockSupplier lockSupplier, final long expireTime, final TimeUnit expireTimeUnit)
    {
        myLockSupplier = lockSupplier;

        myFailureCache = Caffeine.newBuilder()
                .expireAfterWrite(expireTime, expireTimeUnit)
                .executor(Runnable::run)
                .build();
    }

    public Optional<LockException> getCachedFailure(final String dataCenter, final String resource)
    {
        return getCachedFailure(new LockKey(dataCenter, resource));
    }

    public DistributedLock getLock(final String dataCenter,
                                   final String resource,
                                   final int priority,
                                   final Map<String, String> metadata)
                                                                       throws LockException
    {
        LockKey lockKey = new LockKey(dataCenter, resource);

        Optional<LockException> cachedFailure = getCachedFailure(lockKey);

        if (cachedFailure.isPresent())
        {
            throwCachedLockException(cachedFailure.get());
        }

        try
        {
            return myLockSupplier.getLock(dataCenter, resource, priority, metadata);
        }
        catch (LockException e)
        {
            myFailureCache.put(lockKey, e);
            throw e;
        }
    }

    private void throwCachedLockException(final LockException e) throws LockException
    {
        LOG.debug("Encountered cached locking failure, throwing exception", e);
        throw e;
    }

    private Optional<LockException> getCachedFailure(final LockKey lockKey)
    {
        return Optional.ofNullable(myFailureCache.getIfPresent(lockKey));
    }

    @FunctionalInterface
    public interface LockSupplier
    {
        DistributedLock getLock(String dataCenter, String resource, int priority, Map<String, String> metadata)
                                                                                                                throws LockException;
    }

    static final class LockKey
    {
        private final String myDataCenter;
        private final String myResourceName;

        LockKey(final String dataCenter, final String resourceName)
        {
            myDataCenter = dataCenter;
            myResourceName = checkNotNull(resourceName);
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            LockKey lockKey = (LockKey) o;
            return Objects.equals(myDataCenter, lockKey.myDataCenter)
                    && Objects.equals(myResourceName, lockKey.myResourceName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(myDataCenter, myResourceName);
        }
    }
}
