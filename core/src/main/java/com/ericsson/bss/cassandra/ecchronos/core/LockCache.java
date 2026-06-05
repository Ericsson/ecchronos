/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory.DistributedLock;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/** Caches distributed locks to reduce Cassandra round-trips. */
public final class LockCache
{
    private static final Logger LOG = LoggerFactory.getLogger(LockCache.class);

    private final Cache<LockKey, LockException> myFailureCache;
    private final LockSupplier myLockSupplier;

    /**
     * Constructs a new LockCache.
     * @param lockSupplier the lock supplier
     * @param expireTimeInSeconds the expire time in seconds
     */
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

    /**
     * Returns the cached failure.
     * @param dataCenter the data center
     * @param resource the resource to lock
     * @return the cached failure
     */
    public Optional<LockException> getCachedFailure(final String dataCenter, final String resource)
    {
        return getCachedFailure(new LockKey(dataCenter, resource));
    }

    /**
     * Returns the lock.
     * @param dataCenter the data center
     * @param resource the resource to lock
     * @param priority the job priority value
     * @param metadata the associated metadata
     * @return the lock
     * @throws LockException if the lock cannot be acquired
     */
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
        LOG.debug("Encountered cached locking failure, throwing exception {}", e.getMessage());
        throw e;
    }

    private Optional<LockException> getCachedFailure(final LockKey lockKey)
    {
        return Optional.ofNullable(myFailureCache.getIfPresent(lockKey));
    }

    /** Functional interface for supplying distributed locks. */
    @FunctionalInterface
    public interface LockSupplier
    {
        /**
         * Returns the lock.
         * @param dataCenter the data center
         * @param resource the resource to lock
         * @param priority the job priority value
         * @param metadata the associated metadata
         * @return the lock
         * @throws LockException if the lock cannot be acquired
         */
        DistributedLock getLock(String dataCenter, String resource, int priority, Map<String, String> metadata)
                throws LockException;
    }

    record LockKey(String dataCenter, @NotNull String resource)
    {
        LockKey
        {
            checkNotNull(resource);
        }
    }
}
