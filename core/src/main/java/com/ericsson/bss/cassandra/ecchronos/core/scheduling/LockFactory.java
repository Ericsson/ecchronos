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
package com.ericsson.bss.cassandra.ecchronos.core.scheduling;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;

/**
 * Interface for distributed lock factories.
 */
public interface LockFactory
{

    /**
     * Try to lock a distributed resource using the provided priority.
     *
     * @param dataCenter
     *            The data center the lock belongs to or null if it's a global lock.
     * @param resource
     *            The resource to lock.
     * @param priority
     *            The priority of the lock.
     * @param metadata
     *            The metadata of the lock.
     * @return The lock if able to lock the resource.
     * @throws LockException
     *             Thrown when unable to lock a resource
     */
    DistributedLock tryLock(String dataCenter, String resource, int priority, Map<String, String> metadata) throws LockException;

    /**
     * Get the metadata of a resource lock.
     *
     * @param dataCenter
     *            The data center the lock belongs to or null if it's a global lock.
     * @param resource
     *            The data center resource:
     *             i.e "RepairResource-DC1-1".
     * @return The metadata of the lock
     *          containing keyspace and table to repair.
     */
    Map<String, String> getLockMetadata(String dataCenter, String resource);

    /**
     * Checks if local_quorum is met.
     *
     * @param dataCenter
     *            The data center the lock belongs to or null if it's a global lock.
     * @param resource
     *            The data center resource.
     *             i.e "RepairResource-DC1-1".
     * @return boolean
     *            Indicates if local_quorum is met.
     */
    boolean sufficientNodesForLocking(String dataCenter, String resource);

    /**
     * Utility method to return a cached lock exception if one is available.
     *
     * @param dataCenter The data center the lock is for or null if it's a global lock.
     * @param resource The resource the lock is for.
     * @return A cached exception if available.
     */
    default Optional<LockException> getCachedLockException(String dataCenter, String resource)
    {
        return Optional.empty();
    }

    /**
     * A locked resource that gets released by the call of the {@link DistributedLock#close() close()} method.
     */
    interface DistributedLock extends Closeable
    {
        /**
         * Releases the locked resource.
         */
        @Override
        void close();
    }
}
