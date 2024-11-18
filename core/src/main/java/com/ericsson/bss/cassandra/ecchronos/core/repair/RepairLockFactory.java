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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A locking factory for repair jobs.
 */
public interface RepairLockFactory
{
    /**
     * Take a collected lock based on the repair resources provided.
     *
     * @param lockFactory The base lock factory to use.
     * @param repairResources The repair resources to lock.
     * @param metadata The metadata to add to the locks.
     * @param priority The priority of the repair resources.
     * @param nodeId
     * @return The collected lock for the repair resources.
     * @throws LockException Thrown in case there is an issue with taking the locks for the repair resources.
     */
    LockFactory.DistributedLock getLock(LockFactory lockFactory,
                                        Set<RepairResource> repairResources,
                                        Map<String, String> metadata,
                                        int priority,
                                        UUID nodeId) throws LockException;
}
