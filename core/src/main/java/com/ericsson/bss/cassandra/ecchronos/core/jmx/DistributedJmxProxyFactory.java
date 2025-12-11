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
package com.ericsson.bss.cassandra.ecchronos.core.jmx;

import java.io.IOException;

public interface DistributedJmxProxyFactory
{
    /**
     * Connect to the local Cassandra node and get a proxy instance.
     * <p>
     * The returned {@link DistributedJmxProxy} must be closed by the caller.
     *
     * @return The connected {@link DistributedJmxProxy}.
     * @throws IOException Thrown when unable to connect.
     */
    DistributedJmxProxy connect() throws IOException;
    Integer getMyHeathCheckInterval();
}

