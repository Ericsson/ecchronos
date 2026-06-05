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
package com.ericsson.bss.cassandra.ecchronos.connection;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;

import java.io.Closeable;
import java.io.IOException;

/**
 * Provider for native connections.
 */
public interface NativeConnectionProvider extends Closeable
{
    /**
     * Returns the CQL session used for native connections.
     * @return the session
     */
    CqlSession getSession();

    /**
     * Returns the local Cassandra node.
     * @return the local node
     */
    Node getLocalNode();

    /**
     * Returns whether remote routing is enabled.
     * @return the remote routing
     */
    boolean getRemoteRouting();

    @Override
    default void close() throws IOException
    {
    }
}
