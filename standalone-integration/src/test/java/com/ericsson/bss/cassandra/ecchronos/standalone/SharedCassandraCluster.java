/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.standalone;

import cassandracluster.AbstractCassandraCluster;

import java.io.IOException;

public class SharedCassandraCluster extends AbstractCassandraCluster
{
    private static volatile boolean initialized = false;
    private static final Object lock = new Object();

    public static void ensureInitialized() throws IOException, InterruptedException
    {
        if (!initialized)
        {
            synchronized (lock)
            {
                if (!initialized)
                {
                    setup();
                    initialized = true;
                }
            }
        }
    }

    public static String getContainerIP()
    {
        return containerIP;
    }
}