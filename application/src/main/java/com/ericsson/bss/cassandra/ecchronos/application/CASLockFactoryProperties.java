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
package com.ericsson.bss.cassandra.ecchronos.application;

import java.util.Properties;

public class CASLockFactoryProperties
{
    private static final String CONFIG_KEYSPACE = "lockfactory.cas.keyspace";
    private static final String DEFAULT_KEYSPACE = "ecchronos";

    private final String myKeyspaceName;

    private CASLockFactoryProperties(String keyspaceName)
    {
        myKeyspaceName = keyspaceName;
    }

    public String getKeyspaceName()
    {
        return myKeyspaceName;
    }

    @Override
    public String toString()
    {
        return String.format("(keyspace=%s)", myKeyspaceName);
    }

    public static CASLockFactoryProperties from(Properties properties)
    {
        String keyspaceName = properties.getProperty(CONFIG_KEYSPACE, DEFAULT_KEYSPACE);

        return new CASLockFactoryProperties(keyspaceName);
    }
}
