/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config.connection;

import com.ericsson.bss.cassandra.ecchronos.application.DefaultJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;

import java.util.function.Supplier;

public class JmxConnection extends Connection<JmxConnectionProvider>
{
    private static final int DEFAULT_PORT = 7199;

    public JmxConnection()
    {
        try
        {
            setProvider(DefaultJmxConnectionProvider.class);
            setPort(DEFAULT_PORT);
        }
        catch (NoSuchMethodException ignored)
        {
            // Do something useful ...
        }
    }

    @Override
    protected final Class<?>[] expectedConstructor()
    {
        return new Class<?>[]
                {
                        Config.class, Supplier.class
                };
    }
}
