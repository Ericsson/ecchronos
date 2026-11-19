/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestThreadPoolTaskConfig
{
    private final ObjectMapper myMapper = new ObjectMapper();

    @Test
    void rejectsInvalidPoolSizes()
    {
        rejects("{\"corePoolSize\":-1}", "corePoolSize (-1)");
        rejects("{\"maxPoolSize\":0,\"corePoolSize\":0}", "maxPoolSize (0)");
        rejects("{\"maxPoolSize\":5,\"corePoolSize\":10}", "maxPoolSize (5) must be >= corePoolSize (10)");
        rejects("{\"keepAliveSeconds\":-1}", "keepAliveSeconds (-1)");
    }

    @Test
    void rejectsExplicitNulls()
    {
        rejects("null", "connection.threadPool must not be null");
        rejects("{\"corePoolSize\":null}", "corePoolSize (null)");
        rejects("{\"maxPoolSize\":null}", "maxPoolSize (null)");
        rejects("{\"keepAliveSeconds\":null}", "keepAliveSeconds (null)");
        rejects("{\"queueCapacity\":null}", "queueCapacity must not be null");
    }

    @Test
    void acceptsDefaultsAndBoundaries() throws Exception
    {
        ThreadPoolTaskConfig defaults = myMapper.readValue("{}", ConnectionConfig.class).getThreadPoolTaskConfig();
        assertEquals(4, defaults.getCorePoolSize());
        assertEquals(10, defaults.getMaxPoolSize());
        assertEquals(20, defaults.getQueueCapacity());
        assertEquals(60, defaults.getKeepAliveSeconds());

        ThreadPoolTaskConfig equalSizes = readPool("{\"maxPoolSize\":1,\"corePoolSize\":1,\"keepAliveSeconds\":0}");
        assertEquals(1, equalSizes.getCorePoolSize());
        assertEquals(1, equalSizes.getMaxPoolSize());
        assertEquals(0, equalSizes.getKeepAliveSeconds());

        assertEquals(0, readPool("{\"corePoolSize\":0,\"queueCapacity\":0}").getQueueCapacity());
        assertEquals(-1, readPool("{\"queueCapacity\":-1}").getQueueCapacity());
    }

    private ThreadPoolTaskConfig readPool(final String pool) throws Exception
    {
        return myMapper.readValue("{\"threadPool\":" + pool + "}", ConnectionConfig.class)
                .getThreadPoolTaskConfig();
    }

    private void rejects(final String pool, final String message)
    {
        JsonMappingException exception = assertThrows(JsonMappingException.class, () -> readPool(pool));
        assertTrue(exception.getCause().getMessage().contains(message), exception::getMessage);
    }
}
