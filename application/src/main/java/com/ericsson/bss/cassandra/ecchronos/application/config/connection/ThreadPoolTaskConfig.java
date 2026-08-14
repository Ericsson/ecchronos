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
package com.ericsson.bss.cassandra.ecchronos.application.config.connection;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration class for thread pool task executor settings.
 * Provides configurable parameters such as core pool size, max pool size,
 * queue capacity, and keep-alive time for thread pool management.
 */
public final class ThreadPoolTaskConfig
{
    private static final Integer DEFAULT_CORE_POOL_SIZE = 4;
    private static final Integer DEFAULT_MAX_POOL_SIZE = 10;
    private static final Integer DEFAULT_QUEUE_CAPACITY = 20;
    private static final Integer DEFAULT_KEEP_ALIVE_SECONDS = 60;

    private Integer myCorePoolSize = DEFAULT_CORE_POOL_SIZE;
    private Integer myMaxPoolSize = DEFAULT_MAX_POOL_SIZE;
    private Integer myQueueCapacity = DEFAULT_QUEUE_CAPACITY;
    private Integer myKeepAliveSeconds = DEFAULT_KEEP_ALIVE_SECONDS;

    /**
     * Constructs a ThreadPoolTaskConfig with default values.
     */
    public ThreadPoolTaskConfig()
    {
    }

    /**
     * Gets the core pool size for the thread pool.
     *
     * @return the core pool size.
     */
    @JsonProperty("corePoolSize")
    public Integer getCorePoolSize()
    {
        return myCorePoolSize;
    }

    /**
     * Sets the core pool size for the thread pool.
     *
     * @param corePoolSize the core pool size to set.
     */
    @JsonProperty("corePoolSize")
    public void setCorePoolSize(final Integer corePoolSize)
    {
        myCorePoolSize = corePoolSize;
    }

    /**
     * Gets the maximum pool size for the thread pool.
     *
     * @return the maximum pool size.
     */
    @JsonProperty("maxPoolSize")
    public Integer getMaxPoolSize()
    {
        return myMaxPoolSize;
    }

    /**
     * Sets the maximum pool size for the thread pool.
     *
     * @param maxPoolSize the maximum pool size to set.
     */
    @JsonProperty("maxPoolSize")
    public void setMaxPoolSize(final Integer maxPoolSize)
    {
        myMaxPoolSize = maxPoolSize;
    }

    /**
     * Gets the queue capacity for the thread pool task queue.
     *
     * @return the queue capacity.
     */
    @JsonProperty("queueCapacity")
    public Integer getQueueCapacity()
    {
        return myQueueCapacity;
    }

    /**
     * Sets the queue capacity for the thread pool task queue.
     *
     * @param queueCapacity the queue capacity to set.
     */
    @JsonProperty("queueCapacity")
    public void setQueueCapacity(final Integer queueCapacity)
    {
        myQueueCapacity = queueCapacity;
    }

    /**
     * Gets the keep-alive time in seconds for idle threads.
     *
     * @return the keep-alive time in seconds.
     */
    @JsonProperty("keepAliveSeconds")
    public Integer getKeepAliveSeconds()
    {
        return myKeepAliveSeconds;
    }

    /**
     * Sets the keep-alive time in seconds for idle threads.
     *
     * @param keepAliveSeconds the keep-alive time in seconds to set.
     */
    @JsonProperty("keepAliveSeconds")
    public void setKeepAliveSeconds(final Integer keepAliveSeconds)
    {
        myKeepAliveSeconds = keepAliveSeconds;
    }
}
