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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RetryServiceShutdownManager
{
    private static final Logger LOG = LoggerFactory.getLogger(RetryServiceShutdownManager.class);

    private RetryServiceShutdownManager()
    {

    }

    public static void shutdownExecutorService(final ScheduledExecutorService executorService,
                                               final int timeout,
                                               final TimeUnit unit)
    {
        executorService.shutdown();
        try
        {
            if (!executorService.awaitTermination(timeout, unit))
            {
                LOG.warn("Executor did not terminate in time, forcing shutdown...");
                executorService.shutdownNow();
                if (!executorService.awaitTermination(timeout, unit))
                {
                    LOG.error("Executor did not terminate after force shutdown.");
                }
            }
        }
        catch (InterruptedException e)
        {
            LOG.error("Interrupted during shutdown. Forcing shutdown now...", e);
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
