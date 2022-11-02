/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

package com.ericsson.bss.cassandra.ecchronos.core.utils.logging;

import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Logger that throttles log messages per interval.
 * A log message uniqueness is based on the string message.
 *
 * This logger is thread safe.
 */
public class ThrottlingLogger
{
    private final Map<String, ThrottledLogMessage> myThrottledLogMessages = new ConcurrentHashMap<>();
    private final Logger myLogger;
    private final long myIntervalNanos;

    public ThrottlingLogger(final Logger logger, final long interval, final TimeUnit timeUnit)
    {
        myLogger = logger;
        myIntervalNanos = timeUnit.toNanos(interval);
    }

    public final void info(final String message, final Object... objects)
    {
        ThrottledLogMessage throttledLogMessage = getThrottledLogMessage(message);
        throttledLogMessage.info(myLogger, System.nanoTime(), objects);
    }

    public final void warn(final String message, final Object... objects)
    {
        ThrottledLogMessage throttledLogMessage = getThrottledLogMessage(message);
        throttledLogMessage.warn(myLogger, System.nanoTime(), objects);
    }

    public final void error(final String message, final Object... objects)
    {
        ThrottledLogMessage throttledLogMessage = getThrottledLogMessage(message);
        throttledLogMessage.error(myLogger, System.nanoTime(), objects);
    }

    private ThrottledLogMessage getThrottledLogMessage(final String message)
    {
        ThrottledLogMessage throttledLogMessage = myThrottledLogMessages.get(message);
        if (throttledLogMessage == null)
        {
            throttledLogMessage = new ThrottledLogMessage(message, myIntervalNanos);
            ThrottledLogMessage addedMessage = myThrottledLogMessages.putIfAbsent(message, throttledLogMessage);
            if (addedMessage != null)
            {
                throttledLogMessage = addedMessage;
            }
        }
        return throttledLogMessage;
    }
}
