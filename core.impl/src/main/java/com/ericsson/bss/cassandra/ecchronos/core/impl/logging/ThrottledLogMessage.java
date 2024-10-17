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
package com.ericsson.bss.cassandra.ecchronos.core.impl.logging;

import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A utility class for logging messages with throttling capabilities.
 * <p>
 * This class ensures that a specific log message is only logged at specified intervals,
 * preventing log flooding for high-frequency events.
 * </p>
 */
public class ThrottledLogMessage
{
    private final String myMessage;
    private final long myIntervalNanos;
    private final AtomicLong myLastLogTime;

    /**
     * Constructs a ThrottledLogMessage with the specified message and interval.
     *
     * @param message the log message to be throttled. Must not be {@code null}.
     * @param intervalNanos the minimum interval (in nanoseconds) between consecutive log messages. Must be greater than zero.
     * @throws IllegalArgumentException if {@code message} is {@code null} or {@code intervalNanos} is less than or equal to zero.
     */
    public ThrottledLogMessage(final String message, final long intervalNanos)
    {
        myMessage = message;
        myIntervalNanos = intervalNanos;
        myLastLogTime = new AtomicLong(Long.MIN_VALUE);
    }

    /**
     * Checks whether the logging of the message is allowed based on the specified time.
     *
     * @param timeInNanos the current time in nanoseconds.
     * @return {@code true} if the message can be logged; {@code false} otherwise.
     */
    private boolean isAllowedToLog(final long timeInNanos)
    {
        long lastLogTime = myLastLogTime.get();
        return timeInNanos >= lastLogTime && myLastLogTime.compareAndSet(lastLogTime, timeInNanos + myIntervalNanos);
    }

    /**
     * Logs an informational message if the logging is allowed based on the throttling interval.
     *
     * @param logger the logger to log the message to. Must not be {@code null}.
     * @param timeInMs the current time in milliseconds.
     * @param objects optional parameters to be included in the log message.
     * @throws NullPointerException if {@code logger} is {@code null}.
     */
    public final void info(final Logger logger, final long timeInMs, final Object... objects)
    {
        if (isAllowedToLog(timeInMs))
        {
            logger.info(myMessage, objects);
        }
    }

    /**
     * Logs a warning message if the logging is allowed based on the throttling interval.
     *
     * @param logger the logger to log the message to. Must not be {@code null}.
     * @param timeInMs the current time in milliseconds.
     * @param objects optional parameters to be included in the log message.
     * @throws NullPointerException if {@code logger} is {@code null}.
     */
    public final void warn(final Logger logger, final long timeInMs, final Object... objects)
    {
        if (isAllowedToLog(timeInMs))
        {
            logger.warn(myMessage, objects);
        }
    }

    /**
     * Logs an error message if the logging is allowed based on the throttling interval.
     *
     * @param logger the logger to log the message to. Must not be {@code null}.
     * @param timeInMs the current time in milliseconds.
     * @param objects optional parameters to be included in the log message.
     * @throws NullPointerException if {@code logger} is {@code null}.
     */
    public final void error(final Logger logger, final long timeInMs, final Object... objects)
    {
        if (isAllowedToLog(timeInMs))
        {
            logger.error(myMessage, objects);
        }
    }
}


