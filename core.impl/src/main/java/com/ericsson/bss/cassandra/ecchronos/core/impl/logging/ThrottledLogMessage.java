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

public class ThrottledLogMessage
{
    private final String myMessage;
    private final long myIntervalNanos;
    private final AtomicLong myLastLogTime;

    public ThrottledLogMessage(final String message, final long intervalNanos)
    {
        myMessage = message;
        myIntervalNanos = intervalNanos;
        myLastLogTime = new AtomicLong(Long.MIN_VALUE);
    }

    private boolean isAllowedToLog(final long timeInNanos)
    {
        long lastLogTime = myLastLogTime.get();
        return timeInNanos >= lastLogTime && myLastLogTime.compareAndSet(lastLogTime, timeInNanos + myIntervalNanos);
    }

    public final void info(final Logger logger, final long timeInMs, final Object... objects)
    {
        if (isAllowedToLog(timeInMs))
        {
            logger.info(myMessage, objects);
        }
    }

    public final void warn(final Logger logger, final long timeInMs, final Object... objects)
    {
        if (isAllowedToLog(timeInMs))
        {
            logger.warn(myMessage, objects);
        }
    }

    public final void error(final Logger logger, final long timeInMs, final Object... objects)
    {
        if (isAllowedToLog(timeInMs))
        {
            logger.error(myMessage, objects);
        }
    }
}

