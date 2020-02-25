/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.osgi.commands;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

final class PrintUtils
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private PrintUtils()
    {
    }

    static String epochToHumanReadable(long timeInMillis)
    {
        return DATE_FORMATTER.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timeInMillis), ZoneId.systemDefault()));
    }

    static String durationToHumanReadable(long durationInMillis)
    {
        Duration currentDuration = Duration.of(durationInMillis, ChronoUnit.MILLIS);

        long days = currentDuration.toDays();
        currentDuration = currentDuration.minusDays(days);
        long hours = currentDuration.toHours();
        currentDuration = currentDuration.minusHours(hours);
        long minutes = currentDuration.toMinutes();
        currentDuration = currentDuration.minusMinutes(minutes);
        long seconds = currentDuration.getSeconds();

        StringBuilder sb = new StringBuilder();

        if (days > 0)
        {
            sb.append(days).append("d ");
        }
        if (hours > 0)
        {
            sb.append(hours).append("h ");
        }
        if (minutes > 0)
        {
            sb.append(minutes).append("m ");
        }
        if (seconds > 0)
        {
            sb.append(seconds).append("s ");
        }

        return sb.toString().trim();
    }

    static String toPercentage(Double ratio)
    {
        return String.format("%.0f%%", ratio * 100);
    }
}
