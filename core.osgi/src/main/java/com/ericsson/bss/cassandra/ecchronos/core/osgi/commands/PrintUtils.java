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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public final class PrintUtils
{
    static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

    private PrintUtils()
    {
    }

    static String epochToHumanReadable(long timeInMillis)
    {
        return DATE_FORMATTER.format(new Date(timeInMillis));
    }

    static String toPercentage(Double ratio)
    {
        return String.format("%.0f%%", ratio * 100);
    }
}
