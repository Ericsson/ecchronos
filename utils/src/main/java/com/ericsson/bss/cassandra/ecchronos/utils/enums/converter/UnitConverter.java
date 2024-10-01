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
package com.ericsson.bss.cassandra.ecchronos.utils.enums.converter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class UnitConverter
{
    private static final Pattern BYTE_PATTERN = Pattern.compile("^([0-9]+)([kKmMgG]?)$");

    private static final long ONE_KIB = 1024L;
    private static final long ONE_MIB = 1024L * ONE_KIB;
    private static final long ONE_GIB = 1024L * ONE_MIB;

    private UnitConverter()
    {
    }

    public static long toBytes(final String value)
    {
        Matcher matcher = BYTE_PATTERN.matcher(value);
        if (!matcher.matches())
        {
            throw new IllegalArgumentException("Unknown value " + value);
        }
        long baseValue = Long.parseLong(matcher.group(1));

        switch (matcher.group(2))
        {
        case "g":
        case "G":
            return baseValue * ONE_GIB;
        case "m":
        case "M":
            return baseValue * ONE_MIB;
        case "k":
        case "K":
            return baseValue * ONE_KIB;
        default: // Bytes
            return baseValue;
        }
    }
}
