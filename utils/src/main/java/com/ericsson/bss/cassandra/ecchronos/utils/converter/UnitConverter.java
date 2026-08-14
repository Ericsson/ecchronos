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
package com.ericsson.bss.cassandra.ecchronos.utils.converter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for converting human-readable size strings (e.g. "10M", "2G") to byte values.
 */
public final class UnitConverter
{
    private static final Pattern BYTE_PATTERN = Pattern.compile("^([0-9]+)([kKmMgG]?)$");

    private static final long ONE_KIB = 1024L;
    private static final long ONE_MIB = 1024L * ONE_KIB;
    private static final long ONE_GIB = 1024L * ONE_MIB;

    private UnitConverter()
    {
    }

    /**
     * Converts a human-readable size string to bytes.
     * Supports suffixes: k/K (KiB), m/M (MiB), g/G (GiB). No suffix means bytes.
     *
     * @param value the size string to convert (e.g. "512M", "2G", "1024").
     * @return the size in bytes.
     * @throws IllegalArgumentException if the value does not match the expected format.
     */
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
