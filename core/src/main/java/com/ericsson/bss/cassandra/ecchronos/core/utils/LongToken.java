/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import java.util.Objects;

/**
 * A representation of a token in Cassandra.
 */
@Deprecated
public class LongToken implements Comparable<LongToken>
{
    private final long value;

    public LongToken(final long aValue)
    {
        this.value = aValue;
    }

    public final long getValue()
    {
        return value;
    }

    @Override
    public final String toString()
    {
        return Long.toString(getValue());
    }

    @Override
    public final boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        LongToken longToken = (LongToken) o;
        return value == longToken.value;
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(value);
    }

    @Override
    public final int compareTo(final LongToken other)
    {
        return Long.compare(getValue(), other.getValue());
    }
}
