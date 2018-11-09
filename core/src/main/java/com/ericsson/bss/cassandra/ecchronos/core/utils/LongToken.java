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

/**
 * A representation of a token in Cassandra.
 */
public class LongToken implements Comparable<LongToken>
{
    private final long value;

    public LongToken(long value)
    {
        this.value = value;
    }

    public long getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return Long.toString(getValue());
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (value ^ (value >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        LongToken other = (LongToken) obj;
        if (value != other.value)
        {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(LongToken other)
    {
        return Long.compare(getValue(), other.getValue());
    }
}
