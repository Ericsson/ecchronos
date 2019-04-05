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
 * A representation of a token range in Cassandra.
 */
public class LongTokenRange
{
    public final long start;
    public final long end;

    public LongTokenRange(long start, long end)
    {
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString()
    {
        return String.format("(%s,%s]", start, end);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LongTokenRange that = (LongTokenRange) o;

        if (start != that.start) return false;
        return end == that.end;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (start ^ (start >>> 32));
        result = 31 * result + (int) (end ^ (end >>> 32));
        return result;
    }
}
