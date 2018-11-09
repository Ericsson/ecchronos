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
public class LongTokenRange implements Comparable<LongTokenRange>
{
    public final LongToken start;
    public final LongToken end;

    public LongTokenRange(long start, long end)
    {
        this.start = new LongToken(start);
        this.end = new LongToken(end);
    }

    public LongTokenRange(LongToken start, LongToken end)
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
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((end == null) ? 0 : end.hashCode());
        result = prime * result + ((start == null) ? 0 : start.hashCode());
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
        LongTokenRange other = (LongTokenRange) obj;
        if (end == null)
        {
            if (other.end != null)
            {
                return false;
            }
        }
        else if (!end.equals(other.end))
        {
            return false;
        }
        if (start == null)
        {
            if (other.start != null)
            {
                return false;
            }
        }
        else if (!start.equals(other.start))
        {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(LongTokenRange other)
    {
        return start.compareTo(other.start);
    }
}
