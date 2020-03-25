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

import java.math.BigInteger;

/**
 * A representation of a token range in Cassandra.
 */
public class LongTokenRange
{
    static final BigInteger RANGE_END = BigInteger.valueOf(2).pow(63).subtract(BigInteger.ONE); // Long.MAX_VALUE
    static final BigInteger FULL_RANGE = BigInteger.valueOf(2).pow(64);

    public final long start;
    public final long end;

    public LongTokenRange(long start, long end)
    {
        this.start = start;
        this.end = end;
    }

    /**
     * Check if the token range is wrapping around.
     *
     * @return True in case this token range wraps around.
     */
    public boolean isWrapAround()
    {
        return start >= end;
    }

    /**
     * Calculate the size of the token range.
     *
     * @return The size of the token range.
     */
    public BigInteger rangeSize()
    {
        BigInteger tokenStart = BigInteger.valueOf(start);
        BigInteger tokenEnd = BigInteger.valueOf(end);

        BigInteger rangeSize = tokenEnd.subtract(tokenStart);

        if (rangeSize.compareTo(BigInteger.ZERO) <= 0)
        {
            rangeSize = rangeSize.add(FULL_RANGE);
        }

        return rangeSize;
    }

    /**
     * Check if this range covers the other range.
     * <br><br>
     * The range (I, J] covers (K, L] if:
     * <br>
     * I &lt;= K <b>and</b> J &gt;= L if either both are wrapping or not wrapping.
     * <br>
     * I &lt;= K <b>or</b> J &gt;= L if this is wrapping.
     *
     * @param other The token range to check if this is covering.
     * @return True if this token range covers the provided token range.
     */
    public boolean isCovering(LongTokenRange other)
    {
        boolean thisWraps = isWrapAround();
        boolean otherWraps = other.isWrapAround();

        if (thisWraps == otherWraps)
        {
            // Normal case - are we including the other range
            return start <= other.start && end >= other.end;
        }
        else if (thisWraps)
        {
            // If only this wraps we cover it if either:
            // start is before the other start
            // end is after the other end
            return this.start <= other.start || this.end >= other.end;
        }

        // If the other wraps but we don't we can't possibly cover it
        return false;
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
