/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;

import java.math.BigInteger;
import java.util.Objects;

/**
 * A normalized range based on a "base vnode".
 * The range is normalized so that the token range in the base vnode
 * starts at Long.MIN_VALUE and ends (at most) at LONG.MAX_VALUE.
 *
 * When comparing normalized ranges the ones with lowest start comes first.
 * If two normalized ranges have the same start the one including more is sorted first.
 * E.g (5, 10], (5, 15] is ordered as (5, 15], (5, 10]
 */
public class NormalizedRange implements Comparable<NormalizedRange>
{
    private static final BigInteger RANGE_START = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger RANGE_END = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger FULL_RANGE = RANGE_END.subtract(RANGE_START).add(BigInteger.ONE);

    private final long start;
    private final long end;

    private final long repairedAt;

    public NormalizedRange(long start, long end, long repairedAt)
    {
        this.start = start;
        this.end = end;
        this.repairedAt = repairedAt;
    }

    public long start()
    {
        return start;
    }

    public long end()
    {
        return end;
    }

    public long repairedAt()
    {
        return repairedAt;
    }

    public NormalizedRange mutateStart(long newStart)
    {
        return new NormalizedRange(newStart, end, repairedAt);
    }

    public NormalizedRange mutateEnd(long newEnd)
    {
        return new NormalizedRange(start, newEnd, repairedAt);
    }

    public NormalizedRange between(NormalizedRange other, long repairedAt)
    {
        return new NormalizedRange(end, other.start, repairedAt);
    }

    public NormalizedRange split(NormalizedRange other)
    {
        long timestampToUse = Math.max(repairedAt, other.repairedAt);
        return new NormalizedRange(other.start, end, timestampToUse);
    }

    public NormalizedRange combine(NormalizedRange other)
    {
        long timestampToUse = Math.min(repairedAt, other.repairedAt);
        return new NormalizedRange(start, other.end, timestampToUse);
    }

    public boolean isCovering(NormalizedRange other)
    {
        return start <= other.start && end >= other.end;
    }

    /**
     * Transform this normalized range back into a vnode state based on
     * the provided base vnode.
     *
     * The resulting vnode state will have an offset from the base vnode
     * instead of Long.MIN_VALUE.
     *
     * @param baseVnode The base vnode
     * @return The transform vnode state with an offset from the base vnode
     */
    public VnodeRepairState transform(VnodeRepairState baseVnode)
    {
        BigInteger baseStart = BigInteger.valueOf(baseVnode.getTokenRange().start);

        BigInteger startOffset = BigInteger.valueOf(start).subtract(RANGE_START);
        BigInteger endOffset = BigInteger.valueOf(end).subtract(RANGE_START);

        BigInteger realStart = baseStart.add(startOffset);
        BigInteger realEnd = baseStart.add(endOffset);

        if (realStart.compareTo(RANGE_END) >= 0)
        {
            realStart = realStart.subtract(FULL_RANGE);
        }
        if (realEnd.compareTo(RANGE_END) >= 0)
        {
            realEnd = realEnd.subtract(FULL_RANGE);
        }

        return new VnodeRepairState(new LongTokenRange(realStart.longValueExact(), realEnd.longValueExact()), baseVnode.getReplicas(), repairedAt);
    }

    /**
     * Generate a normalized range for the sub range based on the base vnode.
     *
     * The resulting normalized range will have an offset from Long.MIN_VALUE instead
     * of the base vnode.
     *
     * @param baseVnode The base vnode range.
     * @param subRange The sub range to transform.
     * @return The range normalized from Long.MIN_VALUE
     */
    public static NormalizedRange transform(VnodeRepairState baseVnode, VnodeRepairState subRange)
    {
        if (!baseVnode.getTokenRange().isCovering(subRange.getTokenRange()))
        {
            throw new IllegalArgumentException(baseVnode + " is not covering " + subRange);
        }

        BigInteger baseStart = BigInteger.valueOf(baseVnode.getTokenRange().start);

        BigInteger startOffset = BigInteger.valueOf(subRange.getTokenRange().start).subtract(baseStart);
        if (startOffset.compareTo(BigInteger.ZERO) < 0)
        {
            startOffset = startOffset.add(FULL_RANGE);
        }
        BigInteger endOffset = BigInteger.valueOf(subRange.getTokenRange().end).subtract(baseStart);
        if (endOffset.compareTo(BigInteger.ZERO) < 0)
        {
            endOffset = endOffset.add(FULL_RANGE);
        }

        long normalizedStart = RANGE_START.add(startOffset).longValueExact();
        long normalizedEnd = RANGE_START.add(endOffset).longValueExact();

        return new NormalizedRange(normalizedStart, normalizedEnd, subRange.lastRepairedAt());
    }

    @Override
    public int compareTo(NormalizedRange o)
    {
        int cmp = Long.compare(start, o.start);
        if (cmp != 0)
        {
            return cmp;
        }

        return Long.compare(o.end, end);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NormalizedRange that = (NormalizedRange) o;
        return start == that.start &&
                end == that.end &&
                repairedAt == that.repairedAt;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(start, end, repairedAt);
    }

    @Override
    public String toString()
    {
        return String.format("(%d, %d], %d", start, end, repairedAt);
    }
}
