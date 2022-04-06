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

import java.math.BigInteger;
import java.util.Objects;

/**
 * A normalized range based on a "base vnode".
 * The range is normalized so that the token range in the base vnode
 * starts at 0 and ends (at most) at 2^64.
 *
 * When comparing normalized ranges the ones with lowest start comes first.
 * If two normalized ranges have the same start the one including more is sorted first.
 * E.g (5, 10], (5, 15] is ordered as (5, 15], (5, 10]
 */
public class NormalizedRange implements Comparable<NormalizedRange>
{
    private final NormalizedBaseRange base;
    private final BigInteger start;
    private final BigInteger end;

    private final long startedAt;
    private final long finishedAt;

    NormalizedRange(NormalizedBaseRange base, BigInteger start, BigInteger end, long startedAt, long finishedAt)
    {
        this.base = base;
        this.start = start;
        this.end = end;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
    }

    /**
     * Get the normalized start token of this sub range.
     *
     * @return The normalized start token
     */
    public BigInteger start()
    {
        return start;
    }

    /**
     * Get the normalized end token of this sub range.
     *
     * @return The normalized end token
     */
    public BigInteger end()
    {
        return end;
    }

    /**
     * Get the repair timestamp of this sub range.
     *
     * @return The repair timestamp.
     */
    public long getStartedAt()
    {
        return startedAt;
    }

    /**
     * Get the finished repair timestamp of this sub range.
     *
     * @return The finished repair timestamp or -1 if not finished.
     */
    public long getFinishedAt()
    {
        return finishedAt;
    }

    /**
     * Create a new normalized range based on this sub range with the provided start
     * and the current sub range end.
     *
     * @param newStart The new normalized start token to use.
     * @return The new normalized range.
     */
    public NormalizedRange mutateStart(BigInteger newStart)
    {
        if (!base.inRange(newStart))
        {
            throw new IllegalArgumentException("Token " + newStart + " not in range " + base);
        }

        return new NormalizedRange(base, newStart, end, startedAt, finishedAt);
    }

    /**
     * Create a new normalized range based on this sub range with the provided end
     * and the current sub range start.
     *
     * @param newEnd The new normalized end token to use.
     * @return The new normalized range.
     */
    public NormalizedRange mutateEnd(BigInteger newEnd)
    {
        if (!base.inRange(newEnd))
        {
            throw new IllegalArgumentException("Token " + newEnd + " not in range " + base);
        }

        return new NormalizedRange(base, start, newEnd, startedAt, finishedAt);
    }

    /**
     * Create a new normalized range based on this sub range and the provided sub range.
     * The new normalized sub range will span the range between the end of this and the
     * start of the provided sub range.
     *
     * E.g. (5, 15] and (20, 30] generates a range (15, 20]
     *
     * @param other The new normalized start token to use.
     * @param startedAt The repair timestamp to use for the new normalized range.
     * @param finishedAt The repair finish timestamp to use for the new normalized range.
     * @return The new normalized range.
     */
    public NormalizedRange between(NormalizedRange other, long startedAt, long finishedAt)
    {
        verifySameBaseRange(other.base);

        if (end.compareTo(other.start) >= 0)
        {
            throw new IllegalArgumentException("Cannot create range between " + this + " -> " + other);
        }

        return new NormalizedRange(base, end, other.start, startedAt, finishedAt);
    }

    /**
     * Split an overlap between the start of the provided range and the end of this.
     *
     * E.g. (5, 15] and (8, 17] splits to a new range (8, 15]
     *
     * @param other The overlapping range.
     * @return The new normalized range using the highest repair timestamp of the two.
     */
    public NormalizedRange splitEnd(NormalizedRange other)
    {
        verifySameBaseRange(other.base);

        if (start.compareTo(other.start) > 0 || other.start.compareTo(end) >= 0)
        {
            throw new IllegalArgumentException("Cannot split end of " + this + " with " + other);
        }

        long startedAt = Math.max(this.startedAt, other.startedAt);
        long finishedAt = Math.min(this.finishedAt, other.finishedAt);
        return new NormalizedRange(base, other.start, end, startedAt, finishedAt);
    }

    /**
     * Combine this normalized range with the provided range assuming
     * they are adjacent.
     *
     * E.g. (5, 15] and (15, 30] becomes (5, 30]
     *
     * @param other The adjacent range.
     * @return The new normalized range using the lowest repair timestamp of the two.
     */
    public NormalizedRange combine(NormalizedRange other)
    {
        verifySameBaseRange(other.base);

        if (other.start.compareTo(end) != 0)
        {
            throw new IllegalArgumentException("Range " + other + " is not adjacent to " + this);
        }

        long startedAt = Math.min(this.startedAt, other.startedAt);
        long finishedAt = Math.max(this.finishedAt, other.finishedAt);
        return new NormalizedRange(base, start, other.end, startedAt, finishedAt);
    }

    /**
     * Check if this sub range covers the other sub range fully.
     *
     * @param other The sub range to compare
     * @return True if this range covers the provided range.
     */
    public boolean isCovering(NormalizedRange other)
    {
        verifySameBaseRange(other.base);

        return start.compareTo(other.start) <= 0 && end.compareTo(other.end) >= 0;
    }

    private void verifySameBaseRange(NormalizedBaseRange other)
    {
        if (!base.equals(other))
        {
            throw new IllegalArgumentException("Different bases" + base + ":" + other);
        }
    }

    @Override
    public int compareTo(NormalizedRange o)
    {
        verifySameBaseRange(o.base);

        int cmp = start.compareTo(o.start);
        if (cmp != 0)
        {
            return cmp;
        }

        return o.end.compareTo(end);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NormalizedRange that = (NormalizedRange) o;
        return startedAt == that.startedAt &&
                finishedAt == that.finishedAt &&
                base.equals(that.base) &&
                start.equals(that.start) &&
                end.equals(that.end);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base, start, end, startedAt, finishedAt);
    }

    @Override
    public String toString()
    {
        return String.format("(%d, %d], %d-%d", start, end, startedAt, finishedAt);
    }

}
