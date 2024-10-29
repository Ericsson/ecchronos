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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode;

import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import java.math.BigInteger;
import java.util.Objects;

/**
 * A normalized base range (vnode) that can transform sub ranges
 * between a normalized and traditional state.
 *
 * The normalized base range starts from 0 instead of the token.
 * All normalized sub ranges are offset from 0 rather than the
 * start of the vnode.
 * This makes it so that the start of a sub range is strictly
 * smaller than the end.
 * This is useful to avoid dealing with token ranges wrapping around
 * the end of the token range.
 */
@SuppressWarnings("VisibilityModifier")
public class NormalizedBaseRange
{
    private static final BigInteger NORMALIZED_RANGE_START = BigInteger.ZERO;

    private final VnodeRepairState baseVnode;
    final BigInteger end;

    public NormalizedBaseRange(final VnodeRepairState aBaseVnode)
    {
        this.baseVnode = aBaseVnode;
        this.end = baseVnode.getTokenRange().rangeSize();
    }

    /**
     * Check if the provided token is in this normalized range.
     *
     * @param normalizedToken The normalized token.
     * @return True if the token is in this range.
     */
    public boolean inRange(final BigInteger normalizedToken)
    {
        return normalizedToken.compareTo(NORMALIZED_RANGE_START) >= 0 && normalizedToken.compareTo(end) <= 0;
    }

    /**
     * Transform a traditional sub range of this vnode and aligns it's start
     * offset from 0 rather than the vnode start.
     *
     * @param subRange The sub range to transform.
     * @return NormalizedRange
     * @throws IllegalArgumentException Thrown in case the provided sub range is not covered by this vnode.
     */
    public NormalizedRange transform(final VnodeRepairState subRange)
    {
        if (!baseVnode.getTokenRange().isCovering(subRange.getTokenRange()))
        {
            throw new IllegalArgumentException(baseVnode + " is not covering " + subRange);
        }

        BigInteger baseStart = BigInteger.valueOf(baseVnode.getTokenRange().start);

        BigInteger normalizedStart = BigInteger.valueOf(subRange.getTokenRange().start).subtract(baseStart);
        if (normalizedStart.compareTo(BigInteger.ZERO) < 0)
        {
            normalizedStart = normalizedStart.add(LongTokenRange.FULL_RANGE);
        }

        BigInteger normalizedEnd = normalizedStart.add(subRange.getTokenRange().rangeSize());

        return new NormalizedRange(this, normalizedStart, normalizedEnd, subRange.getStartedAt(),
                subRange.getFinishedAt(), subRange.getRepairTime());
    }

    /**
     * Transform a normalized sub range of this vnode back to it's
     * traditional counter-part.
     *
     * This resets the start offset back to the start of the vnode.
     *
     * @param range The normalized sub range to transform.
     * @return The traditional sub range.
     */
    public VnodeRepairState transform(final NormalizedRange range)
    {
        BigInteger baseStart = BigInteger.valueOf(baseVnode.getTokenRange().start);

        BigInteger startOffset = range.start();
        BigInteger endOffset = range.end();

        BigInteger realStart = baseStart.add(startOffset);
        BigInteger realEnd = baseStart.add(endOffset);

        if (realStart.compareTo(LongTokenRange.RANGE_END) > 0)
        {
            realStart = realStart.subtract(LongTokenRange.FULL_RANGE);
        }
        if (realEnd.compareTo(LongTokenRange.RANGE_END) > 0)
        {
            realEnd = realEnd.subtract(LongTokenRange.FULL_RANGE);
        }

        return new VnodeRepairState(new LongTokenRange(realStart.longValueExact(), realEnd.longValueExact()),
                baseVnode.getReplicas(), range.getStartedAt(), range.getFinishedAt(), range.getRepairTime());
    }

    /**
     * Checks equality.
     *
     * @param o Object to test equality with.
     * @return Boolean
     */
    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        NormalizedBaseRange that = (NormalizedBaseRange) o;
        return baseVnode.equals(that.baseVnode) && end.equals(that.end);
    }

    /**
     * Return a hash code representation.
     *
     * @return int
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(baseVnode, end);
    }

    /**
     * Return a string representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("(%d, %d]", NORMALIZED_RANGE_START, end);
    }
}

