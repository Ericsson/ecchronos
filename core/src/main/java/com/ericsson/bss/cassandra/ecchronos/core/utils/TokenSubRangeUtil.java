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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.InternalException;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to split a token range into smaller sub-ranges.
 */
public class TokenSubRangeUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(TokenSubRangeUtil.class);

    private final LongTokenRange tokenRange;
    private final BigInteger tokenStart;
    private final BigInteger totalRangeSize;

    public TokenSubRangeUtil(LongTokenRange tokenRange)
    {
        this.tokenRange = tokenRange;
        this.tokenStart = BigInteger.valueOf(tokenRange.start);
        this.totalRangeSize = this.tokenRange.rangeSize();
    }

    /**
     * Generates a number of sub ranges of mostly equal size.
     * The last sub range can be slightly smaller than the others
     * due to rounding.
     *
     * @param tokenPerSubRange The number of wanted tokens per subrange
     * @return The sub ranges containing the full range.
     */
    public List<LongTokenRange> generateSubRanges(BigInteger tokenPerSubRange)
    {
        if (totalRangeSize.compareTo(tokenPerSubRange) <= 0)
        {
            return Lists.newArrayList(tokenRange); // Full range is smaller than wanted tokens
        }

        long actualSubRangeCount = totalRangeSize.divide(tokenPerSubRange).longValueExact();
        if (totalRangeSize.remainder(tokenPerSubRange).compareTo(BigInteger.ZERO) > 0)
        {
            actualSubRangeCount++;
        }

        List<LongTokenRange> subRanges = new ArrayList<>();
        for (long l = 0; l < actualSubRangeCount - 1; l++)
        {
            subRanges.add(newSubRange(tokenPerSubRange, l));
        }

        LongTokenRange lastRange = subRanges.get(subRanges.size() - 1);
        subRanges.add(new LongTokenRange(lastRange.end, tokenRange.end));

        // Verify sub range size match full range size
        validateSubRangeSize(subRanges);

        return subRanges;
    }

    private void validateSubRangeSize(List<LongTokenRange> subRanges)
    {
        BigInteger subRangeSize = BigInteger.ZERO;

        for (LongTokenRange range : subRanges)
        {
            subRangeSize = subRangeSize.add(range.rangeSize());
        }

        if (subRangeSize.compareTo(totalRangeSize) != 0)
        {
            BigInteger difference = totalRangeSize.subtract(subRangeSize).abs();
            String message = String.format("Unexpected sub-range generation for %s. Difference of %s. Sub-ranges generated: %s", tokenRange, difference, subRanges);

            LOG.error(message);
            throw new InternalException(message);
        }
    }

    private LongTokenRange newSubRange(BigInteger rangeSize, long rangeId)
    {
        BigInteger rangeOffset = rangeSize.multiply(BigInteger.valueOf(rangeId));
        BigInteger rangeStartTmp = tokenStart.add(rangeOffset);
        BigInteger rangeEndTmp = rangeStartTmp.add(rangeSize);

        long rangeStart = enforceValidBounds(rangeStartTmp);
        long rangeEnd = enforceValidBounds(rangeEndTmp);

        return new LongTokenRange(rangeStart, rangeEnd);
    }

    private long enforceValidBounds(BigInteger tokenValue)
    {
        if (tokenValue.compareTo(LongTokenRange.RANGE_END) > 0)
        {
            return tokenValue.subtract(LongTokenRange.FULL_RANGE).longValueExact();
        }

        return tokenValue.longValueExact();
    }
}
