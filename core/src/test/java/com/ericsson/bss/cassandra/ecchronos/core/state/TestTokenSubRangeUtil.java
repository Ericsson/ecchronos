/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;

import org.junit.Test;

public class TestTokenSubRangeUtil
{
    @Test
    public void testSplitAcrossRingBoundaryWithRemainder()
    {
        long start = Long.MAX_VALUE - 2;
        long end = Long.MIN_VALUE + 5;
        TokenSubRangeUtil splitter = new TokenSubRangeUtil(LongTokenRange.of(start, end));

        assertThat(splitter.generateSubRanges(BigInteger.valueOf(3))).containsExactly(
                LongTokenRange.of(start, Long.MIN_VALUE),
                LongTokenRange.of(Long.MIN_VALUE, Long.MIN_VALUE + 3),
                LongTokenRange.of(Long.MIN_VALUE + 3, end));
    }

    @Test
    public void testSplitFullRingIntoEqualHalves()
    {
        TokenSubRangeUtil splitter = new TokenSubRangeUtil(LongTokenRange.of(0, 0));

        assertThat(splitter.generateSubRanges(BigInteger.ONE.shiftLeft(63))).containsExactly(
                LongTokenRange.of(0, Long.MIN_VALUE),
                LongTokenRange.of(Long.MIN_VALUE, 0));
    }
}
