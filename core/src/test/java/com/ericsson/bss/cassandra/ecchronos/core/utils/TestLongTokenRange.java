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

import static org.assertj.core.api.Assertions.assertThat;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import java.math.BigInteger;

public class TestLongTokenRange
{
    private static final BigInteger FULL_RANGE = BigInteger.valueOf(2).pow(64);

    @Test
    public void testRangesEqual()
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(1, 2);

        assertThat(range1).isEqualTo(range2);
        assertThat(range1.hashCode()).isEqualTo(range2.hashCode());
    }

    @Test
    public void testRangesNotEqual()
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);

        assertThat(range1).isNotEqualTo(range2);
    }

    @Test
    public void testIsWrapAroundNonWrapping()
    {
        assertThat(new LongTokenRange(1, 2).isWrapAround()).isFalse();
    }

    @Test
    public void testIsWrapAroundWrapping()
    {
        assertThat(new LongTokenRange(2, 1).isWrapAround()).isTrue();
    }

    @Test
    public void testIsWrapAroundFullRange()
    {
        assertThat(new LongTokenRange(Long.MIN_VALUE, Long.MIN_VALUE).isWrapAround()).isTrue();
    }

    @Test
    public void testRangeSizeFullRange()
    {
        LongTokenRange tokenRange = new LongTokenRange(Long.MIN_VALUE, Long.MIN_VALUE);

        assertThat(tokenRange.rangeSize()).isEqualTo(FULL_RANGE);
    }

    @Test
    public void testRangeSizePositive()
    {
        LongTokenRange tokenRange = new LongTokenRange(10, 123456789);
        BigInteger expectedRangeSize = BigInteger.valueOf(123456779);

        assertThat(tokenRange.rangeSize()).isEqualTo(expectedRangeSize);
    }

    @Test
    public void testRangeSizeNegative()
    {
        LongTokenRange tokenRange = new LongTokenRange(-123456789, -10);
        BigInteger expectedRangeSize = BigInteger.valueOf(123456779);

        assertThat(tokenRange.rangeSize()).isEqualTo(expectedRangeSize);
    }

    @Test
    public void testRangeSizeNegativeToPositive()
    {
        LongTokenRange tokenRange = new LongTokenRange(-500, 1500);
        BigInteger expectedRangeSize = BigInteger.valueOf(2000);

        assertThat(tokenRange.rangeSize()).isEqualTo(expectedRangeSize);
    }

    @Test
    public void testRangeSizeWrapAround()
    {
        LongTokenRange tokenRange = new LongTokenRange(10, -10);
        BigInteger expectedRangeSize = FULL_RANGE.subtract(BigInteger.valueOf(20));

        assertThat(tokenRange.rangeSize()).isEqualTo(expectedRangeSize);
    }

    @Test
    public void testNotCovering()
    {
        LongTokenRange range1 = new LongTokenRange(50, 500);
        LongTokenRange range2 = new LongTokenRange(10, 40);

        assertThat(range1.isCovering(range2)).isFalse();
    }

    @Test
    public void testNotCoveringNegative()
    {
        LongTokenRange range1 = new LongTokenRange(-500, -50);
        LongTokenRange range2 = new LongTokenRange(-40, -10);

        assertThat(range1.isCovering(range2)).isFalse();
    }

    @Test
    public void testNotCoveringOneWrapping()
    {
        LongTokenRange range1 = new LongTokenRange(50, -50);
        LongTokenRange range2 = new LongTokenRange(10, 40);

        assertThat(range1.isCovering(range2)).isFalse();
    }

    @Test
    public void testNotCoveringBothWrapping()
    {
        LongTokenRange range1 = new LongTokenRange(50, -50);
        LongTokenRange range2 = new LongTokenRange(10, -40);

        assertThat(range1.isCovering(range2)).isFalse();
    }

    @Test
    public void testCoveringSubRanges()
    {
        LongTokenRange range1 = new LongTokenRange(50, 500);
        LongTokenRange range2 = new LongTokenRange(50, 200);
        LongTokenRange range3 = new LongTokenRange(200, 350);
        LongTokenRange range4 = new LongTokenRange(350, 500);

        assertThat(range1.isCovering(range2)).isTrue();
        assertThat(range1.isCovering(range3)).isTrue();
        assertThat(range1.isCovering(range4)).isTrue();
    }

    @Test
    public void testCoveringSubRangesNegative()
    {
        LongTokenRange range1 = new LongTokenRange(-500, -50);
        LongTokenRange range2 = new LongTokenRange(-500, -350);
        LongTokenRange range3 = new LongTokenRange(-350, -200);
        LongTokenRange range4 = new LongTokenRange(-200, -50);

        assertThat(range1.isCovering(range2)).isTrue();
        assertThat(range1.isCovering(range3)).isTrue();
        assertThat(range1.isCovering(range4)).isTrue();
    }

    @Test
    public void testCoveringOneWrapping()
    {
        LongTokenRange range1 = new LongTokenRange(50, -50);
        LongTokenRange range2 = new LongTokenRange(60, 70);

        assertThat(range1.isCovering(range2)).isTrue();
    }

    @Test
    public void testCoveringBothWrapping()
    {
        LongTokenRange range1 = new LongTokenRange(50, -50);
        LongTokenRange range2 = new LongTokenRange(60, -60);

        assertThat(range1.isCovering(range2)).isTrue();
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.forClass(LongTokenRange.class).usingGetClass().verify();
    }
}
