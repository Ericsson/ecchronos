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
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@RunWith(MockitoJUnitRunner.class)
public class TestNormalizedRange
{
    private static final BigInteger START = BigInteger.ZERO;
    
    @Mock
    DriverNode mockNode;

    @Test
    public void testMutateStart()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 109L, 1234L, 1235L));
        NormalizedRange normalizedRange = new NormalizedRange(normalizedBaseRange, START, bi(9L), 1234L, 1235L);

        NormalizedRange withNewStart = normalizedRange.mutateStart(bi(8L));
        assertThat(withNewStart.start()).isEqualTo(bi(8L));
        assertThat(withNewStart.end()).isEqualTo(bi(9L));
        assertThat(withNewStart.getStartedAt()).isEqualTo(1234L);
        assertThat(withNewStart.getFinishedAt()).isEqualTo(1235L);
    }

    @Test
    public void testMutateStartOutsideBaseRange()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 109L, 1234L, 1235L));
        NormalizedRange normalizedRange = new NormalizedRange(normalizedBaseRange, START, bi(9L), 1234L, 1235L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> normalizedRange.mutateStart(bi(13L)));
    }

    @Test
    public void testMutateEnd()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 109L, 1234L, 1235L));
        NormalizedRange normalizedRange = new NormalizedRange(normalizedBaseRange, START, bi(9L), 1234L, 1235L);

        NormalizedRange withNewEnd = normalizedRange.mutateEnd(bi(8L));
        assertThat(withNewEnd.start()).isEqualTo(START);
        assertThat(withNewEnd.end()).isEqualTo(bi(8L));
        assertThat(withNewEnd.getStartedAt()).isEqualTo(1234L);
        assertThat(withNewEnd.getFinishedAt()).isEqualTo(1235L);
    }

    @Test
    public void testMutateEndOutsideBaseRange()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 109L, 1234L, 1235L));
        NormalizedRange normalizedRange = new NormalizedRange(normalizedBaseRange, START, bi(9L), 1234L, 1235L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> normalizedRange.mutateEnd(bi(13)));
    }

    @Test
    public void testBetween()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, START, bi(9L), 1234L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, bi(13L), bi(15L), 1235L, 1236L);

        NormalizedRange between = firstRange.between(secondRange, 1236L, 1237L);
        assertThat(between.start()).isEqualTo(bi(9L));
        assertThat(between.end()).isEqualTo(bi(13L));
        assertThat(between.getStartedAt()).isEqualTo(1236L);
        assertThat(between.getFinishedAt()).isEqualTo(1237L);
    }

    @Test
    public void testBetweenDifferentBaseRange()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 130L, 1234L, 1235L));
        NormalizedBaseRange normalizedBaseRange2 = new NormalizedBaseRange(withVnode(0L, 31L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, START, bi(9L), 1234L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange2, bi(13L), bi(15L), 1235L, 1236L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> firstRange.between(secondRange, 1236L, 1237L));
    }

    @Test
    public void testBetweenWrongOrder()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, bi(13L), bi(15L), 1235L, 1236L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, START, bi(9L), 1234L, 1235L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> firstRange.between(secondRange, 1236L, 1237L));
    }

    @Test
    public void testBetweenAdjacent()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, START, bi(9L), 1235L, 1236L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, bi(9L), bi(13L), 1234L, 1235L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> firstRange.between(secondRange, 1236L, 1237L));
    }

    @Test
    public void testSplitEnd()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, START, bi(13L), 1234L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, bi(9L), bi(15L), 1235L, 1236L);

        NormalizedRange between = firstRange.splitEnd(secondRange);
        assertThat(between.start()).isEqualTo(bi(9L));
        assertThat(between.end()).isEqualTo(bi(13L));
        assertThat(between.getStartedAt()).isEqualTo(1235L);
        assertThat(between.getFinishedAt()).isEqualTo(1235L);
    }

    @Test
    public void testSplitEndDifferentBaseRange()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedBaseRange normalizedBaseRange2 = new NormalizedBaseRange(withVnode(100L, 116L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, START, bi(13L), 1234L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange2, bi(9L), bi(15L), 1235L, 1236L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> firstRange.splitEnd(secondRange));
    }

    @Test
    public void testSplitEndWrongOrder()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, bi(9L), bi(15L), 1235L, 1236L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, START, bi(13L), 1234L, 1235L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> firstRange.splitEnd(secondRange));
    }

    @Test
    public void testSplitEndAdjacent()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, START, bi(9L), 1235L, 1236L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, bi(9L), bi(13L), 1234L, 1235L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> firstRange.splitEnd(secondRange));
    }

    @Test
    public void testCombine()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, START, bi(13L), 1234L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, bi(13L), bi(15L), 1235L, 1236L);

        NormalizedRange between = firstRange.combine(secondRange);
        assertThat(between.start()).isEqualTo(START);
        assertThat(between.end()).isEqualTo(bi(15L));
        assertThat(between.getStartedAt()).isEqualTo(1234L);
        assertThat(between.getFinishedAt()).isEqualTo(1236L);
    }

    @Test
    public void testCombineDifferentBaseRange()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedBaseRange normalizedBaseRange2 = new NormalizedBaseRange(withVnode(100L, 116L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, START, bi(13L), 1234L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange2, bi(13L), bi(15L), 1235L, 1236L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> firstRange.combine(secondRange));
    }

    @Test
    public void testCombineWrongOrder()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, START, bi(13L), 1234L, 1235L);
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, bi(13L), bi(15L), 1235L, 1236L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> firstRange.combine(secondRange));
    }

    @Test
    public void testIsCovering()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, START, bi(13L), 1234L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, bi(10L), bi(12L), 1235L, 1236L);

        assertThat(firstRange.isCovering(secondRange)).isTrue();
    }

    @Test
    public void testIsCoveringSameStart()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, START, bi(13L), 1234L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, START, bi(12L), 1235L, 1236L);

        assertThat(firstRange.isCovering(secondRange)).isTrue();
    }

    @Test
    public void testIsCoveringSameEnd()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, START, bi(13L), 1234L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, bi(10L), bi(13L), 1235L, 1236L);

        assertThat(firstRange.isCovering(secondRange)).isTrue();
    }

    @Test
    public void testIsCoveringOutsideStart()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, bi(4L), bi(13L), 1234L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, bi(3L), bi(12L), 1235L, 1236L);

        assertThat(firstRange.isCovering(secondRange)).isFalse();
    }

    @Test
    public void testIsCoveringOutsideEnd()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, bi(4L), bi(13L), 1234L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, bi(5L), bi(15L), 1235L, 1236L);

        assertThat(firstRange.isCovering(secondRange)).isFalse();
    }

    @Test
    public void testIsCoveringDifferentBaseRange()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedBaseRange normalizedBaseRange2 = new NormalizedBaseRange(withVnode(100L, 116L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, START, bi(13L), 1234L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange2, bi(10L), bi(12L), 1235L, 1236L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> firstRange.isCovering(secondRange));
    }

    @Test
    public void testIsCoveringReverse()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 115L, 1234L, 1235L));
        NormalizedRange firstRange = new NormalizedRange(normalizedBaseRange, bi(10L), bi(12L), 1235L, 1235L);
        NormalizedRange secondRange = new NormalizedRange(normalizedBaseRange, START, bi(13L), 1234L, 1235L);

        assertThat(firstRange.isCovering(secondRange)).isFalse();
    }

    @Test
    public void testCompareSimpleFirst()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 99L, 1234L, 1235L));
        NormalizedRange range1 = new NormalizedRange(normalizedBaseRange, bi(1L), bi(5L), 1234L, 1235L);
        NormalizedRange range2 = new NormalizedRange(normalizedBaseRange, bi(5L), bi(10L), 1234L, 1235L);

        assertThat(range1.compareTo(range2)).isLessThan(0);
    }

    @Test
    public void testCompareSimpleAfter()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 99L, 1234L, 1235L));
        NormalizedRange range1 = new NormalizedRange(normalizedBaseRange, bi(1L), bi(5L), 1234L, 1235L);
        NormalizedRange range2 = new NormalizedRange(normalizedBaseRange, bi(5L), bi(10L), 1234L, 1235L);

        assertThat(range2.compareTo(range1)).isGreaterThan(0);
    }

    @Test
    public void testCompareSame()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 99L, 1234L, 1235L));
        NormalizedRange range1 = new NormalizedRange(normalizedBaseRange, bi(1L), bi(5L), 1234L, 1235L);
        NormalizedRange range2 = new NormalizedRange(normalizedBaseRange, bi(1L), bi(5L), 1234L, 1235L);

        assertThat(range2.compareTo(range1)).isEqualTo(0);
    }

    @Test
    public void testCompareSameStart()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 99L, 1234L, 1235L));
        NormalizedRange range1 = new NormalizedRange(normalizedBaseRange, bi(1L), bi(5L), 1234L, 1235L);
        NormalizedRange range2 = new NormalizedRange(normalizedBaseRange, bi(1L), bi(10L), 1234L, 1235L);

        assertThat(range2.compareTo(range1)).isLessThan(0);
    }

    @Test
    public void testCompareDifferentBaseRanges()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 99L, 1234L, 1235L));
        NormalizedBaseRange normalizedBaseRange2 = new NormalizedBaseRange(withVnode(100L, Long.MAX_VALUE, 1234L, 1235L));
        NormalizedRange range1 = new NormalizedRange(normalizedBaseRange, bi(1L), bi(5L), 1234L, 1235L);
        NormalizedRange range2 = new NormalizedRange(normalizedBaseRange2, bi(1L), bi(10L), 1234L, 1235L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> range1.compareTo(range2));
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.forClass(NormalizedRange.class).usingGetClass()
                .withPrefabValues(VnodeRepairState.class, withVnode(0L, 0L, 1234L, 1235L), withVnode(0L, 1L, 1234L, 1236L))
                .withNonnullFields("base", "start", "end")
                .verify();
    }

    private BigInteger bi(long token)
    {
        return BigInteger.valueOf(token);
    }

    private VnodeRepairState withVnode(long start, long end, long startedAt, long finishedAt)
    {
        return new VnodeRepairState(new LongTokenRange(start, end), ImmutableSet.of(mockNode), startedAt, finishedAt);
    }
}
