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

import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@RunWith(MockitoJUnitRunner.class)
public class TestNormalizedBaseRange
{
    private static final BigInteger START = BigInteger.ZERO;

    @Mock
    DriverNode mockNode;

    @Test
    public void testTransformBaseRange()
    {
        VnodeRepairState vnodeRepairState = withVnode(1L, 10L, 1234L, 1235L);
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(vnodeRepairState);

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, START, bi(9L), 1234L, 1235L);

        NormalizedRange actualRange = normalizedBaseRange.transform(vnodeRepairState);

        assertThat(actualRange).isEqualTo(expectedRange);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(vnodeRepairState);
    }

    @Test
    public void testTransformMaxTokenAsStart()
    {
        VnodeRepairState vnodeRepairState = withVnode(Long.MAX_VALUE, Long.MIN_VALUE, 1234L, 1235L);
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(vnodeRepairState);

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, START, bi(1L), 1234L, 1235L);

        NormalizedRange actualRange = normalizedBaseRange.transform(vnodeRepairState);

        assertThat(actualRange).isEqualTo(expectedRange);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(vnodeRepairState);
    }

    @Test
    public void testTransformMaxTokenAsEnd()
    {
        VnodeRepairState vnodeRepairState = withVnode(-5L, Long.MAX_VALUE, 1234L, 1235L);
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(vnodeRepairState);

        BigInteger end = bi(Long.MAX_VALUE).add(BigInteger.valueOf(5));
        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, START, end, 1234L, 1235L);

        NormalizedRange actualRange = normalizedBaseRange.transform(vnodeRepairState);

        assertThat(actualRange).isEqualTo(expectedRange);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(vnodeRepairState);
    }

    @Test
    public void testTransformBaseRangeFullRange()
    {
        VnodeRepairState vnodeRepairState = withVnode(Long.MIN_VALUE, Long.MIN_VALUE, 1234L, 1235L);
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(vnodeRepairState);

        assertThat(normalizedBaseRange.end).isEqualTo(LongTokenRange.FULL_RANGE);

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, START, LongTokenRange.FULL_RANGE, 1234L, 1235L);

        NormalizedRange actualRange = normalizedBaseRange.transform(vnodeRepairState);

        assertThat(actualRange).isEqualTo(expectedRange);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(vnodeRepairState);
    }

    @Test
    public void testTransformSimpleRange()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(1L, 10L, 1234L, 1235L));
        VnodeRepairState subRange = withVnode(2L, 5L, 1235L, 1236L);

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, bi(1L), bi(4L), 1235L, 1236L);

        NormalizedRange actualRange = normalizedBaseRange.transform(subRange);

        assertThat(actualRange).isEqualTo(expectedRange);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(subRange);
    }

    @Test
    public void testTransformWraparoundRangeBeforeWraparound()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(-5L, -6L, 1234L, 1235L));
        VnodeRepairState subRange = withVnode(55L, 1000L, 1234L, 1235L);

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, bi(60L), bi(1005L), 1234L, 1235L);

        NormalizedRange actualRange = normalizedBaseRange.transform(subRange);

        assertThat(actualRange).isEqualTo(expectedRange);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(subRange);
    }

    @Test
    public void testTransformWraparoundRangeAfterWraparound()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(-5L, -6L, 1234L, 1235L));
        VnodeRepairState subRange = withVnode(-20L, -10L, 1234L, 1235L);

        BigInteger start = LongTokenRange.FULL_RANGE.subtract(bi(15L));
        BigInteger end = LongTokenRange.FULL_RANGE.subtract(bi(5L));

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, start, end, 1234L, 1235L);

        NormalizedRange actualRange = normalizedBaseRange.transform(subRange);

        assertThat(actualRange).isEqualTo(expectedRange);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(subRange);
    }

    @Test
    public void testTransformWraparoundRangeInTheMiddle()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(-5L, -6L, 1234L, 1235L));
        VnodeRepairState subRange = withVnode(Long.MAX_VALUE - 5L, Long.MAX_VALUE - 4L, 1234L, 1235L);

        BigInteger start = bi(Long.MAX_VALUE);
        BigInteger end = start.add(bi(1L));

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, start, end, 1234L, 1235L);

        NormalizedRange actualRange = normalizedBaseRange.transform(subRange);

        assertThat(actualRange).isEqualTo(expectedRange);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(subRange);
    }

    @Test
    public void testTransformRangeIntersectingEnd()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(45L, 100L, 1234L, 1235L));
        VnodeRepairState subRange = withVnode(90L, 120L, 1234L, 1235L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> normalizedBaseRange.transform(subRange));
    }

    @Test
    public void testTransformRangeIntersectingStart()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(-50L, 15L, 1234L, 1235L));
        VnodeRepairState subRange = withVnode(-100L, -30L, 1234L, 1235L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> normalizedBaseRange.transform(subRange));
    }

    @Test
    public void testTransformRangeOutsideBoundary()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 200L, 1234L, 1235L));
        VnodeRepairState subRange = withVnode(300L, 400L, 1234L, 1235L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> normalizedBaseRange.transform(subRange));
    }

    @Test
    public void testInRangeBoundary()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 150L, 1234L, 1235L));

        assertThat(normalizedBaseRange.inRange(bi(0L))).isTrue();
        assertThat(normalizedBaseRange.inRange(bi(26L))).isTrue();
        assertThat(normalizedBaseRange.inRange(bi(50L))).isTrue();
    }

    @Test
    public void testOutsideBoundary()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(100L, 150L, 1234L, 1235L));

        assertThat(normalizedBaseRange.inRange(bi(-1L))).isFalse();
        assertThat(normalizedBaseRange.inRange(bi(51L))).isFalse();
    }

    private BigInteger bi(long token)
    {
        return BigInteger.valueOf(token);
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.forClass(NormalizedBaseRange.class).usingGetClass()
                .withPrefabValues(
                        VnodeRepairState.class, withVnode(0L, 0L, 1234L, 1235L), withVnode(0L, 1L, 1234L, 1235L))
                .withNonnullFields("baseVnode", "end")
                .verify();
    }

    private VnodeRepairState withVnode(long start, long end, long startedAt, long finishedAt)
    {
        return new VnodeRepairState(new LongTokenRange(start, end), ImmutableSet.of(mockNode), startedAt, finishedAt);
    }
}
