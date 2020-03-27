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

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
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
public class TestNormalizedBaseRange
{
    private static final BigInteger START = BigInteger.ZERO;

    @Mock
    Host mockHost;

    @Test
    public void testTransformBaseRange()
    {
        VnodeRepairState vnodeRepairState = withVnode(1L, 10L, 1234L);
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(vnodeRepairState);

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, START, bi(9L), 1234L);

        NormalizedRange actualRange = normalizedBaseRange.transform(vnodeRepairState);

        assertThat(actualRange).isEqualTo(expectedRange);
        assertThat(actualRange.start()).isEqualTo(START);
        assertThat(actualRange.end()).isEqualTo(bi(9L));
        assertThat(actualRange.repairedAt()).isEqualTo(1234L);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(vnodeRepairState);
    }

    @Test
    public void testTransformMaxTokenAsStart()
    {
        VnodeRepairState vnodeRepairState = withVnode(Long.MAX_VALUE, Long.MIN_VALUE, 1234L);
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(vnodeRepairState);

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, START, bi(1L), 1234L);

        NormalizedRange actualRange = normalizedBaseRange.transform(vnodeRepairState);

        assertThat(actualRange).isEqualTo(expectedRange);
        assertThat(actualRange.start()).isEqualTo(START);
        assertThat(actualRange.end()).isEqualTo(bi(1L));
        assertThat(actualRange.repairedAt()).isEqualTo(1234L);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(vnodeRepairState);
    }

    @Test
    public void testTransformMaxTokenAsEnd()
    {
        VnodeRepairState vnodeRepairState = withVnode(0L, Long.MAX_VALUE, 1234L);
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(vnodeRepairState);

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, START, bi(Long.MAX_VALUE), 1234L);

        NormalizedRange actualRange = normalizedBaseRange.transform(vnodeRepairState);

        assertThat(actualRange).isEqualTo(expectedRange);
        assertThat(actualRange.start()).isEqualTo(START);
        assertThat(actualRange.end()).isEqualTo(bi(Long.MAX_VALUE));
        assertThat(actualRange.repairedAt()).isEqualTo(1234L);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(vnodeRepairState);
    }

    @Test
    public void testTransformBaseRangeFullRange()
    {
        VnodeRepairState vnodeRepairState = withVnode(0L, 0L, 1234L);
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(vnodeRepairState);

        assertThat(normalizedBaseRange.end).isEqualTo(LongTokenRange.FULL_RANGE);

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, START, LongTokenRange.FULL_RANGE, 1234L);

        NormalizedRange actualRange = normalizedBaseRange.transform(vnodeRepairState);

        assertThat(actualRange).isEqualTo(expectedRange);
        assertThat(actualRange.start()).isEqualTo(START);
        assertThat(actualRange.end()).isEqualTo(LongTokenRange.FULL_RANGE);
        assertThat(actualRange.repairedAt()).isEqualTo(1234L);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(vnodeRepairState);
    }

    @Test
    public void testTransformSimpleRange()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(1L, 10L, 1234L));
        VnodeRepairState subRange = withVnode(2L, 5L, 1235L);

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, bi(1L), bi(4L), 1235L);

        NormalizedRange actualRange = normalizedBaseRange.transform(subRange);

        assertThat(actualRange).isEqualTo(expectedRange);
        assertThat(actualRange.start()).isEqualTo(bi(1L));
        assertThat(actualRange.end()).isEqualTo(bi(4L));
        assertThat(actualRange.repairedAt()).isEqualTo(1235L);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(subRange);
    }

    @Test
    public void testTransformWraparoundRangeBeforeWraparound()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(0L, -1L, 1234L));
        VnodeRepairState subRange = withVnode(55L, 1000L, 1234L);

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, bi(55L), bi(1000L), 1234L);

        NormalizedRange actualRange = normalizedBaseRange.transform(subRange);

        assertThat(actualRange).isEqualTo(expectedRange);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(subRange);
    }

    @Test
    public void testTransformWraparoundRangeAfterWraparound()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(0L, -1L, 1234L));
        VnodeRepairState subRange = withVnode(-20L, -10L, 1234L);

        BigInteger start = LongTokenRange.FULL_RANGE.subtract(bi(20L));
        BigInteger end = LongTokenRange.FULL_RANGE.subtract(bi(10L));

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, start, end, 1234L);

        NormalizedRange actualRange = normalizedBaseRange.transform(subRange);

        assertThat(actualRange).isEqualTo(expectedRange);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(subRange);
    }

    @Test
    public void testTransformWraparoundRangeInTheMiddle()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(0L, -1L, 1234L));
        VnodeRepairState subRange = withVnode(Long.MAX_VALUE, Long.MIN_VALUE + 1, 1234L);

        BigInteger start = bi(Long.MAX_VALUE);
        BigInteger end = start.add(bi(2L));

        NormalizedRange expectedRange = new NormalizedRange(normalizedBaseRange, start, end, 1234L);

        NormalizedRange actualRange = normalizedBaseRange.transform(subRange);

        assertThat(actualRange).isEqualTo(expectedRange);

        VnodeRepairState actualVnode = normalizedBaseRange.transform(actualRange);
        assertThat(actualVnode).isEqualTo(subRange);
    }

    @Test
    public void testTransformRangeIntersectingEnd()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(0L, 50L, 1234L));
        VnodeRepairState subRange = withVnode(45L, 60L, 1234L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> normalizedBaseRange.transform(subRange));
    }

    @Test
    public void testTransformRangeIntersectingStart()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(0L, 50L, 1234L));
        VnodeRepairState subRange = withVnode(-50L, 5L, 1234L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> normalizedBaseRange.transform(subRange));
    }

    @Test
    public void testTransformRangeOutsideBoundary()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(0L, 50L, 1234L));
        VnodeRepairState subRange = withVnode(45L, 60L, 1234L);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> normalizedBaseRange.transform(subRange));
    }

    @Test
    public void testInRangeBoundary()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(0L, 50L, 1234L));

        assertThat(normalizedBaseRange.inRange(bi(0L))).isTrue();
        assertThat(normalizedBaseRange.inRange(bi(26L))).isTrue();
        assertThat(normalizedBaseRange.inRange(bi(50L))).isTrue();
    }

    @Test
    public void testOutsideBoundary()
    {
        NormalizedBaseRange normalizedBaseRange = new NormalizedBaseRange(withVnode(0L, 50L, 1234L));

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
                .withPrefabValues(VnodeRepairState.class, withVnode(0L, 0L, 1234L), withVnode(0L, 1L, 1234L))
                .withNonnullFields("baseVnode", "end")
                .verify();
    }
    
    private VnodeRepairState withVnode(long start, long end, long lastRepairedAt)
    {
        return new VnodeRepairState(new LongTokenRange(start, end), ImmutableSet.of(mockHost), lastRepairedAt);
    }
}
