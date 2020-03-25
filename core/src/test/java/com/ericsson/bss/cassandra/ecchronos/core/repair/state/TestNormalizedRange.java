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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TestNormalizedRange
{
    @Mock
    Host mockHost;

    @Test
    public void testBaseRange()
    {
        VnodeRepairState vnodeRepairState = withVnode(1, 10, 1234L);

        NormalizedRange expectedRange = new NormalizedRange(Long.MIN_VALUE, Long.MIN_VALUE + 9L, 1234L);

        NormalizedRange actualRange = NormalizedRange.transform(vnodeRepairState, vnodeRepairState);

        assertThat(actualRange).isEqualTo(expectedRange);
    }

    @Test
    public void testSimpleRange()
    {
        VnodeRepairState vnodeRepairState = withVnode(1, 10, 1234L);
        VnodeRepairState subRange = withVnode(2, 5, 1234L);

        NormalizedRange expectedRange = new NormalizedRange(Long.MIN_VALUE + 1L, Long.MIN_VALUE + 4L, 1234L);

        NormalizedRange actualRange = NormalizedRange.transform(vnodeRepairState, subRange);

        assertThat(actualRange).isEqualTo(expectedRange);
    }

    @Test
    public void testWraparoundRangeBeforeWraparound()
    {
        VnodeRepairState vnodeRepairState = withVnode(0, -1, 1234L);
        VnodeRepairState subRange = withVnode(55, 1000, 1234L);

        NormalizedRange expectedRange = new NormalizedRange(Long.MIN_VALUE + 55L, Long.MIN_VALUE + 1000L, 1234L);

        NormalizedRange actualRange = NormalizedRange.transform(vnodeRepairState, subRange);

        assertThat(actualRange).isEqualTo(expectedRange);
    }

    @Test
    public void testWraparoundRangeAfterWraparound()
    {
        VnodeRepairState vnodeRepairState = withVnode(0, -1, 1234L);
        VnodeRepairState subRange = withVnode(-20, -10, 1234L);

        NormalizedRange expectedRange = new NormalizedRange(Long.MAX_VALUE - 19, Long.MAX_VALUE - 9L, 1234L);

        NormalizedRange actualRange = NormalizedRange.transform(vnodeRepairState, subRange);

        assertThat(actualRange).isEqualTo(expectedRange);
    }

    @Test
    public void testWraparoundRangeInTheMiddle()
    {
        VnodeRepairState vnodeRepairState = withVnode(0, -1, 1234L);
        VnodeRepairState subRange = withVnode(Long.MAX_VALUE - 1, Long.MIN_VALUE + 1, 1234L);

        NormalizedRange expectedRange = new NormalizedRange(-2, 1, 1234L);

        NormalizedRange actualRange = NormalizedRange.transform(vnodeRepairState, subRange);

        assertThat(actualRange).isEqualTo(expectedRange);
    }

    @Test
    public void testCompareSimpleFirst()
    {
        NormalizedRange range1 = new NormalizedRange(1, 5, 1234L);
        NormalizedRange range2 = new NormalizedRange(5, 10, 1234L);

        assertThat(range1.compareTo(range2)).isLessThan(0);
    }

    @Test
    public void testCompareSimpleAfter()
    {
        NormalizedRange range1 = new NormalizedRange(1, 5, 1234L);
        NormalizedRange range2 = new NormalizedRange(5, 10, 1234L);

        assertThat(range2.compareTo(range1)).isGreaterThan(0);
    }

    @Test
    public void testCompareSame()
    {
        NormalizedRange range1 = new NormalizedRange(1, 5, 1234L);
        NormalizedRange range2 = new NormalizedRange(1, 5, 1234L);

        assertThat(range2.compareTo(range1)).isEqualTo(0);
    }

    @Test
    public void testCompareSameStart()
    {
        NormalizedRange range1 = new NormalizedRange(1, 5, 1234L);
        NormalizedRange range2 = new NormalizedRange(1, 10, 1234L);

        assertThat(range2.compareTo(range1)).isLessThan(0);
    }

    private VnodeRepairState withVnode(long start, long end, long lastRepairedAt)
    {
        return new VnodeRepairState(new LongTokenRange(start, end), ImmutableSet.of(mockHost), lastRepairedAt);
    }
}
