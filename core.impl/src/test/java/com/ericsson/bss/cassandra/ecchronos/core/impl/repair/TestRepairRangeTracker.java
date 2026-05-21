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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import static org.assertj.core.api.Assertions.assertThat;

import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import org.junit.Test;

public class TestRepairRangeTracker
{
    private final RepairRangeTracker tracker = new RepairRangeTracker();

    @Test
    public void testHasFailedRangesReturnsFalseWhenEmpty()
    {
        assertThat(tracker.hasFailedRanges()).isFalse();
        assertThat(tracker.getFailedRanges()).isEmpty();
        assertThat(tracker.getSuccessfulRanges()).isEmpty();
    }

    @Test
    public void testSuccessfulRangeTracked()
    {
        LongTokenRange range = new LongTokenRange(1, 100);
        tracker.onRangeFinished(range, RepairStatus.SUCCESS);

        assertThat(tracker.getSuccessfulRanges()).containsExactly(range);
        assertThat(tracker.hasFailedRanges()).isFalse();
    }

    @Test
    public void testFailedRangeTracked()
    {
        LongTokenRange range = new LongTokenRange(1, 100);
        tracker.onRangeFinished(range, RepairStatus.FAILED);

        assertThat(tracker.getFailedRanges()).containsExactly(range);
        assertThat(tracker.hasFailedRanges()).isTrue();
    }

    @Test
    public void testMixedRangesTrackedCorrectly()
    {
        LongTokenRange successRange = new LongTokenRange(1, 100);
        LongTokenRange failedRange = new LongTokenRange(101, 200);

        tracker.onRangeFinished(successRange, RepairStatus.SUCCESS);
        tracker.onRangeFinished(failedRange, RepairStatus.FAILED);

        assertThat(tracker.getSuccessfulRanges()).containsExactly(successRange);
        assertThat(tracker.getFailedRanges()).containsExactly(failedRange);
        assertThat(tracker.hasFailedRanges()).isTrue();
    }
}
