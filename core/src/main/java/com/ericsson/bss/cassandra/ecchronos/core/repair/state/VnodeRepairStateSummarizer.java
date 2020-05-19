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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Utility class to handle partially repaired ranges and converting them back
 * to full vnodes when possible in order to minimize memory usage.
 */
public final class VnodeRepairStateSummarizer
{
    private static final long ONE_HOUR_IN_MS = TimeUnit.HOURS.toMillis(1);

    private final NormalizedBaseRange myBaseVnode;
    private final List<NormalizedRange> mySummarizedRanges;
    private final MergeStrategy myMergeStrategy;

    private VnodeRepairStateSummarizer(VnodeRepairState baseVnode, Collection<VnodeRepairState> subStates, MergeStrategy mergeStrategy)
    {
        this.myBaseVnode = new NormalizedBaseRange(baseVnode);
        this.mySummarizedRanges = subStates.stream()
                .map(myBaseVnode::transform)
                .sorted()
                .collect(Collectors.toCollection(ArrayList::new));
        this.myMergeStrategy = mergeStrategy;

        // Add the full range first so that we can split out any sub ranges that we are missing
        mySummarizedRanges.add(0, myBaseVnode.transform(baseVnode));
    }

    /**
     * Summarize vnode repair states based on actual vnode data.
     * <br><br>
     * Generates virtual node repair states based on the partial vnodes repaired.
     * If there are partial ranges not covered the base vnode repair state will
     * be filled in there.
     * <br><br>
     * In case of overlapping ranges the ranges will be split in three parts like:<br>
     * (5, 15], (8, 30] will become (5, 8], (8, 15], (15, 30].<br>
     * The middle section will retain the highest repaired at of the two.
     * <br><br>
     * Adjacent ranges repaired within one hour will be merged together.
     *
     * @param baseVnodes The base vnode set retrieved from the keyspace replication.
     * @param partialVnodes The repaired vnodes that can be sub-ranges of the base vnodes.
     * @return The summarized virtual node states.
     */
    public static List<VnodeRepairState> summarizePartialVnodes(List<VnodeRepairState> baseVnodes, Collection<VnodeRepairState> partialVnodes)
    {
        return summarizePartialVnodes(baseVnodes, partialVnodes, VnodeRepairStateSummarizer::isCloseInTime);
    }

    /**
     * Summarize vnode repair states based on actual vnode data.
     * <br><br>
     * Generates virtual node repair states based on the partial vnodes repaired.
     * If there are partial ranges not covered the base vnode repair state will
     * be filled in there.
     * <br><br>
     * In case of overlapping ranges the ranges will be split in three parts like:<br>
     * (5, 15], (8, 30] will become (5, 8], (8, 15], (15, 30].<br>
     * The middle section will retain the highest repaired at of the two.
     * <br><br>
     * Adjacent ranges will be merged based on the provided merge strategy.
     *
     * @param baseVnodes The base vnode set retrieved from the keyspace replication.
     * @param partialVnodes The repaired vnodes that can be sub-ranges of the base vnodes.
     * @param mergeStrategy The merge strategy to use.
     * @return The summarized virtual node states.
     */
    public static List<VnodeRepairState> summarizePartialVnodes(List<VnodeRepairState> baseVnodes, Collection<VnodeRepairState> partialVnodes, MergeStrategy mergeStrategy)
    {
        List<VnodeRepairState> vnodeRepairStates = new ArrayList<>(partialVnodes);

        for (VnodeRepairState baseState : baseVnodes)
        {
            List<VnodeRepairState> covering = new ArrayList<>();
            for (VnodeRepairState actualState : vnodeRepairStates)
            {
                if (baseState.getTokenRange().isCovering(actualState.getTokenRange()))
                {
                    covering.add(actualState);
                }
            }
            if (covering.isEmpty())
            {
                vnodeRepairStates.add(baseState);
            }
            else
            {
                List<VnodeRepairState> replacement = new VnodeRepairStateSummarizer(baseState, covering, mergeStrategy).summarize();
                vnodeRepairStates.removeAll(covering);
                vnodeRepairStates.addAll(replacement);
            }
        }

        return vnodeRepairStates;
    }

    public List<VnodeRepairState> summarize()
    {
        splitOverlapping();

        for (int i = 0; i < mySummarizedRanges.size() - 1; i++)
        {
            NormalizedRange current = mySummarizedRanges.get(i);
            NormalizedRange next = mySummarizedRanges.get(i + 1);

            if (myMergeStrategy.shouldMerge(current, next))
            {
                // If two vnodes are close in time we merge them together using
                // the lowest timestamp of the two
                mySummarizedRanges.add(i, current.combine(next));

                mySummarizedRanges.remove(current);
                mySummarizedRanges.remove(next);

                // Check the newly generated vnode since it might be possible
                // to merge it again
                i--;
            }
        }

        return mySummarizedRanges.stream()
                .map(myBaseVnode::transform)
                .collect(Collectors.toList());
    }

    private void splitOverlapping()
    {
        for (int i = 0; i < mySummarizedRanges.size() - 1; i++)
        {
            NormalizedRange current = mySummarizedRanges.get(i);
            NormalizedRange next = mySummarizedRanges.get(i + 1);

            if (current.isCovering(next))
            {
                splitCoveringRange(current, next);
                i--;
            }
            else if (current.end().compareTo(next.start()) > 0)
            {
                // Replace e.g. "(5, 15], (8, 30]" with "(5, 8], (8, 15], (15, 30]"
                // The middle section (8, 15] gets the highest "repaired at" of the two overlapping ranges
                mySummarizedRanges.remove(current);
                mySummarizedRanges.remove(next);

                insertSorted(current.mutateEnd(next.start()), mySummarizedRanges);
                insertSorted(current.splitEnd(next), mySummarizedRanges);
                insertSorted(next.mutateStart(current.end()), mySummarizedRanges);
                i--;
            }
        }
    }

    private void splitCoveringRange(NormalizedRange covering, NormalizedRange covered)
    {
        if (covering.repairedAt() >= covered.repairedAt())
        {
            // We already cover the sub range with a later repaired at, remove it
            mySummarizedRanges.remove(covered);
        }
        else
        {
            // Since the covering range is repaired earlier than the covered range
            // we replace the covering range with smaller ranges around the covered
            // range. The covered range is already in place in the list so there
            // is no need to modify it.
            mySummarizedRanges.remove(covering);

            if (covering.start().compareTo(covered.start()) != 0)
            {
                insertSorted(covering.mutateEnd(covered.start()), mySummarizedRanges);
            }
            if (covering.end().compareTo(covered.end()) != 0)
            {
                insertSorted(covering.mutateStart(covered.end()), mySummarizedRanges);
            }
        }
    }

    private static void insertSorted(NormalizedRange toInsert, List<NormalizedRange> collection)
    {
        int index = Collections.binarySearch(collection, toInsert);

        if (index < 0)
        {
            index = (-index) - 1;
        }

        collection.add(index, toInsert);
    }

    private static boolean isCloseInTime(NormalizedRange v1, NormalizedRange v2)
    {
        return Math.abs(v1.repairedAt() - v2.repairedAt()) < ONE_HOUR_IN_MS;
    }

    /**
     * A merge strategy for adjacent sub ranges.
     */
    public interface MergeStrategy
    {
        boolean shouldMerge(NormalizedRange range1, NormalizedRange range2);
    }
}
