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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

import org.junit.Test;

public class TestManyToOneIterator
{

    @Test
    public void testEmptyIterables()
    {
        Collection<Collection<Integer>> collections = new ArrayList<>();

        Iterator<Integer> iterator = new ManyToOneIterator<>(collections, new AscendingComparator());

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testEmptyIterator()
    {
        Collection<Collection<Integer>> collections = new ArrayList<>();

        collections.add(Collections.<Integer> emptyList());

        Iterator<Integer> iterator = new ManyToOneIterator<>(collections, new AscendingComparator());

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testMultipleEmptyIterators()
    {
        Collection<Collection<Integer>> collections = new ArrayList<>();

        for (int i = 0; i < 10; i++)
        {
            collections.add(Collections.<Integer> emptyList());
        }

        Iterator<Integer> iterator = new ManyToOneIterator<>(collections, new AscendingComparator());

        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testAscendingSingleIterator()
    {
        Iterator<Integer> iterator = new ManyToOneIterator<>(Arrays.asList(generateIntegerArray()), new AscendingComparator());

        assertAscending(iterator);
    }

    @Test
    public void testDescendingSingleIterator()
    {
        Iterator<Integer> iterator = new ManyToOneIterator<>(Arrays.asList(generateIntegerArray()), new DescendingComparator());

        assertDescending(iterator);
    }

    @Test
    public void testAscendingMultipleIterators()
    {
        Collection<Collection<Integer>> collections = new ArrayList<>();

        for (int i = 0; i < 10; i++)
        {
            collections.add(generateIntegerArray());
        }

        Iterator<Integer> iterator = new ManyToOneIterator<>(collections, new AscendingComparator());

        assertAscending(iterator);
    }

    @Test
    public void testDescendingMultipleIterators()
    {
        Collection<Collection<Integer>> collections = new ArrayList<>();

        for (int i = 0; i < 10; i++)
        {
            collections.add(generateIntegerArray());
        }

        Iterator<Integer> iterator = new ManyToOneIterator<>(collections, new DescendingComparator());

        assertDescending(iterator);
    }

    private void assertAscending(Iterator<Integer> iterator)
    {
        assertThat(iterator.hasNext()).isTrue();

        int last = iterator.next();

        while (iterator.hasNext())
        {
            int current = iterator.next();
            assertThat(current).isGreaterThanOrEqualTo(last);
            last = current;
        }
    }

    private void assertDescending(Iterator<Integer> iterator)
    {
        assertThat(iterator.hasNext()).isTrue();

        int last = iterator.next();

        while (iterator.hasNext())
        {
            int current = iterator.next();
            assertThat(current).isLessThanOrEqualTo(last);
            last = current;
        }
    }

    private Collection<Integer> generateIntegerArray()
    {
        Collection<Integer> elements = new ArrayList<>();

        Random random = new SecureRandom();

        for (int i = 0; i < random.nextInt(15) + 10; i++)
        {
            elements.add(random.nextInt());
        }

        return elements;
    }

    private class AscendingComparator implements Comparator<Integer>
    {

        @Override
        public int compare(Integer x, Integer y)
        {
            return Integer.compare(x, y);
        }

    }

    private class DescendingComparator implements Comparator<Integer>
    {

        @Override
        public int compare(Integer x, Integer y)
        {
            return Integer.compare(y, x);
        }

    }
}
