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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.AbstractIterator;

/**
 * An iterator that takes multiple iterables and merge them together into one iterator by sorting the elements based on
 * the provided comparator.
 */
public class ManyToOneIterator<T> extends AbstractIterator<T>
{
    private final Iterator<T> myIterator;

    /**
     * Construct a new iterator with the provided iterables and comparator.
     *
     * @param iterables
     *            The iterables to iterate over.
     * @param comparator
     *            The comparator to use for comparing the elements.
     */
    public ManyToOneIterator(final Collection<? extends Iterable<T>> iterables, final Comparator<T> comparator)
    {
        List<T> elementList = new ArrayList<>();

        for (Iterable<T> iterable : iterables)
        {
            iterable.forEach(elementList::add);
        }

        elementList.sort(comparator);

        myIterator = elementList.iterator();
    }

    @Override
    protected final T computeNext()
    {
        if (myIterator.hasNext())
        {
            return myIterator.next();
        }

        return endOfData();
    }
}
