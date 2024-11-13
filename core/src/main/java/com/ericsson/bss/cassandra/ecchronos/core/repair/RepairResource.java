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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A lock resource for repair.
 */
public class RepairResource
{
    private final String myDataCenter;
    private final String myResourceName;

    /**
     * Constructor.
     *
     * @param dataCenter The data center.
     * @param resourceName Resource name.
     */
    public RepairResource(final String dataCenter, final String resourceName)
    {
        myDataCenter = dataCenter;
        myResourceName = checkNotNull(resourceName);
    }

    /**
     * Get datacenter.
     *
     * @return String
     */
    public String getDataCenter()
    {
        return myDataCenter;
    }

    /**
     * Get resource name.
     *
     * @param n Resource number.
     * @return String
     */
    public String getResourceName(final int n)
    {
        return String.format("RepairResource-%s-%d", myResourceName, n);
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("RepairResource(dc=%s,resource=%s)", myDataCenter, myResourceName);
    }

    /**
     * Checks equality.
     *
     * @param o Object to check equality with.
     * @return boolean
     */
    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        RepairResource that = (RepairResource) o;
        return Objects.equals(myDataCenter, that.myDataCenter) && Objects.equals(myResourceName, that.myResourceName);
    }

    /**
     * Hash representation.
     *
     * @return int
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(myDataCenter, myResourceName);
    }
}
