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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * A lock resource for repair.
 */
public class RepairResource
{
    private final String myDataCenter;
    private final String myResourceName;

    public RepairResource(String dataCenter, String resourceName)
    {
        Preconditions.checkNotNull(resourceName);
        myDataCenter = dataCenter;
        myResourceName = resourceName;
    }

    public String getDataCenter()
    {
        return myDataCenter;
    }

    public String getResourceName(int n)
    {
        return String.format("RepairResource-%s-%d", myResourceName, n);
    }

    @Override
    public String toString()
    {
        return String.format("RepairResource(dc=%s,resource=%s)", myDataCenter, myResourceName);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepairResource that = (RepairResource) o;
        return Objects.equals(myDataCenter, that.myDataCenter) &&
                myResourceName.equals(that.myResourceName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myDataCenter, myResourceName);
    }
}
