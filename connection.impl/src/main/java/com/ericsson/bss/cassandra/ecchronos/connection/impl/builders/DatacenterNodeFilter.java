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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders;

import com.datastax.oss.driver.api.core.metadata.Node;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class DatacenterNodeFilter implements NodeFilter
{
    private final Set<String> myDatacenterNames;

    public DatacenterNodeFilter(final List<String> datacenterNames)
    {
        myDatacenterNames = new HashSet<>(datacenterNames);
    }

    @Override
    public boolean isValid(final Node node)
    {
        return myDatacenterNames.contains(node.getDatacenter());
    }
}
