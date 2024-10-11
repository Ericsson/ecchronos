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

package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.datastax.oss.driver.api.core.metadata.Node;

import java.util.Comparator;


/***
 * This will compare 2 nodes based on the Host id of the node.
 */
public class NodeComparator implements Comparator<Node>
{
    /**
     *  This will compare 2 nodes based on the Host id of the node.
     * @param firstNode
     * @param secondNode
     * @return
     */
    @Override
    public int compare(final Node firstNode, final Node secondNode)
    {
        return firstNode.getHostId().compareTo( secondNode.getHostId());
    }
}
