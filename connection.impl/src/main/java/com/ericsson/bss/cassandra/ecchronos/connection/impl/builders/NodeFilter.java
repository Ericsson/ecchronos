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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public interface NodeFilter
{
    Logger LOG = LoggerFactory.getLogger(NodeFilter.class);

    /**
     * Resolve the list of nodes matching this filter from the cluster metadata.
     */
    default List<Node> resolve(final CqlSession session)
    {
        List<Node> nodesList = new ArrayList<>();
        for (Node node : session.getMetadata().getNodes().values())
        {
            if (isValid(node))
            {
                nodesList.add(node);
                LOG.debug("Processing Node added to nodesList {}", node.getHostId());
            }
            else
            {
                LOG.debug("Skipping Node {}", node.getHostId());
            }
        }
        return nodesList;
    }

    /**
     * Check if a single node matches this filter.
     */
    boolean isValid(Node node);
}
