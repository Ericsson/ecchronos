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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class NodeListComparator
{
    private static final Logger LOG = LoggerFactory.getLogger(NodeListComparator.class);

    /***
     * Compares 2 lists of nodes, finds new nodes, removed nodes and nodes where the ip address has changed.
     * The lists are sorted before comparison, so lists can be in any order.
     * @param oldNodes
     * @param newNodes
     * @return a list of NodeChangeRecord items, zero items in the list indicate the 2 lists are the same.
     */
    List<NodeChangeRecord> compareNodeLists(final List<Node> oldNodes, final List<Node> newNodes)
    {
        List<NodeChangeRecord> changesList = new LinkedList<NodeChangeRecord>();
        NodeComparator nodeComparator = new NodeComparator();
        oldNodes.sort(nodeComparator);
        newNodes.sort(nodeComparator);

        Iterator<Node> oldIterator = oldNodes.iterator();
        Iterator<Node> newIterator = newNodes.iterator();

        Node oldNode;
        Node newNode;
        oldNode = getNode(oldIterator);
        newNode = getNode(newIterator);

        while (oldNode != null)
        {
            if (newNode == null)
            {
                LOG.info("Node has been removed, Node id: {} ", oldNode.getHostId());
                changesList.add(new NodeChangeRecord(oldNode, NodeChangeRecord.NodeChangeType.DELETE));
                oldNode = getNode(oldIterator);
            }
            else
            {
                if (oldNode.getHostId().equals(newNode.getHostId()))
                {
                    // same host id, now check the ipaddress is still the same
                    if (!oldNode.getListenAddress().equals(newNode.getListenAddress()))
                    {
                        LOG.info("Node id {}, has a different ipaddress, it was {}, it is now {} ", oldNode.getHostId(), oldNode.getListenAddress(), newNode.getListenAddress());
                        changesList.add(new NodeChangeRecord(oldNode, NodeChangeRecord.NodeChangeType.UPDATE));
                    }
                    oldNode = getNode(oldIterator);
                    newNode = getNode(newIterator);
                }
                else
                {
                    if (oldNode.getHostId().compareTo(newNode.getHostId()) == 1)
                    {
                        LOG.info("Node has been added, Node id: {}", newNode.getHostId());
                        changesList.add(new NodeChangeRecord(newNode, NodeChangeRecord.NodeChangeType.INSERT));
                        newNode = getNode(newIterator);
                    }
                    else
                    {
                        LOG.info("Node has been removed, Node id: {}", oldNode.getHostId());
                        changesList.add(new NodeChangeRecord(oldNode, NodeChangeRecord.NodeChangeType.DELETE));
                        oldNode = getNode(oldIterator);
                    }
                }
            }
        }
        while (newNode != null)
        {
            changesList.add(new NodeChangeRecord(newNode, NodeChangeRecord.NodeChangeType.INSERT));
            LOG.info("Node has been added, Node id: {}", newNode.getHostId());
            newNode = getNode(newIterator);
        }
        return changesList;
    }

    private Node getNode(final Iterator<Node> iterator)
    {
        Node node;
        if ( iterator.hasNext()) {
            node = iterator.next();
        }
        else {
            node = null;
        }
        return node;
    }
}
