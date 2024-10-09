package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.datastax.oss.driver.api.core.metadata.Node;

import java.util.Comparator;
import java.util.UUID;

public class NodeComparator implements Comparator<Node>
{
    @Override
    public int compare(Node firstNode, Node secondNode) {
        return firstNode.getHostId().compareTo( secondNode.getHostId());
    }
}
