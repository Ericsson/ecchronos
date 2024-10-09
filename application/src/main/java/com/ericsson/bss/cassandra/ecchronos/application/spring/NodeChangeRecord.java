package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.datastax.oss.driver.api.core.metadata.Node;

public class NodeChangeRecord {
    enum NodeChangeType {
        INSERT,
        DELETE,
        UPDATE
    }

    private final Node node;
    private final NodeChangeType type;

    public NodeChangeRecord(Node node, NodeChangeType type) {
        this.node = node;
        this.type = type;
    }

    public  Node getNode() {
        return node;
    }
    public NodeChangeType getType() {
        return type;
    }
}
