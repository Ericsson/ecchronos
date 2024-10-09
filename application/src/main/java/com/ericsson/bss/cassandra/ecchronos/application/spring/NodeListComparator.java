package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.datastax.oss.driver.api.core.metadata.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class NodeListComparator {

    private static final Logger LOG = LoggerFactory.getLogger(NodeListComparator.class);



    private List<NodeChangeRecord> changesList= new LinkedList<NodeChangeRecord>();
    boolean complareNodeLists(List<Node> oldNodes, List<Node> newNodes )
    {
        changesList.clear();
        boolean isSame = true;
        NodeComparator nodeComparator = new NodeComparator();
        oldNodes.sort(nodeComparator);
        newNodes.sort(nodeComparator);

        Iterator<Node> oldIterator = oldNodes.iterator();
        Iterator<Node> newIterator = newNodes.iterator();

        Node oldNode;
        Node newNode;
        oldNode = getNode(oldIterator);
        newNode = getNode(newIterator);

        while ( oldNode != null  ){

            if (newNode == null)
            {
                LOG.info("new node missing ");
                changesList.add(new NodeChangeRecord(oldNode, NodeChangeRecord.NodeChangeType.DELETE ));
                isSame = false;
                oldNode = getNode(oldIterator);
            }
            else {
                if (oldNode.getHostId().equals(newNode.getHostId())) {
                    // same host id, now check the ipaddress is still the same
                    if (!oldNode.getListenAddress().equals(newNode.getListenAddress())) {
                        LOG.info("different ipaddresses ");
                        changesList.add(new NodeChangeRecord(oldNode, NodeChangeRecord.NodeChangeType.UPDATE));
                        isSame = false;
                    } else
                        LOG.info("All the same  ");
                    oldNode = getNode(oldIterator);
                    newNode = getNode(newIterator);
                } else {
                    if (oldNode.getHostId().compareTo(newNode.getHostId()) == 1) {
                        LOG.info("New Node added");
                        changesList.add(new NodeChangeRecord(newNode, NodeChangeRecord.NodeChangeType.INSERT));
                        newNode = getNode(newIterator);
                    } else {
                        LOG.info("Old Node removed");
                        changesList.add(new NodeChangeRecord(oldNode, NodeChangeRecord.NodeChangeType.DELETE));
                        oldNode = getNode(oldIterator);

                    }
                    isSame = false;

                }
            }



        }
        while ( newNode != null) {

            changesList.add(new NodeChangeRecord(newNode, NodeChangeRecord.NodeChangeType.INSERT ));
            LOG.info("Extra node added");
            isSame=false;
            newNode = getNode(newIterator);
        }


            return isSame;
    }

    private Node getNode(Iterator<Node> oldIterator) {
        Node oldNode;
        if ( oldIterator.hasNext())
            oldNode = oldIterator.next();
        else
            oldNode = null;
        return oldNode;
    }
    public List<NodeChangeRecord> getChangesList() {
        return changesList;
    }
}
