package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.junit.Assert.assertTrue;


public class NodeListComparatorTest {

    NodeListComparator nodeListComparator = new NodeListComparator();

    @Test
    void testSameNodeList() throws UnknownHostException {

        Node node1 = createNode( "127.0.0.1");
        Node node2 = node1;

        List<Node> oldNodes = new LinkedList<Node>();
        List<Node> newNodes = new LinkedList<Node>();
        oldNodes.add(node1);
        newNodes.add(node2);
        Assertions.assertEquals(0,nodeListComparator.compareNodeLists(oldNodes,newNodes).size());
        

    }
    @Test
    void testDifferentNodeList() throws UnknownHostException {

        Node node1 = createNode( "127.0.0.1");
        Node node2 = createNode( "127.0.0.2");

        List<Node> oldNodes = new LinkedList<Node>();
        List<Node> newNodes = new LinkedList<Node>();
        oldNodes.add(node1);
        newNodes.add(node2);
        List<NodeChangeRecord> nodeChangeList = nodeListComparator.compareNodeLists(oldNodes,newNodes);
        Assertions.assertEquals(2,nodeChangeList.size());
        Iterator<NodeChangeRecord> iterator = nodeChangeList.iterator();
        NodeChangeRecord record1 = iterator.next();
        NodeChangeRecord record2 = iterator.next();
        Assertions.assertEquals(node2.getListenAddress().toString(),record1.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.INSERT,record1.getType());
        Assertions.assertEquals(node1.getListenAddress().toString(),record2.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.DELETE,record2.getType());
    }
    @Test
    void testSameNodesDifferentOrder() throws UnknownHostException {

        Node node1 = createNode( "127.0.0.1");
        Node node2 = createNode( "127.0.0.2");
        Node node3 = createNode( "127.0.0.3");

        List<Node> oldNodes = new LinkedList<Node>();
        List<Node> newNodes = new LinkedList<Node>();
        oldNodes.add(node1);
        oldNodes.add(node2);
        oldNodes.add(node3);
        newNodes.add(node2);
        newNodes.add(node3);
        newNodes.add(node1);
        List<NodeChangeRecord> nodeChangeList = nodeListComparator.compareNodeLists(oldNodes,newNodes);
        Assertions.assertEquals(0,nodeChangeList.size());
    }
    @Test
    void testOneNodeRemoved() throws UnknownHostException {

        Node node1 = createNode( "127.0.0.1");
        Node node2 = createNode( "127.0.0.2");
        Node node3 = createNode( "127.0.0.3");

        List<Node> oldNodes = new LinkedList<Node>();
        List<Node> newNodes = new LinkedList<Node>();
        oldNodes.add(node1);
        oldNodes.add(node2);
        oldNodes.add(node3);
        newNodes.add(node2);
        newNodes.add(node3);
        List<NodeChangeRecord> nodeChangeList = nodeListComparator.compareNodeLists(oldNodes,newNodes);
        Assertions.assertEquals(1,nodeChangeList.size());
        Iterator<NodeChangeRecord> iterator = nodeChangeList.iterator();

        NodeChangeRecord record1 = iterator.next();
        Assertions.assertEquals(node1.getListenAddress().toString(),record1.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.DELETE,record1.getType());


    }
    @Test
    void testOneNodeAdded() throws UnknownHostException {

        Node node1 = createNode( "127.0.0.1");
        Node node2 = createNode( "127.0.0.2");
        Node node3 = createNode( "127.0.0.3");

        List<Node> oldNodes = new LinkedList<Node>();
        List<Node> newNodes = new LinkedList<Node>();
        oldNodes.add(node2);
        oldNodes.add(node3);
        newNodes.add(node2);
        newNodes.add(node3);
        newNodes.add(node1);

        List<NodeChangeRecord> nodeChangeList = nodeListComparator.compareNodeLists(oldNodes,newNodes);
        Assertions.assertEquals(1,nodeChangeList.size());
        Iterator<NodeChangeRecord> iterator = nodeChangeList.iterator();

        NodeChangeRecord record1 = iterator.next();
        Assertions.assertEquals(node1.getListenAddress().toString(),record1.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.INSERT,record1.getType());


    }
    @Test
    void testAllNodesRemoved() throws UnknownHostException {

        Node node1 = createNode( "127.0.0.1");
        Node node2 = createNode( "127.0.0.2");
        Node node3 = createNode( "127.0.0.3");

        List<Node> oldNodes = new LinkedList<Node>();
        List<Node> newNodes = new LinkedList<Node>();
        oldNodes.add(node2);
        oldNodes.add(node3);
        oldNodes.add(node1);

        List<NodeChangeRecord> nodeChangeList = nodeListComparator.compareNodeLists(oldNodes,newNodes);
        Assertions.assertEquals(3,nodeChangeList.size());
        Iterator<NodeChangeRecord> iterator = nodeChangeList.iterator();

        NodeChangeRecord record1 = iterator.next();
        NodeChangeRecord record2 = iterator.next();
        NodeChangeRecord record3 = iterator.next();
        Assertions.assertEquals(node3.getListenAddress().toString(),record1.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.DELETE,record1.getType());
        Assertions.assertEquals(node2.getListenAddress().toString(),record2.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.DELETE,record2.getType());
        Assertions.assertEquals(node1.getListenAddress().toString(),record3.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.DELETE,record2.getType());


    }
    @Test
    void testAllNodesAdd() throws UnknownHostException {

        Node node1 = createNode( "127.0.0.1");
        Node node2 = createNode( "127.0.0.2");
        Node node3 = createNode( "127.0.0.3");

        List<Node> oldNodes = new LinkedList<Node>();
        List<Node> newNodes = new LinkedList<Node>();
        newNodes.add(node2);
        newNodes.add(node3);
        newNodes.add(node1);

        List<NodeChangeRecord> nodeChangeList = nodeListComparator.compareNodeLists(oldNodes,newNodes);
        Assertions.assertEquals(3,nodeChangeList.size());
        Iterator<NodeChangeRecord> iterator = nodeChangeList.iterator();

        NodeChangeRecord record1 = iterator.next();
        NodeChangeRecord record2 = iterator.next();
        NodeChangeRecord record3 = iterator.next();
        Assertions.assertEquals(node3.getListenAddress().toString(),record1.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.INSERT,record1.getType());
        Assertions.assertEquals(node2.getListenAddress().toString(),record2.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.INSERT,record2.getType());
        Assertions.assertEquals(node1.getListenAddress().toString(),record3.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.INSERT,record2.getType());

    }
    @Test
    void testChangedIPAddress() throws UnknownHostException {

        Node node1 = createNode( "127.0.0.1");
        Node node2 = createNode( "127.0.0.2");
        Node node3 = createNode( "127.0.0.3");
        Node node3a = createNode( "127.0.0.3");
        ((DummyNode)node3a).setListenAddress(new InetSocketAddress(InetAddress.getByName("127.0.0.4"), 9042));

        List<Node> oldNodes = new LinkedList<Node>();
        List<Node> newNodes = new LinkedList<Node>();
        oldNodes.add(node2);
        oldNodes.add(node3);
        oldNodes.add(node1);
        newNodes.add(node2);
        newNodes.add(node3a);
        newNodes.add(node1);

        List<NodeChangeRecord> nodeChangeList = nodeListComparator.compareNodeLists(oldNodes,newNodes);
        Assertions.assertEquals(1,nodeChangeList.size());
        Iterator<NodeChangeRecord> iterator = nodeChangeList.iterator();

        NodeChangeRecord record1 = iterator.next();
        Assertions.assertEquals(node3.getListenAddress().toString(),record1.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.UPDATE,record1.getType());
    }
    @Test
    void testMiddleNodeRemoved() throws UnknownHostException {

        Node node1 = createNode( "127.0.0.1");
        Node node2 = createNode( "127.0.0.2");
        Node node3 = createNode( "127.0.0.3");

        List<Node> oldNodes = new LinkedList<Node>();
        List<Node> newNodes = new LinkedList<Node>();
        oldNodes.add(node1);
        oldNodes.add(node2);
        oldNodes.add(node3);
        newNodes.add(node1);
        newNodes.add(node3);

        List<NodeChangeRecord> nodeChangeList = nodeListComparator.compareNodeLists(oldNodes,newNodes);
        Assertions.assertEquals(1,nodeChangeList.size());
        Iterator<NodeChangeRecord> iterator = nodeChangeList.iterator();

        NodeChangeRecord record1 = iterator.next();
        Assertions.assertEquals(node2.getListenAddress().toString(),record1.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.DELETE,record1.getType());
    }
    @Test
    void test3NodesAdded() throws UnknownHostException {

        Node node1 = createNode( "127.0.0.1");
        Node node2 = createNode( "127.0.0.2");
        Node node3 = createNode( "127.0.0.3");
        Node node4 = createNode( "127.0.0.4");
        Node node5 = createNode( "127.0.0.5");

        List<Node> oldNodes = new LinkedList<Node>();
        List<Node> newNodes = new LinkedList<Node>();
        oldNodes.add(node1);
        oldNodes.add(node5);
        newNodes.add(node1);
        newNodes.add(node2);
        newNodes.add(node3);
        newNodes.add(node4);
        newNodes.add(node5);

        List<NodeChangeRecord> nodeChangeList = nodeListComparator.compareNodeLists(oldNodes,newNodes);
        Assertions.assertEquals(3,nodeChangeList.size());
        Iterator<NodeChangeRecord> iterator = nodeChangeList.iterator();

         NodeChangeRecord record1 = iterator.next();
        NodeChangeRecord record2 = iterator.next();
        NodeChangeRecord record3 = iterator.next();
        Assertions.assertEquals(node3.getListenAddress().toString(),record1.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.INSERT,record1.getType());
        Assertions.assertEquals(node2.getListenAddress().toString(),record2.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.INSERT,record2.getType());
        Assertions.assertEquals(node4.getListenAddress().toString(),record3.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.INSERT,record2.getType());


    }
    @Test
    void test3NodesAddedInMiddle() throws UnknownHostException {

        Node node1 = createNode( "127.0.0.1");
        Node node2 = createNode( "127.0.0.2");
        Node node3 = createNode( "127.0.0.3");
        Node node4 = createNode( "127.0.0.4");
        Node node5 = createNode( "127.0.0.5");

        List<Node> oldNodes = new LinkedList<Node>();
        List<Node> newNodes = new LinkedList<Node>();
        oldNodes.add(node3);
        oldNodes.add(node4);
        newNodes.add(node1);
        newNodes.add(node2);
        newNodes.add(node3);
        newNodes.add(node4);
        newNodes.add(node5);

        List<NodeChangeRecord> nodeChangeList = nodeListComparator.compareNodeLists(oldNodes,newNodes);
        Assertions.assertEquals(3,nodeChangeList.size());
        Iterator<NodeChangeRecord> iterator = nodeChangeList.iterator();

        NodeChangeRecord record1 = iterator.next();
        NodeChangeRecord record2 = iterator.next();
        NodeChangeRecord record3 = iterator.next();
        Assertions.assertEquals(node5.getListenAddress().toString(),record1.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.INSERT,record1.getType());
        Assertions.assertEquals(node2.getListenAddress().toString(),record2.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.INSERT,record2.getType());
        Assertions.assertEquals(node1.getListenAddress().toString(),record3.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.INSERT,record2.getType());


    }
    @Test
    void test3NodesRemovedFromMiddle() throws UnknownHostException {

        Node node1 = createNode( "127.0.0.1");
        Node node2 = createNode( "127.0.0.2");
        Node node3 = createNode( "127.0.0.3");
        Node node4 = createNode( "127.0.0.4");
        Node node5 = createNode( "127.0.0.5");

        List<Node> oldNodes = new LinkedList<Node>();
        List<Node> newNodes = new LinkedList<Node>();
        newNodes.add(node3);
        newNodes.add(node4);
        oldNodes.add(node1);
        oldNodes.add(node2);
        oldNodes.add(node3);
        oldNodes.add(node4);
        oldNodes.add(node5);

        List<NodeChangeRecord> nodeChangeList = nodeListComparator.compareNodeLists(oldNodes,newNodes);
        Assertions.assertEquals(3,nodeChangeList.size());
        Iterator<NodeChangeRecord> iterator = nodeChangeList.iterator();

        NodeChangeRecord record1 = iterator.next();
        NodeChangeRecord record2 = iterator.next();
        NodeChangeRecord record3 = iterator.next();
        Assertions.assertEquals(node5.getListenAddress().toString(),record1.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.DELETE,record1.getType());
        Assertions.assertEquals(node2.getListenAddress().toString(),record2.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.DELETE,record2.getType());
        Assertions.assertEquals(node1.getListenAddress().toString(),record3.getNode().getListenAddress().toString());
        Assertions.assertEquals(NodeChangeRecord.NodeChangeType.DELETE,record2.getType());


    }




    Node createNode(String ipAddress) throws UnknownHostException {

        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(ipAddress), 9042);
        EndPoint endPoint = new DefaultEndPoint(address);
        UUID nodeUUID= UUID.nameUUIDFromBytes(ipAddress.getBytes());
        Node node = new DummyNode(endPoint, address, nodeUUID);
        return node;

    }


}


