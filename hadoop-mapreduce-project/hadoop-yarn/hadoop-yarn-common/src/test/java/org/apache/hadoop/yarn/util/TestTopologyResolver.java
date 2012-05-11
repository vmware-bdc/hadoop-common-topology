package org.apache.hadoop.yarn.util;

import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.TopologyResolver;
import org.junit.Assert;
import org.junit.Test;

public class TestTopologyResolver {
  private static boolean isOnVirtualization;
  
  @Test
  public void testResolverNative() {
    String host1 = "h1";
    String rack1 = "/rack1";
    isOnVirtualization = false;
    Node node1 = new NodeBase(host1, rack1);
    Assert.assertTrue("Get rack failed.", rack1.equals(TopologyResolver.getRack(node1, isOnVirtualization)));
    Assert.assertNull("Nodegroup should be null for native. ", TopologyResolver.getNodeGroup(node1, isOnVirtualization));
  }

  @Test
  public void testResolverOnVirtualization() {
	String host1 = "h1";
	String rack1 = "/rack1";
	String nodegroup1 = "/nodegroup1";
	isOnVirtualization = true;
	Node node1 = new NodeBase(host1, rack1 + nodegroup1);
	Assert.assertTrue("Get rack failed.", rack1.equals(TopologyResolver.getRack(node1, isOnVirtualization)));
	Assert.assertTrue("Get nodegroup failed.", nodegroup1.equals(TopologyResolver.getNodeGroup(node1, isOnVirtualization)));
  }

}
