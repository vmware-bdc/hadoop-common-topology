package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImplOnVirtualization;

public class VirtualizedSchedulerNode extends SchedulerNode {

  public VirtualizedSchedulerNode(RMNode node) {
	super(node);
  }
	
  public String getNodeGroup() {
	if (!(rmNode instanceof RMNodeImplOnVirtualization)) {
	  // TODO throw a exception here may be better.
	  return null;
	}
	RMNodeImplOnVirtualization rmNodeV = (RMNodeImplOnVirtualization) rmNode;
	return rmNodeV.getNodeGroupName();
  }
}
