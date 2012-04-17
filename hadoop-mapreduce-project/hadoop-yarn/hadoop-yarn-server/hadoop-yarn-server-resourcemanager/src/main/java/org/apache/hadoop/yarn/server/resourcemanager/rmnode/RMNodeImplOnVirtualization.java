package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.TopologyResolver;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

public class RMNodeImplOnVirtualization extends RMNodeImpl {

  public RMNodeImplOnVirtualization(NodeId nodeId, RMContext context,
      String hostName, int cmPort, int httpPort, Node node,
	      Resource capability) {
    super(nodeId, context, hostName, cmPort, httpPort, node, capability);
  }
  
  @Override
  public String getRackName() {
    return TopologyResolver.getRack(node, true);
  }
  
  public String getNodeGroupName() {
	return TopologyResolver.getNodeGroup(node, true);
  }

}
