package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeWithNodeGroup;

public class FifoSchedulerWithNodeGroup extends FifoScheduler {
  @Override
  protected int assignContainersOnNode(SchedulerNode node, 
      SchedulerApp application, Priority priority) {
    // Data-local
	int nodeLocalContainers = 
	    assignNodeLocalContainers(node, application, priority);
	
	// NodeGroup-local
	int nodegroupLocalContainers = 
	  assignNodeGroupLocalContainers(node, application, priority);
		    
    // Rack-local
	int rackLocalContainers = 
	    assignRackLocalContainers(node, application, priority);

    // Off-switch
    int offSwitchContainers =
	    assignOffSwitchContainers(node, application, priority);
    LOG.debug("assignContainersOnNode:" +
        " node=" + node.getRMNode().getNodeAddress() + 
		" application=" + application.getApplicationId().getId() +
		" priority=" + priority.getPriority() + 
		" #assigned=" + 
		(nodeLocalContainers + + nodegroupLocalContainers + 
		    rackLocalContainers + offSwitchContainers));
    return (nodeLocalContainers + nodegroupLocalContainers
        + rackLocalContainers + offSwitchContainers);
  }
  
  private int assignNodeGroupLocalContainers(SchedulerNode node, 
	  SchedulerApp application, Priority priority) {
    int assignedContainers = 0;
    if (!node.isNodeGroupAware()) {
	  return 0;
    }
	if (node.getNodeGroupName() == null)
      return 0;
    ResourceRequest request = 
        application.getResourceRequest(priority, node.getNodeGroupName());
    if (request != null) {
    // Don't allocate on this nodegroup if the application doens't need containers on this rack
    ResourceRequest rackRequest =
        application.getResourceRequest(priority, node.getRackName());
    if (rackRequest.getNumContainers() <= 0) {
      return 0;
    }
		      
    int assignableContainers = 
        Math.min(
            getMaxAllocatableContainers(application, priority, node, 
                NodeType.NODEGROUP_LOCAL), 
            request.getNumContainers());
    assignedContainers = 
        assignContainer(node, application, priority, 
            assignableContainers, request, NodeType.NODEGROUP_LOCAL);
    }
    return assignedContainers;
  }
  @Override
  protected synchronized void addNode(RMNode nodeManager) {
	this.nodes.put(nodeManager.getNodeID(), new SchedulerNodeWithNodeGroup(nodeManager));
    Resources.addTo(clusterResource, nodeManager.getTotalCapability());
  }
}
