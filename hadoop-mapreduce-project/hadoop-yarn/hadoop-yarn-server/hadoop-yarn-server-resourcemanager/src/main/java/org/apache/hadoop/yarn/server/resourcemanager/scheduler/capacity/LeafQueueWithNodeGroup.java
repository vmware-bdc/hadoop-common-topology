package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeWithNodeGroup;

public class LeafQueueWithNodeGroup extends LeafQueue {
	
  public LeafQueueWithNodeGroup(CapacitySchedulerContext cs,
      Configuration conf, String queueName, CSQueue parent,
      Comparator<SchedulerApp> applicationComparator, CSQueue old) {
    super(cs, conf, queueName, parent, applicationComparator, old);
  }

  @Override
  protected CSAssignment assignContainersOnNode(Resource clusterResource, 
      SchedulerNode node, SchedulerApp application, 
      Priority priority, RMContainer reservedContainer) {

    Resource assigned = Resources.none();

    // Data-local
    assigned = 
        assignNodeLocalContainers(clusterResource, node, application, priority,
            reservedContainer); 
    if (Resources.greaterThan(assigned, Resources.none())) {
      return new CSAssignment(assigned, NodeType.NODE_LOCAL);
    }
         
    // NodeGroup-local
    assigned = 
        assignNodeGroupLocalContainers(clusterResource, node, application, priority,
            reservedContainer); 
    if (Resources.greaterThan(assigned, Resources.none())) {
      return new CSAssignment(assigned, NodeType.NODEGROUP_LOCAL);
    }
	         
    // Rack-local
    assigned = 
        assignRackLocalContainers(clusterResource, node, application, priority, 
            reservedContainer);
    if (Resources.greaterThan(assigned, Resources.none())) {
      return new CSAssignment(assigned, NodeType.RACK_LOCAL);
    }
	             
    // Off-switch
    return new CSAssignment(assignOffSwitchContainers(clusterResource, node, application, 
        priority, reservedContainer), NodeType.OFF_SWITCH);
  }
	
  private Resource assignNodeGroupLocalContainers(Resource clusterResource,  
      SchedulerNode node, SchedulerApp application, Priority priority,
      RMContainer reservedContainer) {

    ResourceRequest request = null;
    if (node instanceof SchedulerNodeWithNodeGroup ) {
      SchedulerNodeWithNodeGroup vNode = (SchedulerNodeWithNodeGroup) node;
      request = application.getResourceRequest(
          priority, vNode.getNodeGroup());
    }

    if (request != null) {
      if (canAssign(application, priority, node, NodeType.NODEGROUP_LOCAL, 
          reservedContainer)) {
        return assignContainer(clusterResource, node, application, priority, request, 
            NodeType.NODEGROUP_LOCAL, reservedContainer);
      }
    }
    return Resources.none();
  }
}
