/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeWithNodeGroup;

public class LeafQueueWithNodeGroup extends LeafQueue {
	
  public LeafQueueWithNodeGroup(CapacitySchedulerContext cs,
      String queueName, CSQueue parent,
      Comparator<SchedulerApp> applicationComparator, CSQueue old) {
    super(cs, queueName, parent, applicationComparator, old);
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
    if (node.isNodeGroupAware()) {
      request = application.getResourceRequest(
          priority, node.getNodeGroupName());
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

  @Override 
  boolean canAssign(SchedulerApp application, Priority priority, 
      SchedulerNode node, NodeType type, RMContainer reservedContainer) {

    // Reserved... 
    if (reservedContainer != null) {
      return true;
    }

    // Check if we need containers on this nodegroup
    if (type == NodeType.NODEGROUP_LOCAL) {
      // Now check if we need containers on this nodegroup...
      if (node.isNodeGroupAware()) {
        ResourceRequest nodegroupLocalRequest = 
            application.getResourceRequest(priority, node.getNodeGroupName());
        if (nodegroupLocalRequest != null) {
          return nodegroupLocalRequest.getNumContainers() > 0;
        }
      }
    }

    return super.canAssign(application, priority, node, type, reservedContainer); 
  }

}
