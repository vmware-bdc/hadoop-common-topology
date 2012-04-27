/**
 * Copyright 2012 VMware, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
