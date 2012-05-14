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
