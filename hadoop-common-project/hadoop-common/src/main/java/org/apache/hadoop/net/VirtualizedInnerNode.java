/**
 * Portions Copyright 2012 VMware, Inc.
 *
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
package org.apache.hadoop.net;

class VirtualizedInnerNode extends InnerNode {

	public VirtualizedInnerNode(String name, String location, InnerNode parent,
			int level) {
		super(name, location, parent, level);
	}

	public VirtualizedInnerNode(String name, String location) {
		super(name, location);
	}

	public VirtualizedInnerNode(String path) {
		super(path);
	}

	@Override
	boolean isRack() {
		// it is node group
		if (getChildren().isEmpty()) {
			return false;
		}

		Node firstChild = children.get(0);

		if (firstChild instanceof InnerNode) {
			Node firstGrandChild = (((InnerNode) firstChild).children).get(0);
			if (firstGrandChild instanceof InnerNode) {
				// it is datacenter
				return false;
			} else {
				return true;
			}
		}
		return false;
	}

	/**
	 * Judge if this node represents a node group (hypervisor)
	 * 
	 * @return true if it has no child or its children are not InnerNodes
	 */
	boolean isNodeGroup() {
	  if (children.isEmpty()) {
		return true;
	  }

	  Node firstChild = children.get(0);
	  if (firstChild instanceof InnerNode) {
		// it is rack or datacenter
		return false;
	  }
	  return true;
	}

	@Override
	protected InnerNode doCreateParentNode(String parentName) {
		return new VirtualizedInnerNode(parentName, getPath(this), this,
				this.getLevel() + 1);
	}

	@Override
	protected boolean doAreChildrenLeaves() {
		return isNodeGroup();
	}

}
