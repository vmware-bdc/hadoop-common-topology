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

public class NetworkTopologyWithNodeGroup extends NetworkTopology {

	public final static String DEFAULT_NODEGROUP = "/default-nodegroup";

	public NetworkTopologyWithNodeGroup() {
		clusterMap = new InnerNodeWithNodeGroup(InnerNode.ROOT);
	}

	@Override
	protected Node getNodeForNetworkLocation(Node node) {
		// if node only with default rack info, here we need to add default
		// nodegroup info
		if (NetworkTopology.DEFAULT_RACK.equals(node.getNetworkLocation())) {
			node.setNetworkLocation(node.getNetworkLocation()
					+ DEFAULT_NODEGROUP);
		}
		Node nodeGroup = getNode(node.getNetworkLocation());
		if (nodeGroup == null) {
			nodeGroup = new InnerNode(node.getNetworkLocation());
		}
		return getNode(nodeGroup.getNetworkLocation());
	}


	@Override
	protected String doGetRack(String loc) {
		netlock.readLock().lock();
		try {
			loc = InnerNode.normalize(loc);
			Node locNode = getNode(loc);
			if (locNode instanceof InnerNodeWithNodeGroup) {
				InnerNodeWithNodeGroup node = (InnerNodeWithNodeGroup) locNode;
				if (node.isRack()) {
					return loc;
				} else if (node.isNodeGroup()) {
					return node.getNetworkLocation();
				} else {
					// may be a data center
					return null;
				}
			} else {
				// not in cluster map, don't handle it
				return loc;
			}
		} finally {
			netlock.readLock().unlock();
		}
	}

	/**
	 * Given a string representation of a node group for a specific network
	 * location
	 * 
	 * @param loc
	 *            a path-like string representation of a network location
	 * @return a node group string
	 */
	public String getNodeGroup(String loc) {
		netlock.readLock().lock();
		try {
			loc = InnerNode.normalize(loc);
			Node locNode = getNode(loc);
			if (locNode instanceof InnerNodeWithNodeGroup) {
				InnerNodeWithNodeGroup node = (InnerNodeWithNodeGroup) locNode;
				if (node.isNodeGroup()) {
					return loc;
				} else if (node.isRack()) {
					// not sure the node group for a rack
					return null;
				} else {
					// may be a leaf node
					return getNodeGroup(node.getNetworkLocation());
				}
			} else {
				// not in cluster map, don't handle it
				return loc;
			}
		} finally {
			netlock.readLock().unlock();
		}
	}

	@Override
	protected boolean compareParents(Node node1, Node node2) {
		if (node1.getParent() == null || node2.getParent() == null) {
			LOG.warn("Some nodes in: " + node1.getName() + ", "
					+ node2.getName() + "don't have parent node.");
			return false;
		} else {
			return node1.getParent().getParent() == node2.getParent()
					.getParent();
		}
	}

	/**
	 * Check if two nodes are on the same node group (hypervisor) The
	 * assumption here is: each nodes are leaf nodes.
	 * 
	 * @param node1
	 *            one node (can be null)
	 * @param node2
	 *            another node (can be null)
	 * @return true if node1 and node2 are on the same node group; false
	 *         otherwise
	 * @exception IllegalArgumentException
	 *                when either node1 or node2 is null, or node1 or node2 do
	 *                not belong to the cluster
	 */
	@Override
	public boolean isOnSameNodeGroup(Node node1, Node node2) {
		if (node1 == null || node2 == null) {
			return false;
		}
		netlock.readLock().lock();
		try {
			return node1.getParent() == node2.getParent();
		} finally {
			netlock.readLock().unlock();
		}
	}
	
	/**
	 * Check if network topology is aware of NodeGroup
	 */
	@Override
	public boolean isNodeGroupAware() {
	  return true;
	}
	
	/** Add a leaf node
	 * Update node counter & rack counter if necessary
	 * @param node node to be added; can be null
	 * @exception IllegalArgumentException if add a node to a leave 
	                                       or node to be added is not a leaf
	 */
	@Override
	public void add(Node node) {
	  if (node==null) return;
	  if( node instanceof InnerNode ) {
	    throw new IllegalArgumentException(
	      "Not allow to add an inner node: "+NodeBase.getPath(node));
	  }
	  netlock.writeLock().lock();
	  try {
	    Node rack = null;

	  	// if node only with default rack info, here we need to add default nodegroup info
	    if (NetworkTopology.DEFAULT_RACK.equals(node.getNetworkLocation())) {
	      node.setNetworkLocation(node.getNetworkLocation() + NetworkTopologyWithNodeGroup.DEFAULT_NODEGROUP);
	   	}
	    Node nodeGroup = getNode(node.getNetworkLocation());
	    if (nodeGroup == null) {
	      nodeGroup = new InnerNodeWithNodeGroup(node.getNetworkLocation());
	    }
	    rack = getNode(nodeGroup.getNetworkLocation());
  
	    if (rack != null && !(rack instanceof InnerNode)) {
	      throw new IllegalArgumentException("Unexpected data node " 
	                                         + node.toString() 
	                                         + " at an illegal network location");
	    }
	    if (clusterMap.add(node)) {
	      LOG.info("Adding a new node: " + NodeBase.getPath(node));
	      if (rack == null) {
	    	// We only track rack number here
	        numOfRacks++;
	      }
	    }
	    if(LOG.isDebugEnabled()) {
	      LOG.debug("NetworkTopology became:\n" + this.toString());
	    }
	  } finally {
	    netlock.writeLock().unlock();
	  }
    }
	
	/** Remove a node
	 * Update node counter and rack counter if necessary
	 * @param node node to be removed; can be null
	 */
	@Override
	public void remove(Node node) {
	  if (node==null) return;
	  if( node instanceof InnerNode ) {
	    throw new IllegalArgumentException(
	   "Not allow to remove an inner node: "+NodeBase.getPath(node));
	  }
	  LOG.info("Removing a node: "+NodeBase.getPath(node));
	  netlock.writeLock().lock();
	  try {
	    if (clusterMap.remove(node)) {
	      Node nodeGroup = getNode(node.getNetworkLocation());
		  if (nodeGroup == null) {
		    nodeGroup = new InnerNode(node.getNetworkLocation());
		  }
	      InnerNode rack = (InnerNode)getNode(nodeGroup.getNetworkLocation());
	      if (rack == null) {
	        numOfRacks--;
	      }
	    }
	    if(LOG.isDebugEnabled()) {
	      LOG.debug("NetworkTopology became:\n" + this.toString());
	    }
	  } finally {
	    netlock.writeLock().unlock();
	  }
	}

	  /** Sort nodes array by their distances to <i>reader</i>
	   * It linearly scans the array, if a local node is found, swap it with
	   * the first element of the array.
	   * If a local node group node is found, swap it with the first element following
	   * the local node.
	   * If a local rack node is found, swap it with the first element following
	   * the local node group node.
	   * If neither local node, node group node or local rack node is found, put a random replica
	   * location at position 0.
	   * It leaves the rest nodes untouched.
	   * @param reader the node that wishes to read a block from one of the nodes
	   * @param nodes the list of nodes containing data for the reader
	   */
	  @Override
	  public void pseudoSortByDistance( Node reader, Node[] nodes ) {
		boolean readerInTree = true;  
		if (!this.contains(reader)) {
		  // If reader is not a datanode, we temporary add this node and will remove it at last.
		  // TODO Need to resolve racing for same reader that not in tree.
		  this.add(reader);
		  readerInTree = false;
		}
	    int tempIndex = 0;
	    int localRackNode = -1;
	    int localNodeGroupNode = -1;
	    if (reader != null) {  
	      //scan the array to find the local node & local rack node
	      for (int i = 0; i < nodes.length; i++) {
	        if (tempIndex == 0 && reader == nodes[i]) { //local node
	          //swap the local node and the node at position 0
	          if (i != 0) {
	            swap(nodes, tempIndex, i);
	          }
	          tempIndex=1;
	          
	          if (localRackNode != -1 && (localNodeGroupNode !=-1)) {
	            if (localRackNode == 0) {
	              localRackNode = i;
	            }
	            if (localNodeGroupNode == 0) {
	              localNodeGroupNode = i;	
	            }
	            break;
	          }
	        } else if (localNodeGroupNode == -1 && isOnSameNodeGroup(reader, nodes[i])) {
	            //local node group
	            localNodeGroupNode = i;
	            // node local and rack local are already found
	            if(tempIndex != 0 && localRackNode != -1) break;
	        } else if (localRackNode == -1 && isOnSameRack(reader, nodes[i])) {
	          localRackNode = i;
	          if (tempIndex != 0 && localNodeGroupNode != -1) break;
	        }
	      } 

	      // swap the local nodegroup node and the node at position tempIndex
	      if(localNodeGroupNode != -1 && localNodeGroupNode != tempIndex) {
	        swap(nodes, tempIndex, localNodeGroupNode);
	        if (localRackNode == tempIndex) {
	          localRackNode = localNodeGroupNode;
	        }
	        tempIndex++;
	      }
	      
	      // swap the local rack node and the node at position tempIndex
	      if(localRackNode != -1 && localRackNode != tempIndex) {
	        swap(nodes, tempIndex, localRackNode);
	        tempIndex++;
	      }
	    }
	    
	    // put a random node at position 0 if there is not a local/local-nodegroup/local-rack node
		if (tempIndex == 0 && localNodeGroupNode == -1 && localRackNode == -1
		    && nodes.length != 0) {
	      swap(nodes, 0, r.nextInt(nodes.length));
	    }
		
		if (!readerInTree) {
		  this.remove(reader);
		}
	  }	
}
