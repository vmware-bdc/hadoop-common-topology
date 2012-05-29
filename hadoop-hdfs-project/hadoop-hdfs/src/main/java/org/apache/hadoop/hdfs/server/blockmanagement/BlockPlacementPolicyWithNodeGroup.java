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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy.NotEnoughReplicasException;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NetworkTopologyWithNodeGroup;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.TopologyResolver;

public class BlockPlacementPolicyWithNodeGroup extends
		BlockPlacementPolicyDefault {
	
  BlockPlacementPolicyWithNodeGroup(Configuration conf,  FSClusterStats stats,
      NetworkTopology clusterMap) {
    initialize(conf, stats, clusterMap);
  }
  
  BlockPlacementPolicyWithNodeGroup() {
  }
  
  public void initialize(Configuration conf,  FSClusterStats stats,
          NetworkTopology clusterMap) {
	super.initialize(conf, stats, clusterMap);
  }

  /* choose <i>localMachine</i> as the target.
   * if <i>localMachine</i> is not available, 
   * choose a node on the same rack
   * @return the chosen node
   */
  @Override
  protected DatanodeDescriptor chooseLocalNode(
	  DatanodeDescriptor localMachine,
	  HashMap<Node, Node> excludedNodes,
	  long blocksize,
	  int maxNodesPerRack,
	  List<DatanodeDescriptor> results)
	    throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
	if (localMachine == null)
	  return chooseRandom(NodeBase.ROOT, excludedNodes, 
	      blocksize, maxNodesPerRack, results);
	      
	// otherwise try local machine first
	Node oldNode = excludedNodes.put(localMachine, localMachine);
	if (oldNode == null) { // was not in the excluded list
	  if (isGoodTarget(localMachine, blocksize,
	      maxNodesPerRack, false, results)) {
	    results.add(localMachine);
	    return localMachine;
	  }
	} 

	  // try a node on local node group
	DatanodeDescriptor chosenNode = chooseLocalNodeGroup((NetworkTopologyWithNodeGroup)clusterMap, localMachine, excludedNodes, 
	    blocksize, maxNodesPerRack, results);
	if (chosenNode != null) {
	  return chosenNode;
	}
	// try a node on local rack
	return chooseLocalRack(localMachine, excludedNodes, 
	    blocksize, maxNodesPerRack, results);
  }
  
  /* choose one node from the rack that <i>localMachine</i> is on.
   * if no such node is available, choose one node from the rack where
   * a second replica is on.
   * if still no such node is available, choose a random node 
   * in the cluster.
   * @return the chosen node
   */
  @Override
  protected DatanodeDescriptor chooseLocalRack(
                                             DatanodeDescriptor localMachine,
                                             HashMap<Node, Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeDescriptor> results)
    throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, 
                          blocksize, maxNodesPerRack, results);
    }
      
    // choose one from the local rack
    try {
      //clusterMap.g
      return chooseRandom(clusterMap.getRack(localMachine.getNetworkLocation()),
                          excludedNodes, blocksize, maxNodesPerRack, results);
    } catch (NotEnoughReplicasException e1) {
      // find the second replica
      DatanodeDescriptor newLocal=null;
      for(Iterator<DatanodeDescriptor> iter=results.iterator();
          iter.hasNext();) {
        DatanodeDescriptor nextNode = iter.next();
        if (nextNode != localMachine) {
          newLocal = nextNode;
          break;
        }
      }
      if (newLocal != null) {
        try {
          return chooseRandom(clusterMap.getRack(newLocal.getNetworkLocation()),
                              excludedNodes, blocksize, maxNodesPerRack, results);
        } catch(NotEnoughReplicasException e2) {
          //otherwise randomly choose one from the network
          return chooseRandom(NodeBase.ROOT, excludedNodes,
                              blocksize, maxNodesPerRack, results);
        }
      } else {
        //otherwise randomly choose one from the network
        return chooseRandom(NodeBase.ROOT, excludedNodes,
                            blocksize, maxNodesPerRack, results);
      }
    }
  }
  
  protected void chooseRemoteRack(int numOfReplicas,
          DatanodeDescriptor localMachine,
          HashMap<Node, Node> excludedNodes,
          long blocksize,
          int maxReplicasPerRack,
          List<DatanodeDescriptor> results)
          throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // randomly choose one node from remote racks
    try {
      chooseRandom(numOfReplicas, "~"+TopologyResolver.getRack(localMachine.getNetworkLocation(), true),
      excludedNodes, blocksize, maxReplicasPerRack, results);
      } catch (NotEnoughReplicasException e) {
        chooseRandom(numOfReplicas-(results.size()-oldNumOfReplicas),
        localMachine.getNetworkLocation(), excludedNodes, blocksize, 
        maxReplicasPerRack, results);
      }
  }
	
  private DatanodeDescriptor chooseLocalNodeGroup(NetworkTopologyWithNodeGroup virtClusterMap,
      DatanodeDescriptor localMachine, HashMap<Node, Node> excludedNodes, long blocksize, 
      int maxNodesPerRack, List<DatanodeDescriptor> results) throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, 
      blocksize, maxNodesPerRack, results);
    }

    // choose one from the local node group
    try {
      return chooseRandom(virtClusterMap.getNodeGroup(localMachine.getNetworkLocation()),
      excludedNodes, blocksize, maxNodesPerRack, results);
    } catch (NotEnoughReplicasException e1) {
      // find the second replica
      DatanodeDescriptor newLocal=null;
      for(Iterator<DatanodeDescriptor> iter=results.iterator();
        iter.hasNext();) {
        DatanodeDescriptor nextNode = iter.next();
        if (nextNode != localMachine) {
          newLocal = nextNode;
          break;
        }
      }
      if (newLocal != null) {
        try {
          return chooseRandom(virtClusterMap.getNodeGroup(newLocal.getNetworkLocation()),
            excludedNodes, blocksize, maxNodesPerRack, results);
        } catch(NotEnoughReplicasException e2) {
          //otherwise randomly choose one from the network
          return chooseRandom(NodeBase.ROOT, excludedNodes,
              blocksize, maxNodesPerRack, results);
        }
      } else {
        //otherwise randomly choose one from the network
        return chooseRandom(NodeBase.ROOT, excludedNodes,
            blocksize, maxNodesPerRack, results);
      }
    }
  }

}
