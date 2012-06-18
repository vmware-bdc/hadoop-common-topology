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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NetworkTopologyWithNodeGroup;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.TopologyResolver;

public class ReplicationTargetChooserWithNodeGroup extends
    ReplicationTargetChooser {

  ReplicationTargetChooserWithNodeGroup(boolean considerLoad,
      FSNamesystem fs, NetworkTopology clusterMap) {
    super(considerLoad, fs, clusterMap);
  }
  
  ReplicationTargetChooserWithNodeGroup() {
  }
  
  /** Choose node with other locality other than <i>localMachine</i>.
   * As there is no local node is available, choose one node with nearest 
   * locality.
   * Default to be local rack, but could be overridden in sub-class for other 
   * localities.
   * @return the chosen node
   */
  @Override
  protected DatanodeDescriptor chooseOtherLocalityNode(DatanodeDescriptor localMachine,
      List<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeDescriptor> results) throws NotEnoughReplicasException {
  
    // try a node on local node group
    DatanodeDescriptor chosenNode = chooseLocalNodeGroup((NetworkTopologyWithNodeGroup)clusterMap, localMachine, excludedNodes, 
        blocksize, maxNodesPerRack, results);
    if (chosenNode != null) {
      return chosenNode;
    }

    // if cannot find nodegroup-local node, try a node on local rack
    return chooseLocalRack(localMachine, excludedNodes, 
        blocksize, maxNodesPerRack, results);
  }

  private DatanodeDescriptor chooseLocalNodeGroup(NetworkTopologyWithNodeGroup clusterMap,
      DatanodeDescriptor localMachine, List<Node> excludedNodes, long blocksize, 
    int maxNodesPerRack, List<DatanodeDescriptor> results) throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, 
      blocksize, maxNodesPerRack, results);
    }

    // choose one from the local node group
    try {
      return chooseRandom(clusterMap.getNodeGroup(localMachine.getNetworkLocation()),
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
          return chooseRandom(clusterMap.getNodeGroup(newLocal.getNetworkLocation()),
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
                                             List<Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeDescriptor> results)
    throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, 
                          blocksize, maxNodesPerRack, results);
    }

    // Nodes under same nodegroup should be excluded.
    String nodeGroupScope = localMachine.getNetworkLocation();
    List<Node> leafNodes = clusterMap.getLeaves(nodeGroupScope);
    excludedNodes.addAll(leafNodes);
    // choose one from the local rack, but off-nodegroup
    try {
      return chooseRandom(TopologyResolver.getRack(localMachine.getNetworkLocation(), true),
                          excludedNodes, blocksize, 
                          maxNodesPerRack, results);
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
  
  @Override
  protected void chooseRemoteRack(int numOfReplicas,
          DatanodeDescriptor localMachine,
          List<Node> excludedNodes,
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
  
}
