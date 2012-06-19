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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NetworkTopologyWithNodeGroup;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.TopologyResolver;

public class BlockPlacementPolicyWithNodeGroup extends
    BlockPlacementPolicyDefault {

  BlockPlacementPolicyWithNodeGroup(Configuration conf,  FSClusterStats stats,
      NetworkTopology clusterMap) {
    super(conf, stats, clusterMap);
  }
  
  BlockPlacementPolicyWithNodeGroup() {
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
      HashMap<Node, Node> excludedNodes, long blocksize, int maxNodesPerRack,
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
      DatanodeDescriptor localMachine, HashMap<Node, Node> excludedNodes, long blocksize, 
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

    // Nodes under same nodegroup should be excluded.
    String nodeGroupScope = localMachine.getNetworkLocation();
    List<Node> leafNodes = clusterMap.getLeaves(nodeGroupScope);
    for (Node leafNode : leafNodes) {
      excludedNodes.put(leafNode, leafNode);
    }
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
  
  /**
   * Pick up replica node set for deleting replica as over-replicated. 
   * First set contains replica nodes on rack with more than one
   * replica while second set contains remaining replica nodes.
   * If first is not empty, divide first set into two subsets:
   *   priSet contains nodes on nodegroup with more than one replica
   *   remains contains the remaining nodes in first set
   * then pickup priSet if not empty.
   * If first is empty, then pick second.
   */
  @Override
  public Iterator<DatanodeDescriptor> pickupReplicaSet(
      List<DatanodeDescriptor> first,
      List<DatanodeDescriptor> second) {
    // If no replica within same rack, return directly.
    if (first.isEmpty()) {
      return second.iterator();
    }
    // Split data nodes in the first set into two sets, 
    // priSet contains nodes on nodegroup with more than one replica
    // remains contains the remaining nodes
    Map<String, List<DatanodeDescriptor>> nodeGroupMap = 
        new HashMap<String, List<DatanodeDescriptor>>();
    
    for(final Iterator<DatanodeDescriptor> iter = first.iterator();
        iter.hasNext(); ) {
      final DatanodeDescriptor node = iter.next();
      final String nodeGroupName = 
          TopologyResolver.getNodeGroup(node.getNetworkLocation(), true);
      List<DatanodeDescriptor> datanodeList = 
          nodeGroupMap.get(nodeGroupName);
      if (datanodeList == null) {
        datanodeList = new ArrayList<DatanodeDescriptor>();
        nodeGroupMap.put(nodeGroupName, datanodeList);
      }
      datanodeList.add(node);
    }
    
    final List<DatanodeDescriptor> priSet = new ArrayList<DatanodeDescriptor>();
    final List<DatanodeDescriptor> remains = new ArrayList<DatanodeDescriptor>();
    // split nodes into two sets
    for(List<DatanodeDescriptor> datanodeList : nodeGroupMap.values()) {
      if (datanodeList.size() == 1 ) {
        // remains contains the remaining nodes
        remains.add(datanodeList.get(0));
      } else {
        // priSet contains nodes on nodegroup with more than one replica
        priSet.addAll(datanodeList);
      }
    }
    
    Iterator<DatanodeDescriptor> iter =
        priSet.isEmpty() ? remains.iterator() : priSet.iterator();
    return iter;
  }
  
  @Override
  protected String getRack(DatanodeInfo cur) {
    String nodeGroupString = cur.getNetworkLocation();
    return TopologyResolver.getRack(nodeGroupString, true);
  }
  
}
