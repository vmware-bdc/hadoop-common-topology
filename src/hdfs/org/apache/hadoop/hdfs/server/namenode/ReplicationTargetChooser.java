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

import org.apache.commons.logging.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import java.util.*;

/** The class is responsible for choosing the desired number of targets
 * for placing block replicas.
 * The replica placement strategy is that if the writer is on a datanode,
 * the 1st replica is placed on the local machine, 
 * otherwise a random datanode. The 2nd replica is placed on a datanode
 * that is on a different rack. The 3rd replica is placed on a datanode
 * which is on the same rack as the second replica.
 */
class ReplicationTargetChooser {
  private boolean considerLoad; 
  protected NetworkTopology clusterMap;
  private FSNamesystem fs;
  
  ReplicationTargetChooser() {
  }
  
  ReplicationTargetChooser(boolean considerLoad,  FSNamesystem fs,
                           NetworkTopology clusterMap) {
    initialize(considerLoad, fs, clusterMap);
  }
    
  public void initialize(boolean considerLoad, FSNamesystem fs,
      NetworkTopology clusterMap) {
    this.considerLoad = considerLoad;
    this.fs = fs;
    this.clusterMap = clusterMap;
  }

protected static class NotEnoughReplicasException extends Exception {
    NotEnoughReplicasException(String msg) {
      super(msg);
    }
  }
    
  /**
   * choose <i>numOfReplicas</i> data nodes for <i>writer</i> to replicate
   * a block with size <i>blocksize</i> 
   * If not, return as many as we can.
   * 
   * @param numOfReplicas: number of replicas wanted.
   * @param writer: the writer's machine, null if not in the cluster.
   * @param excludedNodes: datanodesthat should not be considered targets.
   * @param blocksize: size of the data to be written.
   * @return array of DatanodeDescriptor instances chosen as targets
   * and sorted as a pipeline.
   */
  DatanodeDescriptor[] chooseTarget(int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<Node> excludedNodes,
                                    long blocksize) {
    if (excludedNodes == null) {
      excludedNodes = new ArrayList<Node>();
    }
      
    return chooseTarget(numOfReplicas, writer, 
                        new ArrayList<DatanodeDescriptor>(), excludedNodes, blocksize);
  }
    
  /**
   * choose <i>numOfReplicas</i> data nodes for <i>writer</i> 
   * to re-replicate a block with size <i>blocksize</i> 
   * If not, return as many as we can.
   * 
   * @param numOfReplicas: additional number of replicas wanted.
   * @param writer: the writer's machine, null if not in the cluster.
   * @param choosenNodes: datanodes that have been chosen as targets.
   * @param excludedNodes: datanodes that should not be considered targets.
   * @param blocksize: size of the data to be written.
   * @return array of DatanodeDescriptor instances chosen as target 
   * and sorted as a pipeline.
   */
  DatanodeDescriptor[] chooseTarget(int numOfReplicas,
                                    DatanodeDescriptor writer,
                                    List<DatanodeDescriptor> choosenNodes,
                                    List<Node> excludedNodes,
                                    long blocksize) {
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return new DatanodeDescriptor[0];
    }
      
    if (excludedNodes == null) {
      excludedNodes = new ArrayList<Node>();
    }
      
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = choosenNodes.size()+numOfReplicas;
    if (totalNumOfReplicas > clusterSize) {
      numOfReplicas -= (totalNumOfReplicas-clusterSize);
      totalNumOfReplicas = clusterSize;
    }
      
    int maxNodesPerRack = 
      (totalNumOfReplicas-1)/clusterMap.getNumOfRacks()+2;
      
    List<DatanodeDescriptor> results = 
      new ArrayList<DatanodeDescriptor>(choosenNodes);
    excludedNodes.addAll(choosenNodes);
      
    if (!clusterMap.contains(writer)) {
      writer=null;
    }
      
    DatanodeDescriptor localNode = chooseTarget(numOfReplicas, writer, 
                                                excludedNodes, blocksize, maxNodesPerRack, results);
      
    results.removeAll(choosenNodes);
      
    // sorting nodes to form a pipeline
    return getPipeline((writer==null)?localNode:writer,
                       results.toArray(new DatanodeDescriptor[results.size()]));
  }
    
  /* choose <i>numOfReplicas</i> from all data nodes */
  private DatanodeDescriptor chooseTarget(int numOfReplicas,
                                          DatanodeDescriptor writer,
                                          List<Node> excludedNodes,
                                          long blocksize,
                                          int maxNodesPerRack,
                                          List<DatanodeDescriptor> results) {
      
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return writer;
    }
      
    int numOfResults = results.size();
    boolean newBlock = (numOfResults==0);
    if (writer == null && !newBlock) {
      writer = (DatanodeDescriptor)results.get(0);
    }
      
    try {
      switch(numOfResults) {
      case 0:
        writer = chooseLocalNode(writer, excludedNodes, 
                                 blocksize, maxNodesPerRack, results);
        if (--numOfReplicas == 0) {
          break;
        }
      case 1:
        chooseRemoteRack(1, results.get(0), excludedNodes, 
                         blocksize, maxNodesPerRack, results);
        if (--numOfReplicas == 0) {
          break;
        }
      case 2:
        if (clusterMap.isOnSameRack(results.get(0), results.get(1))) {
          chooseRemoteRack(1, results.get(0), excludedNodes,
                           blocksize, maxNodesPerRack, results);
        } else if (newBlock){
          chooseLocalRack(results.get(1), excludedNodes, blocksize, 
                          maxNodesPerRack, results);
        } else {
          chooseLocalRack(writer, excludedNodes, blocksize,
                          maxNodesPerRack, results);
        }
        if (--numOfReplicas == 0) {
          break;
        }
      default:
        chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, 
                     blocksize, maxNodesPerRack, results);
      }
    } catch (NotEnoughReplicasException e) {
      FSNamesystem.LOG.warn("Not able to place enough replicas, still in need of "
               + numOfReplicas);
    }
    return writer;
  }
    
  /* choose <i>localMachine</i> as the target.
   * if <i>localMachine</i> is not available, 
   * choose a node on the same rack
   * @return the chosen node
   */
  private DatanodeDescriptor chooseLocalNode(
                                             DatanodeDescriptor localMachine,
                                             List<Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeDescriptor> results)
    throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
    if (localMachine == null)
      return chooseRandom(NodeBase.ROOT, excludedNodes, 
                          blocksize, maxNodesPerRack, results);
      
    // otherwise try local machine first
    if (!excludedNodes.contains(localMachine)) {
      excludedNodes.add(localMachine);
      if (isGoodTarget(localMachine, blocksize,
                       maxNodesPerRack, false, results)) {
        results.add(localMachine);
        return localMachine;
      }
    } 
    // if local machine is not available, try node with other locality, i.e
    // local rack
    return chooseOtherLocalityNode(localMachine, excludedNodes, blocksize,
        maxNodesPerRack, results);
  }

  /** Choose node with other locality other than <i>localMachine</i>.
   * As there is no local node is available, choose one node with nearest 
   * locality.
   * Default to be local rack, but could be overridden in sub-class for other 
   * localities.
   * @return the chosen node
   */
  protected DatanodeDescriptor chooseOtherLocalityNode(DatanodeDescriptor localMachine,
      List<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeDescriptor> results) throws NotEnoughReplicasException {
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
      
    // choose one from the local rack
    try {
      return chooseRandom(
                          localMachine.getNetworkLocation(),
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
          return chooseRandom(
                              newLocal.getNetworkLocation(),
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
    
  /* choose <i>numOfReplicas</i> nodes from the racks 
   * that <i>localMachine</i> is NOT on.
   * if not enough nodes are available, choose the remaining ones 
   * from the local rack
   */
    
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
      chooseRandom(numOfReplicas, "~"+localMachine.getNetworkLocation(),
                   excludedNodes, blocksize, maxReplicasPerRack, results);
    } catch (NotEnoughReplicasException e) {
      chooseRandom(numOfReplicas-(results.size()-oldNumOfReplicas),
                   localMachine.getNetworkLocation(), excludedNodes, blocksize, 
                   maxReplicasPerRack, results);
    }
  }

  /* Randomly choose one target from <i>nodes</i>.
   * @return the chosen node
   */
  protected DatanodeDescriptor chooseRandom(
                                          String nodes,
                                          List<Node> excludedNodes,
                                          long blocksize,
                                          int maxNodesPerRack,
                                          List<DatanodeDescriptor> results) 
    throws NotEnoughReplicasException {
    DatanodeDescriptor result;
    do {
      DatanodeDescriptor[] selectedNodes = 
        chooseRandom(1, nodes, excludedNodes);
      if (selectedNodes.length == 0) {
        throw new NotEnoughReplicasException(
                                             "Not able to place enough replicas");
      }
      result = (DatanodeDescriptor)(selectedNodes[0]);
    } while(!isGoodTarget(result, blocksize, maxNodesPerRack, results));
    results.add(result);
    return result;
  }
    
  /* Randomly choose <i>numOfReplicas</i> targets from <i>nodes</i>.
   */
  protected void chooseRandom(int numOfReplicas,
                            String nodes,
                            List<Node> excludedNodes,
                            long blocksize,
                            int maxNodesPerRack,
                            List<DatanodeDescriptor> results)
    throws NotEnoughReplicasException {
    boolean toContinue = true;
    do {
      DatanodeDescriptor[] selectedNodes = 
        chooseRandom(numOfReplicas, nodes, excludedNodes);
      if (selectedNodes.length < numOfReplicas) {
        toContinue = false;
      }
      for(int i=0; i<selectedNodes.length; i++) {
        DatanodeDescriptor result = selectedNodes[i];
        if (isGoodTarget(result, blocksize, maxNodesPerRack, results)) {
          numOfReplicas--;
          results.add(result);
        }
      } // end of for
    } while (numOfReplicas>0 && toContinue);
      
    if (numOfReplicas>0) {
      throw new NotEnoughReplicasException(
                                           "Not able to place enough replicas");
    }
  }
    
  /* Randomly choose <i>numOfNodes</i> nodes from <i>scope</i>.
   * @return the chosen nodes
   */
  private DatanodeDescriptor[] chooseRandom(int numOfReplicas, 
                                            String nodes,
                                            List<Node> excludedNodes) {
    List<DatanodeDescriptor> results = 
      new ArrayList<DatanodeDescriptor>();
    int numOfAvailableNodes =
      clusterMap.countNumOfAvailableNodes(nodes, excludedNodes);
    numOfReplicas = (numOfAvailableNodes<numOfReplicas)?
      numOfAvailableNodes:numOfReplicas;
    while(numOfReplicas > 0) {
      DatanodeDescriptor choosenNode = 
        (DatanodeDescriptor)(clusterMap.chooseRandom(nodes));
      if (!excludedNodes.contains(choosenNode)) {
        results.add(choosenNode);
        excludedNodes.add(choosenNode);
        numOfReplicas--;
      }
    }
    return (DatanodeDescriptor[])results.toArray(
                                                 new DatanodeDescriptor[results.size()]);    
  }
    
  /* judge if a node is a good target.
   * return true if <i>node</i> has enough space, 
   * does not have too much load, and the rack does not have too many nodes
   */
  private boolean isGoodTarget(DatanodeDescriptor node,
                               long blockSize, int maxTargetPerLoc,
                               List<DatanodeDescriptor> results) {
    return isGoodTarget(node, blockSize, maxTargetPerLoc,
                        this.considerLoad, results);
  }
    
  private boolean isGoodTarget(DatanodeDescriptor node,
                               long blockSize, int maxTargetPerLoc,
                               boolean considerLoad,
                               List<DatanodeDescriptor> results) {
    Log logr = FSNamesystem.LOG;
    // check if the node is (being) decommissed
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      logr.debug("Node "+NodeBase.getPath(node)+
                " is not chosen because the node is (being) decommissioned");
      return false;
    }

    long remaining = node.getRemaining() - 
                     (node.getBlocksScheduled() * blockSize); 
    // check the remaining capacity of the target machine
    if (blockSize* FSConstants.MIN_BLOCKS_FOR_WRITE>remaining) {
      logr.debug("Node "+NodeBase.getPath(node)+
                " is not chosen because the node does not have enough space");
      return false;
    }
      
    // check the communication traffic of the target machine
    if (considerLoad) {
      double avgLoad = 0;
      int size = clusterMap.getNumOfLeaves();
      if (size != 0) {
        avgLoad = (double)fs.getTotalLoad()/size;
      }
      if (node.getXceiverCount() > (2.0 * avgLoad)) {
        logr.debug("Node "+NodeBase.getPath(node)+
                  " is not chosen because the node is too busy");
        return false;
      }
    }
      
    // check if the target rack has chosen too many nodes
    String rackname = node.getNetworkLocation();
    int counter=1;
    for(Iterator<DatanodeDescriptor> iter = results.iterator();
        iter.hasNext();) {
      Node result = iter.next();
      if (rackname.equals(result.getNetworkLocation())) {
        counter++;
      }
    }
    if (counter>maxTargetPerLoc) {
      logr.debug("Node "+NodeBase.getPath(node)+
                " is not chosen because the rack has too many chosen nodes");
      return false;
    }
    return true;
  }
    
  /* Return a pipeline of nodes.
   * The pipeline is formed finding a shortest path that 
   * starts from the writer and tranverses all <i>nodes</i>
   * This is basically a traveling salesman problem.
   */
  private DatanodeDescriptor[] getPipeline(
                                           DatanodeDescriptor writer,
                                           DatanodeDescriptor[] nodes) {
    if (nodes.length==0) return nodes;
      
    synchronized(clusterMap) {
      int index=0;
      if (writer == null || !clusterMap.contains(writer)) {
        writer = nodes[0];
      }
      for(;index<nodes.length; index++) {
        DatanodeDescriptor shortestNode = nodes[index];
        int shortestDistance = clusterMap.getDistance(writer, shortestNode);
        int shortestIndex = index;
        for(int i=index+1; i<nodes.length; i++) {
          DatanodeDescriptor currentNode = nodes[i];
          int currentDistance = clusterMap.getDistance(writer, currentNode);
          if (shortestDistance>currentDistance) {
            shortestDistance = currentDistance;
            shortestNode = currentNode;
            shortestIndex = i;
          }
        }
        //switch position index & shortestIndex
        if (index != shortestIndex) {
          nodes[shortestIndex] = nodes[index];
          nodes[index] = shortestNode;
        }
        writer = shortestNode;
      }
    }
    return nodes;
  }
  
  /**
   * Decide to delete the specified replica of the block still makes 
   * the block conform to the configured block placement policy.
   * 
   * @param priSet The replica locations of this block that are present
                  on at least two unique racks. 
   * @param remains Replica locations of this block that are not
                   listed in the previous parameter.
   * @return the replica that is the best candidate for deletion
   */
  public DatanodeDescriptor chooseReplicaToDelete(
      List<DatanodeDescriptor> priSet,
      List<DatanodeDescriptor> remains) {
    long minSpace = Long.MAX_VALUE;
    DatanodeDescriptor cur = null;

    // pick replica from the priSet. If priSet is empty, then pick replicas
    // from remains set.
    Iterator<DatanodeDescriptor> iter = pickupReplicaSet(priSet, remains);
    while( iter.hasNext() ) {
      DatanodeDescriptor node = iter.next();
      long free = node.getRemaining();
      if (minSpace > free) {
        minSpace = free;
        cur = node;
      }
    }
    return cur;
  }

  /**
   * Adjust rackmap, priSet, and remains after choosing replica to delete.
   */
  protected void adjustSetsWithChosenReplica(
      Map<String, List<DatanodeDescriptor>> rackMap,
      List<DatanodeDescriptor> priSet,
      List<DatanodeDescriptor> remains, DatanodeInfo cur) {
    // adjust rackmap, priSet, and remains
    String rack = getLocalityGroupForSplit(cur);
    List<DatanodeDescriptor> datanodes = rackMap.get(rack);
    datanodes.remove(cur);
    if(datanodes.isEmpty()) {
      rackMap.remove(rack);
    }
    if( priSet.remove(cur) ) {
      if (datanodes.size() == 1) {
        priSet.remove(datanodes.get(0));
        remains.add(datanodes.get(0));
      }
    } else {
      remains.remove(cur);
    }
  }

  /**
   * Get specific level of locality group from a data node for judging 
   * over-replicated. 
   * Default to be rack, can be overridden in sub class with other locality 
   * group for other split policy.
   */
  protected String getLocalityGroupForSplit(DatanodeInfo cur) {
    return getRack(cur);
  }

  protected String getRack(DatanodeInfo cur) {
    return cur.getNetworkLocation();
  }
  
  /**
   * Split data nodes into two sets, one set includes nodes on rack with
   * more than one  replica, the other set contains the remaining nodes.
   * 
   * @param dataNodes
   * @param rackMap a map from rack to datanodes
   * @param priSet contains nodes on rack with more than one replica
   * @param remains remains contains the remaining nodes
   */
  protected void splitNodesWithLocalityGroup(
      Collection<DatanodeDescriptor> dataNodes,
      Map<String, List<DatanodeDescriptor>> rackMap,
      List<DatanodeDescriptor> priSet,
      List<DatanodeDescriptor> remains) {
    for (Iterator<DatanodeDescriptor> iter = dataNodes.iterator();
        iter.hasNext();) {
      DatanodeDescriptor node = iter.next();
      String rackName = getLocalityGroupForSplit(node);
      List<DatanodeDescriptor> datanodeList = rackMap.get(rackName);
      if(datanodeList==null) {
        datanodeList = new ArrayList<DatanodeDescriptor>();
      }
      datanodeList.add(node);
      rackMap.put(rackName, datanodeList);
    }
    
    // split nodes into two sets
    for(List<DatanodeDescriptor> datanodeList : rackMap.values()) {
      if (datanodeList.size() == 1 ) {
        // remains contains the remaining nodes
        remains.add(datanodeList.get(0));
      } else {
        // priSet contains nodes on rack with more than one replica
        priSet.addAll(datanodeList);
      }
    }
  }
  
  /**
   * Pick up replica node set for deleting replica as over-replicated. 
   * First set contains replica nodes on rack with more than one
   * replica while second set contains remaining replica nodes.
   * So pick up first set if not empty. If first is empty, then pick second.
   */
  protected Iterator<DatanodeDescriptor> pickupReplicaSet(
      List<DatanodeDescriptor> priSet,
      List<DatanodeDescriptor> remains) {
    Iterator<DatanodeDescriptor> iter = 
        priSet.isEmpty() ? remains.iterator() : priSet.iterator();
    return iter;
  }

  /**
   * Verify that the block is replicated on at least 2 different racks
   * if there is more than one rack in the system.
   * 
   * @param lBlk block with locations
   * @param cluster 
   * @return 1 if the block must be relicated on additional rack,
   * or 0 if the number of racks is sufficient.
   */
  public static int verifyBlockPlacement(LocatedBlock lBlk,
                                         short replication,
                                         NetworkTopology cluster) {
    int numRacks = verifyBlockPlacement(lBlk, Math.min(2,replication), cluster);
    return numRacks < 0 ? 0 : numRacks;
  }

  /**
   * Verify that the block is replicated on at least minRacks different racks
   * if there is more than minRacks rack in the system.
   * 
   * @param lBlk block with locations
   * @param minRacks number of racks the block should be replicated to
   * @param cluster 
   * @return the difference between the required and the actual number of racks
   * the block is replicated to.
   */
  public static int verifyBlockPlacement(LocatedBlock lBlk,
                                         int minRacks,
                                         NetworkTopology cluster) {
    DatanodeInfo[] locs = lBlk.getLocations();
    if (locs == null)
      locs = new DatanodeInfo[0];
    int numRacks = cluster.getNumOfRacks();
    if(numRacks <= 1) // only one rack
      return 0;
    minRacks = Math.min(minRacks, numRacks);
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    Set<String> racks = new TreeSet<String>();
    for (DatanodeInfo dn : locs)
      racks.add(dn.getNetworkLocation());
    return minRacks - racks.size();
  }
  
  
} //end of Replicator

