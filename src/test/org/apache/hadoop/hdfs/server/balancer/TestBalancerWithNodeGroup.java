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
package org.apache.hadoop.hdfs.server.balancer;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSClusterWithNodeGroup;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;

/**
 * This class tests if a balancer schedules tasks correctly.
 */
public class TestBalancerWithNodeGroup extends TestCase {
  private static final Log LOG = LogFactory.getLog(
  "org.apache.hadoop.hdfs.TestReplication");
  
  final private static long CAPACITY = 500L;
  final private static String RACK0 = "/rack0";
  final private static String RACK1 = "/rack1";
  final private static String RACK2 = "/rack2";
  final private static String NODEGROUP0 = "/nodegroup0";
  final private static String NODEGROUP1 = "/nodegroup1";
  final private static String NODEGROUP2 = "/nodegroup2";
  final static private String fileName = "/tmp.txt";
  final static private Path filePath = new Path(fileName);
  MiniDFSClusterWithNodeGroup cluster;

  ClientProtocol client;

  static final long TIMEOUT = 20000L; //msec
  static final double CAPACITY_ALLOWED_VARIANCE = 0.005;  // 0.5%
  static final double BALANCE_ALLOWED_VARIANCE = 0.11;    // 10%+delta
  static final int DEFAULT_BLOCK_SIZE = 10;
  private static final Random r = new Random();

  static {
    Balancer.setBlockMoveWaitTime(1000L) ;
  }
  
  private void initConf(Configuration conf) {
    conf.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    conf.setInt("io.bytes.per.checksum", DEFAULT_BLOCK_SIZE);
    conf.setLong("dfs.heartbeat.interval", 1L);
    conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    conf.setLong("dfs.balancer.movedWinWidth", 2000L);
    conf.set("net.topology.impl", "org.apache.hadoop.net.NetworkTopologyWithNodeGroup");
  }

  /* create a file with a length of <code>fileLen</code> */
  private void createFile(long fileLen, short replicationFactor)
      throws IOException {
    FileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, filePath, fileLen, 
        replicationFactor, r.nextLong());
    DFSTestUtil.waitReplication(fs, filePath, replicationFactor);
  }

  /* fill up a cluster with <code>numNodes</code> datanodes 
   * whose used space to be <code>size</code>
   */
  private Block[] generateBlocks(Configuration conf, long size,
      short numNodes) throws IOException {
    cluster = new MiniDFSClusterWithNodeGroup(
        0, conf, numNodes, true, true, null, null, null);
    try {
      cluster.waitActive();
      client = DFSClient.createNamenode(conf);

      short replicationFactor = (short)(numNodes-1);
      long fileLen = size/replicationFactor;
      createFile(fileLen, replicationFactor);

      List<LocatedBlock> locatedBlocks = client.
      getBlockLocations(fileName, 0, fileLen).getLocatedBlocks();

      int numOfBlocks = locatedBlocks.size();
      Block[] blocks = new Block[numOfBlocks];
      for(int i=0; i<numOfBlocks; i++) {
        Block b = locatedBlocks.get(i).getBlock();
        blocks[i] = new Block(b.getBlockId(), b.getNumBytes(),
                    b.getGenerationStamp());
      }
      return blocks;
    } finally {
      cluster.shutdown();
    }
  }

  /* we first start a cluster and fill the cluster up to a certain size.
   * then redistribute blocks according the required distribution.
   * Afterwards a balancer is running to balance the cluster.
   */
  private void testUnevenDistribution(Configuration conf,
      long distribution[], long capacities[], String[] racks, String[] nodeGroups) throws Exception {
    int numDatanodes = distribution.length;
    if (capacities.length != numDatanodes || racks.length != numDatanodes) {
      throw new IllegalArgumentException("Array length is not the same");
    }

    // calculate total space that need to be filled
    long totalUsedSpace = 0L;
    for(int i=0; i<distribution.length; i++) {
      totalUsedSpace += distribution[i];
    }

    // fill the cluster
    Block[] blocks = generateBlocks(conf, totalUsedSpace,
        (short) numDatanodes);

    // redistribute blocks
    Block[][] blocksDN = TestBalancer.distributeBlocks(
        blocks, (short)(numDatanodes-1), distribution);

    // restart the cluster: do NOT format the cluster
    conf.set(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, "0.0f");
    
    MiniDFSClusterWithNodeGroup.setNodeGroups(nodeGroups);
    cluster = new MiniDFSClusterWithNodeGroup(0, conf,numDatanodes, 
              false, true, null, racks, capacities);
    cluster.waitActive();
    client = DFSClient.createNamenode(conf);

    cluster.injectBlocks(blocksDN);

    long totalCapacity = 0L;
    for(long capacity:capacities) {
      totalCapacity += capacity;
    }
    TestBalancer.runBalancer(cluster, client, new Balancer(conf), 
        totalUsedSpace, totalCapacity);
    cluster.shutdown();
  }

  /** This test start a cluster with specified number of nodes, 
   * and fills it to be 30% full (with a single file replicated identically
   * to all datanodes);
   * It then adds one new empty node and starts balancing.
   * 
   * @param conf - configuration
   * @param capacities - array of capacities of original nodes in cluster
   * @param racks - array of racks for original nodes in cluster
   * @param nodeGroups - array of nodeGroups for original nodes in cluster
   * @param newCapacity - new node's capacity
   * @param newRack - new node's rack
   * @param newNodeGroiup - new node's nodegroup
   * @throws Exception
   */
  private void doTest(Configuration conf, long[] capacities, String[] racks, 
      String[] nodeGroups, long newCapacity, String newRack, 
      String newNodeGroup) throws Exception {
    assertEquals(capacities.length, racks.length);
    int numOfDatanodes = capacities.length;
    
    MiniDFSClusterWithNodeGroup.setNodeGroups(nodeGroups);
    cluster = new MiniDFSClusterWithNodeGroup(0, conf, capacities.length,
        true, true, null, racks, capacities);
    try {
      cluster.waitActive();
      client = DFSClient.createNamenode(conf);

      long totalCapacity=0L;
      for(long capacity:capacities) {
        totalCapacity += capacity;
      }
      
      // fill up the cluster to be 30% full
      long totalUsedSpace = totalCapacity*3/10;
      createFile(totalUsedSpace/numOfDatanodes, (short)numOfDatanodes);
      // start up an empty node with the same capacity and on the same rack
      cluster.startDataNodes(conf, 1, true, null, new String[]{newRack}, 
          new String[]{newNodeGroup}, new long[]{newCapacity});

      totalCapacity += newCapacity;

      // run balancer and validate results
      TestBalancer.runBalancer(cluster, client, new Balancer(conf),
          totalUsedSpace, totalCapacity);
    } finally {
      cluster.shutdown();
    }
  }
  
  /** one-node cluster test*/
  private void oneNodeTest(Configuration conf) throws Exception {
    // add an empty node with half of the CAPACITY & the same rack
    doTest(conf, new long[]{CAPACITY}, new String[]{RACK0}, new String[]{NODEGROUP0}, 
        CAPACITY/2, RACK0, NODEGROUP0);
  }
  
  /** two-node cluster test */
  private void twoNodeTest(Configuration conf) throws Exception {
    doTest(conf, new long[]{CAPACITY, CAPACITY}, new String[]{RACK0, RACK1},
        new String[]{NODEGROUP0, NODEGROUP1}, CAPACITY, RACK2, NODEGROUP2);
  }

  /** Test a cluster with even distribution, 
   * then a new empty node is added to the cluster*/
  public void testBalancer0() throws Exception {
    Configuration conf = new Configuration();
    initConf(conf);
    oneNodeTest(conf);
    twoNodeTest(conf);
  }

  /** Test unevenly distributed cluster */
  public void testBalancer1() throws Exception {
    Configuration conf = new Configuration();
    initConf(conf);
    testUnevenDistribution(conf,
        new long[]{50*CAPACITY/100, 10*CAPACITY/100},
        new long[]{CAPACITY, CAPACITY}, new String[] {RACK0, RACK1}, 
        new String[]{NODEGROUP0, NODEGROUP1});
  }

  public void testBalancer2() throws Exception {
    Configuration conf = new Configuration();
    initConf(conf);
    testBalancerDefaultConstructor(conf, new long[]{CAPACITY, CAPACITY},
        new String[]{RACK0, RACK1}, new String[]{NODEGROUP0, NODEGROUP1}, 
        CAPACITY, RACK2, NODEGROUP2);
  }

  private void testBalancerDefaultConstructor(Configuration conf,
      long[] capacities, String[] racks, String[] nodeGroups, long newCapacity, 
      String newRack, String newNodeGroup)
      throws Exception {
    int numOfDatanodes = capacities.length;
    assertEquals(numOfDatanodes, racks.length);
    // Set nodeGroups prior to constructor
    MiniDFSClusterWithNodeGroup.setNodeGroups(nodeGroups);
    cluster = new MiniDFSClusterWithNodeGroup(0, conf, capacities.length, true, true,
            null, racks, capacities);
    try {
      cluster.waitActive();
      client = DFSClient.createNamenode(conf);

      long totalCapacity=0L;
      for(long capacity:capacities) {
        totalCapacity += capacity;
      }

      // fill up the cluster to be 30% full
      long totalUsedSpace = totalCapacity * 3 / 10;
      createFile(totalUsedSpace / numOfDatanodes, (short) numOfDatanodes);
      // start up an empty node with the same capacity and on the same rack
      cluster.startDataNodes(conf, 1, true, null, new String[]{newRack},
          new String[]{newNodeGroup}, new long[] {newCapacity});

      totalCapacity += newCapacity;

      // run balancer and validate results
      TestBalancer.runBalancer(cluster, client, new Balancer(conf), 
          totalUsedSpace, totalCapacity);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    TestBalancerWithNodeGroup balancerTest = new TestBalancerWithNodeGroup();
    balancerTest.testBalancer0();
    balancerTest.testBalancer1();
    balancerTest.testBalancer2();
  }
}

