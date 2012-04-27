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
package org.apache.hadoop.hdfs.server.balancer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniVirtualDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * This class tests if a balancer schedules tasks correctly.
 */
public class TestBalancerOnVirtualization extends TestCase {
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
  MiniVirtualDFSCluster cluster;

  ClientProtocol client;

  static final long TIMEOUT = 20000L; //msec
  static final double CAPACITY_ALLOWED_VARIANCE = 0.005;  // 0.5%
  static final double BALANCE_ALLOWED_VARIANCE = 0.11;    // 10%+delta
  static final int DEFAULT_BLOCK_SIZE = 10;
  private static final Random r = new Random();

  static {
    Balancer.setBlockMoveWaitTime(1000L) ;
  }

  static Configuration createConf() {
	Configuration conf = new HdfsConfiguration();
	TestBalancer.initConf(conf);
    conf.setBoolean(CommonConfigurationKeysPublic.NET_TOPOLOGY_ENVIRONMENT_TYPE_KEY, true);
    return conf;
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
  private ExtendedBlock[] generateBlocks(Configuration conf, long size,
      short numNodes) throws IOException {
	MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf).numDataNodes(numNodes);
    cluster = new MiniVirtualDFSCluster (builder);
    try {
      cluster.waitActive();
      client = DFSUtil.createNamenode(conf);

      short replicationFactor = (short)(numNodes-1);
      long fileLen = size/replicationFactor;
      createFile(fileLen, replicationFactor);

      List<LocatedBlock> locatedBlocks = client.
      getBlockLocations(fileName, 0, fileLen).getLocatedBlocks();

      int numOfBlocks = locatedBlocks.size();
      ExtendedBlock[] blocks = new ExtendedBlock[numOfBlocks];
      for(int i=0; i<numOfBlocks; i++) {
        ExtendedBlock b = locatedBlocks.get(i).getBlock();
        blocks[i] = new ExtendedBlock(b.getBlockPoolId(), b.getBlockId(), b
            .getNumBytes(), b.getGenerationStamp());
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
    final long totalUsedSpace = TestBalancer.sum(distribution);

    // fill the cluster
    ExtendedBlock[] blocks = generateBlocks(conf, totalUsedSpace,
        (short) numDatanodes);

    // redistribute blocks
    Block[][] blocksDN = TestBalancer.distributeBlocks(
        blocks, (short)(numDatanodes-1), distribution);

    // restart the cluster: do NOT format the cluster
    conf.set(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, "0.0f"); 
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf).numDataNodes(numDatanodes)
                                              .format(false)
                                              .racks(racks)
                                              .simulatedCapacities(capacities);
    MiniVirtualDFSCluster.setNodeGroups(nodeGroups);
    cluster = new MiniVirtualDFSCluster(builder);
    cluster.waitActive();
    client = DFSUtil.createNamenode(conf);

    for(int i = 0; i < blocksDN.length; i++)
      cluster.injectBlocks(i, Arrays.asList(blocksDN[i]));

    final long totalCapacity = TestBalancer.sum(capacities);
    runBalancer(conf, totalUsedSpace, totalCapacity);
    cluster.shutdown();
  }

  /**
   * Wait until heartbeat gives expected results, within CAPACITY_ALLOWED_VARIANCE, 
   * summed over all nodes.  Times out after TIMEOUT msec.
   * @param expectedUsedSpace
   * @param expectedTotalSpace
   * @throws IOException - if getStats() fails
   * @throws TimeoutException
   */
  private void waitForHeartBeat(long expectedUsedSpace, long expectedTotalSpace)
  throws IOException, TimeoutException {
    long timeout = TIMEOUT;
    long failtime = (timeout <= 0L) ? Long.MAX_VALUE
             : System.currentTimeMillis() + timeout;
    
    while (true) {
      long[] status = client.getStats();
      double totalSpaceVariance = Math.abs((double)status[0] - expectedTotalSpace) 
          / expectedTotalSpace;
      double usedSpaceVariance = Math.abs((double)status[1] - expectedUsedSpace) 
          / expectedUsedSpace;
      if (totalSpaceVariance < CAPACITY_ALLOWED_VARIANCE 
          && usedSpaceVariance < CAPACITY_ALLOWED_VARIANCE)
        break; //done

      if (System.currentTimeMillis() > failtime) {
        throw new TimeoutException("Cluster failed to reached expected values of "
            + "totalSpace (current: " + status[0] 
            + ", expected: " + expectedTotalSpace 
            + "), or usedSpace (current: " + status[1] 
            + ", expected: " + expectedUsedSpace
            + "), in more than " + timeout + " msec.");
      }
      try {
        Thread.sleep(100L);
      } catch(InterruptedException ignored) {
      }
    }
  }
  
  /**
   * Wait until balanced: each datanode gives utilization within 
   * BALANCE_ALLOWED_VARIANCE of average
   * @throws IOException
   * @throws TimeoutException
   */
  private void waitForBalancer(long totalUsedSpace, long totalCapacity) 
  throws IOException, TimeoutException {
    long timeout = TIMEOUT;
    long failtime = (timeout <= 0L) ? Long.MAX_VALUE
        : System.currentTimeMillis() + timeout;
    final double avgUtilization = ((double)totalUsedSpace) / totalCapacity;
    boolean balanced;
    do {
      DatanodeInfo[] datanodeReport = 
          client.getDatanodeReport(DatanodeReportType.ALL);
      assertEquals(datanodeReport.length, cluster.getDataNodes().size());
      balanced = true;
      for (DatanodeInfo datanode : datanodeReport) {
        double nodeUtilization = ((double)datanode.getDfsUsed())
            / datanode.getCapacity();
        if (Math.abs(avgUtilization - nodeUtilization) > BALANCE_ALLOWED_VARIANCE) {
          balanced = false;
          if (System.currentTimeMillis() > failtime) {
            throw new TimeoutException(
                "Rebalancing expected avg utilization to become "
                + avgUtilization + ", but on datanode " + datanode
                + " it remains at " + nodeUtilization
                + " after more than " + TIMEOUT + " msec.");
          }
          try {
            Thread.sleep(100);
          } catch (InterruptedException ignored) {
          }
          break;
        }
      }
    } while (!balanced);
  }

  /** This test start a cluster with specified number of nodes, 
   * and fills it to be 30% full (with a single file replicated identically
   * to all datanodes);
   * It then adds one new empty node and starts balancing.
   * 
   * @param conf - configuration
   * @param capacities - array of capacities of original nodes in cluster
   * @param racks - array of racks for original nodes in cluster
   * @param newCapacity - new node's capacity
   * @param newRack - new node's rack
   * @throws Exception
   */
  private void doTest(Configuration conf, long[] capacities, String[] racks, 
      String[] nodeGroups, long newCapacity, String newRack, String newNodeGroup) throws Exception {
    assertEquals(capacities.length, racks.length);
    int numOfDatanodes = capacities.length;
    
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(capacities.length)
                                .racks(racks)
                                .simulatedCapacities(capacities);
    MiniVirtualDFSCluster.setNodeGroups(nodeGroups);
    cluster = new MiniVirtualDFSCluster(builder);
    try {
      cluster.waitActive();
      client = DFSUtil.createNamenode(conf);

      long totalCapacity = TestBalancer.sum(capacities);
      
      // fill up the cluster to be 30% full
      long totalUsedSpace = totalCapacity*3/10;
      createFile(totalUsedSpace/numOfDatanodes, (short)numOfDatanodes);
      // start up an empty node with the same capacity and on the same rack
      cluster.startDataNodes(conf, 1, true, null, new String[]{newRack}, 
          new long[]{newCapacity}, new String[]{newNodeGroup});

      totalCapacity += newCapacity;

      // run balancer and validate results
      runBalancer(conf, totalUsedSpace, totalCapacity);
    } finally {
      cluster.shutdown();
    }
  }

  private void runBalancer(Configuration conf,
      long totalUsedSpace, long totalCapacity) throws Exception {
    waitForHeartBeat(totalUsedSpace, totalCapacity);

    // start rebalancing
    final List<InetSocketAddress> namenodes =new ArrayList<InetSocketAddress>();
    namenodes.add(NameNode.getServiceAddress(conf, true));
    final int r = Balancer.run(namenodes, Balancer.Parameters.DEFALUT, conf);
    assertEquals(Balancer.ReturnStatus.SUCCESS.code, r);

    waitForHeartBeat(totalUsedSpace, totalCapacity);
    LOG.info("Rebalancing with default ctor.");
    waitForBalancer(totalUsedSpace, totalCapacity);
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
    Configuration conf = createConf();
    oneNodeTest(conf);
    twoNodeTest(conf);
  }

  /** Test unevenly distributed cluster */
  public void testBalancer1() throws Exception {
    Configuration conf = createConf();
    testUnevenDistribution(conf,
        new long[]{50*CAPACITY/100, 10*CAPACITY/100},
        new long[]{CAPACITY, CAPACITY}, new String[] {RACK0, RACK1}, 
        new String[]{NODEGROUP0, NODEGROUP1});
  }
  
  public void testBalancer2() throws Exception {
    Configuration conf = createConf();
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
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf)
                                .numDataNodes(capacities.length)
                                .racks(racks)
                                .simulatedCapacities(capacities);
    MiniVirtualDFSCluster.setNodeGroups(nodeGroups);
    cluster = new MiniVirtualDFSCluster(builder);
    try {
      cluster.waitActive();
      client = DFSUtil.createNamenode(conf);

      long totalCapacity = TestBalancer.sum(capacities);

      // fill up the cluster to be 30% full
      long totalUsedSpace = totalCapacity * 3 / 10;
      createFile(totalUsedSpace / numOfDatanodes, (short) numOfDatanodes);
      // start up an empty node with the same capacity and on the same rack
      cluster.startDataNodes(conf, 1, true, null, new String[]{newRack},
          new long[] {newCapacity}, new String[]{newNodeGroup});

      totalCapacity += newCapacity;

      // run balancer and validate results
      runBalancer(conf, totalUsedSpace, totalCapacity);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    TestBalancer balancerTest = new TestBalancer();
    balancerTest.testBalancer0();
    balancerTest.testBalancer1();
    balancerTest.testBalancer2();
  }
}
