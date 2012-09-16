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
import java.util.Random;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSClusterWithNodeGroup;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.junit.Test;

/**
 * This class tests if a balancer schedules tasks correctly.
 */
public class TestBalancerWithNodeGroup extends TestCase {
  private static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.hdfs.TestBalancerWithNodeGroup");
  
  final private static long CAPACITY = 500L;
  final private static String RACK0 = "/rack0";
  final private static String RACK1 = "/rack1";
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
  static final int DEFAULT_BLOCK_SIZE = 5;
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
    conf.set("dfs.block.replicator.classname",
        "org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicyWithNodeGroup");
  }

  /* create a file with a length of <code>fileLen</code> */
  private void createFile(long fileLen, short replicationFactor)
      throws IOException {
    FileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, filePath, fileLen, 
        replicationFactor, r.nextLong());
    DFSTestUtil.waitReplication(fs, filePath, replicationFactor);
  }

  /** Create a cluster with even distribution, and a new empty node is added to
   *  the cluster, then test rack locality for balancer policy. 
   **/
  @Test
  public void testBalancerWithNodeGroup() throws Exception {
    Configuration conf = createConf();
    long[] capacities = new long[]{CAPACITY, CAPACITY};
    String[] racks = new String[]{RACK0, RACK1};
    String[] nodeGroups = new String[]{NODEGROUP0, NODEGROUP1};
    
    int numOfDatanodes = capacities.length;
    assertEquals(numOfDatanodes, racks.length);
    MiniDFSClusterWithNodeGroup.setNodeGroups(nodeGroups);
    cluster = new MiniDFSClusterWithNodeGroup(0, conf, capacities.length,
        true, true, null, racks, capacities);
    try {
      cluster.waitActive();
      client = DFSClient.createNamenode(conf);
      
      long totalCapacity = 0L;
      for(long capacity : capacities) {
        totalCapacity += capacity;
      }
      
      // fill up the cluster to be 30% full
      long totalUsedSpace = totalCapacity*3/10;
      
      createFile(totalUsedSpace / numOfDatanodes, (short) numOfDatanodes);
      
      long newCapacity = CAPACITY;
      String newRack = RACK1;
      String newNodeGroup = NODEGROUP2;
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
  
  // create and initiate conf for balancer
  private Configuration createConf() {
    Configuration conf = new Configuration();
    initConf(conf);
    return conf;
  }

}

