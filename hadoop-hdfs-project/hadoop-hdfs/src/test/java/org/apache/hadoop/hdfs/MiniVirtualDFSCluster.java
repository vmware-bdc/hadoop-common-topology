package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.StaticMapping;

public class MiniVirtualDFSCluster extends MiniDFSCluster {
	
  private static String[] NODE_GROUPS = null;
  private static final Log LOG = LogFactory.getLog(MiniVirtualDFSCluster.class);
  
  public MiniVirtualDFSCluster(Builder builder) throws IOException {
	super(builder);
  }
  
  public static void setNodeGroups (String[] nodeGroups) {
	NODE_GROUPS = nodeGroups;
  }
  
  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
      boolean manageDfsDirs, StartupOption operation, 
      String[] racks, String[] nodeGroups, String[] hosts,
      long[] simulatedCapacities,
      boolean setupHostsFile,
      boolean checkDataNodeAddrConfig) throws IOException {
    conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "127.0.0.1");

    int curDatanodesNum = dataNodes.size();
    // for mincluster's the default initialDelay for BRs is 0
    if (conf.get(DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY) == null) {
      conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY, 0);
    }
    // If minicluster's name node is null assume that the conf has been
    // set with the right address:port of the name node.
    //
    if (racks != null && numDataNodes > racks.length ) {
      throw new IllegalArgumentException( "The length of racks [" + racks.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }

    if (nodeGroups != null && numDataNodes > nodeGroups.length ) {
      throw new IllegalArgumentException( "The length of nodeGroups [" + nodeGroups.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }

    if (hosts != null && numDataNodes > hosts.length ) {
      throw new IllegalArgumentException( "The length of hosts [" + hosts.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }
    //Generate some hostnames if required
    if (racks != null && hosts == null) {
      hosts = new String[numDataNodes];
      for (int i = curDatanodesNum; i < curDatanodesNum + numDataNodes; i++) {
        hosts[i - curDatanodesNum] = "host" + i + ".foo.com";
      }
    }

    if (simulatedCapacities != null 
        && numDataNodes > simulatedCapacities.length) {
      throw new IllegalArgumentException( "The length of simulatedCapacities [" 
          + simulatedCapacities.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }

    String [] dnArgs = (operation == null ||
    operation != StartupOption.ROLLBACK) ?
        null : new String[] {operation.getName()};

    for (int i = curDatanodesNum; i < curDatanodesNum+numDataNodes; i++) {
      Configuration dnConf = new HdfsConfiguration(conf);
      // Set up datanode address
      setupDatanodeAddress(dnConf, setupHostsFile, checkDataNodeAddrConfig);
      if (manageDfsDirs) {
        File dir1 = getInstanceStorageDir(i, 0);
        File dir2 = getInstanceStorageDir(i, 1);
        dir1.mkdirs();
        dir2.mkdirs();
        if (!dir1.isDirectory() || !dir2.isDirectory()) { 
          throw new IOException("Mkdirs failed to create directory for DataNode "
              + i + ": " + dir1 + " or " + dir2);
        }
        String dirs = fileAsURI(dir1) + "," + fileAsURI(dir2);
        dnConf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dirs);
        conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dirs);
      }
      if (simulatedCapacities != null) {
        dnConf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
        dnConf.setLong(SimulatedFSDataset.CONFIG_PROPERTY_CAPACITY,
        simulatedCapacities[i-curDatanodesNum]);
      }
      LOG.info("Starting DataNode " + i + " with "
          + DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY + ": "
          + dnConf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY));
      if (hosts != null) {
        dnConf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, hosts[i - curDatanodesNum]);
        LOG.info("Starting DataNode " + i + " with hostname set to: "
            + dnConf.get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY));
      }
      if (racks != null) {
        String name = hosts[i - curDatanodesNum];
        if (nodeGroups == null) {
          LOG.info("Adding node with hostname : " + name + " to rack " +
             racks[i-curDatanodesNum]);
          StaticMapping.addNodeToRack(name,racks[i-curDatanodesNum]);
        } else {
          LOG.info("Adding node with hostname : " + name + " to serverGroup " +
              nodeGroups[i-curDatanodesNum] + " and rack " +
              racks[i-curDatanodesNum]);
          StaticMapping.addNodeToRack(name,racks[i-curDatanodesNum] + 
              nodeGroups[i-curDatanodesNum]);
        }
      }
      Configuration newconf = new HdfsConfiguration(dnConf); // save config
      if (hosts != null) {
        NetUtils.addStaticResolution(hosts[i - curDatanodesNum], "localhost");
      }
      DataNode dn = DataNode.instantiateDataNode(dnArgs, dnConf);
      if(dn == null)
        throw new IOException("Cannot start DataNode in "
          + dnConf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY));
      //since the HDFS does things based on IP:port, we need to add the mapping
      //for IP:port to rackId
      String ipAddr = dn.getSelfAddr().getAddress().getHostAddress();
      if (racks != null) {
        int port = dn.getSelfAddr().getPort();
        if (nodeGroups == null) {
          LOG.info("Adding node with IP:port : " + ipAddr + ":" + port +
              " to rack " + racks[i-curDatanodesNum]);
          StaticMapping.addNodeToRack(ipAddr + ":" + port,
              racks[i-curDatanodesNum]);
        } else {
          LOG.info("Adding node with IP:port : " + ipAddr + ":" + port + " to nodeGroup " +
          nodeGroups[i-curDatanodesNum] + " and rack " + racks[i-curDatanodesNum]);
          StaticMapping.addNodeToRack(ipAddr + ":" + port, racks[i-curDatanodesNum] + 
              nodeGroups[i-curDatanodesNum]);
        }
      }
      dn.runDatanodeDaemon();
      dataNodes.add(new DataNodeProperties(dn, newconf, dnArgs));
    }
    curDatanodesNum += numDataNodes;
    this.numDataNodes += numDataNodes;
    waitActive();
  }
  
  public void startDataNodes(Configuration conf, int numDataNodes, 
	  boolean manageDfsDirs, StartupOption operation, 
	  String[] racks, String[] nodeGroups
	  ) throws IOException {
	startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, nodeGroups,
	    null, null, false);
  }
  
  public synchronized void startDataNodes(Configuration conf, int numDataNodes, 
      boolean manageDfsDirs, StartupOption operation, 
      String[] racks, String[] nodeGroups, String[] hosts,
      long[] simulatedCapacities,
      boolean setupHostsFile) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, nodeGroups, 
        hosts, simulatedCapacities, setupHostsFile, false);
  }
  
  public void startDataNodes(Configuration conf, int numDataNodes, 
      boolean manageDfsDirs, StartupOption operation, 
      String[] racks, long[] simulatedCapacities,
      String[] nodeGroups) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, nodeGroups,
        null, simulatedCapacities, false);
  }
  
  // This is for initialize from parent class.
  @Override
  public synchronized void startDataNodes(Configuration conf, int numDataNodes, 
      boolean manageDfsDirs, StartupOption operation, 
      String[] racks, String[] hosts,
      long[] simulatedCapacities,
      boolean setupHostsFile) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, NODE_GROUPS, hosts,
        simulatedCapacities, setupHostsFile, false);
  }
  
}
