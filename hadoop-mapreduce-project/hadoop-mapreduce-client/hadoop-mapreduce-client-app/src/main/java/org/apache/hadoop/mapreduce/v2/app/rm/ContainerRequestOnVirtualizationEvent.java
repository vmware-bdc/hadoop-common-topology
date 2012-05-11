package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;

public class ContainerRequestOnVirtualizationEvent extends
		ContainerRequestEvent {
  private String[] nodegroups;

  public ContainerRequestOnVirtualizationEvent(TaskAttemptId attemptID,
	  Resource capability, String[] hosts, String[] nodegroups, String[] racks) {
    super(attemptID, capability, hosts, racks);
    this.nodegroups = nodegroups;
  }
  
  public String[] getNodeGroup() {
	return nodegroups;
  }

}
