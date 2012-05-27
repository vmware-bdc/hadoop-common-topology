package org.apache.hadoop.mapreduce.v2.app.rm;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerRequestor.ContainerRequest;
import org.apache.hadoop.net.TopologyResolver;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.util.RackResolver;

public class ScheduledRequestsWithNodeGroup extends ScheduledRequests {
  
  private final Map<String, LinkedList<TaskAttemptId>> mapsNodeGroupMapping = 
	new HashMap<String, LinkedList<TaskAttemptId>>();
	
  ScheduledRequestsWithNodeGroup(RMContainerAllocator rmContainerAllocator) {
    super(rmContainerAllocator);
  }
  
  @Override
  void addMap(ContainerRequestEvent event) {
    ContainerRequest request = null;
	      
	if (event.getEarlierAttemptFailed()) {
	  earlierFailedMaps.add(event.getAttemptID());
	  request = new ContainerRequest(event, RMContainerAllocator.PRIORITY_FAST_FAIL_MAP);
	  RMContainerAllocator.LOG.info("Added "+event.getAttemptID()+" to list of failed maps");
	} else {
	  for (String host : event.getHosts()) {
	    LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
	    if (list == null) {
	      list = new LinkedList<TaskAttemptId>();
	      mapsHostMapping.put(host, list);
	    }
	    list.add(event.getAttemptID());
	    if (RMContainerAllocator.LOG.isDebugEnabled()) {
	      RMContainerAllocator.LOG.debug("Added attempt req to host " + host);
	    }
	  }

	  doNodeGroupMapping(event);

	  for (String rack: event.getRacks()) {
	    LinkedList<TaskAttemptId> list = mapsRackMapping.get(rack);
	    if (list == null) {
	      list = new LinkedList<TaskAttemptId>();
	      mapsRackMapping.put(rack, list);
	    }
	    list.add(event.getAttemptID());
	    if (RMContainerAllocator.LOG.isDebugEnabled()) {
	      RMContainerAllocator.LOG.debug("Added attempt req to rack " + rack);
	    }
	  }
	  request = new ContainerRequest(event, RMContainerAllocator.PRIORITY_MAP);
	}
	maps.put(event.getAttemptID(), request);
	this.rmContainerAllocator.addContainerReq(request);
  }
  
  protected ContainerRequest assignToNodeGroup(String host, LinkedList<TaskAttemptId> list) {
    ContainerRequest assigned = null;
    // Try to assign nodegroup-local		
    String nodegroup = TopologyResolver.getNodeGroup(RackResolver.resolve(host), true);
    if (nodegroup != null)  
      list = mapsNodeGroupMapping.get(nodegroup);
    while (list != null && list.size() > 0) {
      TaskAttemptId tId = list.removeFirst();
      if (maps.containsKey(tId)) {
        assigned = maps.remove(tId);
        JobCounterUpdateEvent jce =
            new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
        jce.addCounterUpdate(JobCounter.NODEGROUP_LOCAL_MAPS, 1);
        this.rmContainerAllocator.eventHandler.handle(jce);
        this.rmContainerAllocator.nodegroupLocalAssigned++;
        RMContainerAllocator.LOG.info("Assigned based on nodegroup match " + nodegroup);
        break;
      }
    }  
    return assigned;
  }

  protected void doNodeGroupMapping(ContainerRequestEvent event) {
    if (event instanceof ContainerRequestWithNodeGroupEvent) {
      for (String nodegroup: ((ContainerRequestWithNodeGroupEvent)event).getNodeGroups()) {
        LinkedList<TaskAttemptId> list = mapsNodeGroupMapping.get(nodegroup);
        if (list == null) {
          list = new LinkedList<TaskAttemptId>();
          mapsNodeGroupMapping.put(nodegroup, list);
        }
        list.add(event.getAttemptID());
        RMContainerAllocator.LOG.info("Added attempt req to nodegroup " + nodegroup);
      }
    }
  }
  
  @Override
  @SuppressWarnings("unchecked")
  protected ContainerRequest assignToMap(Container allocated) {
    //try to assign to maps if present 
    //first by host, then by rack, followed by *
    ContainerRequest assigned = null;
    while (assigned == null && maps.size() > 0) {
      String host = allocated.getNodeId().getHost();
      LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
      while (list != null && list.size() > 0) {
        if (RMContainerAllocator.LOG.isDebugEnabled()) {
          RMContainerAllocator.LOG.debug("Host matched to the request list " + host);
        }
        TaskAttemptId tId = list.removeFirst();
        if (maps.containsKey(tId)) {
          assigned = maps.remove(tId);
          JobCounterUpdateEvent jce =
            new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.DATA_LOCAL_MAPS, 1);
          this.rmContainerAllocator.eventHandler.handle(jce);
          this.rmContainerAllocator.hostLocalAssigned++;
          if (RMContainerAllocator.LOG.isDebugEnabled()) {
            RMContainerAllocator.LOG.debug("Assigned based on host match " + host);
          }
          break;
        }
      }
      if (assigned == null) {
        	
        assigned = assignToNodeGroup(host, list);

        // Try to assign rack-local
        if (assigned == null && maps.size() > 0) {
          String rack = TopologyResolver.getRack(RackResolver.resolve(host), true);
          list = mapsRackMapping.get(rack);
          while (list != null && list.size() > 0) {
            TaskAttemptId tId = list.removeFirst();
            if (maps.containsKey(tId)) {
              assigned = maps.remove(tId);
              JobCounterUpdateEvent jce =
                  new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
              jce.addCounterUpdate(JobCounter.RACK_LOCAL_MAPS, 1);
              this.rmContainerAllocator.eventHandler.handle(jce);
              this.rmContainerAllocator.rackLocalAssigned++;
              RMContainerAllocator.LOG.info("Assigned based on rack match " + rack);
              break;
            }
          }
        }

        if (assigned == null && maps.size() > 0) {
          TaskAttemptId tId = maps.keySet().iterator().next();
          assigned = maps.remove(tId);
          JobCounterUpdateEvent jce =
              new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
          this.rmContainerAllocator.eventHandler.handle(jce);
          if (RMContainerAllocator.LOG.isDebugEnabled()) {
            RMContainerAllocator.LOG.debug("Assigned based on * match");
          }
          break;
        }
      }
    }
    return assigned;
  }

}
