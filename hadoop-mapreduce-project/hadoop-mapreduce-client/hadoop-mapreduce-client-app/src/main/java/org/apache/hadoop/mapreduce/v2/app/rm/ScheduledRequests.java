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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerRequestor.ContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.util.RackResolver;

public class ScheduledRequests {

  protected final RMContainerAllocator rmContainerAllocator;

  /**
   * @param rmContainerAllocator
   */
  ScheduledRequests(RMContainerAllocator rmContainerAllocator) {
    this.rmContainerAllocator = rmContainerAllocator;
  }

  protected final LinkedList<TaskAttemptId> earlierFailedMaps = 
      new LinkedList<TaskAttemptId>();
    
  /** Maps from a host to a list of Map tasks with data on the host */
  protected final Map<String, LinkedList<TaskAttemptId>> mapsHostMapping = 
    new HashMap<String, LinkedList<TaskAttemptId>>();
  protected final Map<String, LinkedList<TaskAttemptId>> mapsRackMapping = 
    new HashMap<String, LinkedList<TaskAttemptId>>();
  final Map<TaskAttemptId, ContainerRequest> maps = 
    new LinkedHashMap<TaskAttemptId, ContainerRequest>();
    
  final LinkedHashMap<TaskAttemptId, ContainerRequest> reduces = 
    new LinkedHashMap<TaskAttemptId, ContainerRequest>();
    
  boolean remove(TaskAttemptId tId) {
    ContainerRequest req = null;
    if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
      req = maps.remove(tId);
    } else {
      req = reduces.remove(tId);
    }

    if (req == null) {
      return false;
    } else {
      this.rmContainerAllocator.decContainerReq(req);
      return true;
    }
  }

  ContainerRequest removeReduce() {
    Iterator<Entry<TaskAttemptId, ContainerRequest>> it = reduces.entrySet().iterator();
    if (it.hasNext()) {
      Entry<TaskAttemptId, ContainerRequest> entry = it.next();
      it.remove();
      this.rmContainerAllocator.decContainerReq(entry.getValue());
      return entry.getValue();
    }
    return null;
  }

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

  void addReduce(ContainerRequest req) {
    reduces.put(req.attemptID, req);
    this.rmContainerAllocator.addContainerReq(req);
  }
    
  @SuppressWarnings("unchecked") 
  void assign(List<Container> allocatedContainers) {
    Iterator<Container> it = allocatedContainers.iterator();
    RMContainerAllocator.LOG.info("Got allocated containers " + allocatedContainers.size());
    this.rmContainerAllocator.containersAllocated += allocatedContainers.size();
    while (it.hasNext()) {
      Container allocated = it.next();
      if (RMContainerAllocator.LOG.isDebugEnabled()) {
        RMContainerAllocator.LOG.debug("Assigning container " + allocated.getId()
            + " with priority " + allocated.getPriority() + " to NM "
            + allocated.getNodeId());
      }

      // check if allocated container meets memory requirements 
      // and whether we have any scheduled tasks that need 
      // a container to be assigned
      boolean isAssignable = true;
      Priority priority = allocated.getPriority();
      int allocatedMemory = allocated.getResource().getMemory();
      if (RMContainerAllocator.PRIORITY_FAST_FAIL_MAP.equals(priority) 
          || RMContainerAllocator.PRIORITY_MAP.equals(priority)) {
        if (allocatedMemory < this.rmContainerAllocator.mapResourceReqt
            || maps.isEmpty()) {
          RMContainerAllocator.LOG.info("Cannot assign container " + allocated 
              + " for a map as either "
              + " container memory less than required " + this.rmContainerAllocator.mapResourceReqt
              + " or no pending map tasks - maps.isEmpty=" 
              + maps.isEmpty()); 
          isAssignable = false; 
        }
      }
      else if (RMContainerAllocator.PRIORITY_REDUCE.equals(priority)) {
        if (allocatedMemory < this.rmContainerAllocator.reduceResourceReqt
            || reduces.isEmpty()) {
          RMContainerAllocator.LOG.info("Cannot assign container " + allocated 
              + " for a reduce as either "
              + " container memory less than required " + this.rmContainerAllocator.reduceResourceReqt
              + " or no pending reduce tasks - reduces.isEmpty=" 
              + reduces.isEmpty()); 
          isAssignable = false;
        }
      }

      boolean blackListed = false;
      ContainerRequest assigned = null;

      ContainerId allocatedContainerId = allocated.getId();
      if (isAssignable) {
        // do not assign if allocated container is on a  
        // blacklisted host
        String allocatedHost = allocated.getNodeId().getHost();
        blackListed = this.rmContainerAllocator.isNodeBlacklisted(allocatedHost);
        if (blackListed) {
          // we need to request for a new container 
          // and release the current one
          RMContainerAllocator.LOG.info("Got allocated container on a blacklisted "
              + " host "+allocatedHost
              +". Releasing container " + allocated);

          // find the request matching this allocated container 
          // and replace it with a new one 
          ContainerRequest toBeReplacedReq = 
              getContainerReqToReplace(allocated);
          if (toBeReplacedReq != null) {
            RMContainerAllocator.LOG.info("Placing a new container request for task attempt " 
                + toBeReplacedReq.attemptID);
            ContainerRequest newReq = 
                this.rmContainerAllocator.getFilteredContainerRequest(toBeReplacedReq);
            this.rmContainerAllocator.decContainerReq(toBeReplacedReq);
            if (toBeReplacedReq.attemptID.getTaskId().getTaskType() ==
                TaskType.MAP) {
              maps.put(newReq.attemptID, newReq);
            }
            else {
              reduces.put(newReq.attemptID, newReq);
            }
            this.rmContainerAllocator.addContainerReq(newReq);
          }
          else {
            RMContainerAllocator.LOG.info("Could not map allocated container to a valid request."
                + " Releasing allocated container " + allocated);
          }
        }
        else {
          assigned = assign(allocated);
          if (assigned != null) {
            // Update resource requests
            this.rmContainerAllocator.decContainerReq(assigned);

            // send the container-assigned event to task attempt
            this.rmContainerAllocator.eventHandler.handle(new TaskAttemptContainerAssignedEvent(
                assigned.attemptID, allocated, this.rmContainerAllocator.applicationACLs));

            this.rmContainerAllocator.assignedRequests.add(allocatedContainerId, assigned.attemptID);

            if (RMContainerAllocator.LOG.isDebugEnabled()) {
              RMContainerAllocator.LOG.info("Assigned container (" + allocated + ") "
                  + " to task " + assigned.attemptID + " on node "
                  + allocated.getNodeId().toString());
            }
          }
          else {
            //not assigned to any request, release the container
            RMContainerAllocator.LOG.info("Releasing unassigned and invalid container " 
                + allocated + ". RM has gone crazy, someone go look!"
                + " Hey RM, if you are so rich, go donate to non-profits!");
          }
        }
      }

      // release container if it was blacklisted 
      // or if we could not assign it 
      if (blackListed || assigned == null) {
        this.rmContainerAllocator.containersReleased++;
        this.rmContainerAllocator.release(allocatedContainerId);
      }
    }
  }

  private ContainerRequest assign(Container allocated) {
    ContainerRequest assigned = null;

    Priority priority = allocated.getPriority();
    if (RMContainerAllocator.PRIORITY_FAST_FAIL_MAP.equals(priority)) {
      RMContainerAllocator.LOG.info("Assigning container " + allocated + " to fast fail map");
      assigned = assignToFailedMap(allocated);
    } else if (RMContainerAllocator.PRIORITY_REDUCE.equals(priority)) {
      if (RMContainerAllocator.LOG.isDebugEnabled()) {
        RMContainerAllocator.LOG.debug("Assigning container " + allocated + " to reduce");
      }
      assigned = assignToReduce(allocated);
    } else if (RMContainerAllocator.PRIORITY_MAP.equals(priority)) {
      if (RMContainerAllocator.LOG.isDebugEnabled()) {
        RMContainerAllocator.LOG.debug("Assigning container " + allocated + " to map");
      }
      assigned = assignToMap(allocated);
    } else {
      RMContainerAllocator.LOG.warn("Container allocated at unwanted priority: " + priority + 
          ". Returning to RM...");
    }
    return assigned;
  }

  private ContainerRequest getContainerReqToReplace(Container allocated) {
    RMContainerAllocator.LOG.info("Finding containerReq for allocated container: " + allocated);
    Priority priority = allocated.getPriority();
    ContainerRequest toBeReplaced = null;
    if (RMContainerAllocator.PRIORITY_FAST_FAIL_MAP.equals(priority)) {
      RMContainerAllocator.LOG.info("Replacing FAST_FAIL_MAP container " + allocated.getId());
      Iterator<TaskAttemptId> iter = earlierFailedMaps.iterator();
      while (toBeReplaced == null && iter.hasNext()) {
        toBeReplaced = maps.get(iter.next());
      }
      RMContainerAllocator.LOG.info("Found replacement: " + toBeReplaced);
      return toBeReplaced;
    }
    else if (RMContainerAllocator.PRIORITY_MAP.equals(priority)) {
      RMContainerAllocator.LOG.info("Replacing MAP container " + allocated.getId());
      // allocated container was for a map
      String host = allocated.getNodeId().getHost();
      LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
      if (list != null && list.size() > 0) {
        TaskAttemptId tId = list.removeLast();
        if (maps.containsKey(tId)) {
          toBeReplaced = maps.remove(tId);
        }
      }
      else {
        TaskAttemptId tId = maps.keySet().iterator().next();
        toBeReplaced = maps.remove(tId);
      }
    }
    else if (RMContainerAllocator.PRIORITY_REDUCE.equals(priority)) {
      TaskAttemptId tId = reduces.keySet().iterator().next();
      toBeReplaced = reduces.remove(tId);
    }
    RMContainerAllocator.LOG.info("Found replacement: " + toBeReplaced);
    return toBeReplaced;
  }

  @SuppressWarnings("unchecked")
  private ContainerRequest assignToFailedMap(Container allocated) {
    //try to assign to earlierFailedMaps if present
    ContainerRequest assigned = null;
    while (assigned == null && earlierFailedMaps.size() > 0) {
      TaskAttemptId tId = earlierFailedMaps.removeFirst();      
      if (maps.containsKey(tId)) {
        assigned = maps.remove(tId);
        JobCounterUpdateEvent jce =
          new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
        jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
        this.rmContainerAllocator.eventHandler.handle(jce);
        RMContainerAllocator.LOG.info("Assigned from earlierFailedMaps");
        break;
      }
    }
    return assigned;
  }

  private ContainerRequest assignToReduce(Container allocated) {
    ContainerRequest assigned = null;
    //try to assign to reduces if present
    if (assigned == null && reduces.size() > 0) {
      TaskAttemptId tId = reduces.keySet().iterator().next();
      assigned = reduces.remove(tId);
      RMContainerAllocator.LOG.info("Assigned to reduce");
    }
    return assigned;
  }

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
        // Try to assign rack-local
        if (assigned == null && maps.size() > 0) {
          String rack = RackResolver.resolve(host).getNetworkLocation();
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