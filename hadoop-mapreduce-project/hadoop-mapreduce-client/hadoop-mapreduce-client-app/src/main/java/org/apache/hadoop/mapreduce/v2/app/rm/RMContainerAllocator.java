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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.NormalizedResourceEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.util.RackResolver;

/**
 * Allocates the container from the ResourceManager scheduler.
 */
public class RMContainerAllocator extends RMContainerRequestor
    implements ContainerAllocator {

  static final Log LOG = LogFactory.getLog(RMContainerAllocator.class);
  
  public static final 
  float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;
  
  static final Priority PRIORITY_FAST_FAIL_MAP;
  static final Priority PRIORITY_REDUCE;
  static final Priority PRIORITY_MAP;

  private Thread eventHandlingThread;
  private volatile boolean stopEventHandling;

  static {
    PRIORITY_FAST_FAIL_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_FAST_FAIL_MAP.setPriority(5);
    PRIORITY_REDUCE = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_REDUCE.setPriority(10);
    PRIORITY_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_MAP.setPriority(20);
  }
  
  /*
  Vocabulary Used: 
  pending -> requests which are NOT yet sent to RM
  scheduled -> requests which are sent to RM but not yet assigned
  assigned -> requests which are assigned to a container
  completed -> request corresponding to which container has completed
  
  Lifecycle of map
  scheduled->assigned->completed
  
  Lifecycle of reduce
  pending->scheduled->assigned->completed
  
  Maps are scheduled as soon as their requests are received. Reduces are 
  added to the pending and are ramped up (added to scheduled) based 
  on completed maps and current availability in the cluster.
  */
  
  //reduces which are not yet scheduled
  private final LinkedList<ContainerRequest> pendingReduces = 
    new LinkedList<ContainerRequest>();

  //holds information about the assigned containers to task attempts
  final AssignedRequests assignedRequests = new AssignedRequests();
  
  //holds scheduled requests to be fulfilled by RM
  private ScheduledRequests scheduledRequests;
  
  int containersAllocated = 0;
  int containersReleased = 0;
  int hostLocalAssigned = 0;
  int nodegroupLocalAssigned = 0;
  int rackLocalAssigned = 0;
  
  private boolean recalculateReduceSchedule = false;
  int mapResourceReqt;//memory
  int reduceResourceReqt;//memory
  
  private boolean reduceStarted = false;
  private float maxReduceRampupLimit = 0;
  private float maxReducePreemptionLimit = 0;
  private float reduceSlowStart = 0;
  private long retryInterval;
  private long retrystartTime;
/*  
  // Mark if cluster is deployed on environment with nodegroup layer in topology, default is not
  boolean topologyWithNodeGroup = false;*/

  BlockingQueue<ContainerAllocatorEvent> eventQueue
    = new LinkedBlockingQueue<ContainerAllocatorEvent>();

  public RMContainerAllocator(ClientService clientService, AppContext context) {
    super(clientService, context);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    reduceSlowStart = conf.getFloat(
        MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 
        DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART);
    maxReduceRampupLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_RAMPUP_UP_LIMIT, 
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_RAMP_UP_LIMIT);
    maxReducePreemptionLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_PREEMPTION_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_PREEMPTION_LIMIT);
    RackResolver.init(conf);

    Class<? extends ScheduledRequests> scheduledRequestsClass =
        conf.getClass(YarnConfiguration.RM_SCHEDULED_REQUESTS_CLASS_KEY,
            ScheduledRequests.class, ScheduledRequests.class);
    try {
      Constructor<? extends ScheduledRequests> meth = scheduledRequestsClass.getDeclaredConstructor(new Class[] {RMContainerAllocator.class});
      meth.setAccessible(true);
      scheduledRequests = meth.newInstance(this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    retryInterval = getConfig().getLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS,
                                MRJobConfig.DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS);
    // Init startTime to current time. If all goes well, it will be reset after
    // first attempt to contact RM.
    retrystartTime = System.currentTimeMillis();
  }

  @Override
  public void start() {
    this.eventHandlingThread = new Thread() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {

        ContainerAllocatorEvent event;

        while (!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            event = RMContainerAllocator.this.eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return;
          }

          try {
            handleEvent(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " to the ContainreAllocator", t);
            // Kill the AM
            eventHandler.handle(new JobEvent(getJob().getID(),
              JobEventType.INTERNAL_ERROR));
            return;
          }
        }
      }
    };
    this.eventHandlingThread.start();
    super.start();
  }

  @Override
  protected synchronized void heartbeat() throws Exception {
    LOG.info("Before Scheduling: " + getStat());
    List<Container> allocatedContainers = getResources();
    LOG.info("After Scheduling: " + getStat());
    if (allocatedContainers.size() > 0) {
      LOG.info("Before Assign: " + getStat());
      scheduledRequests.assign(allocatedContainers);
      LOG.info("After Assign: " + getStat());
    }
    
    if (recalculateReduceSchedule) {
      preemptReducesIfNeeded();
      scheduleReduces(
          getJob().getTotalMaps(), getJob().getCompletedMaps(),
          scheduledRequests.maps.size(), scheduledRequests.reduces.size(), 
          assignedRequests.maps.size(), assignedRequests.reduces.size(),
          mapResourceReqt, reduceResourceReqt,
          pendingReduces.size(), 
          maxReduceRampupLimit, reduceSlowStart);
      recalculateReduceSchedule = false;
    }
  }

  @Override
  public void stop() {
    this.stopEventHandling = true;
    eventHandlingThread.interrupt();
    super.stop();
    LOG.info("Final Stats: " + getStat());
  }

  public boolean getIsReduceStarted() {
    return reduceStarted;
  }
  
  public void setIsReduceStarted(boolean reduceStarted) {
    this.reduceStarted = reduceStarted; 
  }

  @Override
  public void handle(ContainerAllocatorEvent event) {
    int qSize = eventQueue.size();
    if (qSize != 0 && qSize % 1000 == 0) {
      LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
    }
    int remCapacity = eventQueue.remainingCapacity();
    if (remCapacity < 1000) {
      LOG.warn("Very low remaining capacity in the event-queue "
          + "of RMContainerAllocator: " + remCapacity);
    }
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }

  @SuppressWarnings({ "unchecked" })
  protected synchronized void handleEvent(ContainerAllocatorEvent event) {
    recalculateReduceSchedule = true;
    if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
      ContainerRequestEvent reqEvent = (ContainerRequestEvent) event;
      JobId jobId = getJob().getID();
      int supportedMaxContainerCapability =
          getMaxContainerCapability().getMemory();
      if (reqEvent.getAttemptID().getTaskId().getTaskType().equals(TaskType.MAP)) {
        if (mapResourceReqt == 0) {
          mapResourceReqt = reqEvent.getCapability().getMemory();
          int minSlotMemSize = getMinContainerCapability().getMemory();
          mapResourceReqt = (int) Math.ceil((float) mapResourceReqt/minSlotMemSize)
              * minSlotMemSize;
          eventHandler.handle(new JobHistoryEvent(jobId, 
              new NormalizedResourceEvent(org.apache.hadoop.mapreduce.TaskType.MAP,
              mapResourceReqt)));
          LOG.info("mapResourceReqt:"+mapResourceReqt);
          if (mapResourceReqt > supportedMaxContainerCapability) {
            String diagMsg = "MAP capability required is more than the supported " +
            "max container capability in the cluster. Killing the Job. mapResourceReqt: " + 
            mapResourceReqt + " maxContainerCapability:" + supportedMaxContainerCapability;
            LOG.info(diagMsg);
            eventHandler.handle(new JobDiagnosticsUpdateEvent(
                jobId, diagMsg));
            eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
          }
        }
        //set the rounded off memory
        reqEvent.getCapability().setMemory(mapResourceReqt);
        scheduledRequests.addMap(reqEvent);//maps are immediately scheduled
      } else {
        if (reduceResourceReqt == 0) {
          reduceResourceReqt = reqEvent.getCapability().getMemory();
          int minSlotMemSize = getMinContainerCapability().getMemory();
          //round off on slotsize
          reduceResourceReqt = (int) Math.ceil((float) 
              reduceResourceReqt/minSlotMemSize) * minSlotMemSize;
          eventHandler.handle(new JobHistoryEvent(jobId, 
              new NormalizedResourceEvent(
                  org.apache.hadoop.mapreduce.TaskType.REDUCE,
              reduceResourceReqt)));
          LOG.info("reduceResourceReqt:"+reduceResourceReqt);
          if (reduceResourceReqt > supportedMaxContainerCapability) {
            String diagMsg = "REDUCE capability required is more than the " +
            		"supported max container capability in the cluster. Killing the " +
            		"Job. reduceResourceReqt: " + reduceResourceReqt +
            		" maxContainerCapability:" + supportedMaxContainerCapability;
            LOG.info(diagMsg);
            eventHandler.handle(new JobDiagnosticsUpdateEvent(
                jobId, diagMsg));
            eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
          }
        }
        //set the rounded off memory
        reqEvent.getCapability().setMemory(reduceResourceReqt);
        if (reqEvent.getEarlierAttemptFailed()) {
          //add to the front of queue for fail fast
          pendingReduces.addFirst(new ContainerRequest(reqEvent, PRIORITY_REDUCE));
        } else {
          pendingReduces.add(new ContainerRequest(reqEvent, PRIORITY_REDUCE));
          //reduces are added to pending and are slowly ramped up
        }
      }
      
    } else if (
        event.getType() == ContainerAllocator.EventType.CONTAINER_DEALLOCATE) {
  
      LOG.info("Processing the event " + event.toString());

      TaskAttemptId aId = event.getAttemptID();
      
      boolean removed = scheduledRequests.remove(aId);
      if (!removed) {
        ContainerId containerId = assignedRequests.get(aId);
        if (containerId != null) {
          removed = true;
          assignedRequests.remove(aId);
          containersReleased++;
          release(containerId);
        }
      }
      if (!removed) {
        LOG.error("Could not deallocate container for task attemptId " + 
            aId);
      }
    } else if (
        event.getType() == ContainerAllocator.EventType.CONTAINER_FAILED) {
      ContainerFailedEvent fEv = (ContainerFailedEvent) event;
      String host = getHost(fEv.getContMgrAddress());
      containerFailedOnHost(host);
    }
  }

  private static String getHost(String contMgrAddress) {
    String host = contMgrAddress;
    String[] hostport = host.split(":");
    if (hostport.length == 2) {
      host = hostport[0];
    }
    return host;
  }

  private void preemptReducesIfNeeded() {
    if (reduceResourceReqt == 0) {
      return; //no reduces
    }
    //check if reduces have taken over the whole cluster and there are 
    //unassigned maps
    if (scheduledRequests.maps.size() > 0) {
      int memLimit = getMemLimit();
      int availableMemForMap = memLimit - ((assignedRequests.reduces.size() -
          assignedRequests.preemptionWaitingReduces.size()) * reduceResourceReqt);
      //availableMemForMap must be sufficient to run atleast 1 map
      if (availableMemForMap < mapResourceReqt) {
        //to make sure new containers are given to maps and not reduces
        //ramp down all scheduled reduces if any
        //(since reduces are scheduled at higher priority than maps)
        LOG.info("Ramping down all scheduled reduces:" + scheduledRequests.reduces.size());
        for (ContainerRequest req : scheduledRequests.reduces.values()) {
          pendingReduces.add(req);
        }
        scheduledRequests.reduces.clear();
        
        //preempt for making space for atleast one map
        int premeptionLimit = Math.max(mapResourceReqt, 
            (int) (maxReducePreemptionLimit * memLimit));
        
        int preemptMem = Math.min(scheduledRequests.maps.size() * mapResourceReqt, 
            premeptionLimit);
        
        int toPreempt = (int) Math.ceil((float) preemptMem/reduceResourceReqt);
        toPreempt = Math.min(toPreempt, assignedRequests.reduces.size());
        
        LOG.info("Going to preempt " + toPreempt);
        assignedRequests.preemptReduce(toPreempt);
      }
    }
  }
  
  @Private
  public void scheduleReduces(
      int totalMaps, int completedMaps,
      int scheduledMaps, int scheduledReduces,
      int assignedMaps, int assignedReduces,
      int mapResourceReqt, int reduceResourceReqt,
      int numPendingReduces,
      float maxReduceRampupLimit, float reduceSlowStart) {
    
    if (numPendingReduces == 0) {
      return;
    }
    
    LOG.info("Recalculating schedule...");
    
    //if all maps are assigned, then ramp up all reduces irrespective of the 
    //headroom
    if (scheduledMaps == 0 && numPendingReduces > 0) {
      LOG.info("All maps assigned. " +
      		"Ramping up all remaining reduces:" + numPendingReduces);
      scheduleAllReduces();
      return;
    }
    
    //check for slow start
    if (!getIsReduceStarted()) {//not set yet
      int completedMapsForReduceSlowstart = (int)Math.ceil(reduceSlowStart * 
                      totalMaps);
      if(completedMaps < completedMapsForReduceSlowstart) {
        LOG.info("Reduce slow start threshold not met. " +
              "completedMapsForReduceSlowstart " + 
            completedMapsForReduceSlowstart);
        return;
      } else {
        LOG.info("Reduce slow start threshold reached. Scheduling reduces.");
        setIsReduceStarted(true);
      }
    }
    
    float completedMapPercent = 0f;
    if (totalMaps != 0) {//support for 0 maps
      completedMapPercent = (float)completedMaps/totalMaps;
    } else {
      completedMapPercent = 1;
    }
    
    int netScheduledMapMem = 
        (scheduledMaps + assignedMaps) * mapResourceReqt;

    int netScheduledReduceMem = 
        (scheduledReduces + assignedReduces) * reduceResourceReqt;

    int finalMapMemLimit = 0;
    int finalReduceMemLimit = 0;
    
    // ramp up the reduces based on completed map percentage
    int totalMemLimit = getMemLimit();
    int idealReduceMemLimit = 
        Math.min(
            (int)(completedMapPercent * totalMemLimit),
            (int) (maxReduceRampupLimit * totalMemLimit));
    int idealMapMemLimit = totalMemLimit - idealReduceMemLimit;

    // check if there aren't enough maps scheduled, give the free map capacity
    // to reduce
    if (idealMapMemLimit > netScheduledMapMem) {
      int unusedMapMemLimit = idealMapMemLimit - netScheduledMapMem;
      finalReduceMemLimit = idealReduceMemLimit + unusedMapMemLimit;
      finalMapMemLimit = totalMemLimit - finalReduceMemLimit;
    } else {
      finalMapMemLimit = idealMapMemLimit;
      finalReduceMemLimit = idealReduceMemLimit;
    }
    
    LOG.info("completedMapPercent " + completedMapPercent +
        " totalMemLimit:" + totalMemLimit +
        " finalMapMemLimit:" + finalMapMemLimit +
        " finalReduceMemLimit:" + finalReduceMemLimit + 
        " netScheduledMapMem:" + netScheduledMapMem +
        " netScheduledReduceMem:" + netScheduledReduceMem);
    
    int rampUp = 
        (finalReduceMemLimit - netScheduledReduceMem) / reduceResourceReqt;
    
    if (rampUp > 0) {
      rampUp = Math.min(rampUp, numPendingReduces);
      LOG.info("Ramping up " + rampUp);
      rampUpReduces(rampUp);
    } else if (rampUp < 0){
      int rampDown = -1 * rampUp;
      rampDown = Math.min(rampDown, scheduledReduces);
      LOG.info("Ramping down " + rampDown);
      rampDownReduces(rampDown);
    }
  }

  private void scheduleAllReduces() {
    for (ContainerRequest req : pendingReduces) {
      scheduledRequests.addReduce(req);
    }
    pendingReduces.clear();
  }
  
  @Private
  public void rampUpReduces(int rampUp) {
    //more reduce to be scheduled
    for (int i = 0; i < rampUp; i++) {
      ContainerRequest request = pendingReduces.removeFirst();
      scheduledRequests.addReduce(request);
    }
  }
  
  @Private
  public void rampDownReduces(int rampDown) {
    //remove from the scheduled and move back to pending
    for (int i = 0; i < rampDown; i++) {
      ContainerRequest request = scheduledRequests.removeReduce();
      pendingReduces.add(request);
    }
  }
  
  /**
   * Synchronized to avoid findbugs warnings
   */
  private synchronized String getStat() {
    return "PendingReduces:" + pendingReduces.size() +
        " ScheduledMaps:" + scheduledRequests.maps.size() +
        " ScheduledReduces:" + scheduledRequests.reduces.size() +
        " AssignedMaps:" + assignedRequests.maps.size() + 
        " AssignedReduces:" + assignedRequests.reduces.size() +
        " completedMaps:" + getJob().getCompletedMaps() + 
        " completedReduces:" + getJob().getCompletedReduces() +
        " containersAllocated:" + containersAllocated +
        " containersReleased:" + containersReleased +
        " hostLocalAssigned:" + hostLocalAssigned + 
        " nodegroupLocalAssigned:" + nodegroupLocalAssigned + 
        " rackLocalAssigned:" + rackLocalAssigned +
        " availableResources(headroom):" + getAvailableResources();
  }

  @SuppressWarnings("unchecked")
  private List<Container> getResources() throws Exception {
    int headRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;//first time it would be null
    AMResponse response;
    /*
     * If contact with RM is lost, the AM will wait MR_AM_TO_RM_WAIT_INTERVAL_MS
     * milliseconds before aborting. During this interval, AM will still try
     * to contact the RM.
     */
    try {
      response = makeRemoteRequest();
      // Reset retry count if no exception occurred.
      retrystartTime = System.currentTimeMillis();
    } catch (Exception e) {
      // This can happen when the connection to the RM has gone down. Keep
      // re-trying until the retryInterval has expired.
      if (System.currentTimeMillis() - retrystartTime >= retryInterval) {
        LOG.error("Could not contact RM after " + retryInterval + " milliseconds.");
        eventHandler.handle(new JobEvent(this.getJob().getID(),
                                         JobEventType.INTERNAL_ERROR));
        throw new YarnException("Could not contact RM after " +
                                retryInterval + " milliseconds.");
      }
      // Throw this up to the caller, which may decide to ignore it and
      // continue to attempt to contact the RM.
      throw e;
    }
    if (response.getReboot()) {
      // This can happen if the RM has been restarted. If it is in that state,
      // this application must clean itself up.
      eventHandler.handle(new JobEvent(this.getJob().getID(),
                                       JobEventType.INTERNAL_ERROR));
      throw new YarnException("Resource Manager doesn't recognize AttemptId: " +
                               this.getContext().getApplicationID());
    }
    int newHeadRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;
    List<Container> newContainers = response.getAllocatedContainers();
    List<ContainerStatus> finishedContainers = response.getCompletedContainersStatuses();
    if (newContainers.size() + finishedContainers.size() > 0 || headRoom != newHeadRoom) {
      //something changed
      recalculateReduceSchedule = true;
    }

    if (LOG.isDebugEnabled()) {
      for (Container cont : newContainers) {
        LOG.debug("Received new Container :" + cont);
      }
    }

    //Called on each allocation. Will know about newly blacklisted/added hosts.
    computeIgnoreBlacklisting();
    
    for (ContainerStatus cont : finishedContainers) {
      LOG.info("Received completed container " + cont.getContainerId());
      TaskAttemptId attemptID = assignedRequests.get(cont.getContainerId());
      if (attemptID == null) {
        LOG.error("Container complete event for unknown container id "
            + cont.getContainerId());
      } else {
        assignedRequests.remove(attemptID);
        
        // send the container completed event to Task attempt
        eventHandler.handle(new TaskAttemptEvent(attemptID,
            TaskAttemptEventType.TA_CONTAINER_COMPLETED));
        // Send the diagnostics
        String diagnostics = cont.getDiagnostics();
        eventHandler.handle(new TaskAttemptDiagnosticsUpdateEvent(attemptID,
            diagnostics));
      }
    }
    return newContainers;
  }

  @Private
  public int getMemLimit() {
    int headRoom = getAvailableResources() != null ? getAvailableResources().getMemory() : 0;
    return headRoom + assignedRequests.maps.size() * mapResourceReqt + 
       assignedRequests.reduces.size() * reduceResourceReqt;
  }
  
  class AssignedRequests {
    private final Map<ContainerId, TaskAttemptId> containerToAttemptMap =
      new HashMap<ContainerId, TaskAttemptId>();
    private final LinkedHashMap<TaskAttemptId, ContainerId> maps = 
      new LinkedHashMap<TaskAttemptId, ContainerId>();
    private final LinkedHashMap<TaskAttemptId, ContainerId> reduces = 
      new LinkedHashMap<TaskAttemptId, ContainerId>();
    private final Set<TaskAttemptId> preemptionWaitingReduces = 
      new HashSet<TaskAttemptId>();
    
    void add(ContainerId containerId, TaskAttemptId tId) {
      LOG.info("Assigned container " + containerId.toString() + " to " + tId);
      containerToAttemptMap.put(containerId, tId);
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        maps.put(tId, containerId);
      } else {
        reduces.put(tId, containerId);
      }
    }

    @SuppressWarnings("unchecked")
    void preemptReduce(int toPreempt) {
      List<TaskAttemptId> reduceList = new ArrayList<TaskAttemptId>
        (reduces.keySet());
      //sort reduces on progress
      Collections.sort(reduceList,
          new Comparator<TaskAttemptId>() {
        @Override
        public int compare(TaskAttemptId o1, TaskAttemptId o2) {
          float p = getJob().getTask(o1.getTaskId()).getAttempt(o1).getProgress() -
              getJob().getTask(o2.getTaskId()).getAttempt(o2).getProgress();
          return p >= 0 ? 1 : -1;
        }
      });
      
      for (int i = 0; i < toPreempt && reduceList.size() > 0; i++) {
        TaskAttemptId id = reduceList.remove(0);//remove the one on top
        LOG.info("Preempting " + id);
        preemptionWaitingReduces.add(id);
        eventHandler.handle(new TaskAttemptEvent(id, TaskAttemptEventType.TA_KILL));
      }
    }
    
    boolean remove(TaskAttemptId tId) {
      ContainerId containerId = null;
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        containerId = maps.remove(tId);
      } else {
        containerId = reduces.remove(tId);
        if (containerId != null) {
          boolean preempted = preemptionWaitingReduces.remove(tId);
          if (preempted) {
            LOG.info("Reduce preemption successful " + tId);
          }
        }
      }
      
      if (containerId != null) {
        containerToAttemptMap.remove(containerId);
        return true;
      }
      return false;
    }
    
    TaskAttemptId get(ContainerId cId) {
      return containerToAttemptMap.get(cId);
    }

    ContainerId get(TaskAttemptId tId) {
      if (tId.getTaskId().getTaskType().equals(TaskType.MAP)) {
        return maps.get(tId);
      } else {
        return reduces.get(tId);
      }
    }
  }
}
