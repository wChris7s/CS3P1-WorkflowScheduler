package com.cwa.solaligue.app.simulator;

import com.cwa.solaligue.app.graph.Edge;
import com.cwa.solaligue.app.graph.Operator;
import com.cwa.solaligue.app.scheduler.Plan;
import com.cwa.solaligue.app.scheduler.RuntimeConstants;
import com.cwa.solaligue.app.utilities.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;


public class ExecutionState {

  public HashMap<Long, ArrayList<Long>> opIdtoChildren;
  public HashMap<Long, ArrayList<Long>> opIdtoParents;

  public HashMap<Long, Long> opIdtoContStartTime;
  public HashMap<Long, Long> opIdtoContEndTime;

  public int operatorsNo;

  public HashMap<Long, Long> activeAtWorker = new HashMap<>();

  public HashSet<Long> readyOps;
  public HashSet<Long> runningOps;
  public HashSet<Long> doneButDTOps;
  public HashSet<Long> doneOps;

  public HashSet<Long> terminationCondition;

  public Plan plan;

  public HashMap<Long, Pair<Long, Long>> workerStartEndTime_MS;

  public HashMap<Long, Boolean> isWorkerAvailable;
  public HashMap<Long, Long> workerAvailableAt;

  long masterStartTime_ms = -1L;
  long masterEndTime_ms = -1L;


  public ExecutionState(Plan p) {

    operatorsNo = p.graph.operators.size();

    plan = p;

    workerAvailableAt = new HashMap<>();
    for (Long cid : plan.cluster.containers.keySet()) {
      workerAvailableAt.put(cid, 0L);
    }

    workerStartEndTime_MS = new HashMap<>();
    for (Long cid : plan.cluster.containers.keySet()) {
      workerStartEndTime_MS.put(cid, new Pair<Long, Long>(-1L, -1L));
    }

    this.isWorkerAvailable = new HashMap<>();
    for (Long cid : plan.cluster.containers.keySet()) {
      isWorkerAvailable.put(cid, true);
    }

    readyOps = new HashSet<>();
    runningOps = new HashSet<>();

    doneButDTOps = new HashSet<>();
    doneOps = new HashSet<>();

    terminationCondition = new HashSet<>();

    opIdtoChildren = new HashMap<>();
    opIdtoParents = new HashMap<>();
    opIdtoContStartTime = new HashMap<>();
    opIdtoContEndTime = new HashMap<>();

    activeAtWorker = new HashMap<>();

    for (Operator op : p.graph.operatorsList) {
      opIdtoChildren.put(op.getId(), new ArrayList<>());
      opIdtoParents.put(op.getId(), new ArrayList<>());
    }

    for (Operator op : p.graph.operatorsList) {
      for (Edge childE : p.graph.getChildren(op.getId())) {
        opIdtoChildren.get(op.getId()).add(childE.to);
      }
      for (Edge parentE : p.graph.getParents(op.getId())) {
        opIdtoParents.get(op.getId()).add(parentE.from);
      }

    }
  }

  public void findRoots() {

    for (Long opid : opIdtoParents.keySet()) {
      if (opIdtoParents.get(opid).size() == 0) {
        readyOps.add(opid);
      }
    }

  }


  public boolean hasplanterminated(long clock) {
    if (terminationCondition.size() == operatorsNo) {
      masterEndTime_ms = clock;
      return true;
    }
    return false;
  }

  public boolean canSchedule(Long nextOpid, Long contID, long clock) {
    boolean result = true;
    if (plan.opIdtoStartEndProcessing_MS.get(nextOpid).a > clock) {
      return false;
    }
    if (workerAvailableAt.get(contID) > clock) {
      return false;
    }
    if (!isWorkerAvailable.get(contID)) {
      return false;
    }

    return result;
  }

  public void schedule(Long nextOpid, Long contID, Long clock) {
    isWorkerAvailable.put(contID, false);
    workerAvailableAt.put(contID, clock + plan.opIdToProcessingTime_MS.get(nextOpid));

    runningOps.add(nextOpid);
    activeAtWorker.put(contID, nextOpid);

    opIdtoContStartTime.put(nextOpid, clock);
    opIdtoContEndTime.put(nextOpid, plan.opIdToProcessingTime_MS.get(nextOpid) + clock);

    /////only for the first op////
    if (masterStartTime_ms == -1) {
      masterEndTime_ms = clock;
    }
    /////////////////////////////
    if (workerStartEndTime_MS.get(contID).a == -1) {
      workerStartEndTime_MS.get(contID).a = clock;
    }

    readyOps.remove(nextOpid);


  }

  /// /calculate next clock from
  /// ops that are running end
  /// already readyOps and their start time
  /// dt that are done
  public Long calculateNextClock(Long clock) {
    long minclock = Long.MAX_VALUE;

    /////////////////////////////////////////minWorkerEndContainer
    long tempmin = Long.MAX_VALUE;
    for (Long key : workerAvailableAt.keySet()) {
      if (!isWorkerAvailable.get(key)) {
        Long t = workerAvailableAt.get(key);
        tempmin = Math.min(tempmin, t);
      }
    }
    if (tempmin > clock) {
      minclock = Math.min(minclock, tempmin);
    }
    /////////////////////////////////////////minDTtoHappen
    tempmin = Long.MAX_VALUE;
    for (Long t : doneButDTOps) {
      for (Long childid : opIdtoChildren.get(t)) {
        long dtTime = calculateNetworkDelayBetweenOps(t, childid);
        //(long)Math.ceil(plan.graph.edgesMap.get(t).get(childid).data.size_B / RuntimeConstants.distributed_storage_speed_B_MS);
        tempmin = Math.min(tempmin, opIdtoContEndTime.get(t) + dtTime);
      }
    }
    if (tempmin > clock) {
      minclock = Math.min(minclock, tempmin);
    }
    /////////////////////////////////////////minStartTimeofReadyOps
    tempmin = Long.MAX_VALUE;
    for (Long t : readyOps) {
      tempmin = Math.min(tempmin, plan.opIdtoStartEndProcessing_MS.get(t).a);
    }
    if (tempmin > clock) {
      minclock = Math.min(minclock, tempmin);
    }

    if (minclock < clock) {
      minclock = clock;
      System.out.println("EROOOORRR #55");
    }
    return minclock;
  }

  public ArrayList<Long> getNowOps(long clock) {
    ArrayList<Long> ret = new ArrayList<>();

    /////////////////////////////////////////check if something from readyOps can go
    ArrayList<Long> torm = new ArrayList<>();
    for (Long t : readyOps) {
      if (plan.opIdtoStartEndProcessing_MS.get(t).a <= clock) {
        ret.add(t);
        runningOps.add(t);
        torm.add(t);
      }
    }
    for (Long trm : torm) {
      readyOps.remove(trm);
    }

    return ret;
  }

  public void solveDependencies(Long clock) {
    for (Long cid : workerAvailableAt.keySet()) {
      if (workerAvailableAt.get(cid) <= clock) {
        if (!isWorkerAvailable.get(cid)) {
          long opid = activeAtWorker.get(cid);
          activeAtWorker.remove(cid);
          runningOps.remove(opid);
          doneButDTOps.add(opid);
          terminationCondition.add(opid);
          workerStartEndTime_MS.get(cid).b = clock;
          isWorkerAvailable.put(cid, true);
        }
      }
    }
    ArrayList<Long> donebutTOREMOVE = new ArrayList<>();
    for (Long t : doneButDTOps) {
      ArrayList<Long> toremove = new ArrayList<>();
      for (Long childid : opIdtoChildren.get(t)) {
        long dtTime = calculateNetworkDelayBetweenOps(t, childid);
        if (opIdtoContEndTime.get(t) + dtTime <= clock) {
          toremove.add(childid);
//                    opIdtoChildren.get(t).remove(childid);
          opIdtoParents.get(childid).remove(t);

        }
        if (opIdtoParents.get(childid).size() == 0) {
          readyOps.add(childid);

        }
      }
      for (Long trm : toremove) {
        opIdtoChildren.get(t).remove(trm);

      }
      if (opIdtoChildren.get(t).size() == 0) {
        donebutTOREMOVE.remove(t);
        doneOps.add(t);
      }
    }
    for (Long trm : donebutTOREMOVE) {
      doneButDTOps.remove(trm);
    }
  }

  public Long calculateNetworkDelayBetweenOps(Long parentId, Long childId) {
    long netdelay = 0L;
    if (plan.assignments.get(parentId) == plan.assignments.get(childId)) {
      return netdelay;
    }
    for (Edge e : plan.graph.edges.get(parentId)) {
      if (e.to == childId) {
        netdelay = (long) (Math.ceil(e.data.getSizeB() / RuntimeConstants.network_speed_B_MS));
        break;
      }
    }
    return netdelay;
  }
}
