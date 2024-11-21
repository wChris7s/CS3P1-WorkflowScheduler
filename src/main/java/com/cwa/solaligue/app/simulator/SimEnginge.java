package com.cwa.solaligue.app.simulator;

import com.cwa.solaligue.app.scheduler.Plan;
import com.cwa.solaligue.app.scheduler.RuntimeConstants;
import com.cwa.solaligue.app.utilities.Pair;

import java.util.ArrayList;
import java.util.Comparator;

public class SimEnginge {

  ExecutionState state;

  public SimEnginge() {
  }

  public void execute(Plan p) {

    Long clock = 0L;

    this.state = new ExecutionState(p);

    state.findRoots();

    ArrayList<Long> toScheduleOps = new ArrayList<>();

    while (!state.hasplanterminated(clock)) {

      toScheduleOps.addAll(state.getNowOps(clock));

      toScheduleOps.sort(new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
          return (int) (state.plan.opIdtoStartEndProcessing_MS.get(o1).a - state.plan.opIdtoStartEndProcessing_MS.get(o2).a);
        }
      });

      ArrayList<Long> notScheduled = new ArrayList<>();
      for (Long nextOpid : toScheduleOps) {

        Long contID = state.plan.assignments.get(nextOpid);

        if (state.canSchedule(nextOpid, contID, clock)) {
          state.schedule(nextOpid, contID, clock);

        } else {
          notScheduled.add(nextOpid);
        }
      }
      toScheduleOps = notScheduled;
      notScheduled.clear();

      clock = state.calculateNextClock(clock);

      state.solveDependencies(clock);

    }

    double money = calculateMoney();

    System.out.println("SimEngine plan simulated (sched,sim) Time: (" + state.plan.stats.runtime_MS + "," + state.masterEndTime_ms + ") Money: ("
        + state.plan.stats.money + "," + money + ")");
  }

  private double calculateMoney() {
    // int quanta = 0;
    double money = 0.0;
    for (Long cid : state.workerStartEndTime_MS.keySet()) {
      Pair<Long, Long> se = state.workerStartEndTime_MS.get(cid);
      int localQuanta = (int) Math.ceil((double) (se.b - se.a) / RuntimeConstants.quantum_MS);
      // quanta+=localQuanta;
      money += localQuanta * state.plan.cluster.getContainer(cid).contType.container_price;
    }
    return money;
  }
}

