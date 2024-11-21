package com.cwa.solaligue.app;

import com.cwa.solaligue.app.graph.DAG;
import com.cwa.solaligue.app.lattice.LatticeGenerator;
import com.cwa.solaligue.app.parser.PegasusDaxParser;
import com.cwa.solaligue.app.scheduler.*;
import com.cwa.solaligue.app.simulator.SimEnginge;
import com.cwa.solaligue.app.utilities.PairPlot;
import com.cwa.solaligue.app.utilities.Plot;
import com.cwa.solaligue.app.utilities.RandomParameters;
import com.cwa.solaligue.app.utilities.Triple;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class Main {

  static boolean validate = true;

  public static void main(String[] args) {
    String resourcesPath = "src/main/resources/dataset/";

    String rankMethod = "dagMerge";
    boolean multiObjective = true;
    List<Map.Entry<String, Integer>> ensembleSizeList = List.of(
        Map.entry("MONTAGE", 50),
        Map.entry("LIGO", 50)
    );
    RuntimeConstants.quantum_MS = RuntimeConstants.OneSec_MS;
    int pruningFee = 10;
    int constraintMode = 0;
    double moneyConstraint = 2000;
    long timeConstraint = 100000;

    ArrayList<Triple<String, Integer, Integer>> flowsAndParams = new ArrayList<>();
    ensembleSizeList.forEach(entry -> {
      if (RuntimeConstants.quantum_MS == RuntimeConstants.OneHour_MS)
        flowsAndParams.add(
            new Triple<>(resourcesPath + entry.getKey() + ".n." + entry.getValue() + ".0.dax", 1000, 100));
      else
        flowsAndParams.add(
            new Triple<>(resourcesPath + entry.getKey() + ".n." + entry.getValue() + ".0.dax", 1, 1));
    });

    ArrayList<Plan> ensemblePlans = new ArrayList<>();
    DAG dag = runMultipleFlows(flowsAndParams, ensemblePlans, rankMethod, multiObjective, pruningFee,
        constraintMode, moneyConstraint, timeConstraint);

    log.info(dag.toString());

    var tittleTable = String.format(
        "%-12s\t%-12s\t%-12s\t%-12s\t%-20s\t%-12s\t%-12s\t%12s%n",
        "Plan",
        "Money",
        "Runtime_MS",
        "Unfairness",
        "UnfairnessNormalized",
        "AvgSlowdown",
        "AvgStretch",
        "MaxStretch"
    );
    log.info(tittleTable);

    ensemblePlans.forEach(plan -> {
      var content = String.format(
          "%-12d\t%-12f\t%-12d\t%-12f\t%-20f\t%-12f\t%-12f\t%12f%n",
          ensemblePlans.indexOf(plan),
          plan.stats.money,
          plan.stats.runtime_MS,
          plan.stats.unfairness,
          plan.stats.unfairnessNorm,
          plan.stats.subdagMeanSlowdown,
          plan.stats.subdagMeanResponseTime,
          plan.stats.subdagMaxResponseTime
      );
      log.info(content);
    });

    if (validate) {
      log.info("Running sims");
      SimEnginge simeng = new SimEnginge();
      for (Plan p : ensemblePlans) {
        simeng.execute(p);
      }
    }

    List<PairPlot<Double, Long>> data = new ArrayList<>();
    ensemblePlans.forEach(p ->
        data.add(new PairPlot<>(p.stats.runtime_MS, p.stats.money)));
    Plot plot = new Plot("Time/Money Plot", data, "src/main/resources/output/plot.png");
    plot.pack();
    plot.setVisible(true);
  }

  private static DAG runMultipleFlows(ArrayList<Triple<String, Integer, Integer>> flowsAndParams,
                                      ArrayList<Plan> plans,
                                      String rankMethod,
                                      boolean multiObjective,
                                      int pruningK,
                                      int constraintMode,
                                      double moneyConstraint,
                                      long timeConstraint) {
    log.info("Scheduling flows:");
    flowsAndParams.forEach(tr -> log.info(tr.a));

    DAG graph = new DAG();
    ArrayList<DAG> graphs = createGraphs(flowsAndParams);

    graphs.forEach(g -> {
      HashMap<Long, Long> oldIdToNewId = new HashMap<>();
      graph.add(g, oldIdToNewId);
      graph.superDAG.addSubDAG(g, oldIdToNewId);
    });

    runDAG(graph, plans, rankMethod, multiObjective, pruningK, constraintMode, moneyConstraint, timeConstraint);
    return graph;
  }

  private static ArrayList<DAG> createGraphs(ArrayList<Triple<String, Integer, Integer>> flowsAndParams) {
    ArrayList<DAG> graphs = new ArrayList<>();
    Long dagId = 0L;

    for (Triple<String, Integer, Integer> p : flowsAndParams) {
      dagId++;
      try {
        if (p.a.contains("lattice") || p.a.contains("main/java/Lattice")) {
          graphs.add(createLatticeGraph(p));
        } else {
          PegasusDaxParser parser = new PegasusDaxParser(p.b, p.c);
          graphs.add(parser.parseDax(p.a, dagId));
        }
      } catch (Exception e) {
        log.error(e.getMessage());
      }
    }
    return graphs;
  }

  private static DAG createLatticeGraph(Triple<String, Integer, Integer> p) {
    double z = 1.0;
    double randType = 0.0;
    double[] runTime = {0.2, 0.4, 0.6, 0.8, 1.0};
    double[] cpuUtil = {1.0};
    double[] memory = {0.3};
    double[] dataout = {0.2, 0.4, 0.6, 0.8, 1.0};
    RandomParameters params = new RandomParameters(z, randType, runTime, cpuUtil, memory, dataout);
    return LatticeGenerator.createLatticeGraph(p.b, p.c, params, 0, RuntimeConstants.quantum_MS);
  }

  public static void runDAG(DAG graph, List<Plan> hhdsPlans, String rankMethod, boolean multiObjective,
                            Integer pruningK, int constraintMode, double moneyConstraint, long timeConstraint) {
    SolutionSpace combined = new SolutionSpace();
    SolutionSpace paretoToCompare = execute(graph, true, "Knee", rankMethod,
        combined, multiObjective, pruningK, constraintMode, moneyConstraint, timeConstraint);
    hhdsPlans.addAll(paretoToCompare.results);
  }

  public static SolutionSpace execute(DAG graph, boolean prune, String method, String rankMethod,
                                      SolutionSpace combined,
                                      Boolean multiObjective, Integer pruningK, int constraintMode,
                                      double moneyConstraint, long timeConstraint) {
    Cluster cluster = new Cluster();
    Scheduler scheduler = new HhdsEnsemble(graph, cluster, prune, method, rankMethod, multiObjective, pruningK,
        constraintMode, moneyConstraint, timeConstraint);
    SolutionSpace space = scheduler.schedule();
    combined.addAll(space);
    return space;
  }
}