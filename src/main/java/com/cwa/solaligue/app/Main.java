package com.cwa.solaligue.app;

import com.cwa.solaligue.app.graph.DAG;
import com.cwa.solaligue.app.lattice.LatticeGenerator;
import com.cwa.solaligue.app.parser.PegasusDaxParser;
import com.cwa.solaligue.app.scheduler.*;
import com.cwa.solaligue.app.simulator.SimEnginge;
import com.cwa.solaligue.app.utilities.*;
import lombok.extern.slf4j.Slf4j;
import org.jfree.data.xy.XYSeries;

import java.awt.*;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cwa.solaligue.app.utilities.PlotCSV.createChart;

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
    int pruningFee = 20;
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

    String outputFilePath = "src/main/resources/output/plans.csv";
    writePlansToCSV(ensemblePlans, outputFilePath);
    plotCSV();

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

  private static void plotCSV() {
    String csvPath = "src/main/resources/output/plans.csv";
    List<String> lines;
    try {
      lines = Files.readAllLines(Paths.get(csvPath));
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    XYSeries seriesMoney = new XYSeries("Money");
    XYSeries seriesRuntime = new XYSeries("Runtime_MS");
    XYSeries seriesInequity = new XYSeries("Inequity");

    for (int i = 1; i < lines.size(); i++) {
      String[] values = lines.get(i).split(";");
      int plan = Integer.parseInt(values[0]);
      double money = Double.parseDouble(values[1].replace(',', '.'));
      long runtime = Long.parseLong(values[2]);
      double inequity = Double.parseDouble(values[3].replace(',', '.'));

      seriesMoney.add(plan, money);
      seriesRuntime.add(plan, runtime);
      seriesInequity.add(plan, inequity);
    }

    createChart(seriesMoney,"Plan vs Money", "Plan", "Money", "src/main/resources/output/plan_vs_money.png", Color.BLACK);
    createChart(seriesRuntime, "Plan vs Runtime_MS", "Plan", "Runtime_MS", "src/main/resources/output/plan_vs_runtime.png", Color.BLUE);
    createChart(seriesInequity, "Plan vs Inequity", "Plan", "Inequity", "src/main/resources/output/plan_vs_inequity.png", Color.RED);
  }

  private static void writePlansToCSV(List<Plan> plans, String filePath) {
    try (FileWriter writer = new FileWriter(filePath)) {
      writer.write("Plan;Money;Runtime_MS;Inequity\n");
      for (int i = 0; i < plans.size(); i++) {
        Plan plan = plans.get(i);
        writer.write(String.format(
            "%d;%f;%d;%f%n",
            i,
            plan.stats.money,
            plan.stats.runtime_MS,
            plan.stats.inequityStdDev
        ));
      }
      log.info("Datos guardados en: {}", filePath);
    } catch (IOException e) {
      log.error("Error al escribir el archivo CSV", e);
    }
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