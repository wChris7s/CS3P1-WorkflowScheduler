package com.cwa.solaligue.app.lattice;

import com.cwa.solaligue.app.graph.*;
import com.cwa.solaligue.app.scheduler.RuntimeConstants;
import com.cwa.solaligue.app.utilities.RandomParameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;


public class LatticeGenerator {

  public static void main(String[] args) {
    long seed = 0;

    double z = 1.0;
    double randType = 0.0;
    double[] runTime = {0.2, 0.4, 0.6, 0.8, 1.0};
    double[] cpuUtil = {1.0};
    double[] memory = {0.3};
    double[] dataout = {0.2, 0.4, 0.6, 0.8, 1.0};

    RandomParameters
        params = new RandomParameters(z, randType, runTime, cpuUtil, memory, dataout);

    DAG graph = createLatticeGraph(3, 498, params, seed, RuntimeConstants.quantum_MS);

    graph.printEdges();
    System.out.println(graph.sumdata_B / 1073741824);
  }

  public static DAG createLatticeGraph(int depth,
                                       int breadth, RandomParameters params, long seed, long quantumSizeForGeneration) {

    HashMap<Long, Long> opIdToOutDataSize = new HashMap<Long, Long>();

    DAG graph = new DAG();

    Random rand = new Random(seed);

    LinkedList<LinkedList<Long>> operatorsLevelsUp =
        new LinkedList<LinkedList<Long>>();

    LinkedList<LinkedList<Long>> operatorsLevelsDown =
        new LinkedList<LinkedList<Long>>();

    LinkedList<Long> middleOperators =
        new LinkedList<Long>();

    int opNum = 0;
    for (int i = 0; i < depth / 2; i++) {
      LinkedList<Long> upOperators = new LinkedList<Long>();
      LinkedList<Long> downOperators = new LinkedList<Long>();

      // up
      for (int j = 0; j < Math.pow(breadth, i); j++) {
        opNum++;
        Operator op = createADDOperator("op" + opNum, 1, params, rand, opIdToOutDataSize, graph, quantumSizeForGeneration);
        upOperators.add(op.getId());
      }

      // down
      for (int j = 0; j < Math.pow(breadth, i); j++) {
        opNum++;
        Operator op = createADDOperator("op" + opNum, breadth, params, rand, opIdToOutDataSize, graph, quantumSizeForGeneration);
        downOperators.add(op.getId());
      }

      operatorsLevelsUp.addLast(upOperators);
      operatorsLevelsDown.addFirst(downOperators);
    }

    for (int j = 0; j < Math.pow(breadth, (depth / 2)); j++) {
      opNum++;
      Operator op = createADDOperator("op" + opNum, 1, params, rand, opIdToOutDataSize, graph, quantumSizeForGeneration);
      middleOperators.add(op.getId());
    }

    for (int i = operatorsLevelsUp.size() - 1; i > 0; i--) {
      LinkedList<Long> opList = operatorsLevelsUp.get(i);
      LinkedList<Long> opListParent = operatorsLevelsUp.get(i - 1);

      for (int j = 0; j < opList.size(); j++) {
        Long from = opList.get(j);
        Long to = opListParent.get((j / breadth));

        Long dataOutSize = opIdToOutDataSize.get(from);
        Data df = new Data("noname", dataOutSize);
        Edge edge = new Edge(from, to, df);

        graph.addEdge(edge);
      }
    }

    for (int j = 0; j < middleOperators.size(); j++) {
      Long from = middleOperators.get(j);
      Long to = operatorsLevelsUp.get(operatorsLevelsUp.size() - 1).get((j / breadth));

      Long dataSizeOut = opIdToOutDataSize.get(from);
      Data df = new Data("noname", dataSizeOut);
      Edge edge = new Edge(from, to, df);


      graph.addEdge(edge);
    }

    for (int j = 0; j < middleOperators.size(); j++) {
      Long to = middleOperators.get(j);
      Long from = operatorsLevelsDown.get(0).get((j / breadth));

      Long tempIndex = (long) (j % breadth);
      Long dataSizeOut = opIdToOutDataSize.get(tempIndex);
      Data df = new Data("noname", dataSizeOut);
      Edge edge = new Edge(from, to, df);


      graph.addEdge(edge);
    }

    for (int i = 1; i < operatorsLevelsDown.size(); i++) {
      LinkedList<Long> opList = operatorsLevelsDown.get(i);
      LinkedList<Long> opListParent = operatorsLevelsDown.get(i - 1);

      for (int j = 0; j < opListParent.size(); j++) {
        Long to = opListParent.get(j);
        Long from = opList.get((j / breadth));

        long tempIndex = j % breadth;
        Long dataSizeOut = opIdToOutDataSize.get(tempIndex);
        Data df = new Data("noname", dataSizeOut);
        Edge edge = new Edge(from, to, df);

        graph.addEdge(edge);
      }
    }


    graph.sumdata_B = 0;
    for (ArrayList<Edge> ae : graph.edges.values()) {
      for (Edge e : ae) {
        graph.sumdata_B += e.data.getSizeB();
      }
    }
    return graph;
  }


  public static Operator createADDOperator(
      String id,
      int fanOut,
      RandomParameters params,
      Random rand,
      HashMap<Long, Long> opIdToOutDataSize,
      DAG graph,
      long quantumSizeForGeneration) {
    double runTimeValue = params.runTime[params.runTimeDist.next()];
    long runtime_MS = (long) (runTimeValue * quantumSizeForGeneration);

    runtime_MS *= 25;

    ResourcesRequirements rr = new ResourcesRequirements(runtime_MS,
        100);

    Operator op = new Operator(
        id,
        rr);

    double quantums = params.dataout[params.dataoutDist.next()];
    double bytesPerQuantum =
        RuntimeConstants.distributed_storage_speed_B_MS
            * quantumSizeForGeneration;


    graph.addOperator(op);

    long dataCount = 0;
    for (int i = 0; i < fanOut; i++) {
      dataCount += quantums * bytesPerQuantum;
    }
    dataCount /= 200;
    opIdToOutDataSize.put(op.getId(), dataCount);
    return op;
  }

}
