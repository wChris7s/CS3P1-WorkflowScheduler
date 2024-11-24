package com.cwa.solaligue.app.graph;

import com.cwa.solaligue.app.scheduler.ContainerType;
import com.cwa.solaligue.app.scheduler.RuntimeConstants;
import com.cwa.solaligue.app.scheduler.TopologicalSorting;

import java.util.ArrayList;
import java.util.HashMap;


public class DAG {
  public Long dagId;
  public String name;
  public long sumdata_B;
  public HashMap<Long, Operator> operators;
  public ArrayList<Operator> operatorsList;
  public HashMap<Long, ArrayList<Edge>> edges;
  public HashMap<Long, HashMap<Long, Edge>> edgesMap;
  private HashMap<Long, ArrayList<Edge>> reverseEdges;
  private Long nextId;
  public DAGmerged superDAG;

  public DAG() {
    dagId = 0L;
    name = "";
    operators = new HashMap<>();
    edges = new HashMap<>();
    reverseEdges = new HashMap<>();
    edgesMap = new HashMap<>();
    nextId = 0L;
    operatorsList = new ArrayList<>();
    sumdata_B = 0;
    superDAG = new DAGmerged();
  }

  public DAG(Long id) {
    name = "";
    operators = new HashMap<>();
    edges = new HashMap<>();
    reverseEdges = new HashMap<>();
    edgesMap = new HashMap<>();
    nextId = 0L;
    operatorsList = new ArrayList<>();
    sumdata_B = 0;
    dagId = id;
    superDAG = new DAGmerged();

  }

  public Long addOperator(Operator op) {

    op.setId(nextId);
    operators.put(nextId, op);
    operatorsList.add(op);

    edges.put(nextId, new ArrayList<Edge>());
    reverseEdges.put(nextId, new ArrayList<Edge>());

    edgesMap.put(nextId, new HashMap<Long, Edge>());

    if (op.dagID == -1L) {
      op.setDAGId(dagId);
    }
    return nextId++;
  }

  public Operator getOperator(Long id) {
    return operators.get(id);
  }

  public void addEdge(Edge e) {

    edges.get(e.from).add(e);
    edgesMap.get(e.from).put(e.to, e);
    reverseEdges.get(e.to).add(e);

  }

  public ArrayList<Edge> getChildren(Long node) {
    return edges.get(node);
  }

  public Edge getEdge(Long from, Long to) {
    return edgesMap.get(from).get(to);
  }

  public ArrayList<Edge> getParents(Long node) {
    return reverseEdges.get(node);
  }

  public ArrayList<Operator> getOperators() {
    return operatorsList;
  }

  public void printEdges() {
    for (Long opid : edges.keySet()) {
      System.out.printf(opid + ": ");
      for (Edge out : getChildren(opid)) {
        System.out.printf(" " + out.to + "(" + out.data.getSizeB() + ")");
      }
      System.out.println();
    }
  }

  public int sumEdges() {
    int sum = 0;
    for (ArrayList<Edge> a : edges.values()) {
      sum += a.size();
    }
    return sum;
  }

  public double computeCCR() {
    Long dataSum_B = 0L;
    for (ArrayList<Edge> ed : edges.values()) {
      for (Edge edge : ed) {
        dataSum_B += edge.data.getSizeB();
      }
    }
    Double comm = (2 * dataSum_B) / RuntimeConstants.distributed_storage_speed_B_MS;

    long comp = 0;

    for (Operator op : operatorsList) {
      comp += op.getRunTime_MS();
    }


    Double ccr = comm / comp;
    return ccr;
  }

  public DAG add(DAG g, HashMap<Long, Long> OldIdToNewId) {

    Long oldid, newid;
    for (Operator op : g.getOperators()) {
      oldid = op.getId();
      Operator newop = new Operator(op.name, op.resourcesRequirements);
      newop.setDAGId(g.dagId);
      newid = this.addOperator(newop);
      OldIdToNewId.put(oldid, newid);
    }

    for (ArrayList<Edge> ae : g.edges.values()) {
      for (Edge e : ae) {
        this.addEdge(new Edge(OldIdToNewId.get(e.from), OldIdToNewId.get(e.to), e.data));
      }
    }

    this.sumdata_B += g.sumdata_B;
    return this;
  }

  public double computeCrPathLength(ContainerType contTypes[]) {
    double crPathLength = 0.0;
    TopologicalSorting topOrder = new TopologicalSorting(this);
    HashMap<Long, Double> rank = new HashMap<>();
    int types = ContainerType.values().length;

    for (Long opId : topOrder.iterator()) {

      double maxRankParent = 0.0;
      for (Edge inLink : this.getParents(opId))
        maxRankParent = Math.max(maxRankParent, rank.get(inLink.from));

      double w = 0.0;
      for (ContainerType contType : contTypes)
        w += this.getOperator(opId).getRunTime_MS() / contType.getContainerCPU();

      w = w / (double) types;
      Double opRank = w + maxRankParent;
      rank.put(opId, opRank);

      crPathLength = Math.max(crPathLength, opRank);
    }
    return crPathLength;
  }


  public HashMap<Long, Double> computePathToExit(ContainerType contTypes[]) {
    TopologicalSorting topOrder = new TopologicalSorting(this);
    HashMap<Long, Double> b_rank = new HashMap<>();
    HashMap<Long, Double> w_mean = new HashMap<>();

    for (Long opId : topOrder.iteratorReverse()) {
      double maxRankChild = 0.0;
      for (Edge childEdge : this.getChildren(opId)) {
        double comCostChild = 0.0;
        for (Edge parentofChildEdge : this.getParents(childEdge.to)) {
          if (parentofChildEdge.from.equals(opId)) {
            comCostChild = Math.ceil(parentofChildEdge.data.getSizeB() / RuntimeConstants.network_speed_B_MS);
          }
        }

        maxRankChild = Math.max(maxRankChild, comCostChild + b_rank.get(childEdge.to));
      }

      double wcur = 0.0;
      for (ContainerType contType : ContainerType.values())
        wcur += this.getOperator(opId).getRunTime_MS() / contType.getContainerCPU();

      int types = ContainerType.values().length;
      double w = wcur / (double) types;
      b_rank.put(opId, (w + maxRankChild));
      w_mean.put(opId, w);

    }
    return b_rank;
  }


  public HashMap<Long, Double> computePathFromEntry(ContainerType contTypes[]) {


    TopologicalSorting topOrder = new TopologicalSorting(this);
    HashMap<Long, Double> t_rank = new HashMap<>();
    HashMap<Long, Double> w_mean = new HashMap<>();

    for (Long opId : topOrder.iterator()) {
      double maxRankParent = 0.0;
      for (Edge inLink : this.getParents(opId)) {
        double comCostParent = Math.ceil(inLink.data.getSizeB() / RuntimeConstants.network_speed_B_MS);
        maxRankParent = Math.max(maxRankParent, comCostParent + t_rank.get(inLink.from) + w_mean.get(inLink.from));
      }

      double wcur = 0.0;
      for (ContainerType contType : ContainerType.values())
        wcur += this.getOperator(opId).getRunTime_MS() / contType.getContainerCPU();
      int types = ContainerType.values().length;
      double w = wcur / (double) types;
      w_mean.put(opId, w);

      t_rank.put(opId, maxRankParent);
    }

    return t_rank;
  }


  public HashMap<Long, Double> computePath(ContainerType contTypes[]) {

    TopologicalSorting topOrder = new TopologicalSorting(this);
    HashMap<Long, Double> t_rank = computePathFromEntry(contTypes);
    HashMap<Long, Double> b_rank = computePathToExit(contTypes);
    HashMap<Long, Double> tb_rank = new HashMap<>();

    for (Long opId : topOrder.iterator()) {
      Double sumRank = t_rank.get(opId) + b_rank.get(opId);
      tb_rank.put(opId, sumRank);
    }

    return tb_rank;
  }

  public Double computeMaxPath(ContainerType contTypes[]) {

    TopologicalSorting topOrder = new TopologicalSorting(this);
    HashMap<Long, Double> t_rank = computePathFromEntry(contTypes);
    HashMap<Long, Double> b_rank = computePathToExit(contTypes);

    double maxPath = 0.0;
    for (Long opId : topOrder.iterator()) {
      Double sumRank = t_rank.get(opId) + b_rank.get(opId);
      maxPath = Double.max(sumRank, maxPath);
    }

    return maxPath;
  }
}
