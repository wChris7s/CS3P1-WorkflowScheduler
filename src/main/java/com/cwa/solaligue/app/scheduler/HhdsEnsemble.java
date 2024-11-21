package com.cwa.solaligue.app.scheduler;

import com.cwa.solaligue.app.graph.DAG;
import com.cwa.solaligue.app.graph.Edge;
import com.cwa.solaligue.app.graph.Operator;
import com.cwa.solaligue.app.utilities.MultiplePlotInfo;

import java.util.*;

public class HhdsEnsemble implements Scheduler {

  public SolutionSpace space;
  public Cluster cluster;
  public DAG graph;

  public String rankingMethod = ""; //commonEntry, perDag, dagMerge: default

  public LinkedList<Long> opsSorted;

  public int pruneSkylineSize;
  public int homoPlanstoKeep = 10;

  public int maxContainers = 100000000;

  public boolean backfilling = false;
  public boolean backfillingUpgrade = false;

  public boolean heteroEnabled = true;
  public boolean pruneEnabled;
  public String PruneMethod;
  public boolean multi;
  public int constraint_mode;
  public double money_constraint;
  public long time_constraint;

  private HashMap<Long, Integer> opLevel;


  public HhdsEnsemble(DAG graph, Cluster cl, boolean prune, String PruneMethod, String rankingMethod, Boolean multi,
                      int pruning_k, int constraint_mode, double money_constraint, long time_constraint) {
    this.rankingMethod = rankingMethod;
    this.pruneEnabled = prune;
    this.space = new SolutionSpace();
    this.graph = graph;
    this.cluster = cl;
    this.opsSorted = new LinkedList<>();
    this.opLevel = new HashMap<>();
    this.PruneMethod = PruneMethod;
    this.multi = multi;
    this.pruneSkylineSize = pruning_k;
    this.constraint_mode = constraint_mode;
    this.money_constraint = money_constraint;
    this.time_constraint = time_constraint;
  }

  @Override
  public SolutionSpace schedule() {
    long startCPU_MS = System.currentTimeMillis();

    MultiplePlotInfo mpinfo = new MultiplePlotInfo();
    SolutionSpace skylinePlans = new SolutionSpace();
    SolutionSpace skylinePlans_INC = new SolutionSpace();
    SolutionSpace skylinePlans_DEC = new SolutionSpace();
    SolutionSpace skylinePlans_INCDEC = new SolutionSpace();
    SolutionSpace paretoPlans = new SolutionSpace();

    computeRankings();

    skylinePlans.clear();

    if (heteroEnabled) {
      // We have plans with containers of different types
      // hetero = heterogeneous
      for (ContainerType cType : ContainerType.values()) {

        if (maxContainers == 1) {
          skylinePlans.add(onlyOneContainer());
        } else {
          ////INC DEC/////
          //                    System.out.println("calc "+cType.name);
          if (cType.equals(ContainerType.getLargest())) {
            ArrayList<ContainerType> cTypes = new ArrayList<>();
            cTypes.add(cType);

            skylinePlans_DEC.addAll(this.createAssignments("decreasing", cTypes, true));
            //                    plotPlans("dec",skylinePlans);
            //                    System.out.println("s1 "+skylinePlans.size());

          } else if (cType.equals(ContainerType.getSmallest())) {
            ArrayList<ContainerType> cTypes = new ArrayList<>();
            cTypes.add(cType);

            skylinePlans_INC.addAll(this.createAssignments("increasing", cTypes, true));
            //                    plotPlans("inc",skylinePlans);
            //                    System.out.println("s2 "+skylinePlans.size());
          } else {
            ArrayList<ContainerType> cTypes = new ArrayList<>();
            cTypes.add(cType);


            skylinePlans_INCDEC
                .addAll(this.createAssignments("increasing/decreasing", cTypes, true));
            //                    plotPlans("inc,dec",skylinePlans);
            //                    System.out.println("s3 "+skylinePlans.size());
          }

        }

      }

      skylinePlans.addAll(skylinePlans_DEC.results);
      skylinePlans.addAll(skylinePlans_INC.results);
      skylinePlans.addAll(skylinePlans_INCDEC.results);
    } else {
      ContainerType cType = ContainerType.getSmallest();
      if (maxContainers == 1) {
        skylinePlans.add(onlyOneContainer());
      } else {
        ArrayList<ContainerType> cTypes = new ArrayList<>();
        cTypes.add(cType);
        skylinePlans_INCDEC.addAll(this.createAssignments("increasing", cTypes, true));

      }
      skylinePlans.addAll(skylinePlans_INCDEC.results);
    }

    paretoPlans.addAll(skylinePlans.results);

    // compute the skyline for all the homogeneous plans
    paretoPlans.computeSkyline(pruneEnabled, homoPlanstoKeep, false, PruneMethod, multi,
        false, 0, money_constraint, time_constraint);

    mpinfo.add("pareto", paretoPlans.results);

    long homoEnd = System.currentTimeMillis();

    skylinePlans.clear();

    for (Plan pp : paretoPlans.results) {
      if (pp.vmUpgrading.equals("increasing/decreasing")) {

        pp.vmUpgrading = "increasing";
        skylinePlans.add(pp);

        Plan newpp = new Plan(pp);
        newpp.vmUpgrading = "decreasing";
        skylinePlans.add(newpp);
      } else {
        skylinePlans.add(pp);
      }
    }

    paretoPlans.clear();
//        space.smallPrint();

    // We want plans with heterogeneous container types
    if (heteroEnabled) {
      paretoPlans.addAll(homoToHetero(skylinePlans)); //returns only hetero
    }

    paretoPlans.addAll(skylinePlans);

    space.addAll(paretoPlans);

    long endCPU_MS = System.currentTimeMillis();
    space.setOptimizationTime(endCPU_MS - startCPU_MS);

    mpinfo.add("final space", space.results);

//        space.smallPrint();

    if (constraint_mode == 1 || constraint_mode == 2 || constraint_mode == 3) {
      // by fixing constraint mode to 1 only one plan is returned. This happens only when constraints are applied.
      space.computeSkyline(pruneEnabled, pruneSkylineSize, false, PruneMethod, multi, false,
          1, money_constraint, time_constraint);
    } else {
      space.computeSkyline(pruneEnabled, pruneSkylineSize, false, PruneMethod, multi, false,
          0, money_constraint, time_constraint);
    }
//        space.smallPrint();
    return space;

  }

  private void computeSlack(Plan plan, HashMap<Long, Double> contSlack, HashMap<Long, Integer> contOps, HashMap<Long, Long> opSlack, ArrayList<Long> opSortedBySlack) {
    contSlack.clear();
    contOps.clear();
    opSlack.clear();
    opSortedBySlack.clear();

    computeSlackOps(plan, opSlack, opSortedBySlack);

    for (Long opId : opsSortedReversed()) {
      double slackPerCont = opSlack.get(opId);
      long opContID = plan.assignments.get(opId);
      int opsPerCont = 1;
      if (contOps.containsKey(opContID)) {
        slackPerCont += contSlack.get(opContID);
        opsPerCont = contOps.get(opContID) + 1;
      }

      contSlack.put(opContID, slackPerCont);
      contOps.put(opContID, opsPerCont);
    }
    for (Long contId : contSlack.keySet()) {
      double contAvgSlack = contSlack.get(contId) / (double) contOps.get(contId);
      contSlack.put(contId, contAvgSlack);
    }
  }

  // Covnert plans with homogeneous container types to heterogeneous
  private SolutionSpace homoToHetero(SolutionSpace plans) {
    if (plans.isEmpty()) {
      return plans;
    }

    SolutionSpace plansInner = new SolutionSpace();//deepcopy of input
    for (Plan p : plans.results) {
      plansInner.add(new Plan(p));
    }

    SolutionSpace result = new SolutionSpace();//keeps all the solutions at the current pareto

    for (Plan p : plans) {
      result.add(new Plan(p));
    }
    //look at each plan and upgrade one by one the LARGE containers

    HashMap<Long, Long> opSlack = new HashMap<>();

    SolutionSpace skylinePlansNew = new SolutionSpace();
    // the set of plans from the newly modified plans (plans with upgraded/degraded vm types)
    // that belong to the current pareto

    int updateSkyline = 1;

    int loop = 0;

    while (updateSkyline == 1) {
      loop++;
      updateSkyline = 0;
      int innerloop = 0;
      for (final Plan plan : plansInner) {                                                                         //for every plan
        innerloop++;
        LinkedList<Long> planContainersTobeModified = new LinkedList<>();
//                System.out.println(loop + ": plan" + innerloop);
        ArrayList<Long> opSortedBySlack = new ArrayList<>();
        //compute avg slack per container/VM

        HashMap<Long, Double> contSlack = new HashMap<>();
        HashMap<Long, Integer> contOps = new HashMap<>();

        computeSlack(plan, contSlack, contOps, opSlack, opSortedBySlack);

        for (Long i : plan.cluster.containers.keySet()) {                                                           // for each cont change it
          Container cont = plan.cluster.getContainer(i);

          if (plan.vmUpgrading == null) {
            System.out.println("bug line 254");
            break;
          }

          if ((cont.contType == ContainerType.getLargest() && plan.vmUpgrading.equals("increasing"))
              || (cont.contType == ContainerType.getSmallest() && plan.vmUpgrading.equals("decreasing"))) {
            //the container has the largest vm type so it will be ignored as a candidate for upgrading
            continue;
          } else {//the container is a candidate for upgrading
            planContainersTobeModified.add(i);
            updateSkyline = 1;
          }
        }

        //if the list of candidate containers for upgrading is empty then continue to the next plan
        if (planContainersTobeModified.size() == 0)
          continue;


        Comparator<Long> contSlackComparator = new Comparator<Long>() {
          @Override
          public int compare(Long vm1, Long vm2) {
            double s1;
            double s2;
            if (!contSlack.containsKey(vm1)) {
              s1 = Double.MAX_VALUE;
            } else {
              s1 = contSlack.get(vm1);///(double)contOps.get(vm1);
            }

            if (!contSlack.containsKey(vm2)) {
              s2 = Double.MAX_VALUE;//TODO check asap
            } else {
              s2 = contSlack.get(vm2);///(double)contOps.get(vm1);
            }
            if (s1 > s2)//TODO: add precision error
              return -1;
            else if (s1 < s2)
              return 1;
            else
              return 0;
          }
        };

        if (plan.vmUpgrading.contains("decreasing")) {
//                    System.out.println("ss " + planContainersTobeModified.size()+" "+planContainersTobeModified);
          Collections.sort(planContainersTobeModified, contSlackComparator);
        } else
          Collections.sort(planContainersTobeModified, Collections.reverseOrder(contSlackComparator));

        Plan newPlan = null;

        for (Long k : planContainersTobeModified) {            //for every cont that can be modified, create a new plan

          if (plan.vmUpgrading == null) {
            System.out.println("bug line 297");
            break;
          }

          newPlan = new Plan(graph, new Cluster());
          newPlan.vmUpgrading = plan.vmUpgrading;
          for (Container contcont : plan.cluster.containersList) {
            newPlan.cluster.addContainer(contcont.contType);
          }


          Container cont = newPlan.cluster.containers.get(k);

          if (plan.vmUpgrading.equals("increasing"))                                      //modify the container -- TODO check if we could add all smaller and bigger conts
            newPlan.cluster.update(cont.id, ContainerType.getNextLarger(cont.contType));
          else
            newPlan.cluster.update(cont.id, ContainerType.getNextSmaller(cont.contType));


          int opsAssigned = 0;                                                              //assign all the ops again to the new plan
          HashSet<Long> opsAssignedSet = new HashSet<>();
          HashSet<Long> readyOps = new HashSet<>();

          findRoots(readyOps);

          while (readyOps.size() > 0) {//iterate on the ready to schedule ops

            opsAssigned++;
            long nextOpID = nextOperator(readyOps);
            Operator nextOp = graph.getOperator(nextOpID);
//                        System.out.println("\nHomoToHetero scheduling "+nextOpID + " "+readyOps.toString());

            newPlan.assignOperator(nextOpID, plan.assignments.get(nextOpID), backfillingUpgrade);

            findNextReadyOps(readyOps, opsAssignedSet, nextOpID);
          }

          //use Double.compare
          if (newPlan.stats.money >= plan.stats.money && newPlan.stats.runtime_MS >= plan.stats.runtime_MS)//we could use a threshold. e.g. if savings less than 0.1%
          {
            break; //no more containers for this plan are going to be modified. it breaks
          }
          skylinePlansNew.add(newPlan);
        }

      }

      plansInner.clear();
      plansInner.addAll(result.results);


      plansInner.addAll(skylinePlansNew);

      plansInner.computeSkyline(pruneEnabled, pruneSkylineSize, true, PruneMethod, multi,
          false, constraint_mode, money_constraint, time_constraint);

      plansInner.retainAllAndKeep(skylinePlansNew, pruneSkylineSize);

      result.addAll(plansInner);

      skylinePlansNew.clear();
    }


    for (Plan plan : result) { //TODO ilia check if this is needed?
      if (plan == null) {
        continue;
      }

      HashMap<ContainerType, Double> avgOpTime = new HashMap<>();
      HashMap<ContainerType, Integer> opNumber = new HashMap<>();

      for (Long opId : plan.assignments.keySet()) {


        ContainerType cType = plan.cluster.getContainer(plan.assignments.get(opId)).contType;

        int ops = 0;
        double opProcessTime = plan.opIdtoStartEndProcessing_MS
            .get(opId).b - plan.opIdtoStartEndProcessing_MS.get(opId).a;

        if (opNumber.get(cType) == null) {
          ops = 1;
          avgOpTime.put(cType, opProcessTime);
        } else {
          ops = opNumber.get(cType);
          double processTime = avgOpTime.get(cType) * ops + opProcessTime;
          ops++;
          avgOpTime.put(cType, processTime / ops);

        }
        opNumber.put(cType, ops);
      }

    }


    return result;
  }

  private void findRoots(HashSet<Long> readyOps) {
    for (Long opid : graph.operators.keySet()) {
      if (graph.getParents(opid).size() == 0) {
        readyOps.add(opid);
      }
    }
  }

  private void findNextReadyOps(HashSet<Long> readyOps, HashSet<Long> opsAssignedSet, Long justScheduledOpId) {
    Boolean allAssigned;               //find new readyops
    readyOps.remove(justScheduledOpId);
    opsAssignedSet.add(justScheduledOpId);
    for (Edge childEdge : graph.getChildren(justScheduledOpId)) {
      Long childopId = childEdge.to;
      allAssigned = true;
      for (Edge ParentChildEdge : graph.getParents(childopId)) {
        Long ParentChildOpId = ParentChildEdge.from;
        if (!opsAssignedSet.contains(ParentChildOpId)) {
          allAssigned = false;
        }
      }
      if (allAssigned) {
        readyOps.add(childopId);
      }
    }
  }

  private SolutionSpace createAssignments(String vmUpgrading, ArrayList<ContainerType> cTypes, boolean homo) {

    int constraint_mode_local = constraint_mode;
    if (homo) {
      constraint_mode_local = 0;
    }
//        System.out.println("createass start");
    Plan firstPlan = new Plan(graph, cluster);
    firstPlan.vmUpgrading = vmUpgrading;

    SolutionSpace allCandidates = new SolutionSpace();
    SolutionSpace plans = new SolutionSpace();
    plans.add(firstPlan);

    int opsAssigned = 0;

    HashSet<Long> opsAssignedSet = new HashSet<>();
    HashSet<Long> readyOps = new HashSet<>();

    findRoots(readyOps);

    int prevPrune = -1;

    while (readyOps.size() > 0) {

      opsAssigned++;

      // Get the most expensive operator from the ready ones
      long nextOpID = nextOperator(readyOps);
      Operator nextOp = graph.getOperator(nextOpID);
//            System.out.println(cTypes.get(0).name + ". Next:" + nextOpID + ". Assigned " + opsAssigned + " ops");
//               System.out.println("scheduling "+nextOpID + " "+readyOps.toString());

      allCandidates.clear();
      for (Plan plan : plans) {

        if (plan == null) {
          continue;
        }

//                System.out.println("\nnewly created plans");
        scheduleToCandidateContainers(nextOpID, plan, cTypes, allCandidates);//allCanditates is an out param

      }
      plans.clear();

      plans = new SolutionSpace();
      plans.addAll(allCandidates.results);
      plans.computeSkyline(pruneEnabled, pruneSkylineSize, false, PruneMethod, multi, true,
          0, money_constraint, time_constraint);
      if (plans.isEmpty()) {
        return plans;
      }

      findNextReadyOps(readyOps, opsAssignedSet, nextOpID);

    }
//        System.out.println("createass end");

    return plans;
  }

  private void scheduleToCandidateContainers(Long opId, Plan plan, ArrayList<ContainerType> contTypes, SolutionSpace planEstimations) {
    //assume that not empty containers exist

    for (Long contId : plan.cluster.containers.keySet()) { //add to every existing container
      Plan newPlan = new Plan(plan);
      newPlan.assignOperator(opId, contId, backfilling);
      planEstimations.add(newPlan);
    }
    if (plan.cluster.contUsed.size() < maxContainers) {  //add a nwe container of contType and assign the op to that
      for (ContainerType contType : contTypes) {//uncomment to add every ctype
        Plan newPlan = new Plan(plan);
        Long newContId = newPlan.cluster.addContainer(contType);
        newPlan.assignOperator(opId, newContId, backfilling);
        planEstimations.add(newPlan);

      }
    }

  }

  public Plan onlyOneContainer() {
    ContainerType contType = ContainerType.getSmallest();//maybe check for every container later
    Plan plan = new Plan(graph, cluster);

    plan.cluster.addContainer(contType.getSmallest());
    plan.vmUpgrading = "increasing";

    cluster.addContainer(contType);

    for (Operator op : graph.getOperators()) {
      plan.assignOperator(op.getId(), plan.cluster.getContainer(0L).id, backfilling);
    }
    //  plan.printAssignments();
    return plan;
  }

  public long nextOperator(HashSet<Long> readyOps) {

    long minRankOpID = 0;
    Integer minRank = Integer.MAX_VALUE;
    for (Long opId : readyOps) {
      Integer opRank = opsSorted.indexOf(opId);
      if (opRank < minRank) {
        minRankOpID = opId;
        minRank = opRank;
      }
    }

    return minRankOpID;
  }

  private void computeRankings() {
    final HashMap<Long, Double> b_rank = new HashMap<>(); //opidTobRank
    final HashMap<Long, Double> t_rank = new HashMap<>();
    final HashMap<Long, Double> w_mean = new HashMap<>();
    final HashMap<Long, Double> sum_rank = new HashMap<>();
    // final HashMap<Long, Double> slacktime = new HashMap<>();
    final LinkedList<Long> opsSumRankSorted = new LinkedList<>();
    final LinkedList<Long> opsBySlack = new LinkedList<>();
///   private HashMap<Long, Double> opSlack = new HashMap<>();

    final TopologicalSorting topOrder = new TopologicalSorting(graph);

    HashMap<Integer, Integer> opLevelperLevel = new HashMap<>();
    HashMap<Integer, ArrayList<Long>> opLevelList = new HashMap<>();


    //TODO:initalize b_rank and t_rank!!

    int numLevels = 0;
    for (int i = 0; i < 20; ++i) {
      opLevelList.put(i, new ArrayList<Long>());
    }

    for (Long opId : topOrder.iterator()) {
      int level = 0;
      for (Edge parentEdge : graph.getParents(opId)) {

        Integer plevel = opLevel.get(parentEdge.from);
        level = Math.max(plevel + 1, level);
      }
      opLevel.put(opId, level);
      numLevels = Math.max(level + 1, numLevels);
      if (opLevelperLevel.containsKey(level))
        opLevelperLevel.put(level, opLevelperLevel.get(level) + 1);
      else
        opLevelperLevel.put(level, 1);

      opLevelList.get(level).add(opId);

      //  System.out.println("op "+ opId + " level " +level);

    }


    //Double crPathLength=0.0;
    for (Long opId : topOrder.iteratorReverse()) {
      double maxRankChild = 0.0;
      for (Edge childEdge : graph.getChildren(opId)) {
        double comCostChild = 0.0;
        for (Edge parentofChildEdge : graph.getParents(childEdge.to)) {
          if (parentofChildEdge.from.equals(opId)) {// if((long)parentofChildEdge.from==(long)opId) {//
            comCostChild = Math.ceil(parentofChildEdge.data.getSizeB() / RuntimeConstants.network_speed_B_MS);
          }
        }
        //assumptions for output data and communication cost
        maxRankChild = Math.max(maxRankChild, comCostChild + b_rank.get(childEdge.to));
      }

      double wcur = 0.0;
      for (ContainerType contType : ContainerType.values())
        wcur += graph.getOperator(opId).getRunTime_MS() / contType.container_CPU; //TODO ji check if S or MS
      int types = ContainerType.values().length;
      double w = wcur / (double) types;//average execution cost for operator op
      b_rank.put(opId, (w + maxRankChild));//b_rank.put(opId, (w+maxRankChild));
      w_mean.put(opId, w);

    }

    for (Long opId : topOrder.iterator()) {
      double maxRankParent = 0.0;
      for (Edge inLink : graph.getParents(opId)) {
//                Operator opParent=graph.getOperator(inLink.from.getopID());
        double comCostParent = Math.ceil(inLink.data.getSizeB() / RuntimeConstants.network_speed_B_MS);
        maxRankParent = Math.max(maxRankParent, comCostParent + t_rank.get(inLink.from) + w_mean.get(inLink.from));
      }

      double wcur = 0.0;
      for (ContainerType contType : ContainerType.values())
        wcur += graph.getOperator(opId).getRunTime_MS() / contType.container_CPU;
      int types = ContainerType.values().length;
      double w = wcur / (double) types;//average execution cost for operator op
      t_rank.put(opId, (maxRankParent));
      Double opRank = b_rank.get(opId) + t_rank.get(opId);// -w;
      sum_rank.put(opId, opRank);
      //  crPathLength =Math.max(crPathLength, opRank);
    }

    for (Long op : topOrder.iterator()) {
      opsSumRankSorted.add(op);
      opsBySlack.add(op);
      Double opRank = sum_rank.get(op);
      //  double opSlacktime = crPathLength - opRank;
      //  slacktime.put(op, opSlacktime);
    }

    final HashMap<Long, Double> rankU = new HashMap<>();

    for (Long opId : topOrder.iteratorReverse()) {

      double maxRankChild = 0.0;
      for (Edge outLink : graph.getChildren(opId)) {
        double comCostChild = Math.ceil(outLink.data.getSizeB() / RuntimeConstants.network_speed_B_MS);
        //assumptions for output data and communication cost
        maxRankChild = Math.max(maxRankChild, comCostChild + rankU.get(outLink.to));
      }

      double wcur = 0.0;
      for (ContainerType contType : ContainerType.values()) {
        long mst = graph.getOperator(opId).getRunTime_MS();
        double cput = contType.container_CPU;
        wcur += graph.getOperator(opId).getRunTime_MS() / contType.container_CPU;
      }
      int types = ContainerType.values().length;
      double w = wcur / (double) types;//average execution cost for operator op
      rankU.put(opId, (w + maxRankChild));

    }

    for (Long op : topOrder.iterator()) {
      //     System.out.println(op + " sumrank " +  sum_rank.get(op));
      opsSorted.add(op);
    }


    Comparator<Long> sumrankComparator = new Comparator<Long>() {//sumrank for tasks of a single dag (superdag based) instead the opposite order for crpathlength-sumrank
      @Override
      public int compare(Long op1, Long op2) {
        double r1 = sum_rank.get(op1);
        double r2 = sum_rank.get(op2);
        if (r1 > r2)//TODO: add precision error
          return -1;
        else if (r1 < r2)
          return 1;
        else
          return 0;
      }
    };
    Collections.sort(opsSorted, sumrankComparator);

    Comparator<Long> levelRankComparator = new Comparator<Long>() {
      @Override
      public int compare(Long op1, Long op2) {
        double r1 = opLevel.get(op1);
        double r2 = opLevel.get(op2);
        if (r1 < r2)//TODO: add precision error
          return -1;
        else if (r1 > r2)
          return 1;
        else
          return 0;
      }
    };
    Collections.sort(opsSorted, levelRankComparator);  //should it be the same as 297????


    Comparator<Long> dagIdComparator = new Comparator<Long>() {
      @Override
      public int compare(Long op1, Long op2) {
        Long d1 = graph.getOperator(op1).dagID;
        Long d2 = graph.getOperator(op2).dagID;
        if (d1 < d2)
          return -1;
        else if (d1 > d2)
          return 1;
        else
          return 0;
      }
    };

    //default: commonentry which has by slack and level
    if (rankingMethod.equals("perDag"))
      Collections.sort(opsSorted, dagIdComparator);


    if (rankingMethod.equals("dagMerge")) {

      Comparator<Long> subdagComparator = new Comparator<Long>() {
        @Override
        public int compare(Long op1, Long op2) {
          Integer r1 = opsSorted.indexOf(op1);
          Integer r2 = opsSorted.indexOf(op2);
          if (r1 < r2)
            return -1;
          else if (r1 > r2)
            return 1;
          else
            return 0;
        }
      };

      LinkedList<Long> opsToSchedule = new LinkedList<>();
      HashMap<Long, LinkedList<Long>> subdagOpsList = new HashMap<>();
      HashMap<Long, Long> subdagNext = new HashMap<>();
      HashMap<Long, ListIterator<Long>> iteratorPerSubdag = new HashMap<>();

      for (DAG subdag : graph.superDAG.subDAGList.values()) {
        LinkedList<Long> opsSubdag = new LinkedList();
        //  System.out.println("subdagid " + subdag.dagId);
        Long subdagId = subdag.dagId;
        for (Operator op : subdag.getOperators())
          opsSubdag.add(graph.superDAG.subdagToDagOpIds.get(subdag.dagId).get(op.getId()));//op.getId());
        Collections.sort(opsSubdag, subdagComparator);
        subdagOpsList.put(subdagId, opsSubdag);
        iteratorPerSubdag.put(subdagId, subdagOpsList.get(subdagId).listIterator());
        Long opc = iteratorPerSubdag.get(subdagId).next();
        subdagNext.put(subdagId, opc);
      }

      while (opsToSchedule.size() < graph.getOperators().size()) {
        double maxSumrank = 0.0;
        double minSlack = Double.MAX_VALUE;
        double maxPriority = 0.0;
        long nextToAdd = -1L;
        for (long opnext : subdagNext.values()) {

          double crPathLength = graph.superDAG.getSubDAG(graph.getOperator(opnext).dagID).computeCrPathLength(ContainerType.values());

          HashMap<Long, Double> pathToExit = graph.superDAG.getSubDAG(graph.getOperator(opnext).dagID).computePathToExit(ContainerType.values());

          double tasksUnScheduledPerc = ((double) graph.superDAG.getSubDAG(graph.getOperator(opnext).dagID).getOperators().size() - iteratorPerSubdag.get(graph.getOperator(opnext).dagID).previousIndex()) / (double) graph.superDAG.getSubDAG(graph.getOperator(opnext).dagID).getOperators().size();


          double br = pathToExit.get(graph.superDAG.dagToSubdagOpIds.get(opnext));
          double c = (br / crPathLength) * tasksUnScheduledPerc;

          if (c >= maxPriority) {
            //if equal select hte one with the smallest level
            nextToAdd = opnext;
            maxPriority = c;

          }


        }

        opsToSchedule.addLast(nextToAdd);

//            System.out.println(opsAdded+". adds " + opsToSchedule.getLast()+ " " + nextToAdd + " of ");

        long dagToUse = graph.getOperator(nextToAdd).dagID;//or directly from row structure

//            System.out.println(opsAdded+". adds " + opsToSchedule.getLast()+ " " + nextToAdd + " of "+ dagToUse);

        if (!iteratorPerSubdag.get(dagToUse).hasNext())
          subdagNext.remove(dagToUse);
        else {
          subdagNext.put(dagToUse, iteratorPerSubdag.get(dagToUse).next());
          //int indexNext = subdagOpsList.get(dagToUse).indexOf(nextToAdd);
        }

      }
      opsSorted.clear();
      opsSorted.addAll(opsToSchedule);


    }

  }


  public HashMap<Long, Long> computeLST(Plan plan) {

    HashMap<Long, Long> opLST = new HashMap<>();

    for (Long opId : opsSortedReversed()) {
      Long lst = Long.MAX_VALUE;


      //also checks succ at container
      long succStartTime = Long.MAX_VALUE;
      long templst = Long.MAX_VALUE;
      Long succId = null;
      Long contId = plan.assignments.get(opId);
//            plan.printInfo();
      for (Long nextOpId : plan.contAssignments.get(contId)) {
        if (plan.opIdtoStartEndProcessing_MS.get(nextOpId).a < succStartTime && plan.opIdtoStartEndProcessing_MS.get(nextOpId).a > plan.opIdtoStartEndProcessing_MS.get(opId).a) {
          succStartTime = plan.opIdtoStartEndProcessing_MS.get(nextOpId).a;

          templst = succStartTime - (plan.calculateDelayDistributedStorage(opId, succId));//plan.opIdToBeforeDTDuration_MS.get(succId);
          succId = nextOpId;
        }
      }

      if (succId != null)
        lst = Math.min(templst - plan.calculateDelayDistributedStorage(opId, succId) - plan.opIdToProcessingTime_MS.get(opId), lst);//lst = Math.min(lst, templst - plan.opIdToBeforeDTDuration_MS.get(succId));


      if (graph.getChildren(opId).isEmpty()) { //if exit node
//                System.out.println(opId + " runtime " + plan.opIdToProcessingTime_MS.get(opId) + " dt from " + opId + " to " + " dt after op: " + plan.opIdToAfterDTDuration_MS.get(opId));

        lst = plan.stats.runtime_MS - plan.opIdToProcessingTime_MS.get(opId);
      } else {
        for (Edge outEdge : graph.getChildren(opId)) {
          succId = outEdge.to;

          succStartTime = plan.opIdtoStartEndProcessing_MS.get(succId).a;

          templst = succStartTime - (plan.calculateDelayDistributedStorage(opId, succId));//plan.opIdToBeforeDTDuration_MS.get(succId);

//TODO: add somewhere +1 for data transfer? It starts at the next interval every time...
          lst = Math.min(templst - plan.calculateDelayDistributedStorage(opId, succId) - plan.opIdToProcessingTime_MS.get(opId), lst);
        }
      }

      opLST.put(opId, lst);
    }

    return opLST;
  }


  public HashMap<Long, Long> computeEST(Plan plan) {//TODO: when predID and opID at same container do not include dataTransfer time from predID to opID opIdToBeforeDTDuration_MS.get(predId)?
    HashMap<Long, Long> opEST = new HashMap<>();

    for (Long opId : opsSorted) {
      Long est = Long.MIN_VALUE;
      if (graph.getParents(opId).isEmpty()) {
        est = 0L;
      } else {
        for (Edge inEdge : graph.getParents(opId)) {
          Long predId = inEdge.from;

          Long predEndTime = plan.opIdtoStartEndProcessing_MS.get(predId).b +
              //opIdToAfterDTDuration_MS.get(predId) +//This dt is computed only for parents not vm pred even if it was to be included
              2 * (plan.calculateDelayDistributedStorage(predId, opId, plan.assignments.get(opId)));//plan.opIdToBeforeDTDuration_MS.get(opId);//TODO: do we need any +/-1?


//                    System.out.println(opId + " runtime " + plan.opIdToProcessingTime_MS.get(opId) + " dt from " + opId + " to " + predId + " dt after pred: " + plan.opIdToAfterDTDuration_MS.get(predId) + " finishing at " + predEndTime);

          est = Math.max(predEndTime, est);
        }
      }
      opEST.put(opId, est);
//            System.out.println(opId + " est " + est);
    }
    return opEST;
  }

  //output opSlack,opSortedBySlack
  public HashMap<Long, Long> computeSlackOps(Plan plan, final HashMap<Long, Long> opSlack, ArrayList<Long> opSortedBySlack) {
    HashMap<Long, Long> opLST = computeLST(plan);
    HashMap<Long, Long> opEST = computeEST(plan);

    for (Long opId : graph.operators.keySet()) {
      Long LST = opLST.get(opId);
      Long EST = opEST.get(opId);
      opSlack.put(opId, LST - EST);
      opSortedBySlack.add(opId);
      //  System.out.println(opId + " slack " + (LST-EST));
    }

    Collections.sort(opSortedBySlack, new Comparator<Long>() {
      @Override
      public int compare(Long o1, Long o2) {
        return (int) (opSlack.get(o1) - opSlack.get(o2));
      }
    });
    return opEST;
  }


  public Iterable<Long> opsSortedReversed() {
    return new Iterable<Long>() {
      @Override
      public Iterator<Long> iterator() {
        return opsSorted.descendingIterator();
      }
    };
  }


}