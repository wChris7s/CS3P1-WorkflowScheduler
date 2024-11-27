package com.cwa.solaligue.app.scheduler;

import com.cwa.solaligue.app.graph.DAG;
import com.cwa.solaligue.app.graph.Edge;
import java.util.HashMap;
//xio
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class Statistics {
    //xio
    public Double inequityStdDev = 0.0;
    public HashMap<Long, Double> subdagOverspending = new HashMap<>();
    public HashMap<Long, Double> subdagLossRate = new HashMap<>();


    public long runtime_MS;
    public int quanta;
    public double money;//assumption: a container is active for consecutive quanta. if not needed for a time quantum, it is switched off. then if it is required for a new quantum, a new container is switched on.
    // in that way a VM is leased only for the quanta used and elastic provisioning is dynamic throughout
    public long containersUsed;
    public double contUtilization;
    public double partialUnfairness;

    public int meanContainersUsed;

    public HashMap  <Long, Long> subdagStartTime = new HashMap<>();//dagId, time
    public HashMap  <Long, Long> subdagFinishTime = new HashMap<>();//dagId, time
    public HashMap  <Long, Long> subdagMakespan = new HashMap<>();//dagId, time
    public Double subdagMeanMakespan=0.0;//dagId, time
    public Double unfairness;//dagId, time
    public double unfairnessNorm=0.0;
    public Double subdagMaxMakespan=0.0;//dagId, time
    public Double subdagMinMakespan = Double.MAX_VALUE;//dagId, time
    public Double subdagMeanMoneyFragment;//dagId, time
    public HashMap <Long, Double> subdagMoneyFragment = new HashMap<>();//dagId, time
    public HashMap  <Long, Long> subdagResponseTime = new HashMap<>();//dagId, time
    public Double subdagMeanResponseTime=0.0;//dagId, time
    public Double subdagMeanSlowdown=0.0;//dagId, time
    public HashMap  <Long, Double> subdagSlowdown = new HashMap<>();//dagId, time
    public Double subdagMaxResponseTime=0.0;//dagId, time
    public Double subdagMinResponseTime = Double.MAX_VALUE;//dagId, time
    public HashMap <Long, Double> subdagPartialCP = new HashMap<>();//dagId, time


    public Statistics(Plan plan){
        double totalLossRate = 0.0;
        List<Double> lossRates = new ArrayList<>();

        meanContainersUsed=0;
        runtime_MS = 0;
        quanta = 0;
        money = 0;
        contUtilization=0.0;
        partialUnfairness = 0.0;
        unfairness=0.0;
        unfairnessNorm=0.0;


        for(Container c:plan.cluster.containersList){
            if(!plan.cluster.contUsed.contains(c.id)){
                if(c.startofUse_MS>-1){
                    try {
                        throw new Exception("BUGSSS");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                continue;}

            //the last slot of a container may affect previously computed response time and partial crpath (maxtrank)
            long opCurId= c.opsschedule.get(c.opsschedule.size()-1).opId;//last slot in container

            Long dId = plan.graph.operators.get(opCurId).dagID;

            Double subdagMaxPath=0.0;
            if(subdagPartialCP.get(dId)!=null)
                subdagMaxPath = subdagPartialCP.get(dId);
            Double path=0.0;//trank of operator = ;//
            subdagMaxPath = Math.max(subdagMaxPath, path);
            subdagPartialCP.put(dId, subdagMaxPath);
            runtime_MS = Math.max(c.UsedUpTo_MS,runtime_MS);
            int localQuanta = (int) Math.ceil((double)(c.UsedUpTo_MS-c.startofUse_MS)/RuntimeConstants.quantum_MS);
            if(localQuanta == 0 ){
                System.out.println("NO QUANTA IS USED IN CONTAINER");
            }
            quanta+=localQuanta;
            money+=localQuanta*c.contType.getContainerPrice();

        }
        containersUsed = plan.cluster.contUsed.size();


       int makespanQuanta = (int)Math.ceil(runtime_MS/(double)RuntimeConstants.quantum_MS);
        meanContainersUsed = (int) Math.ceil(quanta/(double)(makespanQuanta));

        double minUtil = 10.0;
        double maxUtil = -5.0;
        double AvgUtil= 0;
        double AvgQUtil = 0;
        double MinQUtil = 10.0;
        double MaxQUtil = -5.0;

        for(Container c:plan.cluster.containersList){
            long dtTime = 0;
            long proTime = 0;
            long contTime = 0;
            long ftime = 0;
            double Util = 0.0;
            double Util2 = 0.0;
            double QUtil = 0.0;
            double Util3;



            for(Long opId: plan.assignments.keySet()){
                if(plan.assignments.get(opId) == c.id){
                  //  System.out.println(plan.opIdtoStartEndProcessing_MS.get(opId).a);
                    dtTime += plan.opIdToBeforeDTDuration_MS.get(opId) + plan.opIdToAfterDTDuration_MS.get(opId);
                    proTime += plan.opIdToProcessingTime_MS.get(opId);
                }
            }
            contTime = c.UsedUpTo_MS - c.startofUse_MS;
            Util = (double)(dtTime+proTime) / (double)contTime;

            int quantaUsed = (int) Math.ceil((double)(c.UsedUpTo_MS-c.startofUse_MS)/RuntimeConstants.quantum_MS);
            QUtil = (double)(dtTime+proTime) / (double)(quantaUsed*RuntimeConstants.quantum_MS);


            for(Slot s: c.freeSlots){
                ftime += s.end_MS - s.start_MS;

            }

            Util2 = (double)(contTime - ftime) / (double)contTime;

            long o=0;

            for(Slot s: c.opsschedule){
                o += s.end_MS - s.start_MS;

            }

            Util3 = (double)(o) / (double)contTime;


            if(Math.abs(Util-Util2)>0.2 || Math.abs(Util-Util3)>0.2 || Math.abs(Util3-Util2)>0.2 ){
                try {
                    throw new Exception("Problem!!!");
                } catch (Exception e) {
                    // e.printStackTrace();
                }
            }



            minUtil = Math.min(minUtil,Util);
            maxUtil = Math.max(maxUtil,Util);

            MinQUtil = Math.min(MinQUtil,QUtil);
            MaxQUtil = Math.max(MaxQUtil,QUtil);

            AvgQUtil+=QUtil;
            AvgUtil+=Util;
        }

        AvgUtil = AvgUtil / plan.cluster.contUsed.size();
        contUtilization = AvgUtil;

        if(Boolean.TRUE.equals(plan.graph.superDAG.merged)) {

            for(Long opId: plan.assignments.keySet()){
                Long dId = plan.graph.operators.get(opId).dagID;
                Long minStartTime = Long.MAX_VALUE;
                Long maxEndTime = Long.MIN_VALUE;

                if(subdagStartTime.isEmpty() || !subdagStartTime.containsKey(dId)) {
                    minStartTime = plan.opIdtoStartEndProcessing_MS.get(opId).a;
                    maxEndTime = plan.opIdtoStartEndProcessing_MS.get(opId).b;

                    subdagStartTime.put(dId, minStartTime);
                    subdagFinishTime.put(dId, maxEndTime);
                }
                else {
                    Long ts = subdagStartTime.get(dId);
                    Long te = subdagFinishTime.get(dId);
                    minStartTime = Math.min(ts, plan.opIdtoStartEndProcessing_MS.get(opId).a);
                    maxEndTime = Math.max(te, plan.opIdtoStartEndProcessing_MS.get(opId).b);

                    subdagStartTime.put(dId, minStartTime);
                    subdagFinishTime.put(dId, maxEndTime);
                }
            }


            double meanMakespan = 0.0;
            double meanResponseTime = 0.0;
            double meanSlowdown = 0.0;
            double meanPartialResponseTime = 0.0;

            for(Long dgId: subdagFinishTime.keySet()) {
                subdagMakespan.put(dgId, subdagFinishTime.get(dgId) - subdagStartTime.get(dgId));
                meanMakespan += (subdagFinishTime.get(dgId) - subdagStartTime.get(dgId))/plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType});
                subdagMaxMakespan = Math.max(subdagMaxMakespan, (subdagFinishTime.get(dgId) - subdagStartTime.get(dgId))/plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType}));
                subdagMinMakespan = Math.min(subdagMinMakespan, (subdagFinishTime.get(dgId) - subdagStartTime.get(dgId))/plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType}));

                meanPartialResponseTime+= (subdagFinishTime.get(dgId) - 0)/computePartialCP(plan.graph.superDAG.getSubDAG(dgId));
                meanResponseTime += (subdagFinishTime.get(dgId) - 0)/plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType});
                subdagResponseTime.put(dgId, subdagFinishTime.get(dgId));
                subdagMaxResponseTime = Math.max(subdagMaxResponseTime, (subdagFinishTime.get(dgId) - 0)/plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType}));
                subdagMinResponseTime = Math.min(subdagMinResponseTime, (subdagFinishTime.get(dgId) - 0)/plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType}));


                if(Math.abs(subdagFinishTime.get(dgId))>1e-12) {
                    meanSlowdown += plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType}) / (subdagFinishTime.get(dgId) - 0);
                    subdagSlowdown.put(dgId, plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType})/(double) subdagFinishTime.get(dgId));
                }
            }

            // Features ----------------------------

            for (Long dagId : subdagFinishTime.keySet()) {
                // Slowdown
                double slowdown = plan.graph.superDAG
                    .getSubDAG(dagId)
                    .computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType}) /
                    (double) subdagFinishTime.get(dagId);

                // Overspending
                double exclusiveCost = subdagMakespan.get(dagId) * plan.cluster.containersList.get(0).contType.getContainerPrice();
                double sharedCost = subdagMoneyFragment.getOrDefault(dagId, 0.0);
                double overspending = sharedCost / exclusiveCost;
                subdagOverspending.put(dagId, overspending);

                // Loss Rate
                double lossRate = (slowdown + overspending) / 2;
                subdagLossRate.put(dagId, lossRate);
                lossRates.add(lossRate);

                totalLossRate += lossRate;
            }

            // Loss Rates
            double meanLossRate = totalLossRate / lossRates.size();

            // Inequality as Standard Deviation
            double variance = 0.0;
            for (double lossRate : lossRates) {
                variance += Math.pow(lossRate - meanLossRate, 2);
            }
            inequityStdDev = Math.sqrt(variance / lossRates.size());

            // -------------------------------------

            if(!subdagFinishTime.isEmpty()) {
                subdagMeanMakespan = meanMakespan / subdagFinishTime.size();
                subdagMeanResponseTime = meanResponseTime / subdagFinishTime.size();
                subdagMeanSlowdown = meanSlowdown / subdagFinishTime.size();
            }

            for (Long dgId : subdagResponseTime.keySet()) {
                partialUnfairness += Math.abs(subdagResponseTime.get(dgId) / computePartialCP(plan.graph.superDAG.getSubDAG(dgId)) - subdagMeanResponseTime);
            }

            for (Long dgId : subdagResponseTime.keySet()) {
                unfairness += Math.abs((double) subdagResponseTime.get(dgId) / plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType}) - subdagMeanResponseTime);
            }

            for (Long dgId : subdagResponseTime.keySet()) {
                if (Math.abs(subdagResponseTime.get(dgId)) > 1e-12)
                    unfairnessNorm += Math.abs(plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType}) / (double) subdagResponseTime.get(dgId) - subdagMeanSlowdown);
            }

            Double sumCostSubdag =0.0;
            for(Container c:plan.cluster.containersList){
                HashMap <Long, Long> timeUsedPerDag = new HashMap<>();

                for(Slot s: c.opsschedule) {
                    Long dId = plan.graph.operators.get(s.opId).dagID;
                    Long tused= plan.opIdToBeforeDTDuration_MS.get(s.opId) + plan.opIdtoStartEndProcessing_MS.get(s.opId).b + plan.opIdToAfterDTDuration_MS.get(s.opId);

                    if(timeUsedPerDag.containsValue(dId)) {
                        tused += timeUsedPerDag.get(dId);
                    }
                    timeUsedPerDag.put(dId, tused);
                }

                int contQuanta = (int) Math.ceil((double)(c.UsedUpTo_MS-c.startofUse_MS)/RuntimeConstants.quantum_MS);
                double contCost =contQuanta*c.contType.getContainerPrice();

                for(Long dgId: timeUsedPerDag.keySet()) {
                    double moneyFrag = contCost * timeUsedPerDag.get(dgId)/(contQuanta*RuntimeConstants.quantum_MS);
                    if(subdagMoneyFragment.containsKey(dgId)) {
                        moneyFrag += subdagMoneyFragment.get(dgId);
                    }

                    subdagMoneyFragment.put(dgId, moneyFrag);
                }
            }

            for(Long dgId: subdagMoneyFragment.keySet()) {
                sumCostSubdag += subdagMoneyFragment.get(dgId);
            }
            subdagMeanMoneyFragment = sumCostSubdag/subdagMoneyFragment.size();
        }
    }

    public double computePartialCP(DAG graph) {
        double maxPath = 0.0;
        HashMap<Long, Double> path = new HashMap<>();
        HashMap<Long, Double> wMean = new HashMap<>();

        TopologicalSorting topOrder = new TopologicalSorting(graph);
        for (Long opId : topOrder.iterator()) {

            double maxRankParent = 0.0;
            for (Edge inLink: graph.getParents(opId)) {
                double comCostParent = Math.ceil(inLink.data.getSizeB() / RuntimeConstants.network_speed_B_MS);
                maxRankParent = Math.max(maxRankParent, comCostParent+path.get(inLink.from)+wMean.get(inLink.from));
            }

            double wcur=0.0;
            for(ContainerType contType: ContainerType.values()) {
                wcur += graph.getOperator(opId).getRunTime_MS() / contType.getContainerCPU();
            }

            int types= ContainerType.values().length;
            double w=wcur/types;
            wMean.put(opId, w);

            path.put(opId, maxRankParent);
            maxPath =Double.max(maxPath, path.get(opId)+wMean.get(opId));
        }
        return maxPath;
    }


    public Statistics(Statistics s) {
        this.runtime_MS = s.runtime_MS;
        this.quanta = s.quanta;
        this.money = s.money;
        this.containersUsed = s.containersUsed;
        this.contUtilization = s.contUtilization;
        this.meanContainersUsed = s.meanContainersUsed;
        this.partialUnfairness = s.partialUnfairness;
        this.unfairness = s.unfairness;
        this.unfairnessNorm = s.unfairnessNorm;
        this.subdagMeanMakespan = s.subdagMeanMakespan;
        this.subdagMaxMakespan = s.subdagMaxMakespan;
        this.subdagMinMakespan = s.subdagMinMakespan;
        this.subdagMeanMoneyFragment = s.subdagMeanMoneyFragment;
        this.subdagMeanResponseTime = s.subdagMeanResponseTime;
        this.subdagMaxResponseTime = s.subdagMaxResponseTime;
        this.subdagMinResponseTime = s.subdagMinResponseTime;
        this.subdagMeanSlowdown = s.subdagMeanSlowdown;

        subdagMoneyFragment = new HashMap<>();
        this.subdagMoneyFragment.putAll(s.subdagMoneyFragment);
        subdagStartTime = new HashMap<>();
        this.subdagStartTime.putAll(s.subdagStartTime);
        subdagFinishTime = new HashMap<>();
        this.subdagFinishTime.putAll(s.subdagFinishTime);
        subdagMakespan = new HashMap<>();
        this.subdagMakespan.putAll(s.subdagMakespan);
        subdagResponseTime = new HashMap<>();
        this.subdagResponseTime.putAll(s.subdagResponseTime);
        subdagSlowdown = new HashMap<>();
        this.subdagSlowdown.putAll(s.subdagSlowdown);
        subdagPartialCP = new HashMap<>();
        this.subdagPartialCP.putAll(s.subdagPartialCP);
    }
}
