package com.cwa.solaligue.app.scheduler;

import com.cwa.solaligue.app.graph.DAG;
import com.cwa.solaligue.app.graph.Edge;

import java.util.HashMap;


public class Statistics {


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
            money+=localQuanta*c.contType.container_price;

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

        AvgUtil = (double) AvgUtil / plan.cluster.contUsed.size();

        contUtilization = AvgUtil;





        //  for the case of ensemble
        if(plan.graph.superDAG.merged) {//add a constraint to compute them only when all dags have been scheduled. not for partial plans

            for(Long opId: plan.assignments.keySet()){//   for(Long opId : plan.graph.operators.keySet()){//for( Long contId: p0.contAssignments.keySet()) {

                Long dId = plan.graph.operators.get(opId).dagID;

                Long minStartTime = Long.MAX_VALUE;
                Long maxEndTime = Long.MIN_VALUE;

                if(subdagStartTime.isEmpty() || !subdagStartTime.containsKey(dId)) {
                    minStartTime = plan.opIdtoStartEndProcessing_MS.get(opId).a;//startTime
                    maxEndTime = plan.opIdtoStartEndProcessing_MS.get(opId).b;//startTime

                    subdagStartTime.put(dId, minStartTime);
                    subdagFinishTime.put(dId, maxEndTime);


                }
                else {
                    Long ts = subdagStartTime.get(dId);
                    Long te = subdagFinishTime.get(dId);
                    minStartTime = Math.min(ts, plan.opIdtoStartEndProcessing_MS.get(opId).a);//startTime
                    maxEndTime = Math.max(te, plan.opIdtoStartEndProcessing_MS.get(opId).b);//startTime

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
                subdagResponseTime.put(dgId, subdagFinishTime.get(dgId));//for now TODO: change later by adding -submitTime
                subdagMaxResponseTime = Math.max(subdagMaxResponseTime, (subdagFinishTime.get(dgId) - 0)/plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType}));
                subdagMinResponseTime = Math.min(subdagMinResponseTime, (subdagFinishTime.get(dgId) - 0)/plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType}));


                if(Math.abs(subdagFinishTime.get(dgId))>1e-12) {
                    meanSlowdown += plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType}) / (double) (subdagFinishTime.get(dgId) - 0);
                    subdagSlowdown.put(dgId, plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType})/(double) subdagFinishTime.get(dgId));//for now TODO: change later by adding -submitTime
                }
            }

            if(subdagFinishTime.size()>0) {
                subdagMeanMakespan = meanMakespan / (double) subdagFinishTime.size();
                subdagMeanResponseTime = meanResponseTime / (double) subdagFinishTime.size();
                subdagMeanSlowdown = meanSlowdown / (double) subdagFinishTime.size();
            }

            for(Long dgId: subdagResponseTime.keySet()) {
                    partialUnfairness += Math.abs(subdagResponseTime.get(dgId) / computePartialCP(plan.graph.superDAG.getSubDAG(dgId)) - subdagMeanResponseTime);
                           }
            for(Long dgId: subdagResponseTime.keySet()) {
                unfairness += Math.abs((double)subdagResponseTime.get(dgId)/plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType}) -subdagMeanResponseTime);//+= Math.abs(subdagResponseTime.get(dgId) / plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new containerType[]{plan.cluster.containersList.get(0).contType}) - subdagMeanResponseTime);
             }

                        for(Long dgId: subdagResponseTime.keySet()) {
                if(Math.abs(subdagResponseTime.get(dgId))>1e-12)
                    unfairnessNorm += Math.abs(plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new ContainerType[]{plan.cluster.containersList.get(0).contType})/(double)subdagResponseTime.get(dgId)-subdagMeanSlowdown);//+= Math.abs(subdagResponseTime.get(dgId) / plan.graph.superDAG.getSubDAG(dgId).computeCrPathLength(new containerType[]{plan.cluster.containersList.get(0).contType}) - subdagMeanResponseTime);
            }

            Double sumCostSubdag =0.0;

            //cost fragmentation per subdag
            for(Container c:plan.cluster.containersList){

                HashMap <Long, Long> timeUsedPerDag = new HashMap<>();

                for(Slot s: c.opsschedule) {
                    Long dId = plan.graph.operators.get(s.opId).dagID;
                    Long tused= plan.opIdToBeforeDTDuration_MS.get(s.opId) + plan.opIdtoStartEndProcessing_MS.get(s.opId).b + plan.opIdToAfterDTDuration_MS.get(s.opId);

                    if(timeUsedPerDag.containsValue(dId))
                    tused += timeUsedPerDag.get(dId);
                    timeUsedPerDag.put(dId, tused);
                }

                int contQuanta = (int) Math.ceil((double)(c.UsedUpTo_MS-c.startofUse_MS)/RuntimeConstants.quantum_MS);
                double contCost =contQuanta*c.contType.container_price;


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

            subdagMeanMoneyFragment = sumCostSubdag/(double) subdagMoneyFragment.size();



        }

    }


    public void printStats() {
        System.out.println("Time_MS: "+runtime_MS+" Money: "+money+" Quanta: "+quanta );
    }

    public double computePartialCP(DAG graph)
    {

        Double maxPath = 0.0;

        HashMap<Long, Double> path = new HashMap<>();
        HashMap<Long, Double> w_mean = new HashMap<>();

        TopologicalSorting topOrder = new TopologicalSorting(graph);
        for (Long opId : topOrder.iterator()) {
            double maxRankParent=0.0;
            for (Edge inLink: graph.getParents(opId)) {
                double comCostParent = Math.ceil(inLink.data.getSizeB() / RuntimeConstants.network_speed_B_MS);
                maxRankParent = Math.max(maxRankParent, comCostParent+path.get(inLink.from)+w_mean.get(inLink.from));
            }
            double wcur=0.0;
            for(ContainerType contType: ContainerType.values())
                wcur+=graph.getOperator(opId).getRunTime_MS()/contType.container_CPU;
            int types= ContainerType.values().length;
            double w=wcur/(double)types;//average execution cost for operator op
            w_mean.put(opId, w);

            path.put(opId, maxRankParent);
            maxPath =Double.max(maxPath, path.get(opId)+w_mean.get(opId));
        }

        return maxPath;
    }


    public Statistics(Statistics s){
        this.runtime_MS = s.runtime_MS;
        this.quanta = s.quanta;
        this.money = s.money;
        this.containersUsed = s.containersUsed;
        this.contUtilization = s.contUtilization;

        this.meanContainersUsed = s.meanContainersUsed;
        this.partialUnfairness = s.partialUnfairness;
        this.unfairness = s.unfairness;//TODO is it required??
        this.unfairnessNorm=s.unfairnessNorm;



        this.subdagMeanMakespan=s.subdagMeanMakespan;
        this.subdagMaxMakespan=s.subdagMaxMakespan;
        this.subdagMinMakespan=s.subdagMinMakespan;
        this.subdagMeanMoneyFragment=s.subdagMeanMoneyFragment;
        this.subdagMeanResponseTime=s.subdagMeanResponseTime;
        this.subdagMaxResponseTime=s.subdagMaxResponseTime;
        this.subdagMinResponseTime=s.subdagMinResponseTime;
        this.subdagMeanSlowdown=s.subdagMeanSlowdown;



        subdagMoneyFragment = new HashMap<>();//dagId, time
        this.subdagMoneyFragment.putAll(s.subdagMoneyFragment);

        subdagStartTime = new HashMap<>();//dagId, time
        this.subdagStartTime.putAll(s.subdagStartTime);

        subdagFinishTime = new HashMap<>();//dagId, time
        this.subdagFinishTime.putAll(s.subdagFinishTime);

        subdagMakespan = new HashMap<>();//dagId, time
        this.subdagMakespan.putAll(s.subdagMakespan);

        subdagResponseTime = new HashMap<>();//dagId, time
        this.subdagResponseTime.putAll(s.subdagResponseTime);

        subdagSlowdown = new HashMap<>();//dagId, time
        this.subdagSlowdown.putAll(s.subdagSlowdown);

        subdagPartialCP = new HashMap<>();//dagId, time
        this.subdagPartialCP.putAll(s.subdagPartialCP);



    }

}
