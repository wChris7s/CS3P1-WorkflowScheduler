package com.cwa.solaligue.app.scheduler;

import com.cwa.solaligue.app.graph.DAG;
import com.cwa.solaligue.app.graph.Edge;
import com.cwa.solaligue.app.graph.Operator;
import com.cwa.solaligue.app.utilities.Pair;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Stack;

@Slf4j
public class Plan  {
    public DAG graph;
    public HashMap<Long, Long> assignments;//opIdToContId;
    public HashMap<Long, ArrayList<Long>> contAssignments; //contId->list<opId>
    public HashMap<Long, ArrayList<Long>> contIdToOpIds;
    public Cluster cluster;
    public Statistics stats;
    Statistics beforeStats;
    String vmUpgrading;
    public ArrayList<Long> opsMigrated = null;

    public HashMap<Long, Pair<Long, Long>> opIdtoStartEndProcessing_MS;

    public HashMap<Long, Long> opIdToContainerRuntime_MS; //runtime for the assigned container;
    public HashMap<Long, Long> opIdToProcessingTime_MS;

    public HashMap<Long, Long> opIdToearliestStartTime_MS; //not sure if we should use it

    public HashMap<Long,Long> opIdToBeforeDTDuration_MS;

    public HashMap<Long,Long> opIdToAfterDTDuration_MS;

    public HashMap<Long, Stack<Long>> contIdToSortedOps = null;


    //copies the cluster
    public Plan(DAG graph, Cluster cluster) {
        this.graph = graph;
        beforeStats = null;
        assignments = new HashMap<>();
        contIdToOpIds = new HashMap<>();
        this.cluster = new Cluster(cluster);
        opIdtoStartEndProcessing_MS = new HashMap<>();
        contAssignments = new HashMap<>();
        opIdtoStartEndProcessing_MS = new HashMap<>();
        stats = new Statistics(this);
        opIdToearliestStartTime_MS = new HashMap<>();
        opIdToContainerRuntime_MS = new HashMap<>();
        opIdToProcessingTime_MS = new HashMap<>();
        opIdToBeforeDTDuration_MS = new HashMap<>();
        opIdToAfterDTDuration_MS = new HashMap<>();

        opsMigrated = new ArrayList<>();

    }



    public Plan(Plan p) {
        this.vmUpgrading = p.vmUpgrading;
        this.graph = p.graph;
        this.beforeStats = p.beforeStats;
        assignments = new HashMap<>();
        for (Long key : p.assignments.keySet()) {
            assignments.put(key, p.assignments.get(key));
        }
        contIdToOpIds = new HashMap<>();
        for (Long key : p.contIdToOpIds.keySet()) {
            ArrayList<Long> list = new ArrayList<>();
            list.addAll(p.contIdToOpIds.get(key));
            contIdToOpIds.put(key, list);
        }
        contAssignments = new HashMap<>();
        for (Long cid : p.contAssignments.keySet()) {
            ArrayList<Long> t = new ArrayList<>();

            t.addAll(p.contAssignments.get(cid));


            contAssignments.put(cid, t);
        }
        opIdtoStartEndProcessing_MS = new HashMap<>();
        for (long oid : p.opIdtoStartEndProcessing_MS.keySet()) {
            opIdtoStartEndProcessing_MS.put(oid,
                new Pair<>(p.opIdtoStartEndProcessing_MS.get(oid).a, p.opIdtoStartEndProcessing_MS.get(oid).b));
        }
        stats = new Statistics(p.stats);

        opIdToearliestStartTime_MS = new HashMap<>();
        for (Long opId : p.opIdToearliestStartTime_MS.keySet()) {
            opIdToearliestStartTime_MS.put(opId, p.opIdToearliestStartTime_MS.get(opId));
        }

        opIdToContainerRuntime_MS = new HashMap<>();
        for(Long opId: p.opIdToContainerRuntime_MS.keySet()){
            opIdToContainerRuntime_MS.put(opId,p.opIdToContainerRuntime_MS.get(opId));
        }

        opIdToProcessingTime_MS = new HashMap<>();
        for(Long opId: p.opIdToProcessingTime_MS.keySet()){
            opIdToProcessingTime_MS.put(opId,p.opIdToProcessingTime_MS.get(opId));
        }

        opIdToBeforeDTDuration_MS = new HashMap<>();
        for(Long opId: p.opIdToBeforeDTDuration_MS.keySet()){
            opIdToBeforeDTDuration_MS.put(opId,p.opIdToBeforeDTDuration_MS.get(opId));
        }

        opIdToAfterDTDuration_MS = new HashMap<>();
        for(Long opId: p.opIdToAfterDTDuration_MS.keySet()){
            opIdToAfterDTDuration_MS.put(opId,p.opIdToAfterDTDuration_MS.get(opId));
        }

        this.cluster = new Cluster(p.cluster);

        opsMigrated = new ArrayList<>();
        for(Long opId: p.opsMigrated){
            opsMigrated.add(opId);
        }

    }

    public void assignOperator(Long opId, Long contId, boolean backfilling) {
        assignments.put(opId, contId);
        if (!contAssignments.containsKey(contId)) {
            contAssignments.put(contId, new ArrayList<Long>());
        }
        contAssignments.get(contId).add(opId);
        beforeStats = new Statistics(stats);

        long startProcessingTime_MS = 0L;
        long endProcessingTime_MS = 0L;

        long beforeDTDuration_MS = 0L;
        long afterDTDuration_MS = 0L;

        long opProcessingDuration_MS = 0L;
        long contOpDuration_MS = 0L;

        long startTimeCont_MS =0L;
        long endTimeCont_MS = 0L; //endTime + max dt duration

        long dependenciesEnd_MS = 0;

        long earliestStartTime_MS = 0L;


        Container cont = cluster.getContainer(contId);
        long contFirstAvailTime_MS = cont.getFirstAvailTime_atEnd_MS();

        ////////////DEPENDENCIES//////////////////
        for (Edge link : graph.getParents(opId)) {
            long fromId = link.from;

            long fromOpEndTimePLUSDTTime =
                opIdtoStartEndProcessing_MS.get(fromId).b + calculateDelayDistributedStorage(fromId,opId,contId);

            dependenciesEnd_MS = Math.max(dependenciesEnd_MS,fromOpEndTimePLUSDTTime);//+1);

        }

        ///////////////BRING DATA TO OPERATOR MACHINE//////////////////
        for (Edge edge : graph.getParents(opId)) {

            long transferTime = calculateDelayDistributedStorage(edge.from,opId,contId);//(long) Math.ceil(edge.data.size_B / RuntimeConstants.distributed_storage_speed_B_MS);

            beforeDTDuration_MS = Math.max(beforeDTDuration_MS, transferTime);
        }


        ///////////////OPERATOR PROCESS TIME///////////////
        Operator op = graph.getOperator(opId);
        opProcessingDuration_MS = (int) Math.ceil(op.getRunTime_MS() / cont.contType.getContainerCPU());

        ////////////////SEND DATA TO DISTRIBUTED STORAGE ////////////////////
        for (Edge edge : graph.getChildren(opId)) {
            int transferTime = (int) Math.ceil(edge.data.getSizeB() / RuntimeConstants.distributed_storage_speed_B_MS);
            afterDTDuration_MS = Math.max(afterDTDuration_MS,transferTime);
        }

        //////////////////INFO///////////////////////
        contOpDuration_MS = beforeDTDuration_MS + opProcessingDuration_MS + afterDTDuration_MS;
        earliestStartTime_MS = dependenciesEnd_MS;
        opIdToearliestStartTime_MS.put(opId, dependenciesEnd_MS);//+1);


        ////////////////BACKFILLING/////////////////////////////////

        boolean backfilled = false;

        if (backfilling) {

            Collections.sort(cont.freeSlots); //sort the free slots from earliest to latest
            Slot toberemoved = null;

            for (int i = 0; i < cont.freeSlots.size() && !backfilled; ++i) {

                Slot fs = cont.freeSlots.get(i);

                if (  fs.end_MS-fs.start_MS >= opProcessingDuration_MS &&
                    earliestStartTime_MS + beforeDTDuration_MS <= fs.end_MS - opProcessingDuration_MS){
                    backfilled = true;

                    long pushForward = 0L;
                    if (earliestStartTime_MS + beforeDTDuration_MS < fs.start_MS) {      //free slot is not available when the op is ready
                        pushForward = fs.start_MS - (earliestStartTime_MS + beforeDTDuration_MS);   //so we push the op start,contStart, end times
                    }

                    startTimeCont_MS = earliestStartTime_MS + pushForward;

                    endTimeCont_MS = startTimeCont_MS + contOpDuration_MS;
                    startProcessingTime_MS = startTimeCont_MS + beforeDTDuration_MS;
                    endProcessingTime_MS = startProcessingTime_MS + opProcessingDuration_MS;


                    if (startProcessingTime_MS > fs.start_MS + 1) {                                             // add possible free Slot at start
                        cont.freeSlots.add(new Slot(fs.start_MS, startProcessingTime_MS - 1));
                    }

                    if (endProcessingTime_MS < fs.end_MS - 1) {                                                     //and at end
                        cont.freeSlots.add(new Slot(endProcessingTime_MS + 1, fs.end_MS));
                    }

                    toberemoved = fs;

                }
                if (toberemoved != null) {
                    cont.freeSlots.remove(toberemoved);
                }
            }
        }

        ///////////////////NO BACKFILLING/////////////////////////

        if (!backfilling || (backfilling && !backfilled)) {

            long pushForward = 0L;
            if (earliestStartTime_MS + beforeDTDuration_MS < contFirstAvailTime_MS) {                   //cont is not available when the op is ready
                pushForward = contFirstAvailTime_MS - (earliestStartTime_MS + beforeDTDuration_MS);  //so we push the op start,contStart, end times

            } else if( earliestStartTime_MS > contFirstAvailTime_MS ){           //if starContTime is after the cont was available
                         // add possible free Slot
                if( cont.opsschedule.size() > 0){
                    cont.freeSlots.add(new Slot(contFirstAvailTime_MS,earliestStartTime_MS+beforeDTDuration_MS));
                }

            }


            startTimeCont_MS = earliestStartTime_MS + pushForward;
            endTimeCont_MS = startTimeCont_MS + contOpDuration_MS;

            startProcessingTime_MS = startTimeCont_MS + beforeDTDuration_MS ;
            endProcessingTime_MS = startProcessingTime_MS + opProcessingDuration_MS ;


        }
        //////////////////////////////////////////////////////////////////


        /////set start and end time for the container
        cont.setStartDT(startTimeCont_MS);
        cont.setUsedUpToDT(endTimeCont_MS);
        cont.setStart(startProcessingTime_MS);
        cont.setUsedUpTo(endProcessingTime_MS);

        ////set start end times for operator////////
        opIdtoStartEndProcessing_MS.put(opId, new Pair<>(startProcessingTime_MS, endProcessingTime_MS)); //processing time ONLY
        opIdToContainerRuntime_MS.put(opId,contOpDuration_MS);
        opIdToProcessingTime_MS.put(opId,opProcessingDuration_MS);
        opIdToBeforeDTDuration_MS.put(opId,beforeDTDuration_MS);
        opIdToAfterDTDuration_MS.put(opId,afterDTDuration_MS);

        ///////set used In cluster
        cluster.contUsed.add(contId);

        //////add scheduled Slot////////
        cont.opsschedule.add(new Slot(opId, startProcessingTime_MS, endProcessingTime_MS)); //add a new scheduled slot for the operator

        //////Update Stats
        stats = new Statistics(this);

    }

    public Long calculateDelayDistributedStorage(Long parentId, Long childId, Long childContId){

        if(!graph.edgesMap.containsKey(parentId))
            return 0L;
        if( !graph.edgesMap.get(parentId).containsKey(childId))
            return 0L;


        if(assignments.get(parentId) == childContId){ //remove to transfer always to distributed storage
            return 0L;
        }
        return (long) Math.ceil(graph.getEdge(parentId,childId).data.getSizeB() / RuntimeConstants.distributed_storage_speed_B_MS);
    }

    public Long calculateDelayDistributedStorage(Long parentId, Long childId){//TODO: if not parent-child set?


        if(!graph.edgesMap.containsKey(parentId))
            return 0L;
        if( !graph.edgesMap.get(parentId).containsKey(childId))
            return 0L;

        if(assignments.get(parentId) == assignments.get(childId) ){ //remove to transfer always to distributed storage
            return 0L;
        }
        return (long) Math.ceil(graph.getEdge(parentId,childId).data.getSizeB() / RuntimeConstants.distributed_storage_speed_B_MS);
    }

    @Override public String toString() {

        StringBuilder i = new StringBuilder();
        StringBuilder sb = new StringBuilder();
        String.format("%10d %06.2f", stats.runtime_MS, stats.money );


        i.append(stats.runtime_MS).append(" ").append(stats.money).append(" ").append("conts ")
            .append(cluster.containersList.size()).append("  ");

        sb.append(String.format("%10d %06.2f", stats.runtime_MS, stats.money));

        double minUtil = 10.0;
        double maxUtil = -5.0;
        double AvgUtil= 0;
        int countfs =0;
        double AvgQUtil = 0;
        double MinQUtil = 10.0;
        double MaxQUtil = -5.0;



        for(Container c:cluster.containersList){
            long dtTime = 0;
            long proTime = 0;
            long contTime = 0;
            long ftime = 0;
            double Util = 0.0;
            double Util2 = 0.0;
            double QUtil = 0.0;
            double Util3;



            for(Long opId: graph.operators.keySet()){
                if(assignments.get(opId) == c.id){
                    dtTime += opIdToBeforeDTDuration_MS.get(opId) + opIdToAfterDTDuration_MS.get(opId);
                    proTime += opIdToProcessingTime_MS.get(opId);
                }
            }
            contTime = c.UsedUpTo_MS - c.startofUse_MS;
            Util = (double)(dtTime+proTime) / (double)contTime;

            int quantaUsed = (int) Math.ceil((double)(c.UsedUpTo_MS-c.startofUse_MS)/RuntimeConstants.quantum_MS);
            QUtil = (double)(dtTime+proTime) / (double)(quantaUsed*RuntimeConstants.quantum_MS);




            for(Slot s: c.freeSlots){
                countfs++;
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


        int a = cluster.containersList.size();
        int b = cluster.contUsed.size();

        AvgUtil = (double) AvgUtil / cluster.contUsed.size();
        AvgQUtil = (double) AvgQUtil / cluster.contUsed.size();




        i.append("\t\t\t");
        i.append(" AvgU: ").append(AvgUtil);
        i.append(" MaxU: ").append(maxUtil);
        i.append(" MinU: ").append(minUtil);
        i.append(" #fs: ").append(countfs);


        sb.append(String.format(" || noQ Q (Avg,Max,Min) (%3.2f, %3.2f, %3.2f) (%3.2f, %3.2f, %3.2f) #fs: %3d ",AvgUtil,maxUtil,minUtil,AvgQUtil,MinQUtil,MaxQUtil,countfs));

        sb.append(String.format(" -- #Conts: %d",cluster.containersList.size()));

        for (ContainerType ct : cluster.countTypes.keySet()) {
            i.append(ct.getName()).append("(").append(cluster.countTypes.get(ct)).append(")")
                .append(" ");

            sb.append(String.format(" %s(%d) ",ct.getName(),cluster.countTypes.get(ct)));
        }

        sb.append(String.format("%n"));


        return sb.toString();
    }

    // En la clase Plan
    public void setReferencePoint(double[] refPoint) {
        this.referencePoint = refPoint;
    }

    // Asegúrate de agregar el campo referencePoint en la clase Plan
    private double[] referencePoint;

    public void setObjectives(double[] objectives) {
        this.objectives = objectives;
    }

    public double[] getObjectives() {
        return this.objectives;
    }

    // Campo adicional
    private double[] objectives;


}
