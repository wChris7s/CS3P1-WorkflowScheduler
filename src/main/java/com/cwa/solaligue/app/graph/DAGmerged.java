package com.cwa.solaligue.app.graph;

import java.util.HashMap;

public class DAGmerged {
  public Boolean merged;
  public HashMap<Long, DAG> subDAGList;
  public HashMap<Long, HashMap<Long, Long>> subdagToDagOpIds;
  public HashMap<Long, Long> dagToSubdagOpIds;

  public DAGmerged() {
    merged = false;
    subDAGList = new HashMap<>();
    subdagToDagOpIds = new HashMap<>();
    dagToSubdagOpIds = new HashMap<>();
  }

  public void addSubDAG(DAG subDAG, HashMap<Long, Long> OldIdToNewId) {
    if (!merged)
      merged = true;
    subDAGList.put(subDAG.dagId, subDAG);
    HashMap<Long, Long> subdagOldIdToNewId = new HashMap<>();

    subdagOldIdToNewId.putAll(OldIdToNewId);
    subdagToDagOpIds.put(subDAG.dagId, subdagOldIdToNewId);
    for (Long opIdSubdag : OldIdToNewId.keySet())
      dagToSubdagOpIds.put(OldIdToNewId.get(opIdSubdag), opIdSubdag);
  }

  public DAG getSubDAG(Long id) {
    return subDAGList.get(id);
  }
}
