
package com.cwa.solaligue.app.scheduler;


import com.cwa.solaligue.app.graph.DAG;
import com.cwa.solaligue.app.graph.Edge;
import com.cwa.solaligue.app.graph.Operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class TopologicalSorting {
  private final List<Long> visited = new ArrayList<>();
  private final LinkedList<Long> topOrder = new LinkedList<>();

  public TopologicalSorting(DAG graph) {
    for (Operator op : graph.getOperators()) {
      if (!visited.contains(op.getId()))
        dfs(graph, op.getId());
    }
    Collections.reverse(topOrder);
    visited.clear();
  }

  private void dfs(DAG graph, Long opId) {
    visited.add(opId);
    for (Edge childEdge : graph.getChildren(opId)) {
      if (!visited.contains(childEdge.to))
        dfs(graph, childEdge.to);
    }
    topOrder.add(opId);
  }

  public Iterable<Long> iterator() {
    return topOrder;
  }

  public Iterable<Long> iteratorReverse() {
    return topOrder::descendingIterator;
  }
}
