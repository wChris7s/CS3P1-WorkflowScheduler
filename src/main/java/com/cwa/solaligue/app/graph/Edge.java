package com.cwa.solaligue.app.graph;

public class Edge {
  public Long from;
  public Long to;
  public Data data;

  public Edge(long fr, long t, Data f) {
    from = fr;
    to = t;
    data = f;
  }
}
