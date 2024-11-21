package com.cwa.solaligue.app.jsonoptiqueparse;

import com.cwa.solaligue.app.graph.DAG;
import com.cwa.solaligue.app.graph.Data;
import com.cwa.solaligue.app.graph.Edge;
import com.cwa.solaligue.app.graph.ResourcesRequirements;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;


public class JsonOptiqueParser {
  long sumdata = 0;
  private Long mult_data = 50L;
  private Long mult_time = 10L;

  public JsonOptiqueParser(long mulTime, long mulData) {
    mult_data = mulData;
    mult_time = mulTime;
  }

  public DAG parse(String filepath) {
    Gson gson = new Gson();

    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(filepath));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    JsonPlan jp = gson.fromJson(br, JsonPlan.class);


    DAG graph = new DAG();

    HashMap<String, Long> opnametoId = new HashMap<>();
    HashMap<Long, Long> opIdtoDisk = new HashMap<>();


    for (Operator opjp : jp.getOperators()) {

      if (opjp.getResources().getTimeMS() == 0) {
        opjp.getResources().setTimeMS(10);
      }
      ResourcesRequirements
          re = new ResourcesRequirements(opjp.getResources().getTimeMS() * mult_time, -1);

      com.cwa.solaligue.app.graph.Operator op = new com.cwa.solaligue.app.graph.Operator(opjp.getOperatorName(), re);

      graph.addOperator(op);

      opnametoId.put(opjp.getOperatorName(), op.getId());

      opIdtoDisk.put(op.getId(), Double.valueOf(opjp.getResources().getDiskMB() * 1000 * 1000).longValue());

    }

    for (OpLink opl : jp.getOpLinks()) {

      Long from = opnametoId.get(opl.getFrom());
      Long to = opnametoId.get(opl.getTo());

      Data d = new Data("emptyName", opIdtoDisk.get(from) * mult_data);
      sumdata += opIdtoDisk.get(from) * mult_data;
      Edge e = new Edge(from, to, d);

      graph.addEdge(e);

    }

    System.out.println("ops#  " + graph.getOperators().size());
    System.out.println("links#  " + jp.getOpLinks().size());
    graph.sumdata_B = sumdata;
    return graph;
  }
}
