package com.cwa.solaligue.app.parser;

import com.cwa.solaligue.app.graph.*;
import lombok.extern.slf4j.Slf4j;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.*;

@Slf4j
public class PegasusDaxParser {

  public final Map<String, List<String>> abstractConcreteOperatorsMap =
      Map.of("mAdd", List.of("mAdd"),
          "mBackground", List.of("mBackground"),
          "mBgModel", List.of("mBgModel"),
          "mConcatFit", List.of("mConcatFit"),
          "mDiffFit", List.of("mDiffFit"),
          "mImgTbl", List.of("mImgTbl"),
          "mJPEG", List.of("mJPEG"),
          "mProjectPP", List.of("mProjectPP"),
          "mShrink", List.of("mShrink"));

  long sumdata;

  double multiply_by_time;
  int multiply_by_data;

  public PegasusDaxParser(double mulTIME, int mulDATA) {
    multiply_by_time = mulTIME;
    multiply_by_data = mulDATA;
    sumdata = 0;

  }

  public DAG parseDax(String url, Long dagId) throws Exception {
    sumdata = 0;
    DAG graph = new DAG(dagId);
    graph.name = url + multiply_by_time + "_" + multiply_by_data;
    ArrayList<Edge> edges = new ArrayList<>();
    HashMap<String, HashMap<String, TempFile>> opin = new HashMap<>();
    HashMap<String, HashMap<String, TempFile>> opout = new HashMap<>();
    HashMap<String, ArrayList<String>> optoop = new HashMap<>();
    LinkedHashMap<String, Long> operatorNameMap = new LinkedHashMap<>();

    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document doc = db.parse(url);
    doc.getDocumentElement().normalize();

    NodeList jobList = doc.getElementsByTagName("job");
    for (int s = 0; s < jobList.getLength(); s++) {

      double runTimeValue = 0;
      int memoryValue = 40;

      Node job = jobList.item(s);
      Element jobElement = (Element) job;

      String name = jobElement.getAttribute("id");
      String application = jobElement.getAttribute("namespace");
      String className = jobElement.getAttribute("name");

      runTimeValue =
          Double.parseDouble(jobElement.getAttribute("runtime")) * multiply_by_time;

      long runtime_MS = (long) (runTimeValue * 1000);

      ResourcesRequirements res = new ResourcesRequirements(runtime_MS, memoryValue);

      Long opid = graph.addOperator(new Operator(name, res));
      graph.getOperator(opid).setDAGId(dagId);

      Operator op = graph.getOperator(opid);
      operatorNameMap.put(name, opid);
      op.className = className;
      op.cpuBoundness = getCpuBoundness(application, className);


      NodeList useElementList = jobElement.getElementsByTagName("uses");
      for (int i = 0; i < useElementList.getLength(); i++) {

        Node use = useElementList.item(i);
        Element useElement = (Element) use;


        long dataSize = (long) Double.parseDouble(useElement.getAttribute("size"));


        long data_B = dataSize * multiply_by_data;

        sumdata += data_B;

        String filename = useElement.getAttribute("file");

        if (useElement.getAttribute("link").equals("output")) {

          if (!opout.containsKey(name)) {
            opout.put(name, new HashMap<String, TempFile>());
          }
          opout.get(name).put(filename, new TempFile(null, name, data_B, filename));
        } else {
          if (!opin.containsKey(name)) {
            opin.put(name, new HashMap<String, TempFile>());
          }
          opin.get(name).put(filename, new TempFile(name, "", data_B, filename));
        }

      }

    }

    NodeList childList = doc.getElementsByTagName("child");
    for (int c = 0; c < childList.getLength(); c++) {
      Node child = childList.item(c);
      Element childElement = (Element) child;

      String to = childElement.getAttribute("ref");
      Long toOpId = operatorNameMap.get(to);
      NodeList parentList = childElement.getElementsByTagName("parent");

      /* Input port names */
      for (int p = 0; p < parentList.getLength(); p++) {
        Node parent = parentList.item(p);
        Element parentElement = (Element) parent;

        String from = parentElement.getAttribute("ref");
        Long fromOpId = operatorNameMap.get(from);

        edges.add(new Edge(fromOpId, toOpId, new Data("", -1)));

        if (!optoop.containsKey(from)) {
          optoop.put(from, new ArrayList<String>());
        }
        optoop.get(from).add(to);

      }
    }


    Long f, t;
    for (String fs : optoop.keySet()) {
      f = operatorNameMap.get(fs);
      for (String ts : optoop.get(fs)) {
        t = operatorNameMap.get(ts);

        ArrayList<String> names = new ArrayList<>();
        long csize = 0;

        for (String StmpflOUT : opout.get(fs).keySet()) {

          if (opin.containsKey(ts) && opin.get(ts).containsKey(StmpflOUT)) {
            names.add(StmpflOUT);
            csize += opout.get(fs).get(StmpflOUT).file_B;
          }
        }
        graph.addEdge(new Edge(f, t, new Data(names, csize)));


      }
    }

    graph.sumdata_B = sumdata;
    return graph;
  }

  private double getCpuBoundness(String application, String className) {
    double cpuBoundness = 1.0;

    if ("SIPHT".equals(application)) {
      if (className.equals("Patser"))
        cpuBoundness = 0.8348;
      else if (className.equals("Patser_concate"))
        cpuBoundness = 0.1889;
      else if (className.equals("Transterm"))
        cpuBoundness = 0.9479;
      else if (className.equals("Findterm"))
        cpuBoundness = 0.9520;
      else if (className.equals("RNAMotif"))
        cpuBoundness = 0.9505;
      else if (className.equals("Blast"))
        cpuBoundness = 0.9387;
      else if (className.equals("SRNA"))
        cpuBoundness = 0.9348;
      else if (className.equals("FFN_Parse"))
        cpuBoundness = 0.8109;
      else if (className.equals("Blast_synteny"))
        cpuBoundness = 0.6101;
      else if (className.equals("Blast_candidate"))
        cpuBoundness = 0.4361;
      else if (className.equals("Blast_QRNA"))
        cpuBoundness = 0.8780;
      else if (className.equals("Blast_paralogues"))
        cpuBoundness = 0.4430;
      else if (className.equals("SRNA_annotate"))
        cpuBoundness = 0.5596;
    }

    if ("MONTAGE".equals(application)) {
      if (className.equals("mProjectPP"))
        cpuBoundness = 0.8696;
      else if (className.equals("mDiffFit"))
        cpuBoundness = 0.2839;
      else if (className.equals("mConcatFit"))
        cpuBoundness = 0.5317;
      else if (className.equals("mBgModel"))
        cpuBoundness = 0.9989;
      else if (className.equals("mBackground"))
        cpuBoundness = 0.0846;
      else if (className.equals("mImgTbl"))
        cpuBoundness = 0.0348;
      else if (className.equals("mAdd"))
        cpuBoundness = 0.0848;
      else if (className.equals("mShrink"))
        cpuBoundness = 0.0230;
      else if (className.equals("mJPEG"))
        cpuBoundness = 0.7714;
    }

    if ("LIGO".equals(application)) {
      if (className.equals("TmpltBank"))
        cpuBoundness = 0.9894;
      else if (className.equals("Inspiral"))
        cpuBoundness = 0.8996;
      else if (className.equals("Thinca"))
        cpuBoundness = 0.4390;
      else if (className.equals("Inca"))
        cpuBoundness = 0.3793;
      else if (className.equals("Data_Find"))
        cpuBoundness = 0.5555;
      else if (className.equals("Inspinj"))
        cpuBoundness = 0.0832;
      else if (className.equals("TrigBank"))
        cpuBoundness = 0.1744;
      else if (className.equals("Sire"))
        cpuBoundness = 0.1415;
      else if (className.equals("Coire"))
        cpuBoundness = 0.0800;

    }

    return cpuBoundness;
  }
}
