package com.cwa.solaligue.app.utilities;

import com.cwa.solaligue.app.scheduler.Plan;
import org.jfree.data.xy.XYSeries;

import java.util.ArrayList;
import java.util.List;


public class MultiplePlotInfo {
  ArrayList<XYSeries> series;

  public MultiplePlotInfo() {
    series = new ArrayList<>();
  }

  public void add(String name, List<Plan> ps) {
    XYSeries xy = new XYSeries(name);
    for (Plan p : ps) {
      xy.add(p.stats.money, p.stats.runtime_MS / 1000);
    }
    series.add(xy);
  }
}
