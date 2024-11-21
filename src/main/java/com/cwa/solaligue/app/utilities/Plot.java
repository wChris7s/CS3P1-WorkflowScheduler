package com.cwa.solaligue.app.utilities;

import com.cwa.solaligue.app.scheduler.Plan;
import com.cwa.solaligue.app.scheduler.SolutionSpace;
import org.jfree.chart.*;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.LegendTitle;
import org.jfree.chart.ui.ApplicationFrame;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Plot extends ApplicationFrame {

  public Plot(final String title, List<PairPlot<Double, Long>> mydata, String path) {

    super(title);
    final XYSeries series = new XYSeries("Random Data");
    for (PairPlot p : mydata) {
      series.add(p.getMoney(), p.getTime() / 60000);
    }

    final XYSeriesCollection data = new XYSeriesCollection(series);
    final JFreeChart chart = ChartFactory
        .createScatterPlot("Time/Money", "Money", "Time (Minutes)", data, PlotOrientation.VERTICAL, true,
            true, false);

    final ChartPanel chartPanel = new ChartPanel(chart);
    chartPanel.setPreferredSize(new java.awt.Dimension(1024, 768));
    setContentPane(chartPanel);

    File f = new File(path);
    try {
      ChartUtils.saveChartAsPNG(f, chart, 1024, 768);
    } catch (IOException e) {
      e.printStackTrace();
    }


  }

  public Plot(MultiplePlotInfo info, String name, String path, Boolean save) {

    super(name);


    final XYSeriesCollection data = new XYSeriesCollection();
    for (XYSeries s : info.series) {
      data.addSeries(s);
    }
    final JFreeChart chart = ChartFactory
        .createScatterPlot("Time/Money", "Money", "Time (Minutes)", data,
            PlotOrientation.VERTICAL, true, true, false);


    chart.getXYPlot().getDomainAxis().setLowerMargin(0);
    chart.getXYPlot().getRangeAxis().setLowerMargin(0);

    ChartPanel chartPanel;
    chartPanel = new ChartPanel(chart);
    chartPanel.setPreferredSize(new java.awt.Dimension(1024, 768));
    setContentPane(chartPanel);


    if (save) {
      File f = new File(path + name + ".png");
      try {
        ChartUtils.saveChartAsPNG(f, chart, 1024, 768);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }


  public Plot(SolutionSpace combined, ArrayList<Pair<String, Double>> legendInfo, MultiplePlotInfo info, String name, String path,
              Boolean save, Boolean show) {
    super(name);

    final XYSeriesCollection data = new XYSeriesCollection();
    for (XYSeries s : info.series) {
      data.addSeries(s);
    }

    Double maxCost = combined.getMaxCost() + 100;
    Double maxRutime = ((double) combined.getMaxRuntime()) + 100;

    combined.computeSkyline(false, false);


    XYSeries comb = new XYSeries("combined");
    for (Plan p : combined) {
      comb.add(p.stats.money, p.stats.runtime_MS);
    }

    data.addSeries(comb);


    JFreeChart chart = ChartFactory.createXYLineChart("Time/Money", "Money", "Time (Minutes)", data, PlotOrientation.VERTICAL, true, true, false);

    XYPlot plot = (XYPlot) chart.getPlot();

    plot.getDomainAxis().setRange(0.0, maxCost);
    plot.getRangeAxis().setRange(0.0, maxRutime);


    XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();


    for (int i = 0; i < info.series.size(); ++i) {
      // "0" is the line plot
      renderer.setSeriesLinesVisible(i, false);
      renderer.setSeriesShapesVisible(i, true);
    }
    // "1" is the scatter plot
    renderer.setSeriesLinesVisible(info.series.size(), true);
    renderer.setSeriesShapesVisible(info.series.size(), false);

    plot.setRenderer(renderer);

    chart.addLegend(new LegendTitle(new LegendItemSource() {
      @Override
      public LegendItemCollection getLegendItems() {

        LegendItemCollection col = new LegendItemCollection();
        for (Pair<String, Double> li : legendInfo) {

          col.add(new LegendItem(li.a + ": " + li.b));
        }
        return col;
      }
    }));

    if (save) {
      File f = new File(path + name + ".png");
      try {
        ChartUtils.saveChartAsPNG(f, chart, 1024, 768);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    if (show) {
      ChartPanel chartPanel;
      chartPanel = new ChartPanel(chart);
      chartPanel.setPreferredSize(new java.awt.Dimension(1024, 768));
      setContentPane(chartPanel);
    }


  }
}
