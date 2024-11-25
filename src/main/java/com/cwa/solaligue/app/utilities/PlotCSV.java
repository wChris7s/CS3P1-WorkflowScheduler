package com.cwa.solaligue.app.utilities;

import lombok.experimental.UtilityClass;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.IOException;

@UtilityClass
public class PlotCSV {

  public static void createChart(XYSeries series, String title, String xAxisLabel, String yAxisLabel, String outputPath, Color color) {
    XYSeriesCollection dataset = new XYSeriesCollection(series);
    JFreeChart chart = ChartFactory.createXYLineChart(
        title,
        xAxisLabel,
        yAxisLabel,
        dataset,
        PlotOrientation.VERTICAL,
        true,
        true,
        false
    );

    XYPlot plot = chart.getXYPlot();
    XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
    renderer.setSeriesLinesVisible(0, true);
    renderer.setSeriesShapesVisible(0, true);
    renderer.setSeriesPaint(0, color);
    plot.setRenderer(renderer);

    try {
      ChartUtils.saveChartAsPNG(new File(outputPath), chart, 1024, 768);
    } catch (IOException e) {
      e.printStackTrace();
    }

    SwingUtilities.invokeLater(() -> {
      JFrame frame = new JFrame(title);
      frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
      frame.setLayout(new BorderLayout());
      ChartPanel chartPanel = new ChartPanel(chart);
      frame.add(chartPanel, BorderLayout.CENTER);
      frame.pack();
      frame.setVisible(true);
    });
  }
}