package com.comparison.visual;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Visualizer {

    public static void generateReport(long[][] results, String[] testNames, String fileName) {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for (int i = 0; i < testNames.length; i++) {
            dataset.addValue(results[i][0], "Spark", testNames[i]);
            dataset.addValue(results[i][1], "JanusGraph", testNames[i]);
        }
        renderBarChart(fileName, "Порівняння Spark vs JanusGraph", dataset);
    }

    public static void saveSparkKMeansChart(Dataset<Row> data, String fileName) {
        XYSeriesCollection dataset = new XYSeriesCollection();
        XYSeries c0 = new XYSeries("Кластер 0 (Support)");
        XYSeries c1 = new XYSeries("Кластер 1 (Average)");
        XYSeries c2 = new XYSeries("Кластер 2 (Carry/Anomaly)");

        List<Row> rows = data.select("h_dmg", "t_dmg", "prediction").limit(2000).collectAsList();

        for (Row row : rows) {
            double x = row.get(0) != null ? Double.parseDouble(row.get(0).toString()) : 0.0;
            double y = row.get(1) != null ? Double.parseDouble(row.get(1).toString()) : 0.0;
            int cluster = row.getInt(2);

            if (cluster == 0) c0.add(x, y);
            else if (cluster == 1) c1.add(x, y);
            else c2.add(x, y);
        }

        dataset.addSeries(c0);
        dataset.addSeries(c1);
        dataset.addSeries(c2);

        renderScatterChart(dataset, fileName, "Spark K-Means: Розподіл шкоди");
    }

    public static void saveJanusKMeansChart(List<Map<String, Object>> data, String fileName) {
        XYSeriesCollection dataset = new XYSeriesCollection();
        XYSeries low = new XYSeries("Janus: Кластер 0 (Support)");
        XYSeries mid = new XYSeries("Janus: Кластер 1 (Average)");
        XYSeries high = new XYSeries("Janus: Кластер 2 (Carry/Anomaly)");

        if (data == null || data.isEmpty()) {
            System.err.println("[Visualizer] Попередження: Дані JanusGraph порожні!");
            return;
        }

        for (Map<String, Object> point : data) {
            try {
                double x = Double.parseDouble(point.get("gpm").toString());
                double y = Double.parseDouble(point.get("win").toString());

                if (x > 20000 || y > 3000) {
                    high.add(x, y);
                } else if (x > 10000 || y > 500) {
                    mid.add(x, y);
                } else {
                    low.add(x, y);
                }
            } catch (Exception e) {}
        }

        dataset.addSeries(low);
        dataset.addSeries(mid);
        dataset.addSeries(high);

        renderScatterChart(dataset, fileName, "JanusGraph: Розподіл шкоди");
    }

    private static void renderScatterChart(XYSeriesCollection dataset, String fileName, String title) {
        JFreeChart chart = ChartFactory.createScatterPlot(
                title,
                "Hero Damage (Aggression)",
                "Tower Damage (Objective)",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false);

        try {
            ChartUtils.saveChartAsPNG(new File(fileName), chart, 800, 600);
            System.out.println("[Visualizer] ГРАФІК ЗБЕРЕЖЕНО: " + fileName);
        } catch (IOException e) {
            System.err.println("Помилка запису PNG: " + e.getMessage());
        }
    }

    private static void renderBarChart(String fileName, String title, DefaultCategoryDataset dataset) {
        JFreeChart chart = ChartFactory.createBarChart(title, "Система", "Час (мс)",
                dataset, PlotOrientation.VERTICAL, true, true, false);
        saveFile(fileName, chart);
    }

    private static void saveFile(String fileName, JFreeChart chart) {
        try {
            ChartUtils.saveChartAsPNG(new File(fileName), chart, 800, 600);
            System.out.println("[Visualizer] ЗБЕРЕЖЕНО: " + fileName);
        } catch (IOException e) {
            System.err.println("Помилка: " + e.getMessage());
        }
    }

    public static void saveGBTImportanceChart(double[] importances, String[] featureNames, String fileName, String title) {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        for (int i = 0; i < importances.length; i++) {
            String label = featureNames[i].toUpperCase().replace("_", " ");
            dataset.addValue(importances[i], "Важливість", label);
        }

        JFreeChart barChart = ChartFactory.createBarChart(
                title,
                "Параметри статистики",
                "Коефіцієнт впливу",
                dataset);

        try {
            ChartUtils.saveChartAsPNG(new File(fileName), barChart, 800, 600);
            System.out.println("   [Visualizer] ЗБЕРЕЖЕНО ГРАФІК: " + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}