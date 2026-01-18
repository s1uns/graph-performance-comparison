package com.comparison;

import com.comparison.janus.JanusSchemaManager;
import com.comparison.janus.DataLoader;
import com.comparison.spark.SparkEngine;
import com.comparison.visual.Visualizer;
import org.janusgraph.core.JanusGraph;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Main {
    public static void main(String[] args) {
        String matchPath = "data/match.csv";
        String playersPath = "data/players.csv";

        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        Logger.getLogger("org").setLevel(Level.OFF);

        System.out.println("=== ЗАПУСК ПОРІВНЯЛЬНОГО ЕКСПЕРИМЕНТУ (DOTA 2) ===");

        try (JanusGraph graph = JanusSchemaManager.createGraph()) {
            JanusSchemaManager.defineSchema(graph);

            System.out.println("\n>>> ПІДГОТОВКА ДАНИХ...");
            if (!graph.traversal().V().hasLabel("player").hasNext()) {
                DataLoader.loadMatches(graph, matchPath);
                DataLoader.loadPlayersAndEdges(graph, playersPath);
            }

            System.out.println("\n>>> ЗАПУСК ТЕСТІВ...");

            System.out.println("\n>>> ТЕСТ 1:  Global Scan");
            long s1 = SparkEngine.runGlobalScan(matchPath);
            long j1 = DataLoader.runJanusScan(graph);
            printComparison("1. Global Scan", s1, j1);

            System.out.println("\n>>> ТЕСТ 2:  N-Hop");
            long s2 = SparkEngine.runTeammatesTest(playersPath, 1L);
            long j2 = DataLoader.runJanusTeammatesTest(graph, 1L);
            printComparison("2. N-Hop", s2, j2);

            System.out.println("\n>>> ТЕСТ 3:  Pattern Search");
            long s3 = SparkEngine.runCommonTeammates(playersPath, 1L, 2L);
            long j3 = DataLoader.runJanusCommonTeammates(graph);
            printComparison("3. Real Social Connections", s3, j3);

            System.out.println("\n>>> ТЕСТ 4:  N-Hop");
            long s4 = SparkEngine.runRealPartnersBenchmark(playersPath);
            long j4 = DataLoader.runJanusRealPartnersBenchmark(graph);
            printComparison("4. Real Social Connections", s4, j4);

            long[][] initialResults = {{s1, j1}, {s2, j2}, {s3, j3}, {s4, j4}};
            String[] initialNames = {"Global Scan", "N-Hop", "Pattern Search", "Social Connections"};
            Visualizer.generateReport(initialResults, initialNames, "benchmark_initial_tests.png");

            System.out.println("\n>>> ТЕСТ 5-6: Алгоритм SSSP");
            long s5 = SparkEngine.runSparkSSSP(playersPath, 1L, 500L);
            long j5 = DataLoader.runJanusSSSP(graph, 1L, 500L);
            printComparison("5. SSSP Unsuccessful", s5, j5);

            long s6 = SparkEngine.runSparkSSSP(playersPath, 3105L, 2703L);
            long j6 = DataLoader.runJanusSSSP(graph, 3105L, 2703L);
            printComparison("6. SSSP Successful", s6, j6);

            long[][] ssspResults = {{s5, j5}, {s6, j6}};
            String[] ssspNames = {"SSSP Unsuccessful", "SSSP Successful"};
            Visualizer.generateReport(ssspResults, ssspNames, "benchmark_sssp.png");

            System.out.println("\n>>> ТЕСТ 7: K-Means кластеризація");
            long s7 = SparkEngine.runSparkKMeans(playersPath, matchPath);
            long j7 = DataLoader.runJanusKMeans(graph);
            printComparison("7. K-Means Performance", s7, j7);

            long[][] kmeansResults = {{s7, j7}};
            String[] kmeansNames = {"K-Means Clustering"};
            Visualizer.generateReport(kmeansResults, kmeansNames, "benchmark_kmeans.png");

            System.out.println("\n>>> ТЕСТ 8: Прогнозування перемоги (GBT)");
            long s8 = SparkEngine.runSparkGBT(playersPath, matchPath);
            long j8 = DataLoader.runJanusGBT(graph);
            printComparison("8. GBT Performance", s8, j8);

            long[][] gbtResults = {{s8, j8}};
            String[] gbtNames = {"GBT Classification"};
            Visualizer.generateReport(gbtResults, gbtNames, "benchmark_gbt.png");

            System.out.println("\n>>> 9. ЗАПУСК СИМУЛЯТОРА ПРИЙНЯТТЯ РІШЕНЬ (Inference Case Study)");
            DataLoader.demonstrateRealTimePrediction(graph, 0L);
            System.out.println("\n=======");
            DataLoader.demonstrateRealTimePrediction(graph, 12L);
            System.out.println("\n=======");
            DataLoader.demonstrateRealTimePrediction(graph, 100L);
            System.out.println("\n=======");
            DataLoader.demonstrateRealTimePrediction(graph, 123L);



            System.out.println("\n=== ЕКСПЕРИМЕНТ ЗАВЕРШЕНО ===");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            SparkEngine.stop();
        }
    }

    private static void printComparison(String title, long sparkTime, long janusTime) {
        System.out.println("\n[ " + title + " ]");
        System.out.printf("%-12s | %-10d ms | %s\n", "Spark", sparkTime, getBar(sparkTime, janusTime));
        System.out.printf("%-12s | %-10d ms | %s\n", "JanusGraph", janusTime, getBar(janusTime, sparkTime));
    }

    private static String getBar(long current, long other) {
        int maxChars = 30;
        long max = Math.max(current, other);
        if (max == 0) max = 1;
        int barLen = (int) ((double) current / max * maxChars);
        return "█".repeat(Math.max(1, barLen)) + " ".repeat(Math.max(0, maxChars - barLen));
    }
}