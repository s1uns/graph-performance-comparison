package com.comparison;

import com.comparison.janus.JanusSchemaManager;
import com.comparison.janus.DataLoader;
import com.comparison.spark.SparkEngine;
import org.janusgraph.core.JanusGraph;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Main {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        Logger.getLogger("org").setLevel(Level.OFF);

        String matchPath = "data/match.csv";
        String playersPath = "data/players.csv";

        System.out.println("=== ЗАПУСК ПОРІВНЯЛЬНОГО ЕКСПЕРИМЕНТУ (DOTA 2) ===");

        try (JanusGraph graph = JanusSchemaManager.createGraph()) {
            JanusSchemaManager.defineSchema(graph);

            System.out.println("\n>>> ПІДГОТОВКА ДАНИХ...");
            DataLoader.loadMatches(graph, matchPath);
            DataLoader.loadPlayersAndEdges(graph, playersPath);

            System.out.println("\n>>> ЗАПУСК ТЕСТІВ...");

            long s1 = SparkEngine.runGlobalScan(matchPath);
            long j1 = DataLoader.runJanusScan(graph);
            printComparison("1. Global Scan (Duration > 2000)", s1, j1);

            long testId = 1L;
            long s2 = SparkEngine.runTeammatesTest(playersPath, testId);
            long j2 = DataLoader.runJanusTeammatesTest(graph, testId);
            printComparison("2. N-Hop (Teammates of Player " + testId + ")", s2, j2);

            long s3 = SparkEngine.runCommonTeammates(playersPath, 1L, 2L);
            long j3 = DataLoader.runJanusCommonTeammates(graph);
            printComparison("3. Pattern Search (Common Teammates)", s3, j3);

            System.out.println("\n>>> ТЕСТ 4: Пошук реальних дуетів (без ID:0)");
            long s4 = SparkEngine.runRealPartnersBenchmark(playersPath);
            long j4 = DataLoader.runJanusRealPartnersBenchmark(graph);
            printComparison("4. Real Social Connections (No ID:0)", s4, j4);
            
            
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