package com.comparison.janus;

import com.comparison.spark.SparkEngine;
import com.comparison.visual.Visualizer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import java.io.*;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import org.apache.tinkerpop.gremlin.process.traversal.P;

public class DataLoader {

    public static void loadMatches(JanusGraph graph, String path) {
        GraphTraversalSource g = graph.traversal();
        if (g.V().hasLabel("match").hasNext()) return;

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            br.readLine();
            String line;
            int c = 0;
            while ((line = br.readLine()) != null) {
                String[] cols = line.split(",");
                g.addV("match").property("matchId", Long.parseLong(cols[0]))
                        .property("duration", Integer.parseInt(cols[2]))
                        .property("radiant_win", Boolean.parseBoolean(cols[9])).iterate();
                if (++c % 10000 == 0) {
                    g.tx().commit();
                    System.out.println("Матчі: " + c);
                }
            }
            g.tx().commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void loadPlayersAndEdges(JanusGraph graph, String path) {
        GraphTraversalSource g = graph.traversal();

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            br.readLine();
            String line;
            int c = 0;
            while ((line = br.readLine()) != null) {
                String[] cols = line.split(",");
                if (cols.length < 17) continue;

                long mId = Long.parseLong(cols[0]);
                long pId = Long.parseLong(cols[1]);

                double gpm = Double.parseDouble(cols[6]);
                int deaths = Integer.parseInt(cols[9]);
                double hDmg = Double.parseDouble(cols[14]);
                double tDmg = Double.parseDouble(cols[16]);

                var matchTraversal = g.V().has("match", "matchId", mId);
                if (!matchTraversal.hasNext()) continue;

                Vertex matchV = matchTraversal.next();

                g.V().has("player", "playerId", pId).fold()
                        .coalesce(__.unfold(), __.addV("player").property("playerId", pId))
                        .property("gold_per_min", gpm)
                        .property("deaths", deaths)
                        .property("hero_damage", hDmg)
                        .property("tower_damage", tDmg)
                        .addE("played_in").to(matchV)
                        .iterate();

                if (++c % 50000 == 0) {
                    g.tx().commit();
                    System.out.println("Оброблено зв'язків: " + c);
                }
            }
            g.tx().commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static long runJanusScan(JanusGraph graph) {
        long s = System.currentTimeMillis();
        graph.traversal().V().has("match", "duration", P.gt(2000)).has("radiant_win", true).count().next();
        return System.currentTimeMillis() - s;
    }

    public static long runJanusTeammatesTest(JanusGraph graph, long pId) {
        long s = System.currentTimeMillis();

        java.util.List<Object> ids = graph.traversal().V()
                .has("player", "playerId", pId)
                .out("played_in").in("played_in")
                .has("playerId", org.apache.tinkerpop.gremlin.process.traversal.P.neq(pId))
                .values("playerId")
                .order().dedup().limit(10).toList();

        long time = System.currentTimeMillis() - s;
        System.out.println("   [Data] Для гравця ID:" + pId + " JanusGraph знайшов тімейтів: " + ids);
        return time;
    }


    public static long runJanusCommonTeammates(JanusGraph graph) {
        long start = System.currentTimeMillis();

        System.out.println("   [Data] Топ-5 стійких дуетів (JanusGraph):");
        try {
            graph.traversal().V().hasLabel("player").as("p1")
                    .out("played_in").in("played_in")
                    .hasLabel("player").as("p2")
                    .where("p1", org.apache.tinkerpop.gremlin.process.traversal.P.lt("p2"))
                    .by("playerId")
                    .groupCount()
                    .by(select("p1", "p2").by("playerId"))
                    .unfold()
                    .order().by(org.apache.tinkerpop.gremlin.structure.Column.values, org.apache.tinkerpop.gremlin.process.traversal.Order.desc)
                    .limit(5)
                    .forEachRemaining(System.out::println);
        } catch (Exception e) {
            System.err.println("Помилка в JanusGraph: " + e.getMessage());
        }

        return System.currentTimeMillis() - start;
    }

    public static long runJanusRealPartnersBenchmark(JanusGraph graph) {
        long start = System.currentTimeMillis();

        System.out.println("   [Data] Топ-5 реальних дуетів (JanusGraph):");
        try {
            graph.traversal().V().hasLabel("player")
                    .has("playerId", org.apache.tinkerpop.gremlin.process.traversal.P.neq(0L)).as("p1")
                    .out("played_in").in("played_in")
                    .hasLabel("player")
                    .has("playerId", org.apache.tinkerpop.gremlin.process.traversal.P.neq(0L)).as("p2")
                    .where("p1", org.apache.tinkerpop.gremlin.process.traversal.P.lt("p2")).by("playerId")
                    .groupCount()
                    .by(select("p1", "p2").by("playerId"))
                    .unfold()
                    .order().by(org.apache.tinkerpop.gremlin.structure.Column.values, org.apache.tinkerpop.gremlin.process.traversal.Order.desc)
                    .limit(5)
                    .forEachRemaining(System.out::println);
        } catch (Exception e) {
            System.err.println("Помилка JanusGraph: " + e.getMessage());
        }

        return System.currentTimeMillis() - start;
    }

    public static long runJanusSSSP(JanusGraph graph, long startId, long endId) {
        long s = System.currentTimeMillis();
        try {
            List<org.apache.tinkerpop.gremlin.process.traversal.Path> paths = graph.traversal().V()
                    .has("player", "playerId", startId)
                    .repeat(
                            __.out("played_in").in("played_in")
                                    .has("playerId", org.apache.tinkerpop.gremlin.process.traversal.P.neq(0L))
                                    .has("playerId", org.apache.tinkerpop.gremlin.process.traversal.P.neq(startId))
                                    .simplePath()
                    )
                    .until(__.has("playerId", endId).or().loops().is(org.apache.tinkerpop.gremlin.process.traversal.P.gte(3)))
                    .has("playerId", endId)
                    .path().limit(1).toList();

            if (!paths.isEmpty()) {
                org.apache.tinkerpop.gremlin.process.traversal.Path path = paths.get(0);
                StringBuilder humanPath = new StringBuilder();

                for (int i = 0; i < path.size(); i++) {
                    org.apache.tinkerpop.gremlin.structure.Vertex v = path.get(i);
                    if (v.label().equals("player")) {
                        humanPath.append("[Player: ").append(v.value("playerId").toString()).append("]");
                    } else {
                        humanPath.append("[Match: ").append(v.value("matchId").toString()).append("]");
                    }

                    if (i < path.size() - 1) humanPath.append(" -> ");
                }

                System.out.println("   [Data] JanusGraph знайшов РЕАЛЬНИЙ зв'язок!");
                System.out.println("   [Detail] Ланцюжок: " + humanPath.toString());
                System.out.println("   [Detail] Глибина (хопів): " + (path.size() / 2));
            } else {
                System.out.println("   [Data] JanusGraph: Реальний шлях між " + startId + " та " + endId + " не знайдено на глибині 3.");
            }
        } catch (Exception e) {
            System.out.println("   [Error] JanusGraph SSSP: " + e.getMessage());
            e.printStackTrace();
        }
        return System.currentTimeMillis() - s;
    }

    public static long runJanusKMeans(JanusGraph graph) {
        long startTime = System.currentTimeMillis();
        var g = graph.traversal();

        try {
            List<Map<String, Object>> stats = g.V().hasLabel("player")
                    .has("hero_damage")
                    .limit(1000)
                    .project("gpm", "win")
                    .by("hero_damage")
                    .by("tower_damage")
                    .toList();

            if (stats.isEmpty()) {
                System.out.println("   [Warning] JanusGraph не знайшов властивостей hero_damage. Перевір DataLoader!");
            }

            Visualizer.saveJanusKMeansChart(stats, "janus_kmeans.png");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return System.currentTimeMillis() - startTime;
    }

    public static long runJanusGBT(JanusGraph graph) {
        long startTime = System.currentTimeMillis();
        var g = graph.traversal();

        try {
            double[] sparkWeights = SparkEngine.getLastImportances();

            if (sparkWeights == null || sparkWeights.length < 6) {
                sparkWeights = new double[]{0.1, 0.1, 0.1, 0.1, 0.1, 0.5};
            }

            double wGpm = sparkWeights[0];
            double wXpm = sparkWeights[1];
            double wKills = sparkWeights[2];
            double wDeaths = sparkWeights[3];
            double wHDmg = sparkWeights[4];
            double wTDmg = sparkWeights[5];

            List<Map<String, Object>> matches = g.V().hasLabel("match")
                    .limit(1000)
                    .project("actual_win", "score")
                    .by("radiant_win")
                    .by(__.in("played_in").as("p")
                            .select("p").values("gold_per_min").mean().as("g")
                            .select("p").values("xp_per_min").mean().as("x")
                            .select("p").values("kills").mean().as("k")
                            .select("p").values("deaths").mean().as("d")
                            .select("p").values("hero_damage").mean().as("hd")
                            .select("p").values("tower_damage").mean().as("td")
                            .math("g*"+wGpm + " + x*"+wXpm + " + k*"+wKills + " - d*"+wDeaths + " + hd*"+wHDmg + " + td*"+wTDmg)
                    ).toList();

            int correct = 0;
            int processed = 0;
            for (Map<String, Object> m : matches) {
                if (m.get("score") != null && m.get("actual_win") != null) {
                    double score = Double.parseDouble(m.get("score").toString());
                    boolean actual = (boolean) m.get("actual_win");

                    boolean predicted = score > 2000;
                    if (predicted == actual) correct++;
                    processed++;
                }
            }

            if (processed > 0) {
                System.out.println("   [Janus GBT-Sync] Використано ваги зі Spark.");
                System.out.println("   [Janus GBT-Sync] Точність на графових даних: " + String.format("%.2f", ((double)correct/processed)*100) + "%");
            }

            Visualizer.saveGBTImportanceChart(sparkWeights, SparkEngine.getFeatureLabels(),
                    "janus_gbt_importance.png", "JanusGraph: Важливість ознак (Sync with Spark)");

        } catch (Exception e) {
            System.err.println("Помилка в Janus GBT: " + e.getMessage());
        }
        return System.currentTimeMillis() - startTime;
    }

    public static void demonstrateRealTimePrediction(JanusGraph graph, long matchId) {
        var g = graph.traversal();
        System.out.println("\n=== ДЕМОНСТРАЦІЯ ПРИЙНЯТТЯ РІШЕННЯ (Case Study) ===");

        try {
            double[] weights = SparkEngine.getLastImportances();
            if (weights == null) return;

            if (!g.V().has("match", "matchId", matchId).hasNext()) {
                matchId = (long) g.V().hasLabel("match").values("matchId").limit(1).next();
            }

            Map<String, Object> data = g.V().has("match", "matchId", matchId)
                    .project("win", "gpm", "td", "deaths")
                    .by("radiant_win")
                    .by(__.in("played_in").values("gold_per_min").mean())
                    .by(__.in("played_in").values("tower_damage").mean())
                    .by(__.in("played_in").values("deaths").mean())
                    .next();

            double gpm = data.get("gpm") != null ? Double.parseDouble(data.get("gpm").toString()) : 0.0;
            double td = data.get("td") != null ? Double.parseDouble(data.get("td").toString()) : 0.0;
            double deaths = data.get("deaths") != null ? Double.parseDouble(data.get("deaths").toString()) : 0.0;
            boolean actualWin = (boolean) data.get("win");

            double predictionScore = (gpm * weights[0]) + (td * weights[5]) - (deaths * weights[3]);

            boolean prediction = predictionScore > 250;

            System.out.println("Аналіз об'єкта ID: " + matchId);
            System.out.printf(" > GPM: %.2f | Tower Dmg: %.2f | Deaths: %.2f\n", gpm, td, deaths);
            System.out.println("-------------------------------------------");
            System.out.println(" > Score: " + String.format("%.2f", predictionScore));
            System.out.println(" > ПРОГНОЗ: " + (prediction ? "ПЕРЕМОГА" : "ПОРАЗКА"));
            System.out.println(" > РЕАЛЬНІСТЬ: " + (actualWin ? "ПЕРЕМОГА" : "ПОРАЗКА"));
            System.out.println(" > РЕЗУЛЬТАТ: " + (prediction == actualWin ? "ВІРНО ✅" : "ПОМИЛКА ❌"));

        } catch (Exception e) {
            System.err.println("Помилка симуляції: " + e.getMessage());
        }
    }
}