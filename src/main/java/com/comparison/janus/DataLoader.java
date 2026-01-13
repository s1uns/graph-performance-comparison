package com.comparison.janus;

import org.janusgraph.core.JanusGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import java.io.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import org.apache.tinkerpop.gremlin.process.traversal.P;

public class DataLoader {

    public static void loadMatches(JanusGraph graph, String path) {
        GraphTraversalSource g = graph.traversal();
        if (g.V().hasLabel("match").hasNext()) return; // Вже завантажено

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            br.readLine(); String line; int c = 0;
            while ((line = br.readLine()) != null) {
                String[] cols = line.split(",");
                g.addV("match").property("matchId", Long.parseLong(cols[0]))
                        .property("duration", Integer.parseInt(cols[2]))
                        .property("radiant_win", Boolean.parseBoolean(cols[9])).iterate();
                if (++c % 5000 == 0) { g.tx().commit(); System.out.println("Matches: " + c); }
            }
            g.tx().commit();
        } catch (Exception e) { e.printStackTrace(); }
    }

    public static void loadPlayersAndEdges(JanusGraph graph, String path) {
        GraphTraversalSource g = graph.traversal();
        if (g.V().hasLabel("player").hasNext()) return;

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            br.readLine(); String line; int c = 0;
            while ((line = br.readLine()) != null) {
                String[] cols = line.split(",");
                long mId = Long.parseLong(cols[0]);
                long pId = Long.parseLong(cols[1]);

                Vertex matchV = g.V().has("match", "matchId", mId).next();
                g.V().has("player", "playerId", pId).fold()
                        .coalesce(unfold(), addV("player").property("playerId", pId))
                        .addE("played_in").to(matchV).iterate();

                if (++c % 5000 == 0) { g.tx().commit(); System.out.println("Edges: " + c); }
            }
            g.tx().commit();
        } catch (Exception e) { e.printStackTrace(); }
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
                    // ФІКС ДУБЛІКАТІВ: залишаємо лише пари, де ID1 < ID2
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
}