package com.comparison.janus;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class JanusSchemaManager {

 
    public static JanusGraph createGraph() {
        return JanusGraphFactory.build()
                .set("storage.backend", "berkeleyje")
                .set("storage.directory", "db/janusgraph")
                .open();
    }
    
    public static void defineSchema(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();

        try {
            if (mgmt.getVertexLabel("match") == null) {
                mgmt.makeVertexLabel("match").make();
            }
            if (mgmt.getVertexLabel("player") == null) {
                mgmt.makeVertexLabel("player").make();
            }

            if (mgmt.getEdgeLabel("played_in") == null) {
                mgmt.makeEdgeLabel("played_in").make();
            }

            PropertyKey matchIdKey = mgmt.getPropertyKey("matchId");
            if (matchIdKey == null) {
                matchIdKey = mgmt.makePropertyKey("matchId").dataType(Long.class).make();
            }

            PropertyKey playerIdKey = mgmt.getPropertyKey("playerId");
            if (playerIdKey == null) {
                playerIdKey = mgmt.makePropertyKey("playerId").dataType(Long.class).make();
            }

            if (mgmt.getPropertyKey("duration") == null) {
                mgmt.makePropertyKey("duration").dataType(Integer.class).make();
            }
            if (mgmt.getPropertyKey("radiant_win") == null) {
                mgmt.makePropertyKey("radiant_win").dataType(Boolean.class).make();
            }
            if (mgmt.getPropertyKey("game_mode") == null) {
                mgmt.makePropertyKey("game_mode").dataType(Integer.class).make();
            }

            if (mgmt.getPropertyKey("hero_damage") == null) {
                mgmt.makePropertyKey("hero_damage").dataType(Double.class).make();
            }
            if (mgmt.getPropertyKey("tower_damage") == null) {
                mgmt.makePropertyKey("tower_damage").dataType(Double.class).make();
            }

            if (mgmt.getGraphIndex("byMatchId") == null) {
                mgmt.buildIndex("byMatchId", Vertex.class)
                        .addKey(matchIdKey)
                        .unique()
                        .buildCompositeIndex();
            }

            if (mgmt.getPropertyKey("gold_per_min") == null) {
                mgmt.makePropertyKey("gold_per_min").dataType(Double.class).make();
            }
            if (mgmt.getPropertyKey("xp_per_min") == null) {
                mgmt.makePropertyKey("xp_per_min").dataType(Double.class).make();
            }
            if (mgmt.getPropertyKey("kills") == null) {
                mgmt.makePropertyKey("kills").dataType(Integer.class).make();
            }
            if (mgmt.getPropertyKey("deaths") == null) {
                mgmt.makePropertyKey("deaths").dataType(Integer.class).make();
            }
            if (mgmt.getPropertyKey("assists") == null) {
                mgmt.makePropertyKey("assists").dataType(Integer.class).make();
            }

            if (mgmt.getPropertyKey("radiant_win") == null) {
                mgmt.makePropertyKey("radiant_win").dataType(Boolean.class).make();
            }

            if (mgmt.getGraphIndex("byPlayerId") == null) {
                mgmt.buildIndex("byPlayerId", Vertex.class)
                        .addKey(playerIdKey)
                        .unique()
                        .buildCompositeIndex();
            }

            mgmt.commit();
            System.out.println("Схема JanusGraph успішно ініціалізована!");

        } catch (Exception e) {
            mgmt.rollback();
            System.err.println("Помилка при створенні схеми: " + e.getMessage());
            throw e;
        }
    }
}