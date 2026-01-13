package com.comparison.spark;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class SparkEngine {
    private static SparkSession spark;

    public static void init() {
        if (spark == null) {
            spark = SparkSession.builder()
                    .appName("DotaAnalysis")
                    .config("spark.master", "local[*]")
                    .getOrCreate();
            spark.sparkContext().setLogLevel("ERROR");
        }
    }

    public static long runGlobalScan(String path) {
        init();
        long start = System.currentTimeMillis();
        spark.read().option("header", "true").option("inferSchema", "true").csv(path)
                .filter("duration > 2000 AND radiant_win = true").count();
        return System.currentTimeMillis() - start;
    }

    public static long runTeammatesTest(String path, long pId) {
        init();
        long start = System.currentTimeMillis();
        Dataset<Row> df = spark.read().option("header", "true").csv(path);

        Dataset<Row> matches = df.filter(col("account_id").equalTo(pId)).select("match_id");

        Dataset<Row> teammates = df.join(matches, "match_id")
                // ВИПРАВЛЕНО: використовуємо notEqual замість neq
                .filter(col("account_id").notEqual(pId))
                .select("account_id")
                .distinct()
                .orderBy(asc("account_id"));

        java.util.List<String> results = teammates.limit(10).collectAsList().stream()
                .map(r -> r.getString(0)).toList();

        System.out.println("   [Data] Для гравця ID:" + pId + " Spark знайшов тімейтів: " + results);

        teammates.count();
        return System.currentTimeMillis() - start;
    }
    public static long runCommonTeammates(String path, long p1, long p2) {
        init();
        long start = System.currentTimeMillis();

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path)
                .select("match_id", "account_id");

        Dataset<Row> pairs = df.as("p1")
                .join(df.as("p2"), col("p1.match_id").equalTo(col("p2.match_id")))
                .filter(col("p1.account_id").lt(col("p2.account_id")));

        Dataset<Row> frequentPairs = pairs.groupBy("p1.account_id", "p2.account_id")
                .count()
                .filter(col("count").geq(3)) // Шукаємо 3+ спільних матчі
                .orderBy(desc("count"))
                .limit(5);

        System.out.println("   [Data] Топ-5 стійких дуетів за (Spark):");
        frequentPairs.show();

        return System.currentTimeMillis() - start;
    }


    public static long runRealPartnersBenchmark(String path) {
        init();
        long start = System.currentTimeMillis();

        Dataset<Row> players = spark.read().option("header", "true").option("inferSchema", "true").csv(path)
                .select("match_id", "account_id")
                .filter(col("account_id").notEqual(0));

        Dataset<Row> pairs = players.as("p1")
                .join(players.as("p2"), col("p1.match_id").equalTo(col("p2.match_id")))
                .filter(col("p1.account_id").lt(col("p2.account_id")));

        Dataset<Row> result = pairs.groupBy("p1.account_id", "p2.account_id")
                .count()
                .orderBy(desc("count"))
                .limit(5);

        System.out.println("   [Data] Топ-5 реальних дуетів (Spark):");
        result.show();

        return System.currentTimeMillis() - start;
    }

    
    public static void stop() { if (spark != null) spark.stop(); }
}