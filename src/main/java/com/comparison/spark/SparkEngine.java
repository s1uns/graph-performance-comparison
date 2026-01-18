package com.comparison.spark;

import com.comparison.visual.Visualizer;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;

public class SparkEngine {
    private static SparkSession spark;
    private static double[] lastGbtImportances;
    private static final String[] featureLabels = {"GPM", "XPM", "Kills", "Deaths", "H_Dmg", "T_Dmg"};

    public static double[] getLastImportances() { return lastGbtImportances; }
    public static String[] getFeatureLabels() { return featureLabels; }

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
                .filter(col("count").geq(3))
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

    public static long runSparkSSSP(String path, long startId, long endId) {
        if (spark == null) {
            init();
        }

        long startTime = System.currentTimeMillis();
        System.out.println("   [Spark] Початок пошуку реального шляху між " + startId + " та " + endId);

        try {
            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .csv(path)
                    .select(col("match_id"), col("account_id").cast("long"))
                    .filter(col("account_id").isNotNull());

            Dataset<Row> currentLevel = df.filter(col("account_id").equalTo(startId))
                    .select(
                            col("match_id"),
                            col("account_id"),
                            concat(lit("[Player: "), col("account_id").cast("string")).alias("full_path")
                    ).distinct();

            boolean found = false;

            for (int i = 1; i <= 3; i++) {
                System.out.println("   [Spark] Аналіз рівня " + i + "...");

                Dataset<Row> teammates = df.as("all")
                        .join(currentLevel.as("curr"), col("all.match_id").equalTo(col("curr.match_id")))
                        .filter(col("all.account_id").notEqual(0))
                        .select(
                                col("all.match_id"),
                                col("all.account_id"),
                                concat(
                                        col("curr.full_path"),
                                        lit("] -> [Match: "), col("all.match_id").cast("string"),
                                        lit("] -> [Player: "), col("all.account_id").cast("string")
                                ).alias("full_path")
                        ).distinct();

                Dataset<Row> targetRows = teammates.filter(col("account_id").equalTo(endId));

                if (!targetRows.isEmpty()) {
                    Row target = targetRows.first();
                    System.out.println("   [Data] Spark знайшов шлях на рівні " + i + "!");
                    System.out.println("   [Detail] Ланцюжок: " + target.getAs("full_path") + "]");
                    found = true;
                    break;
                }

                if (i < 3) {
                    currentLevel = df.as("next_df")
                            .join(teammates.select("account_id", "full_path").as("tm"), "account_id")
                            .select(
                                    col("next_df.match_id"),
                                    col("account_id"),
                                    col("tm.full_path")
                            ).distinct();
                }
            }

            if (!found) {
                System.out.println("   [Data] Spark: Реальний зв'язок не знайдено на глибині 3.");
            }

        } catch (Exception e) {
            System.err.println("   [Error] Помилка Spark SSSP: " + e.getMessage());
            e.printStackTrace();
        }

        return System.currentTimeMillis() - startTime;
    }

    public static long runSparkKMeans(String playersPath, String matchesPath) {
        if (spark == null) init();
        long startTime = System.currentTimeMillis();

        try {
            Dataset<Row> players = spark.read().option("header", "true").csv(playersPath);

            Dataset<Row> data = players.select(
                    col("account_id").cast("long"),
                    col("hero_damage").cast("double").alias("h_dmg"),
                    col("tower_damage").cast("double").alias("t_dmg")
            ).filter(col("account_id").notEqual(0)).na().fill(0.0);

            VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(new String[]{"h_dmg", "t_dmg"})
                    .setOutputCol("rawFeatures");

            Dataset<Row> featurizedData = assembler.transform(data);

            StandardScaler scaler = new StandardScaler()
                    .setInputCol("rawFeatures")
                    .setOutputCol("features")
                    .setWithStd(true)
                    .setWithMean(true);

            Dataset<Row> scaledData = scaler.fit(featurizedData).transform(featurizedData);

            KMeans kmeans = new KMeans()
                    .setK(3)
                    .setSeed(42L)
                    .setFeaturesCol("features")
                    .setPredictionCol("prediction");

            KMeansModel model = kmeans.fit(scaledData);
            Dataset<Row> predictions = model.transform(scaledData);

            Visualizer.saveSparkKMeansChart(predictions, "spark_kmeans.png");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return System.currentTimeMillis() - startTime;
    }

    public static long runSparkGBT(String playersPath, String matchesPath) {
        if (spark == null) init();
        long startTime = System.currentTimeMillis();

        try {
            Dataset<Row> players = spark.read().option("header", "true").csv(playersPath);
            Dataset<Row> matches = spark.read().option("header", "true").csv(matchesPath);

            Dataset<Row> data = players.join(matches, "match_id")
                    .select(
                            col("radiant_win").cast("double").alias("label"),
                            col("gold_per_min").cast("double"),
                            col("xp_per_min").cast("double"),
                            col("kills").cast("double"),
                            col("deaths").cast("double"),
                            col("hero_damage").cast("double"),
                            col("tower_damage").cast("double")
                    ).na().fill(0.0);

            VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(new String[]{"gold_per_min", "xp_per_min", "kills", "deaths", "hero_damage", "tower_damage"})
                    .setOutputCol("features");

            Dataset<Row> finalData = assembler.transform(data);

            GBTClassifier gbt = new GBTClassifier().setLabelCol("label").setFeaturesCol("features").setMaxIter(10);
            GBTClassificationModel model = gbt.fit(finalData);

            lastGbtImportances = model.featureImportances().toArray();

            Dataset<Row> predictions = model.transform(finalData);
            double accuracy = new MulticlassClassificationEvaluator().setLabelCol("label").setMetricName("accuracy").evaluate(predictions);
            System.out.println("   [Spark GBT] Точність моделі: " + String.format("%.2f", accuracy * 100) + "%");

            Visualizer.saveGBTImportanceChart(lastGbtImportances, featureLabels, "spark_gbt_importance.png", "Spark GBT: Важливість факторів");

        } catch (Exception e) { e.printStackTrace(); }
        return System.currentTimeMillis() - startTime;
    }

    public static void stop() { if (spark != null) spark.stop(); }
}