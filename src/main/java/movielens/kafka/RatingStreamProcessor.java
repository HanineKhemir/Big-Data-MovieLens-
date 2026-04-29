package movielens.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.split;

public class RatingStreamProcessor {

    private static final String BROKERS = "localhost:9092";
    private static final String TOPIC = "movie-rating";

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Usage: RatingStreamProcessor <output_path>");
            System.exit(1);
        }

        String outputPath = args[0];

        SparkSession spark = SparkSession.builder()
                .appName("MovieLens Stream Processor")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BROKERS)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> parsed = raw
                .selectExpr("CAST(value AS STRING) as raw")
                .filter(col("raw").isNotNull())
                .select(
                        split(col("raw"), "::").getItem(0)
                                .cast("int").alias("userId"),
                        split(col("raw"), "::").getItem(1)
                                .cast("int").alias("movieId"),
                        split(col("raw"), "::").getItem(2)
                                .cast("double").alias("rating")
                )
                .filter(col("movieId").isNotNull())
                .filter(col("rating").isNotNull());

        Dataset<Row> aggregated = parsed
                .groupBy("movieId")
                .agg(
                        round(avg("rating"), 2).alias("avgRating"),
                        count("rating").alias("totalVotes")
                );

        StreamingQuery consoleQuery = aggregated
                .writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .option("numRows", 20)
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        StreamingQuery hdfsQuery = parsed
                .writeStream()
                .outputMode("append")
                .format("csv")
                .option("path", outputPath + "/raw-stream")
                .option("checkpointLocation", outputPath + "/checkpoint")
                .option("header", "true")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        System.out.println("Stream processor démarré !");
        System.out.println("En attente de messages sur : " + TOPIC);

        consoleQuery.awaitTermination();
        hdfsQuery.awaitTermination();
    }
}