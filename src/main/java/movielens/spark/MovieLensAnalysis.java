package movielens.spark;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class MovieLensAnalysis {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: MovieLensAnalysis " +
                "<ratings_path> <movies_path> <output_path>");
            System.exit(1);
        }

        String ratingsPath = args[0];
        String moviesPath  = args[1];
        String outputPath  = args[2];

        // 1. SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("MovieLens Batch Analysis")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // 2. Lire ratings.dat → userId::movieId::rating::timestamp
        Dataset<Row> ratings = spark.read()
                .text(ratingsPath)
                .filter(col("value").isNotNull())
                .select(
                    split(col("value"), "::").getItem(0).cast("int").alias("userId"),
                    split(col("value"), "::").getItem(1).cast("int").alias("movieId"),
                    split(col("value"), "::").getItem(2).cast("double").alias("rating"),
                    split(col("value"), "::").getItem(3).cast("long").alias("timestamp")
                );

        // 3. Lire movies.dat → movieId::title::genres
        Dataset<Row> movies = spark.read()
                .text(moviesPath)
                .filter(col("value").isNotNull())
                .select(
                    split(col("value"), "::").getItem(0).cast("int").alias("movieId"),
                    split(col("value"), "::").getItem(1).alias("title"),
                    split(col("value"), "::").getItem(2).alias("genres")
                );

        // 4. Moyenne et total votes par film
        Dataset<Row> ratingStats = ratings
                .groupBy("movieId")
                .agg(
                    round(avg("rating"), 2).alias("avgRating"),
                    count("rating").alias("totalVotes")
                );

        Dataset<Row> moviesWithStats = ratingStats
                .join(movies, "movieId")
                .select("movieId", "title", "avgRating", "totalVotes");

        // 5. Top 10 films (min 100 votes)
        Dataset<Row> top10 = moviesWithStats
                .filter(col("totalVotes").geq(100))
                .orderBy(col("avgRating").desc())
                .limit(10);

        System.out.println("\n========== TOP 10 FILMS ==========");
        top10.show(10, false);

        // 6. Stats par genre
        Dataset<Row> moviesExploded = movies
                .withColumn("genre", explode(split(col("genres"), "\\|")));

        Dataset<Row> genreStats = ratings
                .join(moviesExploded.select("movieId", "genre"), "movieId")
                .groupBy("genre")
                .agg(
                    round(avg("rating"), 2).alias("avgRating"),
                    count("rating").alias("totalRatings")
                )
                .orderBy(col("avgRating").desc());

        System.out.println("\n========== STATS PAR GENRE ==========");
        genreStats.show(20, false);

        // 7. Distribution des notes
        Dataset<Row> ratingDist = ratings
                .groupBy("rating")
                .agg(count("rating").alias("count"))
                .orderBy("rating");

        System.out.println("\n========== DISTRIBUTION DES NOTES ==========");
        ratingDist.show(false);

        // 8. Sauvegarder dans HDFS
        moviesWithStats
                .orderBy(col("avgRating").desc())
                .coalesce(1)
                .write().mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(outputPath + "/movie-stats");

        top10
                .coalesce(1)
                .write().mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(outputPath + "/top10");

        genreStats
                .coalesce(1)
                .write().mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(outputPath + "/genre-stats");

        ratingDist
                .coalesce(1)
                .write().mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(outputPath + "/rating-distribution");

        System.out.println("\nRésultats sauvegardés dans : " + outputPath);
        spark.stop();
    }
}