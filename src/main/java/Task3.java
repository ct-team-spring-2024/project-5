import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Task3 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Destination Analysis")
                .getOrCreate();

        // Path to the taxi trip data (Parquet file)
        String tripDataPath = "hdfs://hadoop-cluster/data/taxi/yellow_tripdata_2025-06.parquet";

        // Path to the taxi zone lookup data (CSV file)
        String zoneLookupPath = "hdfs://hadoop-cluster/data/taxi/taxi_zone_lookup.csv";

        // Output path for the results
        String outputPath = "hdfs://hadoop-cluster/output/q3_destination_stats.parquet";

        // Read the Parquet file
        Dataset<Row> tripDataDf = spark.read()
                .parquet(tripDataPath);

        // Read the CSV file, assuming it has a header
        Dataset<Row> zoneLookupDf = spark.read()
                .option("header", "true")
                .csv(zoneLookupPath);

        // Join the two DataFrames on the destination location ID
        Dataset<Row> joinedDf = tripDataDf.join(
            zoneLookupDf,
            tripDataDf.col("DOLocationID").equalTo(zoneLookupDf.col("LocationID")),
            "inner"
        );

        // Group by Borough and calculate the sum of total_amount and the count of trips
        Dataset<Row> destinationStats = joinedDf.groupBy("Borough")
                                                .agg(sum("total_amount").as("total_amount_sum"),
                                                     count("*").as("trip_count"))
                                                .orderBy(desc("total_amount_sum"));

        // Instead of showing the results, save the DataFrame to HDFS
        destinationStats.write()
                        .mode(SaveMode.Overwrite)
                        .parquet(outputPath);

        System.out.println("Destination stats saved to: " + outputPath);

        spark.stop();
    }
}