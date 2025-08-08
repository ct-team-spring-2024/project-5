import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class Task1 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Task1")
                .getOrCreate();

        // Path to the dataset in HDFS
        String hdfsPath = "hdfs://hadoop-cluster/data/taxi/yellow_tripdata_2025-06.parquet";
        // Output path in HDFS
        String outputPath = "hdfs://hadoop-cluster/output/q1_long_trips.parquet";

        // Read the Parquet file from HDFS
        Dataset<Row> df = spark.read()
                .parquet(hdfsPath);

        // Filter for trips with distance > 5 miles and passenger_count > 2
        // Add a new column for trip duration in minutes
        // Order the result by duration in descending order
        Dataset<Row> filteredTrips = df.filter(col("trip_distance").gt(5))
                                       .filter(col("passenger_count").gt(2))
                                       .withColumn("duration_minutes",
                                            (unix_timestamp(col("tpep_dropoff_datetime"))
                                           .minus(unix_timestamp(col("tpep_pickup_datetime"))))
                                           .divide(60).cast(DataTypes.IntegerType))
                                       .orderBy(desc("duration_minutes"));

        // Save the filtered DataFrame as a Parquet file to the specified HDFS path
        // Use SaveMode.Overwrite to replace the file if it already exists
        filteredTrips.write()
                     .mode(SaveMode.Overwrite)
                     .parquet(outputPath);

        System.out.println("Output saved to: " + outputPath);

        spark.stop();
    }
}