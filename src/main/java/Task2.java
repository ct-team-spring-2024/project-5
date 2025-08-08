import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Task2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Taxi Fare Analysis")
                .getOrCreate();

        // Path to the taxi trip data (Parquet file)
        String tripDataPath = "hdfs://hadoop-cluster/data/taxi/yellow_tripdata_2025-06.parquet";

        // Path to the taxi zone lookup data (CSV file)
        String zoneLookupPath = "hdfs://hadoop-cluster/data/taxi/taxi_zone_lookup.csv";

        // Output path for the results
        String outputPath = "hdfs://hadoop-cluster/output/q2_avg_fare_by_zone.parquet";

        // Read the Parquet file
        Dataset<Row> tripDataDf = spark.read()
                .parquet(tripDataPath);

        // Read the CSV file. We assume it has a header.
        Dataset<Row> zoneLookupDf = spark.read()
                .option("header", "true")
                .csv(zoneLookupPath);

        // Join the two DataFrames on the pickup location ID
        Dataset<Row> joinedDf = tripDataDf.join(
            zoneLookupDf,
            tripDataDf.col("PULocationID").equalTo(zoneLookupDf.col("LocationID")),
            "inner"
        );

        // Group by Zone and Borough and calculate the average fare_amount
        Dataset<Row> avgFares = joinedDf.groupBy("Zone", "Borough")
                                        .agg(avg("fare_amount").as("average_fare"))
                                        .orderBy(desc("average_fare"));

        // Save the DataFrame to HDFS in Parquet format
        avgFares.write()
                .mode(SaveMode.Overwrite)
                .parquet(outputPath);

        System.out.println("Average fare amount by Zone and Borough saved to: " + outputPath);

        spark.stop();
    }
}