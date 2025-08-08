import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Task4 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Daily Max Tip")
                .getOrCreate();

        // Path to the dataset in HDFS
        String hdfsPath = "hdfs://hadoop-cluster/data/taxi/yellow_tripdata_2025-06.parquet";

        // Output path for the results
        String outputPath = "hdfs://hadoop-cluster/output/q4_max_tip_per_day.parquet";

        // Read the Parquet file from HDFS
        Dataset<Row> df = spark.read()
                .parquet(hdfsPath);

        // Extract the date from the pickup datetime and calculate the max tip amount for each day
        Dataset<Row> dailyMaxTip = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
                                     .groupBy("pickup_date")
                                     .agg(max("tip_amount").as("max_tip_amount"))
                                     .orderBy(col("pickup_date"));

        // Save the DataFrame to HDFS in Parquet format
        dailyMaxTip.write()
                   .mode(SaveMode.Overwrite)
                   .parquet(outputPath);

        System.out.println("Highest tip amount for each day saved to: " + outputPath);

        spark.stop();
    }
}