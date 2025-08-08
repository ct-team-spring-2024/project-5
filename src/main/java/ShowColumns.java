import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ShowColumns {
    public static void main(String[] args) {
        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Show Parquet Columns")
                .getOrCreate();

        // Path to the dataset in HDFS
        String hdfsPath = "hdfs://hadoop-cluster/data/taxi/yellow_tripdata_2025-06.parquet";

        // Read the Parquet file from HDFS
        Dataset<Row> df = spark.read()
                .parquet(hdfsPath);

        // Print the schema to show all columns and their data types
        System.out.println("Schema of the Parquet file:");
        df.printSchema();

        // To get an array of just the column names
        String[] columnNames = df.columns();
        System.out.println("\nColumn names:");
        for (String col : columnNames) {
            System.out.println(col);
        }

        // Stop the SparkSession
        spark.stop();
    }
}