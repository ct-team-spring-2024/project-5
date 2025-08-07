import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class ParquetTLCTripAnalyzer {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Parquet TLC Trip Data Analysis")
                .getOrCreate();

        // Path to the dataset in HDFS (assuming a Parquet file)
        String hdfsPath = "hdfs://hadoop-cluster/data/taxi/yellow_tripdata_2024-03.parquet";

        // Read the Parquet file from HDFS
        Dataset<Row> df = spark.read()
                .parquet(hdfsPath);

        // Perform a simple analysis: count trips by payment type
        Dataset<Row> paymentCounts = df.groupBy("payment_type")
                                     .count()
                                     .orderBy(desc("count"));

        // Show the results
        System.out.println("Trip counts by payment type:");
        paymentCounts.show();

        spark.stop();
    }
}