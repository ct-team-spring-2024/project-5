import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadHDFSFile {
    public static void main(String[] args) {
        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Read HDFS File in Java")
                .master("local[*]") // Use local mode; change for cluster
                .getOrCreate();

        // HDFS path to the file. This now refers to the HDFS NameService.
        String hdfsPath = "hdfs://hadoop-cluster/user/data/sample.csv";

        // Read CSV file into a DataFrame
        Dataset<Row> df = spark.read()
                .option("header", "true")           // Use first line as header
                .option("inferSchema", "true")      // Infer column types
                .csv(hdfsPath);

        // Show the first 10 rows
        df.show(10);

        // Print schema
        df.printSchema();

        // Count number of rows
        System.out.println("Total rows: " + df.count());

        // Stop SparkSession
        spark.stop();
    }
}
