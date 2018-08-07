package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Test {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Connect Server")
                .config("spark.sql.parquet.binaryAsString", "true")
                .config("spark.yarn.access.hadoopFileSystems", "hdfs://192.168.23.200:9000")
                .getOrCreate();
        Dataset<Row> logDF = spark.read().parquet(args[0]);
        logDF.show(1000,false);
    }
}
