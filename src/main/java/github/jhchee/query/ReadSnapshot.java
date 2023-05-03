package github.jhchee.query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadSnapshot {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("Read snapshot.")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                                         .enableHiveSupport()
                                         .getOrCreate();

        Dataset<Row> df = spark.sql("SELECT * FROM default.source_a");
        df.show();
        System.out.println(df.count());
    }
}
