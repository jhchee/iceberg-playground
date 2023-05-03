package github.jhchee;

import org.apache.spark.sql.SparkSession;

public class DropTable {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("Drop Iceberg tables.")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .enableHiveSupport()
                                         .getOrCreate();

        spark.sql("DROP TABLE IF EXISTS source_a");
        spark.sql("DROP TABLE IF EXISTS source_b");
        spark.sql("DROP TABLE IF EXISTS target");
    }
}
