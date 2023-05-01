package github.jhchee.agg;

import github.jhchee.schema.TargetTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;

public class MergeSnapshot {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                                         .appName("Merge snapshot read")
                                         .config("spark.sql.warehouse.dir", "s3a://spark/")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                                         .enableHiveSupport()
                                         .getOrCreate();
        Dataset<Row> empty = spark.createDataFrame(Collections.emptyList(), TargetTable.SCHEMA);
        if (!spark.catalog().tableExists("default", "target")) {
            empty.writeTo("default.target")
                 .using("iceberg")
                 .create();
        }

        // snapshot read from a
        spark.table("default.source_a")
             .createOrReplaceTempView("source_a");

        spark.sql("" +
                "MERGE INTO default.target as target USING source_a as source ON target.userId = source.userId " +
                "WHEN MATCHED THEN UPDATE SET target.persona = struct(source.favoriteEsports), target.updatedAt = source.updatedAt " +
                "WHEN NOT MATCHED THEN INSERT (userId, info, persona, updatedAt) " +
                "VALUES (source.userId, NULL, struct(source.favoriteEsports), source.updatedAt)" +
                "");

        Dataset<Row> df = spark.sql("SELECT * FROM default.target");
        df.show();

        spark.table("default.source_b")
             .createOrReplaceTempView("source_b");

        spark.sql("" +
                "MERGE INTO default.target as target USING source_a as source ON target.userId = source.userId " +
                "WHEN MATCHED THEN UPDATE SET target.persona = struct(source.favoriteEsports), target.updatedAt = source.updatedAt " +
                "WHEN NOT MATCHED THEN INSERT (userId, info, persona, updatedAt) " +
                "VALUES (source.userId, NULL, struct(source.favoriteEsports), source.updatedAt)" +
                "");

        System.out.println("Total count: " + df.count());
    }
}
