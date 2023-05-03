package github.jhchee.agg;

import github.jhchee.IcebergUtils;
import github.jhchee.schema.SourceATable;
import github.jhchee.schema.SourceBTable;
import github.jhchee.schema.TargetTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;

public class MergeSnapshot {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                                         .appName("Merge snapshot read")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                                         .getOrCreate();
        // Create table if it doesn't exist
        if (!IcebergUtils.tableExists(spark, TargetTable.TABLE_NAME)) {
            Dataset<Row> empty = spark.createDataFrame(Collections.emptyList(), TargetTable.SCHEMA);
            empty.writeTo(TargetTable.TABLE_NAME)
                 .tableProperty("location", TargetTable.PATH)
                 .using("iceberg")
                 .tableProperty("format-version", "2")
                 .create();
        }

        // Merge from source a
        spark.table(SourceATable.TABLE_NAME).createOrReplaceTempView("source");
        spark.sql("" +
                "MERGE INTO default.target as target USING source ON target.userId = source.userId " +
                "WHEN MATCHED THEN UPDATE SET target.persona = struct(source.favoriteEsports), target.updatedAt = source.updatedAt " +
                "WHEN NOT MATCHED THEN INSERT (userId, info, persona, updatedAt) " +
                "VALUES (source.userId, NULL, struct(source.favoriteEsports), source.updatedAt)" +
                "");

        // Merge from source b
        spark.table(SourceBTable.TABLE_NAME).createOrReplaceTempView("source");
        spark.sql("" +
                "MERGE INTO default.target as target USING source ON target.userId = source.userId " +
                "WHEN MATCHED THEN UPDATE SET target.info = struct(source.name), target.updatedAt = source.updatedAt " +
                "WHEN NOT MATCHED THEN INSERT (userId, info, persona, updatedAt) " +
                "VALUES (source.userId, struct(source.name), NULL, source.updatedAt)" +
                "");

        // Sanity check
        Dataset<Row> df = spark.sql("SELECT * FROM default.target");
        df.show();
        System.out.println("Total count: " + df.count());
    }
}
