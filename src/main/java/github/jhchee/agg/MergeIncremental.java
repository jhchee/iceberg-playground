package github.jhchee.agg;

import github.jhchee.IcebergUtils;
import github.jhchee.schema.SourceATable;
import github.jhchee.schema.SourceBTable;
import github.jhchee.schema.TargetTable;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Collections;
import java.util.concurrent.TimeoutException;

public class MergeIncremental {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                                         .appName("Merge incrementally")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                                         .enableHiveSupport()
                                         .getOrCreate();

        // Create table if it doesn't exist
        if (!IcebergUtils.tableExists(spark, TargetTable.TABLE_NAME)) {
            Dataset<Row> empty = spark.createDataFrame(Collections.emptyList(), TargetTable.SCHEMA);
            empty.writeTo(TargetTable.TABLE_NAME)
                 .using("iceberg")
                 .tableProperty("location", TargetTable.PATH)
                 .create();
        }

        String mergeFromSourceA = "MERGE INTO default.target as target USING source " +
                "ON target.userId = source.userId " +
                "WHEN MATCHED THEN UPDATE SET target.persona = struct(source.favoriteEsports), target.updatedAt = source.updatedAt " +
                "WHEN NOT MATCHED THEN INSERT (userId, info, persona, updatedAt) " +
                "VALUES (source.userId, NULL, struct(source.favoriteEsports), source.updatedAt)";

        String mergeFromSourceB = "MERGE INTO default.target as target USING source ON target.userId = source.userId " +
                "WHEN MATCHED THEN UPDATE SET target.info = struct(source.name), target.updatedAt = source.updatedAt " +
                "WHEN NOT MATCHED THEN INSERT (userId, info, persona, updatedAt) " +
                "VALUES (source.userId, struct(source.name), NULL, source.updatedAt)";

        incrementalMerge(spark, SourceATable.TABLE_NAME, TargetTable.TABLE_NAME, mergeFromSourceA);
        incrementalMerge(spark, SourceBTable.TABLE_NAME, TargetTable.TABLE_NAME, mergeFromSourceB);
    }

    public static void incrementalMerge(SparkSession spark, String sourceTable, String targetTable, String mergeQuery) throws TimeoutException, StreamingQueryException {
        Dataset<Row> source = spark.readStream()
                                   .format("iceberg")
                                   .load(sourceTable);
        String checkpointLocation = String.format("s3a://spark/checkpoint/merge%sInto%s", sourceTable, targetTable);
        DataStreamWriter<Row> dataStreamWriter = source.writeStream()
                                                       .format("iceberg")
                                                       .trigger(Trigger.AvailableNow())
                                                       .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (sourceDf, batchId) -> executeMergeQuery(sourceDf, batchId, mergeQuery))
                                                       .option("checkpointLocation", checkpointLocation);
        StreamingQuery query = dataStreamWriter.start();
        query.awaitTermination();
    }

    public static void executeMergeQuery(Dataset<Row> sourceDf, Long batchId, String mergeQuery) {
        System.out.println("Batch id: " + batchId);
        sourceDf.createOrReplaceTempView("source");
        sourceDf.sparkSession().sql(mergeQuery);
    }
}
