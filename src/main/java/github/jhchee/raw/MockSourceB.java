package github.jhchee.raw;

import com.github.javafaker.Faker;
import github.jhchee.IcebergUtils;
import github.jhchee.schema.SourceBTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class MockSourceB {
    private static final Faker faker = new Faker();

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                                         .appName("Mock data for source A.")
                                         .config("spark.sql.warehouse.dir", "s3a://spark/")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                                         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                                         .enableHiveSupport()
                                         .getOrCreate();

        spark.udf().register("fullName", fullName, DataTypes.StringType);
        spark.udf().register("age", age, DataTypes.IntegerType);
        spark.udf().register("streetName", streetName, DataTypes.StringType);
        spark.udf().register("buildingNumber", buildingNumber, DataTypes.StringType);
        spark.udf().register("city", city, DataTypes.StringType);
        spark.udf().register("country", country, DataTypes.StringType);

        Dataset<Row> mockUser = spark.read()
                                     .option("header", "true")
                                     .csv("s3a://spark/user_ids/")
                                     .withColumn("name", call_udf("fullName"))
                                     .withColumn("age", call_udf("age"))
                                     .withColumn("streetName", call_udf("streetName"))
                                     .withColumn("buildingNumber", call_udf("buildingNumber"))
                                     .withColumn("city", call_udf("city"))
                                     .withColumn("country", call_udf("country"))
                                     .withColumn("updatedAt", lit(current_timestamp()));

        // Create table if it doesn't exist
        if (!IcebergUtils.tableExists(spark, SourceBTable.TABLE_NAME)) {
            mockUser.writeTo(SourceBTable.TABLE_NAME)
                    .using("iceberg")
                    .tableProperty("location", SourceBTable.PATH)
                    .tableProperty("format-version", "2")
                    .create();
            return;
        }
        mockUser.createTempView("source");
        spark.sql("MERGE INTO default.source_b as target\n" +
                "USING source ON target.userId = source.userId\n" +
                "WHEN MATCHED THEN UPDATE SET *\n" +
                "WHEN NOT MATCHED THEN INSERT *");
    }

    public static UDF0<String> fullName = () -> faker.name().fullName();
    public static UDF0<String> streetName = () -> faker.address().streetName();
    public static UDF0<String> buildingNumber = () -> faker.address().buildingNumber();
    public static UDF0<String> city = () -> faker.address().city();
    public static UDF0<String> country = () -> faker.address().country();
    public static UDF0<Integer> age = () -> faker.number().numberBetween(18, 80);
}
