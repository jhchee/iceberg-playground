package github.jhchee.raw;

import com.github.javafaker.Faker;
import github.jhchee.IcebergUtils;
import github.jhchee.ResourceUtils;
import github.jhchee.schema.SourceATable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class MockSourceA {
    private static final Faker faker = new Faker();

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                                         .appName("Mock data for source A.")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                                         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                                         .getOrCreate();

        spark.udf().register("favoriteEsports", favoriteEsports, DataTypes.StringType);
        spark.udf().register("favoriteArtist", favoriteArtist, DataTypes.StringType);
        spark.udf().register("favoriteColor", favoriteColor, DataTypes.StringType);
        spark.udf().register("favoriteHarryPotterCharacter", favoriteHarryPotterCharacter, DataTypes.StringType);

        Dataset<Row> mockUser = spark.read()
                                     .option("header", "true")
                                     .csv("s3a://spark/user_ids/")
                                     .withColumn("favoriteEsports", call_udf("favoriteEsports"))
                                     .withColumn("favoriteArtist", call_udf("favoriteArtist"))
                                     .withColumn("favoriteColor", call_udf("favoriteColor"))
                                     .withColumn("favoriteHarryPotterCharacter", call_udf("favoriteHarryPotterCharacter"))
                                     .withColumn("updatedAt", lit(current_timestamp()));

        // Create table if it doesn't exist
        if (!IcebergUtils.tableExists(spark, SourceATable.TABLE_NAME)) {
            mockUser.writeTo(SourceATable.TABLE_NAME)
                    .using("iceberg")
                    .tableProperty("location", SourceATable.PATH)
                    .tableProperty("format-version", "2")
                    .create();
            return;
        }
        mockUser.createTempView("source");

        spark.sql(ResourceUtils.getSQLQuery("merge_source_a.sql"));
    }

    // faker
    public static UDF0<String> favoriteEsports = () -> faker.esports().game();
    public static UDF0<String> favoriteArtist = () -> faker.artist().name();
    public static UDF0<String> favoriteColor = () -> faker.color().name();
    public static UDF0<String> favoriteHarryPotterCharacter = () -> faker.harryPotter().character();
}
