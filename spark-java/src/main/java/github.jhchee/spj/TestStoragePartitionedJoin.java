package github.jhchee.spj;

import github.jhchee.util.IcebergWrapper;
import github.jhchee.util.MockUtil;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.internal.SQLConf;

import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class TestStoragePartitionedJoin extends IcebergWrapper {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = getSession("TestStoragePartitionedJoin");
        spark.udf().register("randomUUID", new MockUtil.RandomUUID(), StringType);
        spark.udf().register("randomName", new MockUtil.RandomName(), StringType);
        spark.udf().register("randomNationality", new MockUtil.RandomNationality(), StringType);
        prepareSource(spark);
        prepareTarget(spark);
        Map<String, String> ENABLED_SPJ_SQL_CONF =
                Map.of(
                        SQLConf.PREFER_SORTMERGEJOIN().key(),
                        "false",
                        SQLConf.V2_BUCKETING_ENABLED().key(),
                        "true",
                        SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED().key(),
                        "true",
                        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION().key(),
                        "false",
                        SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
                        "false",
                        SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED().key(),
                        "true",
                        SparkSQLProperties.PRESERVE_DATA_GROUPING,
                        "true",
                        "spark.sql.shuffle.partitions",
                        "4");

        Map<String, String> DISABLED_SPJ_SQL_CONF =
                Map.of(
                        SQLConf.V2_BUCKETING_ENABLED().key(),
                        "false",
                        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION().key(),
                        "false",
                        SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
                        "false",
                        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD().key(),
                        "-1",
                        SparkSQLProperties.PRESERVE_DATA_GROUPING,
                        "true");

        DISABLED_SPJ_SQL_CONF.forEach((k, v) -> spark.sql(String.format("SET `%s`=%s", k, v)));

//        spark.sql("SET spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB");
//        spark.sql("SET spark.sql.join.preferSortMergeJoin=false");
//        spark.sql("SET spark.sql.requireAllClusterKeysForCopartition=false");
//        spark.sql("SET `spark.sql.iceberg.planning.preserve-data-grouping`=true");
//        spark.sql("SET spark.sql.sources.v2.bucketing.enabled=true");
//        spark.sql("SET spark.sql.sources.v2.bucketing.pushPartValues.enabled=true");
//        spark.sql("SET spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled=true");

        spark.sql("""
                  MERGE INTO demo.default.users_target t
                  USING demo.default.users s
                    ON t.user_id = s.user_id
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                  """);
    }

    public static void prepareSource(SparkSession spark) throws NoSuchTableException {
        Dataset<Row> df = spark.range(1, 1_000_000)
                               .toDF("user_id")
                               .withColumn("name", org.apache.spark.sql.functions.callUDF("randomName"))
                               .withColumn("uuid", org.apache.spark.sql.functions.callUDF("randomUUID"))
                               .withColumn("nationality", org.apache.spark.sql.functions.callUDF("randomNationality"));

        spark.sql("DROP TABLE IF EXISTS demo.default.users PURGE");

        spark.sql("""
                  CREATE TABLE demo.default.users (
                    user_id bigint,
                    name string,
                    uuid string,
                    nationality string)
                  USING iceberg
                  PARTITIONED BY (bucket(4, user_id))
                  """);
        df.writeTo("demo.default.users")
          .append();
    }

    public static void prepareTarget(SparkSession spark) throws NoSuchTableException {
        Dataset<Row> df = spark.range(1, 1_000_000)
                               .toDF("user_id")
                               .withColumn("name", org.apache.spark.sql.functions.callUDF("randomName"))
                               .withColumn("uuid", org.apache.spark.sql.functions.callUDF("randomUUID"))
                               .withColumn("nationality", org.apache.spark.sql.functions.callUDF("randomNationality"));

        spark.sql("DROP TABLE IF EXISTS demo.default.users_target PURGE");

        spark.sql("""
                  CREATE TABLE demo.default.users_target (
                    user_id bigint,
                    name string,
                    uuid string,
                    nationality string)
                  USING iceberg
                  PARTITIONED BY (bucket(4, user_id))
                  """);

        spark.sql("""
                  ALTER TABLE demo.default.users_target
                  SET TBLPROPERTIES('write.spark.fanout.enabled'='true')
                  """);

        df.writeTo("demo.default.users_target")
          .append();
    }
}