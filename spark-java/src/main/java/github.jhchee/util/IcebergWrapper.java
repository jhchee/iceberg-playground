package github.jhchee.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class IcebergWrapper {
    public static SparkSession getSession(String appName) {
        SparkConf conf = new SparkConf();
        conf.setAppName(appName);
        conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        conf.set("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog");
        conf.set("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        developmentConfig(conf);
        return SparkSession.builder()
                           .config(conf)
                           .getOrCreate();
    }

    public static void developmentConfig(SparkConf conf) {
        conf.set("spark.master", "spark://localhost:7077");
        conf.set("spark.sql.catalogImplementation", "in-memory");
        conf.set("spark.sql.defaultCatalog", "demo");
        conf.set("spark.sql.catalog.demo.type", "rest");
        conf.set("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh/");
        conf.set("spark.sql.catalog.demo.uri", "http://localhost:8181");
        conf.set("spark.sql.catalog.demo.s3.endpoint", "http://127.0.0.1:9000");
        conf.set("spark.sql.catalog.demo.s3.access-key-id", "admin");
        conf.set("spark.sql.catalog.demo.s3.secret-access-key", "password");
    }
}