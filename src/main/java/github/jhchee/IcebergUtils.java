package github.jhchee;

import org.apache.spark.sql.SparkSession;

public class IcebergUtils {
    public static boolean tableExists(SparkSession spark, String tableName) {
        try {
            spark.read().table(tableName);
            return true;
        } catch (Exception exception) {
            return false;
        }
    }
}