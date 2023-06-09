package github.jhchee.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TargetTable {
    public static StructType SCHEMA = root();
    public static String TABLE_NAME = "target";
    public static String PATH = "s3a://spark/target/";

    private static StructType root() {
        StructType schema = new StructType();
        schema = schema.add(new StructField("userId", DataTypes.StringType, false, Metadata.empty()));
        schema = schema.add(new StructField("updatedAt", DataTypes.TimestampType, false, Metadata.empty()));
        schema = schema.add("info", info(), true);
        schema = schema.add("persona", persona(), true);
        return schema;
    }

    private static StructType info() {
        StructType schema = new StructType();
        schema = schema.add(new StructField("name", DataTypes.StringType, true, Metadata.empty()));
        return schema;
    }
    private static StructType persona() {
        StructType schema = new StructType();
        schema = schema.add(new StructField("favoriteEsports", DataTypes.StringType, true, Metadata.empty()));
        return schema;
    }
}
