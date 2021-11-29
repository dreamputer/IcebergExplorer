package com.dreamputer.icebergexplorer;

import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

//import org.apache.iceberg.transforms.Bucket;

public class IcebergTableProperties {

    public static void main(String[] args) {
        // SET UP SPARK CONFIG AND SPARK SESSION
        SparkConf config = new SparkConf();
        config.set("spark.sql.legacy.createHiveTableByDefault", "false");
        config.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        config.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
        config.set("spark.sql.catalog.spark_catalog.type", "hive");
        config.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog");
        config.set("spark.sql.catalog.local.type", "hadoop");
        config.set("spark.sql.catalog.local.warehouse", "spark-warehouse");
        SparkSession spark = SparkSession
                .builder()
                .appName("IcebergTableProperties")
                .master("local")
                .config(config)
                .getOrCreate();

        //static final String LAKEHOUSE_BRONZE_ARRIVAL_TIME_COLUMN_NAME = "815d3b5701b94c78884835c1bea174bb";
        //public static final String LAKEHOUSE_BRONZE_DROPPED_ERROR_TIMESTAMP_COLUMN_NAME = "error_timestamp";

        spark.sql("DROP TABLE IF EXISTS local.db.TestBronzeDay").show();
        spark.sql("DROP TABLE IF EXISTS local.db.TestBronzeMonth").show();

        // CREATE TABLE TestBronzeDay AND INSERT DATA
        String createTestBronzeDaySQL = "CREATE TABLE IF NOT EXISTS local.db.TestBronzeDay (" +
                "Id int, " +
                "Name string, " +
                "partitionKeyColumn timestamp) USING iceberg " +
                "PARTITIONED BY (days(partitionKeyColumn)) " +
                "TBLPROPERTIES('cloud-table.data-retention-sql'='DELETE FROM {{tableName}} WHERE partitionKeyColumn <= date_sub(CAST(current_timestamp() AS DATE), {{retentionDays}})')";
        spark.sql(createTestBronzeDaySQL).show();
        String insertTestBronzeDaySQL = "INSERT INTO local.db.TestBronzeDay VALUES " +
                "(1, 'c1', cast(unix_timestamp('2021-11-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(2, 'c2', cast(unix_timestamp('2021-11-02 01:01:01', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(3, 'c3', cast(unix_timestamp('2021-10-01 02:02:02', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(4, 'c4', cast(unix_timestamp('2021-09-01 03:03:03', 'yyyy-MM-dd HH:mm:ss') as timestamp) )";
        spark.sql(insertTestBronzeDaySQL).show();

        // LOAD ICEBERG TABLE, GET PROPERTIES, AND PERFORM DELETION
        HadoopTables tables = new HadoopTables(spark.sparkContext().hadoopConfiguration());
        Table table = tables.load("spark-warehouse/db/TestBronzeDay");

        String retentionSqlTemplate = table.properties().get("cloud-table.data-retention-sql");
        String retentionSql = retentionSqlTemplate
                .replace("{{tableName}}", "local.db.TestBronzeDay")
                .replace("{{retentionDays}}", "30");

        // DELETE FROM local.db.TestBronzeDay where partitionKeyColumn <= date_sub(CAST(current_timestamp() AS DATE), 30)
        spark.sql(retentionSql).show();
        spark.sql("SELECT * FROM local.db.TestBronzeDay").show();
        //          +---+----+-------------------+
        //          | Id|Name| partitionKeyColumn|
        //          +---+----+-------------------+
        //          |  1|  c1|2021-11-01 00:00:00|
        //          |  2|  c2|2021-11-02 01:01:01|
        //          +---+----+-------------------+

        // SET TABLE PROPERTIES (table.properties() is immutable!) - OK to set existing table properties
        table.updateProperties().set("cloudtable.aaa", "value of cloudtable.aaa").commit();
        table.updateProperties().set("cloud-table.data-retention-sql", "DELETE FROM {{aaa}}}").commit();

        // GET TABLE PROPERTIES
        Map<String, String> properties = new HashMap<>();
        for(var prop: table.properties().entrySet()){
            properties.put(prop.getKey(), prop.getValue());
        }
        System.out.println(properties);

        // CREATE TABLE TestBronzeMonth AND INSERT DATA, PERFORM DELETE
        String createTestBronzeMonthSQL = "CREATE TABLE IF NOT EXISTS local.db.TestBronzeMonth (" +
                "Id int, " +
                "Name string, " +
                "partitionKeyColumn timestamp) USING iceberg " +
                "PARTITIONED BY (months(partitionKeyColumn)) " +
                "TBLPROPERTIES('cloud-table.data-retention-sql'='delete * from {{tableName}} where partitionKeyColumn >= date_sub(CAST(current_timestamp() AS DATE), {{retentionDays}})')";
        spark.sql(createTestBronzeMonthSQL).show();
        String insertTestBronzeMonthSQL = "INSERT INTO local.db.TestBronzeMonth VALUES " +
                "(1, 'c1', cast(unix_timestamp('2021-11-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(2, 'c2', cast(unix_timestamp('2021-11-02 01:01:01', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(3, 'c3', cast(unix_timestamp('2021-10-01 02:02:02', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(4, 'c4', cast(unix_timestamp('2021-09-01 03:03:03', 'yyyy-MM-dd HH:mm:ss') as timestamp) )";
        spark.sql(insertTestBronzeMonthSQL).show();
        spark.sql("SELECT * FROM local.db.TestBronzeMonth").show();
        spark.sql("DELETE FROM local.db.TestBronzeMonth where partitionKeyColumn <= date_sub(CAST(current_timestamp() AS DATE), 30)").show();
        spark.sql("SELECT * FROM local.db.TestBronzeMonth").show();
    }
}
