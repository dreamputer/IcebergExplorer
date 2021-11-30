package com.dreamputer.icebergexplorer;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.iceberg.expressions.Expressions.*;

public class IcebergPartitionEvolution {
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
        SparkSession spark = SparkSession.builder()
                .appName("IcebergPartitionEvolution")
                .master("local")
                .config(config)
                .getOrCreate();

        // ============================================================================
        // Partition Evolution: day() to month()
        // ============================================================================
        // CREATE TABLE TestBronzeDayMonth AND INSERT DATA
        spark.sql("DROP TABLE IF EXISTS local.db.TestBronzeDayMonth").show();
        String createTestBronzeDayMonthSQL = "CREATE TABLE IF NOT EXISTS local.db.TestBronzeDayMonth (" +
                "Id int, " +
                "Name string, " +
                "partitionKeyColumn timestamp) USING iceberg " +
                "PARTITIONED BY (days(partitionKeyColumn)) " +
                "TBLPROPERTIES('cloud-table.data-retention-sql'='DELETE FROM {{tableName}} WHERE partitionKeyColumn <= date_sub(CAST(current_timestamp() AS DATE), {{retentionDays}})')";
        spark.sql(createTestBronzeDayMonthSQL).show();
        String insertTestBronzeDayMonthSQL = "INSERT INTO local.db.TestBronzeDayMonth VALUES " +
                "(1, 'c1', cast(unix_timestamp('2021-11-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(2, 'c2', cast(unix_timestamp('2021-11-02 01:01:01', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(3, 'c3', cast(unix_timestamp('2021-10-01 02:02:02', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(4, 'c4', cast(unix_timestamp('2021-09-01 03:03:03', 'yyyy-MM-dd HH:mm:ss') as timestamp) )";
        spark.sql(insertTestBronzeDayMonthSQL).show();

        // BEFORE PARTITION EVOLUTION:
        // data
        //     partitionKeyColumn_day=2021-09-01
        //     partitionKeyColumn_day=2021-10-01
        //     partitionKeyColumn_day=2021-11-01
        //     partitionKeyColumn_day=2021-11-02

        // LOAD ICEBERG TABLE
        HadoopTables tables = new HadoopTables(spark.sparkContext().hadoopConfiguration());
        Table table = tables.load("spark-warehouse/db/TestBronzeDayMonth");

        // https://www.mail-archive.com/dev@iceberg.apache.org/msg02279.html
        // A dropped partition column is actually converted to a null transform instead of
        // being actually dropped. This is because the partition spec is not versioned in v1.

        /*
        table.updateSpec()
                .addField(month("partitionKeyColumn"))
                .commit();
        for(var field : table.spec().fields()) {
            System.out.println(field.name());
            System.out.println(field.transform());
            // partitionKeyColumn_day      day
            // partitionKeyColumn_month    month
        }
         */

        table.updateSpec()
                .addField(month("partitionKeyColumn"))
                .removeField("partitionKeyColumn_day")  // table.spec().fields().get(table.spec().fields().size() - 1).name()
                .commit();
        for(var field : table.spec().fields()) {
            System.out.println(field.name());
            System.out.println(field.transform());
            // partitionKeyColumn_day    void
            // partitionKeyColumn_month  month
            //     table.spec().fields().get(1).transform().getClass().getName()
            //     org.apache.iceberg.transforms.VoidTransform   org.apache.iceberg.transforms.Timestamps
        }

        // FORCE TABLE REFRESH
        table.refresh();
        spark = spark.cloneSession();

        String insertTestBronzeDayMonthSQL2 = "INSERT INTO local.db.TestBronzeDayMonth VALUES " +
                "(5, 'c5', cast(unix_timestamp('2021-11-03 00:00:00', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(6, 'c6', cast(unix_timestamp('2021-11-04 01:01:01', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(7, 'c7', cast(unix_timestamp('2021-12-01 02:02:02', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(8, 'c8', cast(unix_timestamp('2021-12-02 03:03:03', 'yyyy-MM-dd HH:mm:ss') as timestamp) )";
        spark.sql(insertTestBronzeDayMonthSQL2).show();

        // AFTER PARTITION EVOLUTION:
        // data
        //     partitionKeyColumn_day=2021-09-01
        //     partitionKeyColumn_day=2021-10-01
        //     partitionKeyColumn_day=2021-11-01
        //     partitionKeyColumn_day=2021-11-02
        //     partitionKeyColumn_day=null
        //         partitionKeyColumn_month=2021-11
        //         partitionKeyColumn_month=2021-12


        // ============================================================================
        // Partition Evolution: bucket
        // ============================================================================
        // CREATE TABLE TestBronzeBucket AND INSERT DATA
        spark.sql("DROP TABLE IF EXISTS local.db.TestBronzeBucket").show();
        String createTestBronzeBucketSQL = "CREATE TABLE IF NOT EXISTS local.db.TestBronzeBucket (" +
                "Id int, " +
                "Name string, " +
                "partitionKeyColumn timestamp) USING iceberg " +
                "PARTITIONED BY (bucket(4, id)) " +  //16
                "TBLPROPERTIES('cloud-table.data-retention-sql'='delete * from {{tableName}} where partitionKeyColumn >= date_sub(CAST(current_timestamp() AS DATE), {{retentionDays}})')";
        spark.sql(createTestBronzeBucketSQL).show();
        String insertTestBronzeBucketSQL = "INSERT INTO local.db.TestBronzeBucket VALUES " +
                "(1, 'c1', cast(unix_timestamp('2021-11-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(2, 'c2', cast(unix_timestamp('2021-11-02 01:01:01', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(3, 'c3', cast(unix_timestamp('2021-10-01 02:02:02', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(4, 'c4', cast(unix_timestamp('2021-09-01 03:03:03', 'yyyy-MM-dd HH:mm:ss') as timestamp) )";
        spark.sql(insertTestBronzeBucketSQL).show();

        // LOAD ICEBERG TABLE
        HadoopTables tables1 = new HadoopTables(spark.sparkContext().hadoopConfiguration());
        Table table1 = tables.load("spark-warehouse/db/TestBronzeBucket");

        // https://www.mail-archive.com/dev@iceberg.apache.org/msg02279.html
        // A dropped partition column is actually converted to a null transform instead of
        // being actually dropped. This is because the partition spec is not versioned in v1.

        /*
        table1.updateSpec()
                .addField(bucket("Id", 32))
                .commit();
        for(var field : table1.spec().fields()) {
            System.out.println(field.name());
            System.out.println(field.transform());
            // Id_bucket       bucket[16]
            // Id_bucket_32    bucket[32]
            //     table1.spec().fields().get(0).transform().getClass().getName()
            //     org.apache.iceberg.transforms.Bucket$BucketInteger
        }
        */

        table1.updateSpec()
                .addField(bucket("Id", 8))  //32
                .removeField(table1.spec().fields().get(table1.spec().fields().size() - 1).name())  // "Id_bucket_16"
                .commit();
        for(var field : table1.spec().fields()) {
            System.out.println(field.name());
            System.out.println(field.transform());
            // Id_bucket       void
            // Id_bucket_32    bucket[32]
        }

        // table.spec().fields().get(0).name();
        // table.spec().fields().get(2).transform() instanceof Bucket;
        // table.spec().fields().get(table.spec().fields().size() - 1).transform().getClass().getName();  // org.apache.iceberg.transforms.Bucket$BucketString

        // INSERT MORE RECORDS AFTER PARTITION EVOLUTION
        table1.refresh();
        spark = spark.cloneSession();
        String insertTestBronzeBucketSQL2 = "INSERT INTO local.db.TestBronzeBucket VALUES " +
                "(5, 'c5', cast(unix_timestamp('2021-11-05 00:00:00', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(6, 'c6', cast(unix_timestamp('2021-11-06 01:01:01', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(7, 'c7', cast(unix_timestamp('2021-12-07 02:02:02', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(8, 'c8', cast(unix_timestamp('2021-12-08 03:03:03', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(9, 'c9', cast(unix_timestamp('2021-12-09 00:00:00', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(10, 'c10', cast(unix_timestamp('2022-01-01 01:01:01', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(11, 'c11', cast(unix_timestamp('2022-01-01 02:02:02', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(12, 'c12', cast(unix_timestamp('2022-01-02 03:03:03', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(13, 'c13', cast(unix_timestamp('2022-01-03 00:00:00', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(14, 'c14', cast(unix_timestamp('2022-01-04 01:01:01', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(15, 'c15', cast(unix_timestamp('2022-01-01 02:02:02', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(16, 'c16', cast(unix_timestamp('2022-01-01 03:03:03', 'yyyy-MM-dd HH:mm:ss') as timestamp) )";
        spark.sql(insertTestBronzeBucketSQL2).show();

        // ============================================================================
        // Partition Evolution: (bucket(16, ...), days(...), identityCol, truncate(..., 5))
        // ============================================================================
        // CREATE TABLE TestBronzePartition AND INSERT DATA
        spark.sql("DROP TABLE IF EXISTS local.db.TestBronzePartition").show();
        String createTestBronzePartitionSQL = "CREATE TABLE IF NOT EXISTS local.db.TestBronzePartition (" +
                "Id bigint COMMENT 'unique id', " +
                "Data string, " +
                "Category string, " +
                "ts timestamp) USING iceberg " +
                "PARTITIONED BY (bucket(16, Id), days(ts), Category, truncate(Data, 5)) " +
                "TBLPROPERTIES('cloud-table.data-retention-sql'='delete * from {{tableName}} where partitionKeyColumn >= date_sub(CAST(current_timestamp() AS DATE), {{retentionDays}})')";
        spark.sql(createTestBronzePartitionSQL).show();
        String insertTestBronzePartitionSQL = "INSERT INTO local.db.TestBronzePartition VALUES " +
                "(1, 'd12345',  'c1', cast(unix_timestamp('2021-11-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(2, 'd67890',  'c2', cast(unix_timestamp('2021-11-02 01:01:01', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(3, 'd111213', 'c3', cast(unix_timestamp('2021-10-01 02:02:02', 'yyyy-MM-dd HH:mm:ss') as timestamp) ), " +
                "(4, 'd141516', 'c4', cast(unix_timestamp('2021-09-01 03:03:03', 'yyyy-MM-dd HH:mm:ss') as timestamp) )";
        spark.sql(insertTestBronzePartitionSQL).show();

        // LOAD ICEBERG TABLE
        HadoopTables tables2 = new HadoopTables(spark.sparkContext().hadoopConfiguration());
        Table table2 = tables.load("spark-warehouse/db/TestBronzePartition");

        String partitionFieldToRemove = "";
        for(var field : table2.spec().fields()) {
            System.out.println(field.name());
            System.out.println(field.transform());
            // Id_bucket        bucket[16]
            // ts_day           day
            // Category         identity
            // Data_trunc       truncate[5]
            if(field.name().startsWith("Id_bucket") && field.transform().toString().equals("bucket[16]")) {
                partitionFieldToRemove = field.name();
            }
        }

        table2.updateSpec()
                .addField(bucket("Id", 32))
                .removeField(partitionFieldToRemove)  // "Id_bucket"
                .commit();
        for(var field : table2.spec().fields()) {
            System.out.println(field.name());
            System.out.println(field.transform());
            // Id_bucket       void
            // ts_day          day
            // Category        identity
            // Data_trunc      truncate[5]
            // Id_bucket_32    bucket[32]
        }

        // data
        //     Id_bucket=3
        //         ts_day=2021-10-01
        //             Category=c3
        //                 Data_trunc=d1112


        // ============================================================================
        // Partition Evolution: CompanyXLogs
        // ============================================================================
        // https://developer.ibm.com/articles/the-why-and-how-of-partitioning-in-apache-iceberg/
        spark.sql("DROP TABLE IF EXISTS local.db.CompanyXLogs").show();
        String table3Path = "spark-warehouse/db/CompanyXLogs";

        HadoopTables tables3 = new HadoopTables(spark.sparkContext().hadoopConfiguration());

        var tableschema = new Schema(
                Types.NestedField.optional(1, "time", Types.TimestampType.withZone()),
                Types.NestedField.optional(2, "id", Types.IntegerType.get()),
                Types.NestedField.optional(3, "data", Types.StringType.get())
        );

        var spec = PartitionSpec.builderFor(tableschema).month("time").build();
        Table logtable = tables3.create(tableschema, spec, table3Path);

        String file_location = "src/main/resources/CompanyXLogs.csv";
        var schema = "time INTEGER, id INTEGER, data STRING";

        var data = spark.read().format("csv")
                .option("delimiter", ",")
                .schema(schema)
                .load(file_location);

        try {
            data.select(
                    data.col("time").cast(DataTypes.TimestampType),
                    data.col("id"),
                    data.col("data")
            ).where(data.col("time").lt(1230768000))
                    .sort(data.col("time"))
                    .writeTo("local.db.CompanyXLogs").append(); // NOTE .createOrReplace() overrides spec!!!
        } catch(org.apache.spark.sql.catalyst.analysis.NoSuchTableException ex) {
            System.out.println(ex);
        }

        for(var field : logtable.spec().fields()) {
            System.out.println(field.name());
            System.out.println(field.transform());
            // time_month        month
        }

        // var newspec = PartitionSpec.builderFor(logtable.schema()).day("time").build();
        logtable.updateSpec()
                .addField(day("time"))
                .removeField("time_month")
                .commit();
        logtable.refresh();  // NOT REFRESHED??????

        // WORKAROUND Table.refresh(): https://github.com/apache/iceberg/issues/2972
        spark = spark.cloneSession();
        tables3 = new HadoopTables(spark.sparkContext().hadoopConfiguration());
        logtable = tables3.load(table3Path);
        data = spark.read().format("csv")
                .option("delimiter", ",")
                .schema(schema)
                .load(file_location);

        for(var field : logtable.spec().fields()) {
            System.out.println(field.name());
            System.out.println(field.transform());
            // time_month        void
            // time_day          day
        }

        //val metadata = catalog.newTableOps(name)
        //val baseMetadata = metadata.current()
        //val newMetadata = baseMetadata.updatePartitionSpec(newspec)
        //metadata.commit(baseMetadata, newmeta)
        //table.refresh()

        try {
            data.select(
                    data.col("time").cast(DataTypes.TimestampType),
                    data.col("id"),
                    data.col("data")
            ).where(data.col("time").gt(1230768000))
                    .sort(data.col("time"))
                    .writeTo("local.db.CompanyXLogs").append(); // NOTE .createOrReplace() overrides spec!!!
        } catch(org.apache.spark.sql.catalyst.analysis.NoSuchTableException ex) {
            System.out.println(ex);
        }

        // data
        //     time_month=2008-11
        //     time_month=2008-12
        //     time_month=null
        //         time_day=2009-01-01
        //         time_day=2009-01-03
        //         time_day=2009-01-12

        spark.table("local.db.CompanyXLogs").createOrReplaceTempView("logtable");
        spark.sql("SELECT * FROM logtable WHERE time > '2008-12-14' AND time < '2009-1-14'").show();
    }
}