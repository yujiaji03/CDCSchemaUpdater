package com.cdcUp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlSubmit {

    public static void executeSparkSql(String sparkSql) {
        // 创建SparkSession并连接到Spark集群
        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL Submit")
                .master("local") // 可以根据需要修改为其他master URL，如yarn或spark://host:port
                .config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse")
                .enableHiveSupport()
                .config("hive.metastore.uris", "thrift://hadoop102:9083")
                .config("spark.sql.catalogImplementation", "hive")
                .config("spark.files.encoding", "UTF-8")
                .config("spark.hadoop.dfs.replication", "3")
                .getOrCreate();
        try {
            // 执行传入的SQL语句
            Dataset<Row> result = spark.sql(sparkSql);
            result.show();
            System.out.println("SQL执行成功: " + sparkSql);
        } catch (Exception e) {
            System.err.println("SQL执行失败: " + sparkSql);
            e.printStackTrace();
        } finally {
            // 关闭SparkSession
            spark.close();
        }
    }

    public static void main(String[] args) {
        executeSparkSql("ALTER TABLE hngs_hive_prd.ods_report_xk_chg_el_en_lane_run_quota_d_pt_df RENAME COLUMN `1111` TO `new` ");
    }

}