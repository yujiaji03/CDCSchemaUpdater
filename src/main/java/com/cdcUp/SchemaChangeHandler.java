package com.cdcUp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaChangeHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaChangeHandler.class);
    public static void parseAlterTable(String sql) {
        // 定义正则表达式模式，使关键字大小写不敏感
        Pattern modifyPattern = Pattern.compile("ALTER\\s+TABLE\\s+(\\S+)\\s+MODIFY\\s+COLUMN\\s+(\\S+)\\s+(\\S+)(?:\\s+CHARACTER\\s+SET\\s+\\S+)?(?:\\s+COLLATE\\s+\\S+)?(?:\\s+NULL)?(?:\\s+COMMENT\\s+'(.*?)')?", Pattern.CASE_INSENSITIVE);
        Pattern addPattern = Pattern.compile("ALTER\\s+TABLE\\s+(\\S+)\\s+(?:ADD\\s+COLUMN\\s+|ADD\\s+)(\\S+)\\s+(\\S+)(?:\\s+CHARACTER\\s+SET\\s+\\S+)?(?:\\s+COLLATE\\s+\\S+)?(?:\\s+NULL)?(?:\\s+COMMENT\\s+'(.*?)')?", Pattern.CASE_INSENSITIVE);
        Pattern dropPattern = Pattern.compile("ALTER\\s+TABLE\\s+(\\S+)\\s+(?:DROP\\s+COLUMN\\s+|DROP\\s+)(\\S+)", Pattern.CASE_INSENSITIVE);
        Pattern changePattern = Pattern.compile("ALTER\\s+TABLE\\s+(\\S+)\\s+(?:CHANGE\\s+COLUMN\\s+|CHANGE\\s+)+(\\S+)\\s+(\\S+)\\s+(\\S+)(?:\\s+CHARACTER\\s+SET\\s+\\S+)?(?:\\s+COLLATE\\s+\\S+)?(?:\\s+NULL)?(?:\\s+COMMENT\\s+'(.*?)')?", Pattern.CASE_INSENSITIVE);

        // 匹配 MODIFY COLUMN
        Matcher modifyMatcher = modifyPattern.matcher(sql);
        if (modifyMatcher(modifyMatcher)) {
            LOGGER.info("表注释修改完成");
            return;
        }

        // 匹配 ADD COLUMN
        Matcher addMatcher = addPattern.matcher(sql);
        if (addMatcher(addMatcher)) {
            LOGGER.info("添加列完成");
            return;
        }


        // 匹配 DROP COLUMN
        Matcher dropMatcher = dropPattern.matcher(sql);
        if (dropMatcher(dropMatcher)) {
            LOGGER.info("删除列完成");
            return;
        }


        // 匹配 CHANGE COLUMN
        Matcher changeMatcher = changePattern.matcher(sql);
        if (changeMatcher(changeMatcher)) {
            LOGGER.info("列重命名完成");
            return;
        }

        // 如果未匹配到任何类型
        LOGGER.error("无法解析的 ALTER TABLE 语句: {}", sql);
    }

    private static boolean changeMatcher(Matcher changeMatcher) {
        if (changeMatcher.find()) {
            String fullTableName = changeMatcher.group(1); // 包含库名和表名
            String oldColumnName = changeMatcher.group(2);
            String newColumnName = changeMatcher.group(3);
            String columnType = changeMatcher.group(4);
            String comment = changeMatcher.group(5); // 获取列注释

            // 解析库名和表名
            String databaseName = "";
            String tableName = fullTableName;
            if (fullTableName.contains(".")) {
                String[] parts = fullTableName.split("\\.");
                databaseName = parts[0];
                tableName = parts[1];
            }

            LOGGER.info("ALTER TABLE 类型: CHANGE COLUMN");
            LOGGER.info("库名: {}", databaseName);
            LOGGER.info("表名: {}", tableName);
            LOGGER.info("旧列名: {}", oldColumnName);
            LOGGER.info("新列名: {}", newColumnName);

            // 新建一个新表，结构为旧表的列去掉修改的列，并添加新列
            String newTableName = "tmp_" + tableName; // 新表名
            String createNewTableSql = String.format( //不完善，这个方法行不通
                    "CREATE TABLE hngs_hive_prd.%s AS SELECT * EXCEPT(%s), %s %s FROM hngs_hive_prd.%s", "ods_" + databaseName + "_" + newTableName + "_df", oldColumnName, newColumnName, convertToSparkType(columnType), "ods_" + databaseName + "_" + tableName + "_df");
            LOGGER.info("生成的创建新表 SQL: {}", createNewTableSql);

            try {
//  不完善              SparkSqlSubmit.executeSparkSql(createNewTableSql);
            } catch (Exception e) {
                LOGGER.error("创建新表失败: {}", e.getMessage());
                return true;
            }

            // 数据迁移：将旧表的数据迁移到新表
            String migrateDataSql = String.format("INSERT INTO hngs_hive_prd.%s SELECT * FROM hngs_hive_prd.%s", "ods_" + databaseName + "_" + newTableName + "_df", "ods_" + databaseName + "_" + tableName + "_df");
            LOGGER.info("生成的数据迁移 SQL: {}", migrateDataSql);

            try {
//  不完善              SparkSqlSubmit.executeSparkSql(migrateDataSql);
            } catch (Exception e) {
                System.err.println("数据迁移失败: " + e.getMessage());
                return true;
            }

            // 删除旧表
            String dropOldTableSql = String.format("DROP TABLE hngs_hive_prd.%s", "ods_" + databaseName + "_" + tableName + "_df");
//   不完善
            LOGGER.info("生成的删除旧表 SQL: {}", dropOldTableSql);

            try {
//                SparkSqlSubmit.executeSparkSql(dropOldTableSql);
            } catch (Exception e) {
                LOGGER.error("删除旧表失败: {}", e.getMessage());
                return true;
            }

            // 将新表改名为旧表名
            String renameNewTableSql = String.format("ALTER TABLE hngs_hive_prd.%s RENAME TO hngs_hive_prd.%s", "ods_" + databaseName + "_" + newTableName + "_df", "ods_" + databaseName + "_" + tableName + "_df");
            LOGGER.info("生成的重命名新表 SQL: {}", renameNewTableSql);

            try {
//  不完善              SparkSqlSubmit.executeSparkSql(renameNewTableSql);
            } catch (Exception e) {
                System.err.println("重命名新表失败: " + e.getMessage());
                return true;
            }

            return true;
        }
        return false;
    }

    private static boolean dropMatcher(Matcher dropMatcher) {
        if (dropMatcher.find()) {
            String tableName = dropMatcher.group(1);
            String columnName = dropMatcher.group(2);
            LOGGER.info("ALTER TABLE 类型: DROP COLUMN");
            LOGGER.info("删除的列名: {}", columnName);

            // 生成 Spark SQL 语句
            String sparkSql = String.format("ALTER TABLE %s DROP COLUMN %s", tableName, columnName);
//            不完善
            LOGGER.info("生成的 Spark SQL: {}", sparkSql);
            return true;
        }
        return false;
    }

    private static boolean addMatcher(Matcher addMatcher) {
        if (addMatcher.find()) {
            String fullTableName = addMatcher.group(1); // 包含库名和表名
            String columnName = addMatcher.group(2);
            String columnType = addMatcher.group(3);
            String comment = addMatcher.group(4); // 获取列注释
            String databaseName = "";
            String tableName = fullTableName;
            if (fullTableName.contains(".")) {
                String[] parts = fullTableName.split("\\.");
                databaseName = parts[0];
                tableName = parts[1];
            }
            LOGGER.info("ALTER TABLE 类型: ADD COLUMN");

            // 生成 Spark SQL 语句
            String sparkSql = String.format("ALTER TABLE hngs_hive_prd.%s ADD COLUMNS (%s %s COMMENT '%s') ", "ods_" + databaseName + "_" + tableName + "_df", columnName, convertToSparkType(columnType), comment);
            LOGGER.info("生成的 Spark SQL: {}", sparkSql);
            SparkSqlSubmit.executeSparkSql(sparkSql);
            return true;
        }
        return false;
    }

    private static boolean modifyMatcher(Matcher modifyMatcher) {
        if (modifyMatcher.find()) {
            String fullTableName = modifyMatcher.group(1); // 包含库名和表名
            String columnName = modifyMatcher.group(2);
            String columnType = modifyMatcher.group(3);
            String comment = modifyMatcher.group(4); // 获取列注释

            // 解析库名和表名
            String databaseName = "";
            String tableName = fullTableName;
            if (fullTableName.contains(".")) {
                String[] parts = fullTableName.split("\\.");
                databaseName = parts[0];
                tableName = parts[1];
            }

            LOGGER.info("ALTER TABLE 类型: MODIFY COLUMN");
            LOGGER.info("库名: {}", databaseName);
            LOGGER.info("表名: {}", tableName);
            LOGGER.info("列名: {}", columnName);
            LOGGER.info("字段类型: {}", columnType);
            if (comment != null && !comment.isEmpty()) {
                LOGGER.info("列注释: {}", comment);
            }

            // 生成 Spark SQL 语句
            String sparkSql = String.format("ALTER TABLE hngs_hive_prd.%s CHANGE COLUMN %s %s %s COMMENT '%s' ", "ods_" + databaseName + "_" + tableName + "_df", columnName, columnName, convertToSparkType(columnType), comment);
            LOGGER.info("生成的 Spark SQL: {}", sparkSql);
            SparkSqlSubmit.executeSparkSql(sparkSql);
            return true;
        }
        return false;
    }

    private static String convertToSparkType(String mysqlType) {
        // 直接调用枚举类的静态方法进行类型转换
        return ColumnTypeMapping.convertToSparkType(mysqlType);
    }
}