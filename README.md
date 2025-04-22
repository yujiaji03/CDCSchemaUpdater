# CDCSchemaUpdater

基于 BinaryLogClient 的 MySQL 表结构同步工具

## 简介

CDCSchemaUpdater 是一个用于读取 MySQL binlog 中表结构变更的工具。它将这些变更解析后，通过提交 Spark submit 命令提交到 Spark 集群中运行 Spark SQL 脚本，从而更新 Hive 表结构。

## 配置用法

以下是json配置文件的介绍
```json
{
"MYSQL_HOST": "你的Mysql地址",
"MYSQL_PORT": 你的Mysql端口,
"MYSQL_USERNAME": "你的Mysql用户名",
"MYSQL_PASSWORD": "你的Mysql密码",
"HIVE_METASTORE_THRIFT_URL": "你的Hive Metastore Thrift URL",
"HDFS_URL": "你的HDFS地址"
}
```
以下是一个典型的配置文件示例：
```json
{
"MYSQL_HOST": "192.168.1.1",
"MYSQL_PORT": 3306,
"MYSQL_USERNAME": "root",
"MYSQL_PASSWORD": "000000",
"HIVE_METASTORE_THRIFT_URL": "thrift://192.168.1.1:9083",
"HDFS_URL": "hdfs:///user/hive/warehouse"
}
```