## CDCSchemaUpdater
基于BinaryLogClient的mysql表结构同步工具
## 简介
用于读取mysql binlog中表结构变更，将其解析后通过提交sparksubmit命令提交到spark集群中运行sparksql 脚本更新hive表结构。
