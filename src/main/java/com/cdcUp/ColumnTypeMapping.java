package com.cdcUp;

public enum ColumnTypeMapping {
    VARCHAR("STRING"),
    CHAR("STRING"),
    TINYTEXT("STRING"),
    TEXT("STRING"),
    MEDIUMTEXT("STRING"),
    LONGTEXT("STRING"),
    YEAR("STRING"),
    INT("BIGINT"),
    TINYINT("BIGINT"),
    SMALLINT("BIGINT"),
    MEDIUMINT("BIGINT"),
    BIGINT("BIGINT"),
    FLOAT("DOUBLE"),
    DOUBLE("DOUBLE"),
    DECIMAL("DECIMAL"),
    DATE("STRING"),
    DATETIME("TIMESTAMP"),
    TIMESTAMP("TIMESTAMP"),
    TIME("STRING"),
    BOOLEAN("STRING"),
    BINARY("STRING"),
    VARBINARY("STRING"),
    TINYBLOB("STRING"),
    MEDIUMBLOB("STRING"),
    BLOB("STRING"),
    LONGBLOB("STRING");

    private final String sparkType;

    ColumnTypeMapping(String sparkType) {
        this.sparkType = sparkType;
    }

    public static String convertToSparkType(String mysqlType) {
        mysqlType = mysqlType.toLowerCase();
        for (ColumnTypeMapping mapping : values()) {
            if (mysqlType.startsWith(mapping.name().toLowerCase())) {
                return mapping.getSparkType();
            }
        }
        return mysqlType.toUpperCase();
    }

    public String getSparkType() {
        return sparkType;
    }
}