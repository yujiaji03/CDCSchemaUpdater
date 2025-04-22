package com.cdcUp;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Config {
    public static String MYSQL_HOST;
    public static int MYSQL_PORT;
    public static String MYSQL_USERNAME;
    public static String MYSQL_PASSWORD;
    public static String HIVE_METASTORE_THRIFT_URL;
    public static String HDFS_URL;

    // Method to load configuration from JSON file
    static {
        loadFromJson();
    }

    private static void loadFromJson() {
        try (InputStream inputStream = Config.class.getClassLoader().getResourceAsStream("config.json")) {
            if (inputStream == null) {
                throw new RuntimeException("Failed to find config.json in resources directory");
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                StringBuilder contentBuilder = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    contentBuilder.append(line);
                }
                String jsonContent = contentBuilder.toString();
                JSONObject jsonObject = JSON.parseObject(jsonContent);
                Config.MYSQL_HOST = jsonObject.getString("MYSQL_HOST");
                Config.MYSQL_PORT = jsonObject.getIntValue("MYSQL_PORT");
                Config.MYSQL_USERNAME = jsonObject.getString("MYSQL_USERNAME");
                Config.MYSQL_PASSWORD = jsonObject.getString("MYSQL_PASSWORD");
                Config.HIVE_METASTORE_THRIFT_URL = jsonObject.getString("HIVE_METASTORE_THRIFT_URL");
                Config.HDFS_URL = jsonObject.getString("HDFS_URL");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration from JSON file", e);
        }
    }
}