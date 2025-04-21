package com.cdcUp;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import java.io.IOException;

public class BinlogListener {
    // 提取 HashMap 到类的静态成员变量
    public static void main(String[] args) {
        String hostname = "localhost"; // MySQL 服务器地址
        int port = 3306;              // MySQL 服务器端口
        String username = "root"; // MySQL 用户名
        String password = "yujiaji2013"; // MySQL 密码

        BinaryLogClient client = new BinaryLogClient(hostname, port, username, password);

        EventReader.registerEventListener(client);

        try {
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}