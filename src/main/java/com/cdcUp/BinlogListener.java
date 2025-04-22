package com.cdcUp;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import java.io.IOException;

public class BinlogListener {
    public static void main(String[] args) {
        String hostname = Config.MYSQL_HOST; // MySQL 服务器地址
        int port = Config.MYSQL_PORT;              // MySQL 服务器端口
        String username = Config.MYSQL_USERNAME; // MySQL 用户名
        String password = Config.MYSQL_PASSWORD; // MySQL 密码
        BinaryLogClient client = new BinaryLogClient(hostname, port, username, password);

        EventReader.registerEventListener(client);

        try {
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}