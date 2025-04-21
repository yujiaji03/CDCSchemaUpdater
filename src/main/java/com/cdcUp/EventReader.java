package com.cdcUp;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;

public class EventReader {

    public static void registerEventListener(BinaryLogClient client) {
        client.registerEventListener(event -> {
            EventData data = event.getData();

            System.out.println("data: " + data + "\n");

            if (event.getHeader().getEventType() == EventType.QUERY) {
                QueryEventData queryData = (QueryEventData) data;
                String sql = queryData.getSql();

                // 使用正则表达式移除SQL中的注释部分
                sql = sql.replaceAll("/\\*.*?\\*/", "").trim();
                if (sql.toLowerCase().startsWith("alter table")) {
                    System.out.println("检测到表结构变更: " + sql);

                    // 调用结构变更处理器
                    SchemaChangeHandler.parseAlterTable(sql);
                }
            }
        });
    }
}