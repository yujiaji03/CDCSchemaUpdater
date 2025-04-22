package com.cdcUp;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventReader {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(EventReader.class);
    public static void registerEventListener(BinaryLogClient client) {
        client.registerEventListener(event -> {
            EventData data = event.getData();

            LOGGER.info("Event: {}", data);

            if (event.getHeader().getEventType() == EventType.QUERY) {
                QueryEventData queryData = (QueryEventData) data;
                String sql = queryData.getSql();

                // 使用正则表达式移除SQL中的注释部分
                sql = sql.replaceAll("/\\*.*?\\*/", "").trim();
                if (sql.toLowerCase().startsWith("alter table")) {
                    LOGGER.info("ChangeSql:{}", sql);
                    // 调用结构变更处理器
                    SchemaChangeHandler.parseAlterTable(sql);
                }
            }
        });
    }
}