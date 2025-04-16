package com.example.clickhouse_ingestor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ClickHouseUtil {
    public static Connection getConnection(ClickHouseConfig config) throws SQLException {
        String url = String.format("jdbc:clickhouse://%s:%d/%s",
                config.host, config.port, config.database);

        Properties properties = new Properties();
        properties.setProperty("user", config.user);
        properties.setProperty("password", config.jwtToken); // used as JWT

        return DriverManager.getConnection(url, properties);
    }
}
