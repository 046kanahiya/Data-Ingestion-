package com.example.clickhouse_ingestor.service;

import org.springframework.stereotype.Service;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Service
public class ClickHouseService {

    public Connection connectToClickHouse(String url, String user, String jwtToken) throws SQLException {
        // ClickHouse JDBC URL looks like: jdbc:clickhouse://host:port/database
        // Example: jdbc:clickhouse://localhost:8123/default
        Connection connection = DriverManager.getConnection(url, user, jwtToken);
        return connection;
    }

    public boolean testConnection(String url, String user, String jwtToken) {
        try (Connection conn = connectToClickHouse(url, user, jwtToken)) {
            return conn != null && !conn.isClosed();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }
}
