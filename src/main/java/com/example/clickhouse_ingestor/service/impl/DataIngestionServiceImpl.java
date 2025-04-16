package com.example.clickhouse_ingestor.service.impl;

import com.example.clickhouse_ingestor.model.*;
import com.example.clickhouse_ingestor.service.DataIngestionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DataIngestionServiceImpl implements DataIngestionService {

    @Override
    public List<String> getAvailableTables(ClickHouseConfig config) {
        List<String> tables = new ArrayList<>();
        String url = buildClickHouseUrl(config);
        
        try (Connection conn = DriverManager.getConnection(url, config.getUsername(), config.getJwtToken())) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getTables(config.getDatabase(), null, "%", new String[]{"TABLE"});
            
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
            log.error("Error fetching tables from ClickHouse", e);
            throw new RuntimeException("Failed to fetch tables from ClickHouse", e);
        }
        
        return tables;
    }

    @Override
    public List<String> getTableColumns(ClickHouseConfig config, String tableName) {
        List<String> columns = new ArrayList<>();
        String url = buildClickHouseUrl(config);
        
        try (Connection conn = DriverManager.getConnection(url, config.getUsername(), config.getJwtToken())) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs = metaData.getColumns(config.getDatabase(), null, tableName, null);
            
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            log.error("Error fetching columns from ClickHouse table", e);
            throw new RuntimeException("Failed to fetch columns from ClickHouse table", e);
        }
        
        return columns;
    }

    @Override
    public List<String> getFlatFileColumns(FlatFileConfig config) {
        List<String> columns = new ArrayList<>();
        
        try (Reader reader = new FileReader(config.getFileName())) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.withDelimiter(config.getDelimiter().charAt(0));
            if (config.isHasHeader()) {
                csvFormat = csvFormat.withFirstRecordAsHeader();
            }
            
            CSVParser parser = csvFormat.parse(reader);
            if (config.isHasHeader()) {
                return new ArrayList<>(parser.getHeaderNames());
            } else {
                // If no header, read first row to determine column count
                if (parser.iterator().hasNext()) {
                    CSVRecord firstRecord = parser.iterator().next();
                    for (int i = 0; i < firstRecord.size(); i++) {
                        columns.add("Column" + (i + 1));
                    }
                }
            }
        } catch (IOException e) {
            log.error("Error reading flat file columns", e);
            throw new RuntimeException("Failed to read flat file columns", e);
        }
        
        return columns;
    }

    @Override
    public long ingestData(IngestionRequest request) {
        if (request.getSourceType() == IngestionRequest.SourceType.CLICKHOUSE 
            && request.getTargetType() == IngestionRequest.SourceType.FLAT_FILE) {
            return clickHouseToFlatFile(request);
        } else if (request.getSourceType() == IngestionRequest.SourceType.FLAT_FILE 
                   && request.getTargetType() == IngestionRequest.SourceType.CLICKHOUSE) {
            return flatFileToClickHouse(request);
        }
        
        throw new IllegalArgumentException("Unsupported ingestion direction");
    }

    private long clickHouseToFlatFile(IngestionRequest request) {
        String url = buildClickHouseUrl(request.getClickHouseConfig());
        long recordCount = 0;
        
        try (Connection conn = DriverManager.getConnection(url, 
                                                         request.getClickHouseConfig().getUsername(), 
                                                         request.getClickHouseConfig().getJwtToken())) {
            String columns = String.join(", ", request.getSelectedColumns());
            String query = String.format("SELECT %s FROM %s.%s", 
                                       columns, 
                                       request.getClickHouseConfig().getDatabase(), 
                                       request.getTableName());
            
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                
                CSVFormat csvFormat = CSVFormat.DEFAULT
                    .withDelimiter(request.getFlatFileConfig().getDelimiter().charAt(0))
                    .withHeader(request.getSelectedColumns().toArray(new String[0]));
                
                try (FileWriter writer = new FileWriter(request.getFlatFileConfig().getFileName());
                     CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)) {
                    
                    while (rs.next()) {
                        List<String> record = new ArrayList<>();
                        for (String column : request.getSelectedColumns()) {
                            record.add(rs.getString(column));
                        }
                        csvPrinter.printRecord(record);
                        recordCount++;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error during ClickHouse to Flat File ingestion", e);
            throw new RuntimeException("Failed to ingest data from ClickHouse to Flat File", e);
        }
        
        return recordCount;
    }

    private long flatFileToClickHouse(IngestionRequest request) {
        String url = buildClickHouseUrl(request.getClickHouseConfig());
        long recordCount = 0;
        
        try (Connection conn = DriverManager.getConnection(url, 
                                                         request.getClickHouseConfig().getUsername(), 
                                                         request.getClickHouseConfig().getJwtToken())) {
            
            // Create table if it doesn't exist
            createTableIfNotExists(conn, request);
            
            // Prepare insert statement
            String columns = String.join(", ", request.getSelectedColumns());
            String placeholders = request.getSelectedColumns().stream()
                .map(col -> "?")
                .collect(Collectors.joining(", "));
            
            String insertSql = String.format("INSERT INTO %s.%s (%s) VALUES (%s)",
                                           request.getClickHouseConfig().getDatabase(),
                                           request.getTableName(),
                                           columns,
                                           placeholders);
            
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                CSVFormat csvFormat = CSVFormat.DEFAULT
                    .withDelimiter(request.getFlatFileConfig().getDelimiter().charAt(0));
                
                if (request.getFlatFileConfig().isHasHeader()) {
                    csvFormat = csvFormat.withFirstRecordAsHeader();
                }
                
                try (Reader reader = new FileReader(request.getFlatFileConfig().getFileName());
                     CSVParser parser = csvFormat.parse(reader)) {
                    
                    for (CSVRecord record : parser) {
                        int paramIndex = 1;
                        for (String column : request.getSelectedColumns()) {
                            pstmt.setString(paramIndex++, record.get(column));
                        }
                        pstmt.addBatch();
                        recordCount++;
                        
                        // Execute batch every 1000 records
                        if (recordCount % 1000 == 0) {
                            pstmt.executeBatch();
                        }
                    }
                    
                    // Execute remaining batch
                    pstmt.executeBatch();
                }
            }
        } catch (Exception e) {
            log.error("Error during Flat File to ClickHouse ingestion", e);
            throw new RuntimeException("Failed to ingest data from Flat File to ClickHouse", e);
        }
        
        return recordCount;
    }

    private void createTableIfNotExists(Connection conn, IngestionRequest request) throws SQLException {
        String createTableSql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (",
                                            request.getClickHouseConfig().getDatabase(),
                                            request.getTableName());
        
        List<String> columnDefinitions = request.getSelectedColumns().stream()
            .map(col -> col + " String")
            .collect(Collectors.toList());
        
        createTableSql += String.join(", ", columnDefinitions) + ")";
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSql);
        }
    }

    private String buildClickHouseUrl(ClickHouseConfig config) {
        String protocol = config.isUseSsl() ? "jdbc:clickhouse://" : "jdbc:clickhouse://";
        return String.format("%s%s:%d/%s?ssl=%b",
                           protocol,
                           config.getHost(),
                           config.getPort(),
                           config.getDatabase(),
                           config.isUseSsl());
    }
} 