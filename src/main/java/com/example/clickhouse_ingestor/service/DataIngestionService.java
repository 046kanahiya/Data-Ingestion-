package com.example.clickhouse_ingestor.service;

import com.example.clickhouse_ingestor.model.ClickHouseConfig;
import com.example.clickhouse_ingestor.model.FlatFileConfig;
import com.example.clickhouse_ingestor.model.IngestionRequest;
import java.util.List;

public interface DataIngestionService {
    List<String> getAvailableTables(ClickHouseConfig config);
    List<String> getTableColumns(ClickHouseConfig config, String tableName);
    List<String> getFlatFileColumns(FlatFileConfig config);
    long ingestData(IngestionRequest request);
} 