package com.example.clickhouse_ingestor.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.List;

@Data
public class IngestionRequest {
    @NotNull(message = "Source type is required")
    private SourceType sourceType;
    
    @NotNull(message = "Target type is required")
    private SourceType targetType;
    
    @Valid
    private ClickHouseConfig clickHouseConfig;
    
    @Valid
    private FlatFileConfig flatFileConfig;
    
    @NotEmpty(message = "At least one column must be selected")
    private List<String> selectedColumns;
    
    private String tableName;
    
    public enum SourceType {
        CLICKHOUSE,
        FLAT_FILE
    }
} 