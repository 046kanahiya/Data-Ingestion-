package com.example.clickhouse_ingestor.model;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class FlatFileConfig {
    @NotBlank(message = "File name is required")
    private String fileName;
    
    @NotBlank(message = "Delimiter is required")
    private String delimiter = ",";
    
    private boolean hasHeader = true;
} 