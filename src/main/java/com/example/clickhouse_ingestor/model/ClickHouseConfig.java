package com.example.clickhouse_ingestor.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class ClickHouseConfig {
    @NotBlank(message = "Host is required")
    private String host;
    
    @NotNull(message = "Port is required")
    private Integer port;
    
    @NotBlank(message = "Database is required")
    private String database;
    
    @NotBlank(message = "Username is required")
    private String username;
    
    @NotBlank(message = "JWT token is required")
    private String jwtToken;
    
    private boolean useSsl = true;
} 